import { OperationalInsightsManagementClient, Table, Workspace } from "@azure/arm-operationalinsights";
import { PipelineRequest, SendRequest } from "@azure/core-rest-pipeline";
import { Context } from "@azure/functions";
import { DefaultAzureCredential } from "@azure/identity";
import { LogsQueryClient, LogsQueryResultStatus, LogsTable } from "@azure/monitor-query";
import { LogsIngestionClient } from "@azure/monitor-ingestion";
import { MonitorClient } from "@azure/arm-monitor";
import { Client as GraphClient } from "@microsoft/microsoft-graph-client";
import LogCursor from "./log-cursor";
import * as fs from 'fs';
import * as readline from 'readline';

// Row limit controls the number of records per page request.
// There is a hard 30k per response service limitation this must fall within.
const ROW_LIMIT_PER_PAGE = 5000;
const MAX_PAGES = Number.MAX_SAFE_INTEGER;

export default class SyncService {
  private context: Context;

  // Get the properties from environment
  private subscriptionId = process.env["AZURE_SUBSCRIPTION_ID"];
  private primaryWorkspaceResourceGroup = process.env["PRIMARY_WORKSPACE_RESOURCE_GROUP"];
  private primaryWorkspaceName = process.env["PRIMARY_WORKSPACE_NAME"];
  private maxDaysToSync = process.env["MAX_DAYS_TO_SYNC"];

  // Primary workspace to import from
  private primaryWorkspace: Workspace;

  // Use the default credential which can be obtained through CLI sign-in or Azure managed identity
  private credential = new DefaultAzureCredential();

  // API to query Log Analytics
  private logsQueryClient = new LogsQueryClient(this.credential);

  // API to manage Log Analytics workspaces
  private managementClient = new OperationalInsightsManagementClient(
    this.credential,
    this.subscriptionId
  );

  // API to manage Azure Monitor resources
  private monitorClient = new MonitorClient(this.credential, this.subscriptionId);

  // API to ingest Log Analytics data
  private ingestionClient: LogsIngestionClient;

  // API for Microsoft Graph to communicate with Azure AD
  private graphClient: GraphClient;

  constructor(context: Context) {
    this.context = context;

    // Work around a bug in the SDK where it's using an unsupported API version.
    // https://github.com/Azure/azure-sdk-for-js/issues/24369
    this.managementClient.pipeline.addPolicy({ name: "API Version",
      sendRequest: (request: PipelineRequest, next: SendRequest) => {
        let url = new URL(request.url);
        url.searchParams.set("api-version", "2022-10-01");
        request.url = url.toString();
        return next(request);
    }}, { phase: "Serialize" });
  }

  private log(...args: any[]) {
    this.context.log(...args);
  }

  private warn(...args: any[]) {
    this.context.log.warn(...args);
  }

  private error(...args: any[]) {
    this.context.log.error(...args);
  }

  // Sync all workspaces
  async sync() {
    // Authenticate to Graph
    // See: https://learn.microsoft.com/en-us/azure/app-service/tutorial-connect-app-access-microsoft-graph-as-app-javascript
    const tokenResponse = await this.credential.getToken("https://graph.microsoft.com/.default");
    this.graphClient = GraphClient.init({
      // Use the provided access token to authenticate requests
      authProvider: (done) => {
        done(null, tokenResponse.token);
      }
    });

    // Find primary workspace
    let workspaces = this.managementClient.workspaces.listByResourceGroup(this.primaryWorkspaceResourceGroup);
    for await (const workspace of workspaces) {
      if (workspace.name === this.primaryWorkspaceName) {
        this.primaryWorkspace = workspace;
        break;
      }
    }
    if (!this.primaryWorkspace) {
      this.error("Unable to find primary workspace named", this.primaryWorkspaceName);
      return;
    }

    // Find DCE
    let dces = this.monitorClient.dataCollectionEndpoints.listBySubscription();
    for await (const dce of dces) {
      if (dce.tags?.wufb_scope_id === "tenant") {
        this.log("Initializing ingestion client for endpoint", dce.logsIngestion.endpoint);
        this.ingestionClient = new LogsIngestionClient(dce.logsIngestion.endpoint, this.credential);
      }
    }
    if (!this.ingestionClient) {
      this.error(`Unable to find data collection endpoint with wufb_scope_id = tenant: ${JSON.stringify(dces)}`);
      return;
    }

    // Sync tagged workspaces
    this.log("Syncing workspaces");
    workspaces = this.managementClient.workspaces.list();
    for await (const workspace of workspaces) {
      // Skip workspaces that do not have a wufb scope ID or are at the tenant level
      if (!workspace.tags?.wufb_scope_id || workspace.tags.wufb_scope_id === "tenant")
        continue;

      // Sync workspace
      await this.syncWorkspace(workspace);
    }
    this.log("Finished syncing workspaces");
  }

  // Sync a single workspace
  async syncWorkspace(workspace: Workspace) {
    this.log("Syncing workspace", workspace.name, "with scope", workspace.tags.wufb_scope_id);

    const allowedDevices = await this.getAllowedDevices(workspace);

    // Iterate over all tables
    const tables = this.managementClient.tables.listByWorkspace(this.primaryWorkspaceResourceGroup, this.primaryWorkspace.name);
    for await (const table of tables) {
      // Skip tables that don't begin with UC
      if (!table.name.startsWith("UC"))
        continue;

      // Skip the aggregate table since contains tenant-wide data.
      // Alternatively, synthesize new aggregate records after sync
      // based on scoped records from UCDOStatus.
      if (table.name === "UCDOAggregatedStatus")
        continue;

      await this.syncTableWithCompare(workspace, table, allowedDevices);
      await this.syncTableWithCursor(workspace, table, allowedDevices);
    }

    this.log("Finished syncing workspace", workspace.name, "with scope", workspace.tags.wufb_scope_id);
  }

  async syncTableWithCompare(workspace: Workspace, table: Table, allowedDevices: string[]) {
    let initialCursor = await this.getLatestCursor(workspace, table);
    if (!initialCursor) return;

    this.log("Comparison syncing table", table.name, "from workspace", workspace.name);

    const secondaryKeyName = this.getSecondaryKeyName(table.name);

    // Source: Get count of items
    let kustoQuery = `${table.name}`
      + ` | where TimeGenerated == todatetime("${initialCursor.timeGenerated.toISOString()}")`
      + (allowedDevices.length ? ` and ${secondaryKeyName} in~ (${allowedDevices.map(d => `"${d}"`).join(",")})` : ``)
      + ` | count`;

    // this.log("Query:", kustoQuery);
    let result = await this.logsQueryClient.queryWorkspace(this.primaryWorkspace.customerId, kustoQuery, {
      duration: `P${this.maxDaysToSync}D`
    });
    this.log("Query result:", JSON.stringify(result));

    if (result.status !== LogsQueryResultStatus.Success) {
      throw new Error("Delta count query failed");
    }
    const sourceCount = result.tables[0].rows[0][0] as number;

    // Target: Get count of items
    kustoQuery = `${table.name}_CL | where OriginTimeGenerated_CF == todatetime("${initialCursor.timeGenerated.toISOString()}") | count`;

    this.log("Query:", kustoQuery);
    result = await this.logsQueryClient.queryWorkspace(workspace.customerId, kustoQuery, {
      duration: `P${this.maxDaysToSync}D`
    });
    this.log("Query result:", JSON.stringify(result));

    if (result.status !== LogsQueryResultStatus.Success) {
      throw new Error("Delta count query failed");
    }
    const targetCount = result.tables[0].rows[0][0] as number;

    // If missing records perform delta sync else done
    // if (sourceCount <= targetCount) {
    //   this.log(`No need to sync. { sourceCount = ${sourceCount}, targetCount = ${targetCount} }`);
    //   return;
    // } else {
    //   this.log(`Performing comparison sync since there are ${sourceCount - targetCount} new records.`);
    // }

    // Get all the IDs from the target table
    let cursor = new LogCursor(initialCursor.timeGenerated);
    let pageCount = 0;
    let page: LogsTable;
    const disallowedDevices: string[] = [];
    while ((page = await this.getNextPage(workspace, `${table.name}_CL`, cursor, {
      exactTime: true,
      columns: [secondaryKeyName]
    })).rows.length) {
      const nextCursor = this.getNextCursor(page, cursor.timeGenerated);
      if (JSON.stringify(cursor) === JSON.stringify(nextCursor)) {
        throw new Error(`Pages must have unique cursors, but this page was the same as the last.\n${cursor}\n${page}`);
      }
      cursor = nextCursor;

      if (page.rows.length) {
        disallowedDevices.push(...page.rows.flat(1).map(v => v as string));
      }

      if (++pageCount === MAX_PAGES) break;
    }

    // Get the DCR for this scope
    let ruleId = workspace.tags?.wufb_scope_dcr_immutable_id;
    // for await (let dcr of this.monitorClient.dataCollectionRules.listBySubscription()) {
    //   if (dcr.tags.wufb_scope_id === workspace.tags.wufb_scope_id) {
    //     ruleId = dcr.immutableId;
    //     break;
    //   }
    // }
    if (!ruleId) {
      throw new Error("Unable to find data collection rule tag wufb_scope_dcr_immutable_id on workspace " + workspace.name);
    }

    // Get records from the primary table in batches
    cursor = new LogCursor(initialCursor.timeGenerated);
    pageCount = 0;
    while ((page = await this.getNextPage(this.primaryWorkspace, table.name, cursor, {
      exactTime: true,
      allowedDevices,
      disallowedDevices
    })).rows.length) {
      // Update cursor for next run before filtering
      const nextCursor = this.getNextCursor(page);
      if (JSON.stringify(cursor) === JSON.stringify(nextCursor)) {
        throw new Error(`Pages must have unique cursors, but this page was the same as the last.\n${cursor}\n${page}`);
      }
      cursor = nextCursor;

      // Write remaining data to the target
      if (page.rows.length) {
        await this.writePage(ruleId, `Custom-${table.name}_IN`, page);
      } else {
        this.log("No rows in the processed batch were intended for the target scope and therefore all were omitted.");
      }

      if (++pageCount === MAX_PAGES) break;
    }
    this.log("Finished syncing table", table.name, "from workspace", workspace.name);
  }

  // Sync a single table
  async syncTableWithCursor(workspace: Workspace, table: Table, allowedDevices: string[]) {
    this.log("Syncing table", table.name, "from workspace", workspace.name);

    // Get the cursor to start the sync from
    let cursor = await this.getLatestCursor(workspace, table);
    if (!cursor) {
      cursor = this.createInitialCursor();
      this.log("Initial sync", JSON.stringify(cursor));
    } else {
      this.log("Incremental sync", JSON.stringify(cursor));
    }

    // Get the DCR for this scope
    let ruleId = workspace.tags?.wufb_scope_dcr_immutable_id;
    // for await (let dcr of this.monitorClient.dataCollectionRules.listBySubscription()) {
    //   if (dcr.tags.wufb_scope_id === workspace.tags.wufb_scope_id) {
    //     ruleId = dcr.immutableId;
    //     break;
    //   }
    // }
    if (!ruleId) {
      throw new Error("Unable to find data collection rule tag wufb_scope_dcr_immutable_id on workspace " + workspace.name);
    }

    // Get records from the primary table in batches
    let pageCount = 0;
    let page: LogsTable;
    while ((page = await this.getNextPage(this.primaryWorkspace, table.name, cursor, { allowedDevices })).rows.length) {
      // Update cursor for next run before filtering
      const nextCursor = this.getNextCursor(page);
      if (JSON.stringify(cursor) === JSON.stringify(nextCursor)) {
        throw new Error(`Pages must have unique cursors, but this page was the same as the last.\n${cursor}\n${page}`);
      }
      cursor = nextCursor;

      // Filter page data to remove devices that don't belong to this scope
      // await this.filterPageDevices(page, allowedDevices);

      // Write remaining data to the target
      if (page.rows.length) {
        await this.writePage(ruleId, `Custom-${table.name}_IN`, page);
      } else {
        this.log("No rows in the processed batch were intended for the target scope and therefore all were omitted.");
      }

      if (++pageCount === MAX_PAGES) break;
    }
    this.log("Finished syncing table", table.name, "from workspace", workspace.name);
  }

  // Wrap to make this easier to handle errors
  async createReadStreamAsync(path: fs.PathLike, options?: BufferEncoding | any) : Promise<fs.ReadStream> {
    return new Promise((resolve, reject) => {
        const fileStream = fs.createReadStream(path, options);
        fileStream.on('error', reject).on('open', () => {
          resolve(fileStream);
        });
    });
  }

  // Obtains the allowed devices for the workspace (scope). Workspaces can be tagged with an Azure AD Group ID
  // to restrict the allowed device set.
  private async getAllowedDevices(workspace: Workspace) {
    const results: string[] = [];

    // Validate AD group association
    if (!workspace.tags.wufb_scope_azure_ad_group_id) {
      this.warn(`Workspace ${workspace.name} does not have tag 'wufb_scope_azure_ad_group_id' and therefore will receive all records from the tenant.`);
      return [];
    }

    const deviceListPath = `${this.context.executionContext.functionDirectory}/groups/${workspace.tags.wufb_scope_azure_ad_group_id}.csv`;
    if (!fs.existsSync(deviceListPath)) {
      // A file does not exist so load directly from Graph
      // try {
      //   this.log("Calling Graph ... " + `/groups/${workspace.tags.wufb_scope_azure_ad_group_id}/members/microsoft.graph.device`);
      //   let membersResponse = await this.graphClient.api(`/groups/${workspace.tags.wufb_scope_azure_ad_group_id}/members/microsoft.graph.device`).get();
      //   this.log(membersResponse);
      // } catch(error: any) {
      //   this.error(error);
      // }
      // TODO: Handle @odata.nextLink
    } else {
      // Open file for reading
      try {
        const fileStream = await this.createReadStreamAsync(deviceListPath);

        const rl = readline.createInterface({
          input: fileStream,
          crlfDelay: Infinity
        });

        // Loop over lines of the file
        for await (let line of rl) {
          // Skip header
          if (line.includes("AzureADDeviceId")) {
            continue;
          }

          // Strip quotes
          line = line.replace(/\"/g, "");
          const deviceId = line;
          results.push(deviceId);
        }

        fileStream.close();
      } catch(error) {
        this.warn(error);
      }
    }

    if (!results.length) {
      this.warn(`${workspace.tags.wufb_scope_azure_ad_group_id} had no records and therefore ${workspace.name} will receive all records from the tenant.`);
    }

    return results;
  }

  // Remove rows from the page that are not part of the scope
  private async filterPageDevices(page: LogsTable, allowedDevices: string[]) {
    if (!allowedDevices.length) return;
    const azureADDeviceIdIndex = page.columnDescriptors.findIndex(c => c.name === "AzureADDeviceId");
    for(let r = page.rows.length - 1; r >= 0; r--) {
      // this.log(allowedDevices[0], " ", page.rows[r][azureADDeviceIdIndex]);
      if (!allowedDevices.includes(page.rows[r][azureADDeviceIdIndex] as string)) {
        // if (page.rows[r][azureADDeviceIdIndex].toString().startsWith("1")) {
        //   this.warn(page.rows[r][azureADDeviceIdIndex] as string, "was not in", allowedDevices);
        //   throw new Error();
        // }
        page.rows.splice(r, 1);
      }
    }
  }

  // Write a page of records by connecting to the DCE and uploading records through a stream of the DCR
  private async writePage(ruleId: string, streamName: string, page: LogsTable) {
    // Set the maximum concurrency to 1 to prevent concurrent requests entirely
    this.log(`Writing ${page.rows.length} rows of page to stream ${streamName} of ${ruleId}`);
    const result = await this.ingestionClient.upload(ruleId, streamName, this.getPageAsObjectArray(page), { maxConcurrency: 1 });
    if (result.status !== "Success") {
      this.error("Some logs have failed to complete ingestion. Upload status=", result.status);
      for (const error of result.errors) {
        this.error(`Error - ${JSON.stringify(error.cause)}`);
        this.error(`Log - ${JSON.stringify(error.failedLogs)}`);
      }
      throw new Error(`Some logs have failed to complete ingestion. Upload status=${result.status}`);
    }
    this.log(`Write success.`);
  }

  // Gets the next cursor based on the provided records.
  //
  // Assuming records were sorted ascending, the next cursor is built from the keys in the last record.
  private getNextCursor(page: LogsTable, timeGenerated?: Date) {
    const secondaryKeyName = this.getSecondaryKeyName(page.name);
    const timeGeneratedName = page.name.endsWith("_CL") ? "OriginTimeGenerated_CF" : "TimeGenerated";
    const timeGeneratedIndex = page.columnDescriptors.findIndex(c => c.name === timeGeneratedName);
    const secondaryKeyIndex = page.columnDescriptors.findIndex(c => c.name === secondaryKeyName);

    if ((!timeGenerated && timeGeneratedIndex == -1) || secondaryKeyIndex == -1) {
      throw new Error("Columns not found: " + JSON.stringify(page.columnDescriptors));
    }

    let _timeGenerated = timeGenerated ?? page.rows[page.rows.length - 1][timeGeneratedIndex];
    if ((_timeGenerated as Date).getTime() === new Date(null).getTime())
      _timeGenerated = undefined;

    const secondaryKey = page.rows[page.rows.length - 1][secondaryKeyIndex];

    const cursor = new LogCursor(_timeGenerated as Date, secondaryKey as string);
    this.log(`[${page.name}]: Get next cursor: ${cursor}`);
    return cursor;
  }

  // Some tables use a different column to establish uniqueness
  private getSecondaryKeyName(tableName: string) {
    switch (tableName) {
      case "UCDOAggregatedStatus":
        return "ContentType";
      // case "UCClientReadinessStatus":
      //   return "GlobalDeviceId";
      default:
        return "AzureADDeviceId";
    }
  }

  // Gets the latest cursor (last synced record) from the target table.
  // Use the OriginTimeGenerated which matches TimeGenerated at the source.
  private async getLatestCursor(workspace: Workspace, table: Table) {
    // Query returns a single row of max { OriginTimeGenerated_CF, SecondaryKey }
    const secondaryKeyName = this.getSecondaryKeyName(table.name);
    const kustoQuery = `${table.name}_CL | where isnotempty(${secondaryKeyName})
      | top-nested 1 of OriginTimeGenerated_CF by max(OriginTimeGenerated_CF),
          top-nested of ${secondaryKeyName} by Ignore=max(1)
      | summarize SecondaryKey=max(${secondaryKeyName}) by OriginTimeGenerated_CF`;

    this.log(`[${workspace.name}.${table.name}]: Getting latest cursor`);
    const result = await this.logsQueryClient.queryWorkspace(workspace.customerId, kustoQuery, {
      duration: `P${this.maxDaysToSync}D`
    });
    this.log(`[${workspace.name}.${table.name}]: Query result: ${JSON.stringify(result)}`);

    if (result.status !== LogsQueryResultStatus.Success) {
      throw new Error(`Cursor query failed: ${JSON.stringify(result)}`);
    }

    // Check if any rows were returned
    const tables: LogsTable[] = result.tables;
    if (!tables.length || !tables[0].rows.length) return undefined;

    // If rows returned then init the cursor
    let timeGenerated = tables[0].rows[0][0];
    if ((timeGenerated as Date).getTime() === new Date(null).getTime())
      return undefined;

    const secondaryKey = tables[0].rows[0][1];
    return new LogCursor(timeGenerated as Date, secondaryKey as string);
  }

  // Create the initial cursor if no data has been synced yet.
  // Default to syncing last one week of data.
  private createInitialCursor() {
    const timeGenerated = new Date();
    timeGenerated.setDate(timeGenerated.getDate() - parseInt(this.maxDaysToSync));
    return new LogCursor(timeGenerated);
  }

  // Get the next page of data starting from the cursor.
  // Paging works by ascending sort of records on TimeGenerated and AzureADDeviceId.
  // This will provide enough uniqueness to establish a cursor indexing the record set.
  private async getNextPage(workspace: Workspace, tableName: string, cursor: LogCursor,
    { exactTime = false, columns, allowedDevices = [], disallowedDevices = [] }: { exactTime?: boolean, columns?: string[], allowedDevices?: string[], disallowedDevices?: string[] } = {}): Promise<LogsTable> {
    const secondaryKeyName = this.getSecondaryKeyName(tableName.replace("_CL", ""));
    const timeGeneratedName = tableName.endsWith("_CL") ? "OriginTimeGenerated_CF" : "TimeGenerated";
    const kustoQuery = `${tableName} | where ((${timeGeneratedName} == todatetime("${cursor.timeGenerated.toISOString()}") and isnotempty(${secondaryKeyName})`
      + (cursor.secondaryKey ? ` and strcmp(${secondaryKeyName}, "${cursor.secondaryKey}") > 0)` : ")")
      + (!exactTime ? ` or ${timeGeneratedName} > todatetime("${cursor.timeGenerated.toISOString()}"))` : ")")
      + (allowedDevices.length ? ` and ${secondaryKeyName} in~ (${allowedDevices.map(d => `"${d}"`).join(",")})` : "")
      + (disallowedDevices.length ? ` and ${secondaryKeyName} !in~ (${disallowedDevices.map(d => `"${d}"`).join(",")})` : "")
      + ` | sort by ${timeGeneratedName} asc, ${secondaryKeyName} asc`
      + (columns ? ` | project ${columns.join(",")}` : "")
      + ` | limit ${ROW_LIMIT_PER_PAGE}`;

    this.log(`[${workspace.name}.${tableName}]: Getting records after cursor: ${JSON.stringify(cursor)}`);
    // this.log(`[${workspace.name}.${tableName}]: ${kustoQuery}`);
    const result = await this.logsQueryClient.queryWorkspace(workspace.customerId, kustoQuery, {
      duration: `P${this.maxDaysToSync}D`
    });

    switch(result.status)
    {
      case LogsQueryResultStatus.Success:
        this.log(`[${workspace.name}.${tableName}]: ${result.tables[0].rows.length} rows returned.`);
        return result.tables[0]
      case LogsQueryResultStatus.PartialFailure:
        this.error("Unexpected partial failure", kustoQuery, result.partialError);
        throw new Error("Unexpected partial failure: " + kustoQuery + " " + result.partialError);
      default:
        throw new Error("Unexpected status: " + kustoQuery);
    }
  }

  // Converts a page table into an object array for consumption by the ingestion client.
  private getPageAsObjectArray(page: LogsTable): Record<string, unknown>[] {
    const result = [] as Record<string, unknown>[];

    for (const row of page.rows) {
      const record = {};
      for (let c = 0; c < page.columnDescriptors.length; ++c) {
        record[page.columnDescriptors[c].name] = row[c];
      }
      result.push(record);
    }

    return result;
  }

  private printTables(tablesFromResult: LogsTable[]) {
    // for (const table of tablesFromResult) {
    //     const columnHeaderString = table.columnDescriptors
    //         .map((column) => `${column.name}(${column.type}) `)
    //         .join("| ");
    //     console.log("| " + columnHeaderString);

    //     for (const row of table.rows) {
    //         const columnValuesString = row.map((columnValue) => `'${columnValue}' `).join("| ");
    //         console.log("| " + columnValuesString);
    //     }
    // }

    for (const table of tablesFromResult) {
      for (let r = 0; r < table.rows.length; ++r) {
        for (let c = 0; c < table.columnDescriptors.length; ++c) {
          console.log(table.columnDescriptors[c].name + ": " + table.rows[r][c]);
        }
      }
    }
  }
}