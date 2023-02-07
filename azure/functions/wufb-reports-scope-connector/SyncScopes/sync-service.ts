import "isomorphic-fetch";
import { MonitorClient } from "@azure/arm-monitor";
import { OperationalInsightsManagementClient, Table, Workspace } from "@azure/arm-operationalinsights";
import { PipelineRequest, SendRequest } from "@azure/core-rest-pipeline";
import { Context } from "@azure/functions";
import { DefaultAzureCredential } from "@azure/identity";
import { LogsIngestionClient } from "@azure/monitor-ingestion";
import { LogsQueryClient, LogsQueryResult, LogsQueryResultStatus, LogsTable } from "@azure/monitor-query";
import { Client as GraphClient, PageCollection, PageIterator, PageIteratorCallback } from "@microsoft/microsoft-graph-client";
import * as fs from 'fs';
import * as readline from 'readline';
import findFast from "./find-fast";
import LogCursor from "./log-cursor";

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

  // This sync pass determines the last {timegenerated,device} synced to the target, then performs
  // a full comparison with the source to ensure it has all records.
  //
  // It's necessary because the job could run at the same time data is being written
  // to the source table. Write order to the source table is not guaranteed so
  // unfortunately one cannot assume the last record written to the target included
  // the full set of records that preceded it if a source table write job was in progress.
  async syncTableWithCompare(workspace: Workspace, table: Table, allowedDevices: string[]) {
    let initialCursor = await this.getLatestCursor(workspace, table);
    if (!initialCursor) return;

    this.log("Comparison syncing table", table.name, "from workspace", workspace.name);

    // Get the DCR for this scope
    let ruleId = workspace.tags?.wufb_scope_dcr_immutable_id;
    if (!ruleId) {
      throw new Error("Unable to find data collection rule tag wufb_scope_dcr_immutable_id on workspace " + workspace.name);
    }

    // This alternative was considered where the DCR is tagged with a scope ID instead. For now, obtain from the workspace.
    //
    // for await (let dcr of this.monitorClient.dataCollectionRules.listBySubscription()) {
    //   if (dcr.tags.wufb_scope_id === workspace.tags.wufb_scope_id) {
    //     ruleId = dcr.immutableId;
    //     break;
    //   }
    // }

    // Get records from the primary table in batches
    let cursor = new LogCursor(initialCursor.timeGenerated);
    let pageCount = 0;
    let page: LogsTable;
    while ((page = await this.getNextPage(this.primaryWorkspace, table.name, cursor, {
      exactTime: true,
      excludeRecordsFromWorkspace: workspace.name
    })).rows.length) {
      // Update cursor for next run before filtering
      const nextCursor = this.getNextCursor(page);
      if (JSON.stringify(cursor) === JSON.stringify(nextCursor)) {
        throw new Error(`Pages must have unique cursors, but this page was the same as the last.\n${cursor}\n${page}`);
      }
      cursor = nextCursor;

      // Filter page data to remove devices that don't belong to this scope.
      //
      // Note: Performance could be improved by filtering the devices on the data service side
      // as part of the query, but in order to scale, the device lists would have to
      // be written to blob storage for access as a Kusto external table. This added
      // complexity is out of scope for now and instead apply the filters here.
      const notInScopeCount = await this.filterPageAllowedDevices(page, allowedDevices);
      this.log(`${notInScopeCount} rows were not intended for the target scope.`);

      // Write remaining data to the target
      if (page.rows.length) {
        await this.writePage(ruleId, `Custom-${table.name}_IN`, page);
      } else {
        this.log(`No remaining records to write.`);
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
    if (!ruleId) {
      throw new Error("Unable to find data collection rule tag wufb_scope_dcr_immutable_id on workspace " + workspace.name);
    }

    // This alternative was considered where the DCR is tagged with a scope ID instead. For now, obtain from the workspace.
    //
    // for await (let dcr of this.monitorClient.dataCollectionRules.listBySubscription()) {
    //   if (dcr.tags.wufb_scope_id === workspace.tags.wufb_scope_id) {
    //     ruleId = dcr.immutableId;
    //     break;
    //   }
    // }

    // Get records from the primary table in batches
    let pageCount = 0;
    let page: LogsTable;
    while ((page = await this.getNextPage(this.primaryWorkspace, table.name, cursor)).rows.length) {
      // Update cursor for next run before filtering
      const nextCursor = this.getNextCursor(page);
      if (JSON.stringify(cursor) === JSON.stringify(nextCursor)) {
        throw new Error(`Pages must have unique cursors, but this page was the same as the last.\n${cursor}\n${page}`);
      }
      cursor = nextCursor;

      // Filter page data to remove devices that don't belong to this scope
      const notInScopeCount = await this.filterPageAllowedDevices(page, allowedDevices);
      this.log(`${notInScopeCount} rows were not intended for the target scope.`);

      // Write remaining data to the target
      if (page.rows.length) {
        await this.writePage(ruleId, `Custom-${table.name}_IN`, page);
      } else {
        this.log(`No remaining records to write.`);
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
      try {
        this.log("Calling Graph ... " + `/groups/${workspace.tags.wufb_scope_azure_ad_group_id}/members/microsoft.graph.device`);
        const membersResponse: PageCollection = await this.graphClient.api(`/groups/${workspace.tags.wufb_scope_azure_ad_group_id}/members/microsoft.graph.device`).get();
        this.log(membersResponse);
        const callback: PageIteratorCallback = (data) => {
          this.log(data);
          // results.push(data.deviceId);
          return true;
        };
        const pageIterator = new PageIterator(this.graphClient, membersResponse, callback);
        await pageIterator.iterate();
        throw new Error("DONE");
      } catch(error: any) {
        this.error(error);
      }
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

    results.sort((a,b) => a.localeCompare(b));
    return results;
  }

  // Remove rows from the page that are not part of the scope
  private async filterPageAllowedDevices(page: LogsTable, allowedDevices: string[]) {
    let rowsFiltered = 0;
    if (!allowedDevices.length) return rowsFiltered;
    const azureADDeviceIdIndex = page.columnDescriptors.findIndex(c => c.name === "AzureADDeviceId");
    for(let r = page.rows.length - 1; r >= 0; r--) {
      if (!findFast(allowedDevices, page.rows[r][azureADDeviceIdIndex] as string)) {
        page.rows.splice(r, 1);
        rowsFiltered++;
      }
    }
    return rowsFiltered;
  }

  // Remove rows from the page that are not part of the scope
  private async filterPageDisallowedDevices(page: LogsTable, disallowedDevices: string[]) {
    let rowsFiltered = 0;
    if (!disallowedDevices.length) return rowsFiltered;
    const azureADDeviceIdIndex = page.columnDescriptors.findIndex(c => c.name === "AzureADDeviceId");
    for(let r = page.rows.length - 1; r >= 0; r--) {
      if (findFast(disallowedDevices, page.rows[r][azureADDeviceIdIndex] as string)) {
        page.rows.splice(r, 1);
        rowsFiltered++;
      }
    }
    return rowsFiltered;
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
    { exactTime = false, excludeRecordsFromWorkspace: excludeFromExistingWorkspace, columns, allowedDevices = [], disallowedDevices = [] }: { exactTime?: boolean, excludeRecordsFromWorkspace?: string, columns?: string[], allowedDevices?: string[], disallowedDevices?: string[] } = {}): Promise<LogsTable> {
    const secondaryKeyName = this.getSecondaryKeyName(tableName.replace("_CL", ""));
    const timeGeneratedName = tableName.endsWith("_CL") ? "OriginTimeGenerated_CF" : "TimeGenerated";
    const kustoQuery = `${tableName} | where ((${timeGeneratedName} == todatetime("${cursor.timeGenerated.toISOString()}") and isnotempty(${secondaryKeyName})`
      + (cursor.secondaryKey ? ` and strcmp(${secondaryKeyName}, "${cursor.secondaryKey}") > 0)` : ")")
      + (!exactTime ? ` or ${timeGeneratedName} > todatetime("${cursor.timeGenerated.toISOString()}"))` : ")")
      + (allowedDevices.length ? ` and ${secondaryKeyName} in~ (${allowedDevices.map(d => `"${d}"`).join(",")})` : "")
      + (disallowedDevices.length ? ` and ${secondaryKeyName} !in~ (${disallowedDevices.map(d => `"${d}"`).join(",")})` : "")
      + (excludeFromExistingWorkspace ? ` | join kind=leftanti (workspace("${excludeFromExistingWorkspace}").${tableName}_CL) on AzureADDeviceId, $left.TimeGenerated == $right.OriginTimeGenerated_CF` : "")
      + ` | sort by ${timeGeneratedName} asc, ${secondaryKeyName} asc`
      + (columns ? ` | project ${columns.join(",")}` : "")
      + ` | limit ${ROW_LIMIT_PER_PAGE}`;

    this.log(`[${workspace.name}.${tableName}]: Getting records after cursor: ${JSON.stringify(cursor)}`);

    let result: LogsQueryResult;
    try {
      result = await this.logsQueryClient.queryWorkspace(workspace.customerId, kustoQuery, {
        duration: `P${this.maxDaysToSync}D`
      });
    }
    catch(error) {
      this.log(`[${workspace.name}.${tableName}]: ${kustoQuery}`);
      throw error;
    }

    switch(result.status)
    {
      case LogsQueryResultStatus.Success:
        this.log(`[${workspace.name}.${tableName}]: ${result.tables[0].rows.length} rows returned.`);
        return result.tables[0]
      case LogsQueryResultStatus.PartialFailure:
        this.error("Unexpected partial failure", kustoQuery, "\n", result.partialError);
        throw new Error(`Unexpected partial failure: ${kustoQuery}\n${result.partialError}`);
      default:
        throw new Error(`Unexpected status: ${kustoQuery}\n${result}`);
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