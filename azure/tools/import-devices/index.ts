#!/usr/bin/env node
import "isomorphic-fetch";
import { DefaultAzureCredential } from "@azure/identity";
import { Client as GraphClient } from "@microsoft/microsoft-graph-client";
import { Device, Group } from "@microsoft/microsoft-graph-types";
import { argv } from 'process';
import fs from 'fs';
import readline from 'readline';

async function main() {
  if (argv.length !== 4) {
    console.log("Usage: import-devices <group-object-id> <device-list-file>");
    return;
  }

  const groupName = argv[2];
  const groupId = argv[2];
  const deviceListFilePath = argv[3];

  // Use the default credential which can be obtained through CLI sign-in or Azure managed identity
  const credential = new DefaultAzureCredential();

  // Authenticate to Graph
  // See: https://learn.microsoft.com/en-us/azure/app-service/tutorial-connect-app-access-microsoft-graph-as-app-javascript
  const tokenResponse = await credential.getToken("https://graph.microsoft.com/.default");
  const graphClient = GraphClient.init({
    // Use the provided access token to authenticate requests
    authProvider: (done) => {
      done(null, tokenResponse.token);
    }
  });

  // Find group
  const getGroupResponse = await graphClient.api(`/groups?$filter=displayName eq '${groupName}'`).get();
  let group: Group;
  if (getGroupResponse.value.length !== 1) {
    // if (groups?.length > 1) {
      console.error(`Unable to find exactly one group named ${groupName} in Azure AD.`, getGroupResponse);
      return;
    // }

    // console.log(`Creating new group ${groupName}`);
    // const g = {} as Group;
    // g.displayName = groupName;
    // g.mailEnabled = false;
    // g.mailNickname = groupName;
    // g.securityEnabled = false;
    // g.groupTypes = [];
    // group = await graphClient.api(`/groups`).post(g);
    // console.log(`Created group:`, group);
  } else {
    group = getGroupResponse.value[0];
  }
  // console.log(group);
  // return;

  // Open file for reading
  const fileStream = fs.createReadStream(deviceListFilePath);

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

    // Get device
    const device = await graphClient.api(`/devices(deviceId='${deviceId}')`).get() as Device;
    console.log("Located device", device.id);

    // Add device to group
    const response = await graphClient.api(`/groups/${group.id}/members/$ref`).post(
      {
        "@odata.id": `https://graph.microsoft.com/v1.0/directoryObjects/${device.id}`
      }
    );
    console.log(response);

    break;
  }
}
main();
