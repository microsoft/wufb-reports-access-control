#!/usr/bin/env node
import "isomorphic-fetch";
import { DefaultAzureCredential } from "@azure/identity";
import { Client as GraphClient, GraphError } from "@microsoft/microsoft-graph-client";
import { Device, Group } from "@microsoft/microsoft-graph-types";
import { argv } from 'process';
import fs from 'fs';
import readline from 'readline';

async function main() {
  if (argv.length !== 4 && (argv.length !== 5 || argv[4] !== "-createTestDevices")) {
    console.log("Usage: import-devices <group-name> <device-list-file> [-createTestDevices]");
    return;
  }

  const groupName = argv[2];
  const deviceListFilePath = argv[3];
  const isTest = argv.length === 5;

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
    console.error(`Unable to find exactly one group named ${groupName} in Azure AD.`, getGroupResponse);
    return;
  } else {
    group = getGroupResponse.value[0];
  }
  console.log(`Processing group ${groupName}`);

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

    // Get device to find its directory id
    let device: Device;
    try {
      device = await graphClient.api(`/devices(deviceId='${deviceId}')`).get() as Device;
      console.log("Located device", device.id);
    } catch (error) {
      if (isTest) {
        const graphError = error as GraphError;
        if (graphError.statusCode === 404) {
          // Device not found, create a test device
          console.log("Unable to find device", deviceId);
          const d = {} as Device;
          d.deviceId = deviceId;
          d.displayName = "Test device " + d.deviceId;
          d.operatingSystem = "Windows";
          d.operatingSystemVersion = "10.0.20348.887";
          d.accountEnabled = true;
          d.alternativeSecurityIds = [
            {
              "type": 2,
              "key": "Y3YxN2E1MWFlYw=="
            }
          ];
          device = await graphClient.api(`/devices`).post(d) as Device;
          console.log("Created test device", device.deviceId);
        } else {
          throw error;
        }
      } else {
        throw error;
      }
    }

    // Add device to group
    const response = await graphClient.api(`/groups/${group.id}/members/$ref`).post(
      {
        "@odata.id": `https://graph.microsoft.com/v1.0/directoryObjects/${device.id}`
      }
    );
    console.log(`Added device ${device.deviceId} with object ID ${device.id}`);
  }
}
main();
