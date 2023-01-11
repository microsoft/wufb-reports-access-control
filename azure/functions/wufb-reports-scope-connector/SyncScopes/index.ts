import { AzureFunction, Context } from "@azure/functions";
import SyncService from "./sync-service";

// Trigger on a timer
const timerTrigger: AzureFunction = async function (context: Context, myTimer: any): Promise<void> {
  // Notify if the job is running late
  if (myTimer.isPastDue) {
    context.log.warn('Timer function is running late!');
  }

  // Sync data
  const syncService = new SyncService(context);
  await syncService.sync();
};

export default timerTrigger;
