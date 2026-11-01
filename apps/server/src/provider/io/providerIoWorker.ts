import { claudeAgentSdkStage } from "./claudeAgentSdkStage.ts";
import { runProviderIoWorker } from "./providerIoWorkerRuntime.ts";

runProviderIoWorker([claudeAgentSdkStage]);
