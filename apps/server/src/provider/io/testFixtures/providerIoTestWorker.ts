import { runProviderIoWorker } from "../providerIoWorkerRuntime.ts";
import { asyncStage, echoStage, linesStage } from "./providerIoTestStages.ts";

runProviderIoWorker([echoStage, linesStage, asyncStage]);
