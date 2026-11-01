import { runProviderIoWorker } from "../providerIoWorkerRuntime.ts";
import { echoStage, linesStage } from "./providerIoTestStages.ts";

runProviderIoWorker([echoStage, linesStage]);
