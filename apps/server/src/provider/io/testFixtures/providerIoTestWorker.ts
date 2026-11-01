import { runProviderIoWorker } from "../providerIoWorkerRuntime.ts";
import { echoStage, linesStage } from "./providerIoTestStages.ts";
import { acpWireStage, codexWireStage } from "./providerIoWireStages.ts";

runProviderIoWorker([echoStage, linesStage, codexWireStage, acpWireStage]);
