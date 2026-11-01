import { acpWireStage, codexWireStage } from "./providerIoStages.ts";
import { runProviderIoWorker } from "./providerIoWorkerRuntime.ts";

runProviderIoWorker([codexWireStage, acpWireStage]);
