import { acpIoStage } from "./acpIoStage.ts";
import { codexIoStage } from "./codexIoStage.ts";
import { runProviderIoWorker } from "./providerIoWorkerRuntime.ts";

runProviderIoWorker([codexIoStage, acpIoStage]);
