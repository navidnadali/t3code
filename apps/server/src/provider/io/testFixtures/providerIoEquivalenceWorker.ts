import * as NodeWorkerThreads from "node:worker_threads";

import { runProviderIoWorker } from "../providerIoWorkerRuntime.ts";
import {
  equivalenceWorkerStages,
  type EquivalenceWorkerVariant,
} from "./providerIoEquivalenceStages.ts";

const variant: EquivalenceWorkerVariant = NodeWorkerThreads.workerData ?? { framer: "intact" };
runProviderIoWorker(equivalenceWorkerStages(variant));
