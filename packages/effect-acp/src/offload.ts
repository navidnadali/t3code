// Runs on a provider I/O worker, so it imports nothing that builds the ACP schemas.
import type * as RpcMessage from "effect/rpc/RpcMessage";
import * as RpcSerialization from "effect/rpc/RpcSerialization";

type WireMessage = RpcMessage.FromClientEncoded | RpcMessage.FromServerEncoded;

/** One line decoded off the main thread; main finishes it with `normalizeOffloadedMessages`. */
export type OffloadedLine =
  | { readonly _tag: "Decoded"; readonly messages: ReadonlyArray<WireMessage> }
  | { readonly _tag: "Failed"; readonly cause: unknown };

/** Decodes single lines with the strict parser's codec; never a batch line (main keeps those). */
export const makeOffloadedLineDecoder = () => {
  const codec = RpcSerialization.jsonRpc().makeUnsafe();
  return (line: string): OffloadedLine => {
    try {
      return { _tag: "Decoded", messages: codec.decode(line) as ReadonlyArray<WireMessage> };
    } catch (cause) {
      return { _tag: "Failed", cause };
    }
  };
};
