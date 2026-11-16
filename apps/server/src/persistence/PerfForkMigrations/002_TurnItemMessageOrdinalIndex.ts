import * as Effect from "effect/Effect";

/**
 * The message-ordinal index. Upstream's migration 94 creates it now, under the same name and
 * definition with IF NOT EXISTS, and so keeps the one this migration made. It stays recorded.
 */
export default Effect.void;
