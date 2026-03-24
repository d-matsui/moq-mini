// NAMESPACE message (Section 9.18)
// Sent on the response stream of a SUBSCRIBE_NAMESPACE request.
// Contains the Track Namespace Suffix (the part after the prefix).

import { encodeMessage, MSG_NAMESPACE } from "./message.js";
import { encodeTrackNamespace } from "./track-namespace.js";
import type { TrackNamespace } from "./track-namespace.js";

export interface NamespaceMessage {
  trackNamespaceSuffix: TrackNamespace;
}

export function encodeNamespace(msg: NamespaceMessage): Uint8Array {
  const payload: number[] = [];
  encodeTrackNamespace(msg.trackNamespaceSuffix, payload);
  return encodeMessage(MSG_NAMESPACE, new Uint8Array(payload));
}
