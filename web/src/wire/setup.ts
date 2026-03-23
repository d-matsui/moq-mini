// SETUP message (Section 9.4)
// Type ID: 0x2F00
// Body: setup_options (Key-Value-Pairs)

import { encodeMessage, decodeMessage, MSG_SETUP } from "./message.js";
import { encodeKeyValuePairs, decodeKeyValuePairs } from "./key-value-pair.js";
import type { KeyValuePair } from "./key-value-pair.js";

// Setup option type IDs (must match Rust: setup.rs)
export const SETUP_PATH = 0x01;           // odd = bytes
export const SETUP_IMPLEMENTATION = 0x07; // odd = bytes

export interface SetupMessage {
  options: KeyValuePair[];
}

/** Create a client SETUP message.
 *  Over WebTransport, PATH and AUTHORITY are in the HTTP/3 CONNECT request,
 *  so only IMPLEMENTATION is sent here. */
export function clientSetup(): SetupMessage {
  return {
    options: [
      { typeId: SETUP_IMPLEMENTATION, value: new TextEncoder().encode("moq-minimal") },
    ],
  };
}

/** Encode a SETUP message (full frame with type + length + payload). */
export function encodeSetup(msg: SetupMessage): Uint8Array {
  const payload: number[] = [];
  encodeKeyValuePairs(msg.options, payload);
  return encodeMessage(MSG_SETUP, new Uint8Array(payload));
}

/** Decode a SETUP message from a full frame. */
export function decodeSetup(frame: Uint8Array): SetupMessage {
  const { msgType, payload } = decodeMessage(frame, 0);
  if (msgType !== MSG_SETUP) {
    throw new Error(`expected SETUP (0x2F00), got 0x${msgType.toString(16)}`);
  }
  const { pairs } = decodeKeyValuePairs(payload, 0);
  return { options: pairs };
}
