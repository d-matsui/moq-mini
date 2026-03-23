// SUBSCRIBE_OK message (Section 9.8)

import { encodeVarint, decodeVarint } from "./varint.js";
import { encodeMessage, decodeMessage, MSG_SUBSCRIBE_OK } from "./message.js";
import { encodeParameters, decodeParameters } from "./parameter.js";
import type { MessageParameter } from "./parameter.js";

export interface SubscribeOkMessage {
  trackAlias: number;
  parameters: MessageParameter[];
  trackPropertiesRaw: Uint8Array;
}

export function encodeSubscribeOk(msg: SubscribeOkMessage): Uint8Array {
  const payload: number[] = [];
  payload.push(...encodeVarint(msg.trackAlias));
  encodeParameters(msg.parameters, payload);
  // Track Properties: KVPs directly at end of message (no length prefix)
  payload.push(...msg.trackPropertiesRaw);
  return encodeMessage(MSG_SUBSCRIBE_OK, new Uint8Array(payload));
}

export function decodeSubscribeOk(frame: Uint8Array): SubscribeOkMessage {
  const { msgType, payload } = decodeMessage(frame, 0);
  if (msgType !== MSG_SUBSCRIBE_OK) {
    throw new Error(`expected SUBSCRIBE_OK, got 0x${msgType.toString(16)}`);
  }
  let pos = 0;
  const { value: trackAlias, bytesRead: r1 } = decodeVarint(payload, pos);
  pos += r1;
  const { params: parameters, bytesRead: r2 } = decodeParameters(payload, pos);
  pos += r2;
  // Track Properties: remaining bytes are KVPs (no length prefix)
  const trackPropertiesRaw = pos < payload.length
    ? payload.slice(pos)
    : new Uint8Array(0);
  return { trackAlias, parameters, trackPropertiesRaw };
}
