// FETCH message (Section 9.14)
// Only Relative Joining Fetch (type 0x2) is supported.

import { encodeVarint } from "./varint.js";
import { encodeMessage, MSG_FETCH } from "./message.js";
import { encodeParameters } from "./parameter.js";
import type { MessageParameter } from "./parameter.js";

export const FETCH_TYPE_RELATIVE_JOINING = 0x02;

export interface FetchMessage {
  requestId: number;
  requiredRequestIdDelta: number;
  fetchType: number;
  joiningRequestId: number;
  joiningStart: number;
  parameters: MessageParameter[];
}

export function encodeFetch(msg: FetchMessage): Uint8Array {
  const payload: number[] = [];
  payload.push(...encodeVarint(msg.requestId));
  payload.push(...encodeVarint(msg.requiredRequestIdDelta));
  payload.push(...encodeVarint(msg.fetchType));
  payload.push(...encodeVarint(msg.joiningRequestId));
  payload.push(...encodeVarint(msg.joiningStart));
  encodeParameters(msg.parameters, payload);
  return encodeMessage(MSG_FETCH, new Uint8Array(payload));
}
