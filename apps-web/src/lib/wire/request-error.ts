// REQUEST_ERROR message (Section 9.7)
// Error response to requests such as SUBSCRIBE, SUBSCRIBE_NAMESPACE, etc.

import { encodeMessage, MSG_REQUEST_ERROR } from "./message.js";
import { encodeVarint } from "./varint.js";

// Error codes (Section 14.5.2)
export const ERROR_DOES_NOT_EXIST = 0x10;
export const ERROR_UNINTERESTED = 0x20;

export interface RequestErrorMessage {
  errorCode: number;
  retryInterval: number;
  reasonPhrase: string;
}

export function encodeRequestError(msg: RequestErrorMessage): Uint8Array {
  const payload: number[] = [];
  payload.push(...encodeVarint(msg.errorCode));
  payload.push(...encodeVarint(msg.retryInterval));
  const reasonBytes = new TextEncoder().encode(msg.reasonPhrase);
  payload.push(...encodeVarint(reasonBytes.length));
  payload.push(...reasonBytes);
  return encodeMessage(MSG_REQUEST_ERROR, new Uint8Array(payload));
}
