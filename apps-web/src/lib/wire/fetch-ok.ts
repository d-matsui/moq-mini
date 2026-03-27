// FETCH_OK message (Section 9.15)

import { decodeVarint } from "./varint.js";
import { decodeMessage, MSG_FETCH_OK } from "./message.js";
import { decodeParameters } from "./parameter.js";
import type { MessageParameter } from "./parameter.js";

export interface FetchOkMessage {
  endOfTrack: boolean;
  endGroup: number;
  endObject: number;
  parameters: MessageParameter[];
  trackPropertiesRaw: Uint8Array;
}

export function decodeFetchOk(frame: Uint8Array): FetchOkMessage {
  const { msgType, payload } = decodeMessage(frame, 0);
  if (msgType !== MSG_FETCH_OK) {
    throw new Error(`expected FETCH_OK (0x${MSG_FETCH_OK.toString(16)}), got 0x${msgType.toString(16)}`);
  }
  let pos = 0;
  const endOfTrack = payload[pos] !== 0;
  pos += 1;
  const { value: endGroup, bytesRead: r1 } = decodeVarint(payload, pos);
  pos += r1;
  const { value: endObject, bytesRead: r2 } = decodeVarint(payload, pos);
  pos += r2;
  const { params: parameters, bytesRead: r3 } = decodeParameters(payload, pos);
  pos += r3;
  const trackPropertiesRaw = pos < payload.length
    ? payload.slice(pos)
    : new Uint8Array(0);
  return { endOfTrack, endGroup, endObject, parameters, trackPropertiesRaw };
}
