// Message Parameters (Section 9.3.1)
// Used in SUBSCRIBE, SUBSCRIBE_OK, etc.
// Wire format: count (varint) + (type_delta (varint) + value)...

import { encodeVarint, decodeVarint } from "./varint.js";

// Parameter Type IDs (Section 9.3, Table 11)
const PARAM_DELIVERY_TIMEOUT = 0x02;
const PARAM_AUTHORIZATION_TOKEN = 0x03;
const PARAM_RENDEZVOUS_TIMEOUT = 0x04;
const PARAM_EXPIRES = 0x08;
export const PARAM_LARGEST_OBJECT = 0x09;
const PARAM_FORWARD = 0x10;
const PARAM_SUBSCRIBER_PRIORITY = 0x20;
const PARAM_SUBSCRIPTION_FILTER = 0x21;
const PARAM_GROUP_ORDER = 0x22;
const PARAM_NEW_GROUP_REQUEST = 0x32;

// Subscription Filter Type values (Section 5.1.2)
export const FILTER_NEXT_GROUP_START = 0x01;
export const FILTER_LARGEST_OBJECT = 0x02;

export interface MessageParameter {
  typeId: number;
  value: number | Uint8Array;
  /** For LARGEST_OBJECT: the Object ID portion of the location. */
  objectId?: number;
}

/** Create a NextGroupStart subscription filter parameter. */
export function subscriptionFilterNextGroupStart(): MessageParameter {
  return { typeId: PARAM_SUBSCRIPTION_FILTER, value: FILTER_NEXT_GROUP_START };
}

/** Create a LargestObject subscription filter parameter. */
export function subscriptionFilterLargestObject(): MessageParameter {
  return { typeId: PARAM_SUBSCRIPTION_FILTER, value: FILTER_LARGEST_OBJECT };
}

/** Extract the LARGEST_OBJECT location from parameters, if present. */
export function extractLargestObject(params: MessageParameter[]): { group: number; object: number } | null {
  const param = params.find(p => p.typeId === PARAM_LARGEST_OBJECT);
  if (!param || typeof param.value !== "number") return null;
  return { group: param.value, object: param.objectId ?? 0 };
}

/** Encode message parameters. */
export function encodeParameters(params: MessageParameter[], buf: number[]): void {
  buf.push(...encodeVarint(params.length));
  let prevType = 0;
  for (const param of params) {
    const delta = param.typeId - prevType;
    buf.push(...encodeVarint(delta));
    prevType = param.typeId;

    if (param.typeId === PARAM_SUBSCRIPTION_FILTER) {
      // Length-prefixed: length + Subscription Filter structure
      const filterPayload: number[] = [];
      filterPayload.push(...encodeVarint(param.value as number)); // Filter Type
      buf.push(...encodeVarint(filterPayload.length));
      buf.push(...filterPayload);
    } else if (param.typeId === PARAM_LARGEST_OBJECT) {
      // Location: Group (varint) + Object (varint)
      buf.push(...encodeVarint(param.value as number));
      buf.push(...encodeVarint(param.objectId ?? 0));
    } else if (typeof param.value === "number") {
      // Varint value
      buf.push(...encodeVarint(param.value));
    } else {
      // Length-prefixed bytes
      buf.push(...encodeVarint(param.value.length));
      buf.push(...param.value);
    }
  }
}

/** Decode message parameters. */
export function decodeParameters(
  buf: Uint8Array,
  offset: number
): { params: MessageParameter[]; bytesRead: number } {
  let pos = offset;
  const { value: count, bytesRead: countLen } = decodeVarint(buf, pos);
  pos += countLen;

  const params: MessageParameter[] = [];
  let prevType = 0;
  for (let i = 0; i < count; i++) {
    const { value: delta, bytesRead: deltaLen } = decodeVarint(buf, pos);
    pos += deltaLen;
    const typeId = prevType + delta;
    prevType = typeId;

    // Parameter value encoding depends on the type
    if (typeId === PARAM_SUBSCRIPTION_FILTER) {
      // Length-prefixed: length + Subscription Filter structure
      const { value: len, bytesRead: lenBytes } = decodeVarint(buf, pos);
      pos += lenBytes;
      const { value: filterType } = decodeVarint(buf, pos);
      pos += len; // skip entire filter payload
      params.push({ typeId, value: filterType });
    } else if (typeId === PARAM_LARGEST_OBJECT) {
      // Location: Group (varint) + Object (varint)
      const { value: group, bytesRead: gLen } = decodeVarint(buf, pos);
      pos += gLen;
      const { value: obj, bytesRead: oLen } = decodeVarint(buf, pos);
      pos += oLen;
      params.push({ typeId, value: group, objectId: obj });
    } else if (typeId === PARAM_FORWARD || typeId === PARAM_SUBSCRIBER_PRIORITY || typeId === PARAM_GROUP_ORDER) {
      // uint8
      params.push({ typeId, value: buf[pos] });
      pos += 1;
    } else if (typeId === PARAM_DELIVERY_TIMEOUT || typeId === PARAM_RENDEZVOUS_TIMEOUT
               || typeId === PARAM_EXPIRES || typeId === PARAM_NEW_GROUP_REQUEST) {
      // varint
      const { value, bytesRead: valLen } = decodeVarint(buf, pos);
      pos += valLen;
      params.push({ typeId, value });
    } else if (typeId === PARAM_AUTHORIZATION_TOKEN) {
      // Length-prefixed
      const { value: len, bytesRead: lenBytes } = decodeVarint(buf, pos);
      pos += lenBytes;
      const value = buf.slice(pos, pos + len);
      pos += len;
      params.push({ typeId, value });
    } else {
      // Unknown parameter: close session per spec, but for now skip
      throw new Error(`unknown parameter type: 0x${typeId.toString(16)}`);
    }
  }

  return { params, bytesRead: pos - offset };
}
