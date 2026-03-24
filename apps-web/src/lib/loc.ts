// LOC Header Extensions (draft-ietf-moq-loc-01 Section 2.3)
//
// LOC Header Extensions carry optional metadata for media payloads.
// They are encoded as Object Properties within MOQT data streams.
//
// Encoding rules:
// - Even ID: varint ID + varint value (no length field)
// - Odd ID:  varint ID + varint length + raw bytes

import { encodeVarint, decodeVarint } from "./wire/varint.js";

export const EXT_CAPTURE_TIMESTAMP = 2;
export const EXT_VIDEO_FRAME_MARKING = 4;
export const EXT_AUDIO_LEVEL = 6;
export const EXT_VIDEO_CONFIG = 13;

export type LocExtension =
  | { type: "captureTimestamp"; value: number }
  | { type: "videoFrameMarking"; value: number }
  | { type: "audioLevel"; value: number }
  | { type: "videoConfig"; data: Uint8Array };

function extensionId(ext: LocExtension): number {
  switch (ext.type) {
    case "captureTimestamp":
      return EXT_CAPTURE_TIMESTAMP;
    case "videoFrameMarking":
      return EXT_VIDEO_FRAME_MARKING;
    case "audioLevel":
      return EXT_AUDIO_LEVEL;
    case "videoConfig":
      return EXT_VIDEO_CONFIG;
  }
}

/** Encode LOC Header Extensions into bytes. */
export function encodeExtensions(extensions: LocExtension[]): Uint8Array {
  const parts: number[] = [];
  for (const ext of extensions) {
    const id = extensionId(ext);
    parts.push(...encodeVarint(id));

    if (ext.type === "videoConfig") {
      // Odd ID: varint length + raw bytes
      parts.push(...encodeVarint(ext.data.length));
      parts.push(...ext.data);
    } else {
      // Even ID: varint value
      parts.push(...encodeVarint(ext.value));
    }
  }
  return new Uint8Array(parts);
}

/** Decode LOC Header Extensions from bytes. */
export function decodeExtensions(buf: Uint8Array): LocExtension[] {
  const extensions: LocExtension[] = [];
  let offset = 0;

  while (offset < buf.length) {
    const { value: id, bytesRead: r1 } = decodeVarint(buf, offset);
    offset += r1;

    switch (id) {
      case EXT_CAPTURE_TIMESTAMP: {
        const { value, bytesRead } = decodeVarint(buf, offset);
        offset += bytesRead;
        extensions.push({ type: "captureTimestamp", value });
        break;
      }
      case EXT_VIDEO_FRAME_MARKING: {
        const { value, bytesRead } = decodeVarint(buf, offset);
        offset += bytesRead;
        extensions.push({ type: "videoFrameMarking", value });
        break;
      }
      case EXT_AUDIO_LEVEL: {
        const { value, bytesRead } = decodeVarint(buf, offset);
        offset += bytesRead;
        extensions.push({ type: "audioLevel", value });
        break;
      }
      case EXT_VIDEO_CONFIG: {
        const { value: len, bytesRead } = decodeVarint(buf, offset);
        offset += bytesRead;
        const data = buf.slice(offset, offset + len);
        offset += len;
        extensions.push({ type: "videoConfig", data });
        break;
      }
      default:
        throw new Error(`unknown LOC extension ID: ${id}`);
    }
  }

  return extensions;
}
