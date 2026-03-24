import { describe, it, expect } from "vitest";
import {
  encodeExtensions,
  decodeExtensions,
  type LocExtension,
} from "./loc.js";

describe("loc", () => {
  it("roundtrip: capture timestamp", () => {
    const ext: LocExtension = { type: "captureTimestamp", value: 1_000_000 };
    const bytes = encodeExtensions([ext]);
    const decoded = decodeExtensions(bytes);
    expect(decoded).toEqual([ext]);
  });

  it("roundtrip: video config", () => {
    const ext: LocExtension = {
      type: "videoConfig",
      data: new Uint8Array([0x01, 0x42, 0x00, 0x1e]),
    };
    const bytes = encodeExtensions([ext]);
    const decoded = decodeExtensions(bytes);
    expect(decoded).toEqual([ext]);
  });

  it("roundtrip: multiple extensions", () => {
    const exts: LocExtension[] = [
      { type: "captureTimestamp", value: 1_700_000_000_000_000 },
      { type: "videoFrameMarking", value: 0x80 },
      { type: "videoConfig", data: new Uint8Array([0xaa, 0xbb]) },
    ];
    const bytes = encodeExtensions(exts);
    const decoded = decodeExtensions(bytes);
    expect(decoded).toEqual(exts);
  });

  it("roundtrip: empty", () => {
    const bytes = encodeExtensions([]);
    expect(bytes.length).toBe(0);
    const decoded = decodeExtensions(bytes);
    expect(decoded).toEqual([]);
  });

  it("roundtrip: audio level", () => {
    const ext: LocExtension = { type: "audioLevel", value: 0x4f };
    const bytes = encodeExtensions([ext]);
    const decoded = decodeExtensions(bytes);
    expect(decoded).toEqual([ext]);
  });

  // Known-bytes tests matching Rust msf/src/loc.rs
  it("known bytes: capture timestamp 1000000", () => {
    const ext: LocExtension = { type: "captureTimestamp", value: 1_000_000 };
    const bytes = encodeExtensions([ext]);
    expect(bytes).toEqual(
      new Uint8Array([
        0x02, // ID: 2 (1-byte varint)
        0xcf, 0x42, 0x40, // Value: 1000000 (3-byte varint)
      ])
    );
  });

  it("known bytes: video config [0xAA, 0xBB]", () => {
    const ext: LocExtension = {
      type: "videoConfig",
      data: new Uint8Array([0xaa, 0xbb]),
    };
    const bytes = encodeExtensions([ext]);
    expect(bytes).toEqual(
      new Uint8Array([
        0x0d, // ID: 13 (1-byte varint)
        0x02, // Length: 2
        0xaa, 0xbb, // Value
      ])
    );
  });
});
