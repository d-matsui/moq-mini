// MSF Browser Subscriber
// Subscribes to catalog to discover tracks, then subscribes to
// video (and optionally audio) tracks, decoding with WebCodecs.
// CaptureTimestamp from LOC Header Extensions is used for playback timing.

import { MoqtSession, SubgroupReader } from "../lib/session.js";
import { decodeExtensions } from "../lib/loc.js";

const $ = (id: string) => document.getElementById(id)!;

const CATALOG_TRACK_NAME = "catalog";

let session: MoqtSession | null = null;
let videoDecoder: VideoDecoder | null = null;
let canvas: HTMLCanvasElement;
let ctx: CanvasRenderingContext2D;

// Frame queue: received frames are pushed here, playback loop pops and paces.
interface QueuedFrame {
  type: "key" | "delta";
  timestamp: number; // CaptureTimestamp (μs)
  data: Uint8Array;
}
const frameQueue: QueuedFrame[] = [];
let playbackRunning = false;

// Pacing state
let firstTimestamp: number | null = null;
let startTime: number | null = null;

// --- Catalog types ---

interface MsfCatalog {
  version: number;
  tracks: MsfTrack[];
}

interface MsfTrack {
  name: string;
  packaging: string;
  isLive: boolean;
  role?: string;
  codec?: string;
  width?: number;
  height?: number;
  framerate?: number;
  bitrate?: number;
  sampleRate?: number;
  channelCount?: number;
  initData?: string;
}

// --- LOC helpers ---

/** Extract CaptureTimestamp (μs) from Object Properties. */
function extractTimestamp(properties: Uint8Array | null): number | null {
  if (!properties || properties.length === 0) return null;
  try {
    const exts = decodeExtensions(properties);
    for (const ext of exts) {
      if (ext.type === "captureTimestamp") return ext.value;
    }
  } catch {
    // Ignore malformed extensions
  }
  return null;
}

// --- Playback loop: pop from queue, pace, decode ---

async function startPlaybackLoop() {
  if (playbackRunning) return;
  playbackRunning = true;

  while (playbackRunning) {
    const frame = frameQueue.shift();
    if (!frame) {
      // Queue empty — wait a bit and check again
      await new Promise((resolve) => setTimeout(resolve, 5));
      continue;
    }

    // Pace: wait until it's time to decode this frame
    if (firstTimestamp === null) {
      firstTimestamp = frame.timestamp;
      startTime = performance.now();
    } else {
      const target = (frame.timestamp - firstTimestamp) / 1000; // μs → ms
      const elapsed = performance.now() - startTime!;
      const delay = target - elapsed;
      if (delay > 0) {
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }

    if (videoDecoder && videoDecoder.state === "configured") {
      videoDecoder.decode(
        new EncodedVideoChunk({
          type: frame.type,
          timestamp: frame.timestamp,
          data: frame.data,
        })
      );
    }
  }
}

function drawFrame(frame: VideoFrame) {
  if (
    canvas.width !== frame.displayWidth ||
    canvas.height !== frame.displayHeight
  ) {
    canvas.width = frame.displayWidth;
    canvas.height = frame.displayHeight;
  }
  ctx.drawImage(frame, 0, 0);
  frame.close();
}

// --- Main ---

async function start() {
  const url = ($("relay-url") as HTMLInputElement).value;
  const namespaceRaw = ($("namespace") as HTMLInputElement).value;
  const namespace = namespaceRaw.split("/").filter((s) => s.length > 0);

  canvas = $("canvas") as HTMLCanvasElement;
  ctx = canvas.getContext("2d")!;

  log("Connecting...");
  session = await MoqtSession.connect(url);
  log("Connected. SETUP complete.");

  // Step 1: Subscribe to catalog
  log("Subscribing to catalog...");
  const catalogSub = await session.subscribe(namespace, CATALOG_TRACK_NAME);
  log(`Catalog subscribed (alias=${catalogSub.trackAlias}).`);

  // Step 2: Receive catalog
  const catalogEvent = await session.nextEvent();
  if (catalogEvent.type !== "dataStream") {
    log("ERROR: expected data stream for catalog");
    return;
  }

  const catalogResult = await catalogEvent.reader.readObject();
  if (!catalogResult) {
    log("ERROR: catalog stream empty");
    return;
  }

  const catalogJson = new TextDecoder().decode(catalogResult.payload);
  const catalog: MsfCatalog = JSON.parse(catalogJson);
  log(`Catalog v${catalog.version}: ${catalog.tracks.length} track(s)`);

  // Step 3: Find video track
  const videoTrack = catalog.tracks.find(
    (t) => t.packaging === "loc" && t.role === "video"
  ) ?? catalog.tracks.find((t) => t.packaging === "loc");

  if (!videoTrack) {
    log("ERROR: no video track in catalog");
    return;
  }

  log(`Found video: ${videoTrack.name} (codec=${videoTrack.codec})`);

  // Step 4: Set up video decoder
  videoDecoder = new VideoDecoder({
    output: (frame) => {
      drawFrame(frame);
    },
    error: (e) => log(`Video decoder error: ${e.message}`),
  });

  const videoCodec = videoTrack.codec || "avc1.42001e";
  videoDecoder.configure({ codec: videoCodec });
  log(`Video decoder configured: ${videoCodec}`);

  // Step 5: Subscribe to video track
  log(`Subscribing to ${videoTrack.name}...`);
  const videoSub = await session.subscribe(namespace, videoTrack.name);
  log(`Video subscribed (alias=${videoSub.trackAlias}).`);

  ($("start-btn") as HTMLButtonElement).disabled = true;
  ($("stop-btn") as HTMLButtonElement).disabled = false;

  // Step 6: Start playback loop and receive media
  startPlaybackLoop();
  receiveLoop(videoSub.trackAlias);
}

// --- Receive loop ---

async function receiveLoop(videoAlias: number) {
  if (!session) return;

  try {
    while (session) {
      const event = await session.nextEvent();
      if (event.type !== "dataStream") continue;

      if (event.reader.trackAlias === videoAlias) {
        log(`Video group ${event.reader.groupId} received`);
        processVideoGroup(event.reader);
      }
    }
  } catch (e) {
    log(`Receive ended: ${e}`);
  }
}

async function processVideoGroup(group: SubgroupReader) {
  let isFirstObject = true;

  try {
    while (true) {
      const result = await group.readObject();
      if (result === null) break;

      const type = isFirstObject ? "key" : "delta";
      isFirstObject = false;

      const timestamp = extractTimestamp(result.properties);
      if (timestamp === null) {
        log("WARNING: missing CaptureTimestamp, skipping frame");
        continue;
      }

      // Insert in timestamp order (binary search for position)
      const entry: QueuedFrame = { type, timestamp, data: result.payload };
      let lo = 0;
      let hi = frameQueue.length;
      while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (frameQueue[mid].timestamp < timestamp) lo = mid + 1;
        else hi = mid;
      }
      frameQueue.splice(lo, 0, entry);
    }
  } catch (e) {
    log(`Video group error: ${e}`);
  }
}

async function stop() {
  playbackRunning = false;
  frameQueue.length = 0;

  if (videoDecoder) {
    await videoDecoder.flush();
    videoDecoder.close();
    videoDecoder = null;
  }
  firstTimestamp = null;
  startTime = null;

  if (session) {
    session.close();
    session = null;
  }
  ($("start-btn") as HTMLButtonElement).disabled = false;
  ($("stop-btn") as HTMLButtonElement).disabled = true;
  log("Stopped.");
}

function log(msg: string) {
  const el = $("log");
  el.textContent += msg + "\n";
  el.scrollTop = el.scrollHeight;
  console.log(msg);
}

document.addEventListener("DOMContentLoaded", () => {
  $("start-btn").addEventListener("click", () =>
    start().catch((e) => log(`ERROR: ${e}`))
  );
  $("stop-btn").addEventListener("click", () =>
    stop().catch((e) => log(`ERROR: ${e}`))
  );
});
