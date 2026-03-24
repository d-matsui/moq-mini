// MSF Browser Subscriber
// Subscribes to catalog to discover tracks, then subscribes to
// H.264 video and Opus audio tracks, decoding with WebCodecs.
// LOC CaptureTimestamp is used for A/V lip sync: both audio and video
// are scheduled on a shared timeline (AudioContext clock + buffer offset).

import { MoqtSession, SubgroupReader } from "./session.js";
import { decodeExtensions } from "./loc.js";

const $ = (id: string) => document.getElementById(id)!;

const CATALOG_TRACK_NAME = "catalog";
const BUFFER_SEC = 1.0; // 1000ms buffer for both audio and video

let session: MoqtSession | null = null;
let videoDecoder: VideoDecoder | null = null;
let audioDecoder: AudioDecoder | null = null;
let audioCtx: AudioContext | null = null;
// Shared sync state: captureTimeBase is the first CaptureTimestamp received
// (from either audio or video), and playBase is the AudioContext time at which
// that timestamp should be presented (= audioCtx.currentTime + BUFFER_SEC).
let captureTimeBase: number | null = null;
let playBase: number | null = null;
let nextAudioPlayTime: number | null = null; // accumulated play time for gapless audio
let canvas: HTMLCanvasElement;
let ctx: CanvasRenderingContext2D;

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

/** Extract CaptureTimestamp from Object Properties, if present. */
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

/** Initialize shared sync base on first media timestamp. */
function initSyncBase(rawTimestampUs: number): void {
  if (captureTimeBase !== null) return;
  captureTimeBase = rawTimestampUs;
  playBase = audioCtx!.currentTime + BUFFER_SEC;
}

/** Convert a raw CaptureTimestamp (μs) to an AudioContext play time (seconds). */
function toPlayTime(rawTimestampUs: number): number {
  const captureTimeSec = (rawTimestampUs - captureTimeBase!) / 1_000_000;
  return playBase! + captureTimeSec;
}

// --- Audio playback (AudioBufferSourceNode with accumulated timing) ---

function playAudioData(audioData: AudioData) {
  if (!audioCtx) {
    audioData.close();
    return;
  }

  // Read all properties BEFORE close
  const numberOfFrames = audioData.numberOfFrames;
  const sampleRate = audioData.sampleRate;
  const timestamp = audioData.timestamp;

  const buffer = new AudioBuffer({
    length: numberOfFrames,
    numberOfChannels: 1,
    sampleRate,
  });

  const samples = new Float32Array(numberOfFrames);
  audioData.copyTo(samples, { planeIndex: 0, format: "f32-planar" });
  buffer.copyToChannel(samples, 0);
  audioData.close();

  // First frame: use LOC timestamp for lip sync positioning
  if (nextAudioPlayTime === null) {
    const captureTimeSec = timestamp / 1_000_000;
    nextAudioPlayTime = playBase! + captureTimeSec;
  }

  // If fallen behind, reset
  if (nextAudioPlayTime < audioCtx.currentTime) {
    nextAudioPlayTime = audioCtx.currentTime + BUFFER_SEC;
  }

  const source = audioCtx.createBufferSource();
  source.buffer = buffer;
  source.connect(audioCtx.destination);
  source.start(nextAudioPlayTime);
  nextAudioPlayTime += buffer.duration;
}

// --- Main ---

async function start() {
  const url = ($("relay-url") as HTMLInputElement).value;
  const namespaceRaw = ($("namespace") as HTMLInputElement).value;
  const namespace = namespaceRaw.split("/").filter((s) => s.length > 0);

  canvas = $("canvas") as HTMLCanvasElement;
  ctx = canvas.getContext("2d")!;

  // Create AudioContext in click handler scope to satisfy autoplay policy
  audioCtx = new AudioContext({ sampleRate: 48000 });

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

  // Step 4: Find audio track
  const audioTrack = catalog.tracks.find(
    (t) => t.packaging === "loc" && t.role === "audio"
  );

  if (audioTrack) {
    log(`Found audio: ${audioTrack.name} (codec=${audioTrack.codec})`);
  } else {
    log("No audio track in catalog (video-only mode).");
  }

  // Step 5: Set up video decoder (frames are scheduled via setTimeout for sync)
  videoDecoder = new VideoDecoder({
    output: (frame) => {
      scheduleVideoFrame(frame);
    },
    error: (e) => log(`Video decoder error: ${e.message}`),
  });

  const videoCodec = videoTrack.codec || "avc1.42001e";
  videoDecoder.configure({ codec: videoCodec });
  log(`Video decoder configured: ${videoCodec}`);

  // Step 6: Set up audio decoder with AudioContext playback
  if (audioTrack) {
    audioDecoder = new AudioDecoder({
      output: (audioData) => {
        playAudioData(audioData);
      },
      error: (e) => log(`Audio decoder error: ${e.message}`),
    });

    const audioCodec = audioTrack.codec || "opus";
    const sampleRate = audioTrack.sampleRate || 48000;
    const numberOfChannels = audioTrack.channelCount || 1;
    audioDecoder.configure({
      codec: audioCodec,
      sampleRate,
      numberOfChannels,
    });
    log(`Audio decoder configured: ${audioCodec} ${sampleRate}Hz ${numberOfChannels}ch`);
  }

  // Step 7: Subscribe to video track
  log(`Subscribing to ${videoTrack.name}...`);
  const videoSub = await session.subscribe(namespace, videoTrack.name);
  log(`Video subscribed (alias=${videoSub.trackAlias}).`);

  // Step 8: Subscribe to audio track
  let audioAlias: number | null = null;
  if (audioTrack) {
    log(`Subscribing to ${audioTrack.name}...`);
    const audioSub = await session.subscribe(namespace, audioTrack.name);
    audioAlias = audioSub.trackAlias;
    log(`Audio subscribed (alias=${audioAlias}).`);
  }

  ($("start-btn") as HTMLButtonElement).disabled = true;
  ($("stop-btn") as HTMLButtonElement).disabled = false;

  // Step 9: Receive media
  receiveLoop(videoSub.trackAlias, audioAlias);
}

// --- Video scheduling ---

function scheduleVideoFrame(frame: VideoFrame) {
  if (!audioCtx || playBase === null) {
    // Sync not initialized yet — draw immediately
    drawFrame(frame);
    return;
  }

  // frame.timestamp is the normalized CaptureTimestamp (μs)
  const captureTimeSec = frame.timestamp / 1_000_000;
  const playTime = playBase + captureTimeSec;
  const delayMs = (playTime - audioCtx.currentTime) * 1000;

  if (delayMs <= 0) {
    // Already due or late — draw immediately
    drawFrame(frame);
  } else {
    // Schedule drawing in the future
    setTimeout(() => drawFrame(frame), delayMs);
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

// --- Receive loop ---

async function receiveLoop(videoAlias: number, audioAlias: number | null) {
  if (!session) return;

  try {
    while (session) {
      const event = await session.nextEvent();
      if (event.type !== "dataStream") continue;

      const alias = event.reader.trackAlias;

      if (alias === videoAlias) {
        processVideoGroup(event.reader);
      } else if (audioAlias !== null && alias === audioAlias) {
        processAudioGroup(event.reader);
      }
      // else: skip unknown streams (e.g. catalog updates)
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

      if (videoDecoder && videoDecoder.state === "configured") {
        const type = isFirstObject ? "key" : "delta";
        isFirstObject = false;

        const rawTimestamp = extractTimestamp(result.properties) ?? performance.now() * 1000;
        initSyncBase(rawTimestamp);
        const timestamp = rawTimestamp - captureTimeBase!;

        const chunk = new EncodedVideoChunk({
          type,
          timestamp,
          data: result.payload,
        });

        videoDecoder.decode(chunk);
      }
    }
  } catch (e) {
    log(`Video group error: ${e}`);
  }
}

async function processAudioGroup(group: SubgroupReader) {
  try {
    while (true) {
      const result = await group.readObject();
      if (result === null) break;

      if (audioDecoder && audioDecoder.state === "configured") {
        const rawTimestamp = extractTimestamp(result.properties) ?? performance.now() * 1000;
        initSyncBase(rawTimestamp);
        const timestamp = rawTimestamp - captureTimeBase!;

        const chunk = new EncodedAudioChunk({
          type: "key",
          timestamp,
          data: result.payload,
        });

        audioDecoder.decode(chunk);
      }
    }
  } catch (e) {
    log(`Audio group error: ${e}`);
  }
}

async function stop() {
  if (videoDecoder) {
    await videoDecoder.flush();
    videoDecoder.close();
    videoDecoder = null;
  }
  if (audioDecoder) {
    await audioDecoder.flush();
    audioDecoder.close();
    audioDecoder = null;
  }
  if (audioCtx) {
    await audioCtx.close();
    audioCtx = null;
  }
  captureTimeBase = null;
  playBase = null;
  nextAudioPlayTime = null;

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
