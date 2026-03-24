// MSF Browser Publisher
// Captures video and audio via getUserMedia, encodes with WebCodecs
// (H.264 for video, Opus for audio), publishes a catalog track,
// then streams media as MOQT objects.

import { MoqtSession, SubgroupWriter } from "./session.js";
import { encodeExtensions, type LocExtension } from "./loc.js";

const $ = (id: string) => document.getElementById(id)!;

const CATALOG_TRACK_NAME = "catalog";
const VIDEO_TRACK_NAME = "video";
const AUDIO_TRACK_NAME = "audio";
const KEYFRAME_INTERVAL = 60; // ~2s at 30fps
const AUDIO_GROUP_FRAMES = 25; // New audio group every ~25 Opus frames (~500ms at 20ms/frame)

let session: MoqtSession | null = null;
let videoEncoder: VideoEncoder | null = null;
let audioEncoder: AudioEncoder | null = null;
let currentVideoGroup: SubgroupWriter | null = null;
let currentAudioGroup: SubgroupWriter | null = null;
let videoGroupId = 0;
let audioGroupId = 0;
let videoStreamCount = 0;
let audioStreamCount = 0;
let videoGroupStarted = false;
let frameCount = 0;
let audioFrameCount = 0;
let mediaStream: MediaStream | null = null;
let pendingVideoConfig: Uint8Array | null = null;
let videoTrackAlias = 0;
let audioTrackAlias = 0;
let videoWriteQueue: Promise<void> = Promise.resolve();
let audioWriteQueue: Promise<void> = Promise.resolve();

// --- LOC helpers ---

/** Encode a CaptureTimestamp LOC extension as Object Properties bytes. */
function makeCaptureTimestamp(timestampUs: number): Uint8Array {
  const ext: LocExtension = { type: "captureTimestamp", value: timestampUs };
  return encodeExtensions([ext]);
}

// --- Catalog ---

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
}

async function publishCatalog(
  session: MoqtSession,
  trackAlias: number,
  catalog: MsfCatalog
): Promise<void> {
  const json = JSON.stringify(catalog);
  const data = new TextEncoder().encode(json);
  const group = await session.openSubgroup(trackAlias, 0, 0);
  await group.writeObject(data);
  // Keep stream open for future delta updates
  log(`Catalog published: ${catalog.tracks.length} track(s)`);
}

// --- Main ---

async function start() {
  const url = ($("relay-url") as HTMLInputElement).value;
  const namespaceRaw = ($("namespace") as HTMLInputElement).value;
  const namespace = namespaceRaw.split("/").filter((s) => s.length > 0);

  log("Connecting...");
  session = await MoqtSession.connect(url);
  log("Connected. SETUP complete.");

  log("Registering namespace...");
  await session.publishNamespace(namespace);
  log("PUBLISH_NAMESPACE registered.");

  // Build catalog
  const catalog: MsfCatalog = {
    version: 1,
    tracks: [
      {
        name: VIDEO_TRACK_NAME,
        packaging: "loc",
        isLive: true,
        role: "video",
        codec: "avc1.42001e", // H.264 Baseline Level 3.0
        width: 640,
        height: 480,
        framerate: 30,
        bitrate: 1_000_000,
      },
      {
        name: AUDIO_TRACK_NAME,
        packaging: "loc",
        isLive: true,
        role: "audio",
        codec: "opus",
        sampleRate: 48000,
        channelCount: 1,
        bitrate: 64_000,
      },
    ],
  };

  // Wait for SUBSCRIBEs (catalog, video, audio)
  log("Waiting for SUBSCRIBEs...");

  let catalogDone = false;
  let videoDone = false;
  let audioDone = false;

  while (!catalogDone || !videoDone || !audioDone) {
    const event = await session.nextEvent();
    if (event.type !== "subscribe") continue;

    const trackName = new TextDecoder().decode(event.request.message.trackName);
    log(`Received SUBSCRIBE for: ${trackName}`);

    if (trackName === CATALOG_TRACK_NAME) {
      await event.request.accept(1); // catalog alias = 1
      await publishCatalog(session, 1, catalog);
      catalogDone = true;
    } else if (trackName === VIDEO_TRACK_NAME) {
      videoTrackAlias = 2;
      await event.request.accept(videoTrackAlias);
      videoDone = true;
    } else if (trackName === AUDIO_TRACK_NAME) {
      audioTrackAlias = 3;
      await event.request.accept(audioTrackAlias);
      audioDone = true;
    } else {
      log(`Unknown track: ${trackName}, ignoring`);
    }
  }

  log("All SUBSCRIBEs received. Starting capture...");

  // Start capture
  mediaStream = await navigator.mediaDevices.getUserMedia({
    video: { width: 640, height: 480, frameRate: 30 },
    audio: { sampleRate: 48000, channelCount: 1 },
  });

  const videoTrack = mediaStream.getVideoTracks()[0];
  const settings = videoTrack.getSettings();
  const preview = $("preview") as HTMLVideoElement;
  preview.srcObject = mediaStream;

  // Set up WebCodecs VideoEncoder (H.264)
  videoEncoder = new VideoEncoder({
    output: (chunk, metadata) => {
      videoWriteQueue = videoWriteQueue.then(() =>
        handleEncodedVideoChunk(chunk, metadata ?? undefined)
      );
    },
    error: (e) => log(`Video encoder error: ${e.message}`),
  });

  videoEncoder.configure({
    codec: "avc1.42001e", // H.264 Baseline Level 3.0
    width: settings.width || 640,
    height: settings.height || 480,
    framerate: 30,
    bitrate: 1_000_000,
    latencyMode: "realtime",
    avc: { format: "annexb" },
  });

  // Read frames from video track
  const videoProcessor = new MediaStreamTrackProcessor({ track: videoTrack });
  const frameReader = videoProcessor.readable.getReader();
  readVideoFrames(frameReader);

  // Set up WebCodecs AudioEncoder (Opus)
  const audioTrack = mediaStream.getAudioTracks()[0];
  if (audioTrack) {
    audioEncoder = new AudioEncoder({
      output: (chunk) => {
        audioWriteQueue = audioWriteQueue.then(() =>
          handleEncodedAudioChunk(chunk)
        );
      },
      error: (e) => log(`Audio encoder error: ${e.message}`),
    });

    audioEncoder.configure({
      codec: "opus",
      sampleRate: 48000,
      numberOfChannels: 1,
      bitrate: 64_000,
    });

    const audioProcessor = new MediaStreamTrackProcessor({ track: audioTrack });
    const audioReader = audioProcessor.readable.getReader();
    readAudioFrames(audioReader);

    log("Audio encoder started.");
  } else {
    log("No audio track available.");
  }

  ($("start-btn") as HTMLButtonElement).disabled = true;
  ($("stop-btn") as HTMLButtonElement).disabled = false;
  log("Publishing...");
}

async function readVideoFrames(
  reader: ReadableStreamDefaultReader<VideoFrame>
) {
  while (true) {
    const { value: frame, done } = await reader.read();
    if (done || !videoEncoder) break;
    if (videoEncoder.encodeQueueSize > 3) {
      frame.close();
      continue;
    }
    const requestKeyframe = frameCount % KEYFRAME_INTERVAL === 0;
    videoEncoder.encode(frame, { keyFrame: requestKeyframe });
    frameCount++;
    frame.close();
  }
}

async function readAudioFrames(
  reader: ReadableStreamDefaultReader<AudioData>
) {
  while (true) {
    const { value: data, done } = await reader.read();
    if (done || !audioEncoder) break;
    if (audioEncoder.encodeQueueSize > 10) {
      data.close();
      continue;
    }
    audioEncoder.encode(data);
    data.close();
  }
}

async function handleEncodedVideoChunk(
  chunk: EncodedVideoChunk,
  metadata?: EncodedVideoChunkMetadata
) {
  if (!session) return;

  // Capture video config (H.264 description/extradata) from metadata
  if (metadata?.decoderConfig?.description) {
    const desc = metadata.decoderConfig.description;
    if (desc instanceof ArrayBuffer) {
      pendingVideoConfig = new Uint8Array(desc);
    } else if (desc instanceof Uint8Array) {
      pendingVideoConfig = desc;
    }
    log(`Got video config: ${pendingVideoConfig?.byteLength} bytes`);
  }

  // Keyframe starts a new group
  if (chunk.type === "key" && videoGroupStarted) {
    if (currentVideoGroup) {
      currentVideoGroup.finish().catch(() => {});
      videoStreamCount++;
    }
    videoGroupId++;
  }

  // Open new subgroup if needed
  if (!currentVideoGroup || (chunk.type === "key" && videoGroupStarted)) {
    currentVideoGroup = await session.openSubgroup(videoTrackAlias, videoGroupId, 0, true);
    videoGroupStarted = true;
  }

  // Write frame as one MOQT object with CaptureTimestamp
  const data = new Uint8Array(chunk.byteLength);
  chunk.copyTo(data);
  const props = makeCaptureTimestamp(chunk.timestamp);
  await currentVideoGroup.writeObject(data, props);
}

async function handleEncodedAudioChunk(chunk: EncodedAudioChunk) {
  if (!session) return;

  // Start new group every AUDIO_GROUP_FRAMES frames
  if (currentAudioGroup && audioFrameCount >= AUDIO_GROUP_FRAMES) {
    await currentAudioGroup.finish();
    audioStreamCount++;
    audioGroupId++;
    currentAudioGroup = null;
    audioFrameCount = 0;
  }

  // Open new subgroup if needed
  if (!currentAudioGroup) {
    currentAudioGroup = await session.openSubgroup(audioTrackAlias, audioGroupId, 0, true);
  }

  const data = new Uint8Array(chunk.byteLength);
  chunk.copyTo(data);
  const props = makeCaptureTimestamp(chunk.timestamp);
  await currentAudioGroup.writeObject(data, props);
  audioFrameCount++;
}

async function stop() {
  if (videoEncoder) {
    await videoEncoder.flush();
    videoEncoder.close();
    videoEncoder = null;
  }

  if (audioEncoder) {
    await audioEncoder.flush();
    audioEncoder.close();
    audioEncoder = null;
  }

  if (currentVideoGroup) {
    await currentVideoGroup.finish();
    videoStreamCount++;
  }

  if (currentAudioGroup) {
    await currentAudioGroup.finish();
    audioStreamCount++;
  }

  if (mediaStream) {
    mediaStream.getTracks().forEach((t) => t.stop());
    mediaStream = null;
  }

  if (session) {
    session.close();
    session = null;
  }

  ($("start-btn") as HTMLButtonElement).disabled = false;
  ($("stop-btn") as HTMLButtonElement).disabled = true;
  log(`Stopped. Sent ${videoStreamCount} video groups, ${audioStreamCount} audio groups.`);
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
