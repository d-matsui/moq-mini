// MSF Browser Publisher
// Captures video via getUserMedia, encodes with WebCodecs H.264,
// publishes a catalog track, then streams video as MOQT objects
// with LOC Header Extensions (Video Config).

import { MoqtSession, SubgroupWriter } from "./session.js";

const $ = (id: string) => document.getElementById(id)!;

const CATALOG_TRACK_NAME = "catalog";
const VIDEO_TRACK_NAME = "video";
const KEYFRAME_INTERVAL = 60; // ~2s at 30fps

let session: MoqtSession | null = null;
let encoder: VideoEncoder | null = null;
let currentGroup: SubgroupWriter | null = null;
let groupId = 0;
let streamCount = 0;
let groupStarted = false;
let frameCount = 0;
let mediaStream: MediaStream | null = null;
let pendingVideoConfig: Uint8Array | null = null;

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
    ],
  };

  // Wait for SUBSCRIBEs (catalog and video)
  log("Waiting for SUBSCRIBEs...");

  let catalogDone = false;
  let videoDone = false;

  while (!catalogDone || !videoDone) {
    const event = await session.nextEvent();
    if (event.type !== "subscribe") continue;

    const trackName = new TextDecoder().decode(event.request.message.trackName);
    log(`Received SUBSCRIBE for: ${trackName}`);

    if (trackName === CATALOG_TRACK_NAME) {
      await event.request.accept(1); // catalog alias = 1
      await publishCatalog(session, 1, catalog);
      catalogDone = true;
    } else if (trackName === VIDEO_TRACK_NAME) {
      await event.request.accept(2); // video alias = 2
      videoDone = true;
    } else {
      log(`Unknown track: ${trackName}, ignoring`);
    }
  }

  log("Both SUBSCRIBEs received. Starting capture...");

  // Start capture
  mediaStream = await navigator.mediaDevices.getUserMedia({
    video: { width: 640, height: 480, frameRate: 30 },
    audio: false,
  });

  const videoTrack = mediaStream.getVideoTracks()[0];
  const settings = videoTrack.getSettings();
  const preview = $("preview") as HTMLVideoElement;
  preview.srcObject = mediaStream;

  // Set up WebCodecs VideoEncoder (H.264)
  encoder = new VideoEncoder({
    output: async (chunk, metadata) => {
      await handleEncodedChunk(chunk, metadata ?? undefined);
    },
    error: (e) => log(`Encoder error: ${e.message}`),
  });

  encoder.configure({
    codec: "avc1.42001e", // H.264 Baseline Level 3.0
    width: settings.width || 640,
    height: settings.height || 480,
    framerate: 30,
    bitrate: 1_000_000,
    avc: { format: "annexb" },
  });

  // Read frames from video track
  const processor = new MediaStreamTrackProcessor({ track: videoTrack });
  const frameReader = processor.readable.getReader();

  readFrames(frameReader);
  ($("start-btn") as HTMLButtonElement).disabled = true;
  ($("stop-btn") as HTMLButtonElement).disabled = false;
  log("Publishing...");
}

async function readFrames(
  reader: ReadableStreamDefaultReader<VideoFrame>
) {
  while (true) {
    const { value: frame, done } = await reader.read();
    if (done || !encoder) break;
    if (encoder.encodeQueueSize > 3) {
      frame.close();
      continue;
    }
    const requestKeyframe = frameCount % KEYFRAME_INTERVAL === 0;
    encoder.encode(frame, { keyFrame: requestKeyframe });
    frameCount++;
    frame.close();
  }
}

async function handleEncodedChunk(
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
  if (chunk.type === "key" && groupStarted) {
    if (currentGroup) {
      currentGroup.finish().catch(() => {});
      streamCount++;
    }
    groupId++;
  }

  // Open new subgroup if needed
  if (!currentGroup || (chunk.type === "key" && groupStarted)) {
    currentGroup = await session.openSubgroup(2, groupId, 0);
    groupStarted = true;
  }

  // Write frame as one MOQT object
  const data = new Uint8Array(chunk.byteLength);
  chunk.copyTo(data);
  await currentGroup.writeObject(data);
}

async function stop() {
  if (encoder) {
    await encoder.flush();
    encoder.close();
    encoder = null;
  }

  if (currentGroup) {
    await currentGroup.finish();
    streamCount++;
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
  log(`Stopped. Sent ${streamCount} groups.`);
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
