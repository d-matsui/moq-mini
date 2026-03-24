// MSF Browser Subscriber
// Subscribes to catalog to discover tracks, then subscribes to
// H.264 video track and decodes with WebCodecs.

import { MoqtSession, SubgroupReader } from "./session.js";

const $ = (id: string) => document.getElementById(id)!;

const CATALOG_TRACK_NAME = "catalog";

let session: MoqtSession | null = null;
let decoder: VideoDecoder | null = null;
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
  initData?: string;
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

  const catalogPayload = await catalogEvent.reader.readObject();
  if (!catalogPayload) {
    log("ERROR: catalog stream empty");
    return;
  }

  const catalogJson = new TextDecoder().decode(catalogPayload);
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

  log(`Found: ${videoTrack.name} (codec=${videoTrack.codec})`);

  // Step 4: Set up decoder
  decoder = new VideoDecoder({
    output: (frame) => {
      if (
        canvas.width !== frame.displayWidth ||
        canvas.height !== frame.displayHeight
      ) {
        canvas.width = frame.displayWidth;
        canvas.height = frame.displayHeight;
      }
      ctx.drawImage(frame, 0, 0);
      frame.close();
    },
    error: (e) => log(`Decoder error: ${e.message}`),
  });

  // Configure decoder with codec from catalog
  const codec = videoTrack.codec || "avc1.42001e";
  decoder.configure({ codec });
  log(`Decoder configured: ${codec}`);

  // Step 5: Subscribe to video track
  log(`Subscribing to ${videoTrack.name}...`);
  const videoSub = await session.subscribe(namespace, videoTrack.name);
  log(`Video subscribed (alias=${videoSub.trackAlias}).`);

  ($("start-btn") as HTMLButtonElement).disabled = true;
  ($("stop-btn") as HTMLButtonElement).disabled = false;

  // Step 6: Receive video
  const videoAlias = videoSub.trackAlias;
  receiveLoop(videoAlias);
}

async function receiveLoop(videoAlias: number) {
  if (!session || !decoder) return;

  try {
    while (session) {
      const event = await session.nextEvent();
      if (event.type !== "dataStream") continue;

      // Skip non-video streams (e.g. catalog updates)
      if (event.reader.trackAlias !== videoAlias) continue;

      processGroup(event.reader);
    }
  } catch (e) {
    log(`Receive ended: ${e}`);
  }
}

async function processGroup(group: SubgroupReader) {
  let isFirstObject = true;
  let objectCount = 0;

  try {
    while (true) {
      const payload = await group.readObject();
      if (payload === null) break;

      if (decoder && decoder.state === "configured") {
        const type = isFirstObject ? "key" : "delta";
        isFirstObject = false;

        const chunk = new EncodedVideoChunk({
          type,
          timestamp: performance.now() * 1000,
          data: payload,
        });

        decoder.decode(chunk);
      }
      objectCount++;
    }
  } catch (e) {
    log(`Group error: ${e}`);
  }
}

async function stop() {
  if (decoder) {
    await decoder.flush();
    decoder.close();
    decoder = null;
  }
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
