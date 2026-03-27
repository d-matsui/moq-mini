// MOQT Late Join Demo
//
// Compares time-to-first-frame between:
// 1. NextGroupStart: waits for the next group boundary
// 2. LargestObject + Joining FETCH: gets cached objects immediately

import {
  MoqtSession,
  SubgroupReader,
  FetchStreamReader,
  type FetchedObject,
} from "../lib/session.js";
import {
  subscriptionFilterNextGroupStart,
  subscriptionFilterLargestObject,
  extractLargestObject,
} from "../lib/wire/parameter.js";

const $ = (id: string) => document.getElementById(id)!;

let session: MoqtSession | null = null;
let decoder: VideoDecoder | null = null;
let canvas: HTMLCanvasElement;
let ctx: CanvasRenderingContext2D;
let firstFrameRendered = false;
let joinStart = 0;

function setupDecoder(): void {
  decoder = new VideoDecoder({
    output: (frame) => {
      if (!firstFrameRendered) {
        firstFrameRendered = true;
        const elapsed = performance.now() - joinStart;
        showTiming(elapsed);
      }
      if (canvas.width !== frame.displayWidth || canvas.height !== frame.displayHeight) {
        canvas.width = frame.displayWidth;
        canvas.height = frame.displayHeight;
      }
      ctx.drawImage(frame, 0, 0);
      frame.close();
    },
    error: (e) => log(`Decoder error: ${e.message}`),
  });
  decoder.configure({ codec: "vp8" });
}

function showTiming(ms: number): void {
  const el = $("timing");
  el.style.display = "block";
  $("timing-value").textContent = `${ms.toFixed(1)} ms`;
  log(`*** Time to first frame: ${ms.toFixed(1)} ms ***`);
}

// === NextGroupStart mode ===
async function joinNextGroupStart(): Promise<void> {
  await cleanup();

  const url = ($("relay-url") as HTMLInputElement).value;
  const namespace = ($("namespace") as HTMLInputElement).value.split("/").filter(s => s.length > 0);
  const trackName = ($("track-name") as HTMLInputElement).value;

  canvas = $("canvas") as HTMLCanvasElement;
  ctx = canvas.getContext("2d")!;
  firstFrameRendered = false;

  log("--- NextGroupStart mode ---");
  log("Connecting...");
  session = await MoqtSession.connect(url);
  log("Connected.");

  setupDecoder();

  log("Subscribing (NextGroupStart)...");
  joinStart = performance.now();
  const sub = await session.subscribe(namespace, trackName, [subscriptionFilterNextGroupStart()]);
  log(`Subscribed (alias=${sub.trackAlias}). Waiting for first group...`);

  setButtons(false);
  receiveSubgroupLoop();
}

// === LargestObject + FETCH mode ===
async function joinWithFetch(): Promise<void> {
  await cleanup();

  const url = ($("relay-url") as HTMLInputElement).value;
  const namespace = ($("namespace") as HTMLInputElement).value.split("/").filter(s => s.length > 0);
  const trackName = ($("track-name") as HTMLInputElement).value;
  const fetchGroupsBack = parseInt(($("fetch-groups") as HTMLInputElement).value) || 3;

  canvas = $("canvas") as HTMLCanvasElement;
  ctx = canvas.getContext("2d")!;
  firstFrameRendered = false;

  log("--- LargestObject + FETCH mode ---");
  log("Connecting...");
  session = await MoqtSession.connect(url);
  log("Connected.");

  setupDecoder();

  log("Subscribing (LargestObject)...");
  joinStart = performance.now();
  const sub = await session.subscribe(namespace, trackName, [subscriptionFilterLargestObject()]);

  const largest = extractLargestObject(sub.subscribeOk.parameters);
  if (largest) {
    log(`Subscribed. LARGEST_OBJECT: group=${largest.group}, object=${largest.object}`);
  } else {
    log("Subscribed. No LARGEST_OBJECT (track may be empty). Falling back to live.");
    setButtons(false);
    receiveSubgroupLoop();
    return;
  }

  // Send FETCH to get cached objects
  log(`Sending FETCH (joiningStart=${fetchGroupsBack}, joiningRequestId=${sub.requestId})...`);
  try {
    const fetchOk = await session.fetch(sub.requestId, fetchGroupsBack);
    log(`FETCH_OK: endGroup=${fetchOk.endGroup}, endObject=${fetchOk.endObject}`);

    // Read fetched objects from FETCH_HEADER stream
    const fetchReader = await session.acceptFetchStream();
    log("Reading fetched objects...");

    let currentGroupId = -1;
    let isFirstInGroup = true;
    let fetchCount = 0;

    while (true) {
      const obj = await fetchReader.readObject();
      if (obj === null) break;

      if (obj.groupId !== currentGroupId) {
        currentGroupId = obj.groupId;
        isFirstInGroup = true;
      }

      if (decoder && decoder.state === "configured") {
        const type = isFirstInGroup ? "key" : "delta";
        isFirstInGroup = false;
        const chunk = new EncodedVideoChunk({
          type,
          timestamp: performance.now() * 1000,
          data: obj.payload,
        });
        decoder.decode(chunk);
      }
      fetchCount++;
    }
    log(`Fetched ${fetchCount} objects from cache.`);
  } catch (e) {
    log(`FETCH error: ${e}`);
  }

  // Continue receiving live objects via SUBSCRIBE
  log("Switching to live stream...");
  setButtons(false);
  receiveSubgroupLoop();
}

async function receiveSubgroupLoop(): Promise<void> {
  if (!session || !decoder) return;
  try {
    while (session) {
      const event = await session.nextEvent();
      if (event.type !== "dataStream") continue;
      processGroup(event.reader);
    }
  } catch (e) {
    log(`Receive ended: ${e}`);
  }
}

async function processGroup(group: SubgroupReader): Promise<void> {
  let isFirstObject = true;
  let objectCount = 0;

  try {
    while (true) {
      const result = await group.readObject();
      if (result === null) break;

      if (decoder && decoder.state === "configured") {
        const type = isFirstObject ? "key" : "delta";
        isFirstObject = false;
        const chunk = new EncodedVideoChunk({
          type,
          timestamp: performance.now() * 1000,
          data: result.payload,
        });
        decoder.decode(chunk);
      }
      objectCount++;
    }
  } catch (e) {
    // stream ended
  }
}

async function cleanup(): Promise<void> {
  if (decoder) {
    try { await decoder.flush(); } catch {}
    try { decoder.close(); } catch {}
    decoder = null;
  }
  if (session) {
    session.close();
    session = null;
  }
  $("timing").style.display = "none";
}

function setButtons(enabled: boolean): void {
  ($("btn-next-group") as HTMLButtonElement).disabled = !enabled;
  ($("btn-fetch") as HTMLButtonElement).disabled = !enabled;
  ($("stop-btn") as HTMLButtonElement).disabled = enabled;
}

async function stop(): Promise<void> {
  await cleanup();
  setButtons(true);
  log("Stopped.");
}

function log(msg: string): void {
  const el = $("log");
  el.textContent += msg + "\n";
  el.scrollTop = el.scrollHeight;
  console.log(msg);
}

document.addEventListener("DOMContentLoaded", () => {
  $("btn-next-group").addEventListener("click", () =>
    joinNextGroupStart().catch(e => log(`ERROR: ${e}`))
  );
  $("btn-fetch").addEventListener("click", () =>
    joinWithFetch().catch(e => log(`ERROR: ${e}`))
  );
  $("stop-btn").addEventListener("click", () =>
    stop().catch(e => log(`ERROR: ${e}`))
  );
});
