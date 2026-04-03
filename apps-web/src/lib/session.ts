// MoqtSession: high-level MOQT session API over WebTransport.
//
// Handles SETUP exchange and provides methods for:
// - publishNamespace()
// - subscribe()
// - waitForSubscribe()
// - openSubgroup()
// - nextDataStream()

import { StreamReader } from "./stream/stream-reader.js";
import { encodeSetup, decodeSetup, clientSetup } from "./wire/setup.js";
import {
  encodePublishNamespace,
  decodePublishNamespace,
  type PublishNamespaceMessage,
} from "./wire/publish-namespace.js";
import { encodeSubscribe, decodeSubscribe, type SubscribeMessage } from "./wire/subscribe.js";
import {
  encodeSubscribeOk,
  decodeSubscribeOk,
  type SubscribeOkMessage,
} from "./wire/subscribe-ok.js";
import { encodeRequestOk, decodeRequestOk } from "./wire/request-ok.js";
import { encodePublishDone, decodePublishDone, type PublishDoneMessage } from "./wire/publish-done.js";
import { encodeRequestError, ERROR_DOES_NOT_EXIST, ERROR_UNINTERESTED } from "./wire/request-error.js";
import { decodeMessage } from "./wire/message.js";
import {
  MSG_SUBSCRIBE,
  MSG_SUBSCRIBE_OK,
  MSG_REQUEST_OK,
  MSG_REQUEST_ERROR,
  MSG_PUBLISH_DONE,
  MSG_PUBLISH_NAMESPACE,
  MSG_SUBSCRIBE_NAMESPACE,
  MSG_FETCH_OK,
} from "./wire/message.js";
import {
  encodeSubgroupHeader,
  decodeSubgroupHeader,
  type SubgroupHeader,
} from "./wire/subgroup-header.js";
import { encodeObjectHeader, decodeObjectHeader } from "./wire/object.js";
import { trackNamespaceFrom, type TrackNamespace } from "./wire/track-namespace.js";
import {
  subscriptionFilterNextGroupStart,
  extractLargestObject,
  type MessageParameter,
} from "./wire/parameter.js";
import { encodeFetch, FETCH_TYPE_RELATIVE_JOINING, type FetchMessage } from "./wire/fetch.js";
import { decodeFetchOk, type FetchOkMessage } from "./wire/fetch-ok.js";
import { encodeVarint, decodeVarint } from "./wire/varint.js";
import { decodeTrackNamespace } from "./wire/track-namespace.js";
import { encodeNamespace } from "./wire/namespace.js";

/** Format a TrackNamespace as a human-readable string, e.g. ["anon", "example"]. */
function formatNs(ns: TrackNamespace): string {
  const decoder = new TextDecoder();
  return "[" + ns.fields.map(f => `"${decoder.decode(f)}"`).join(", ") + "]";
}

let openStreams = 0;

/** A writable subgroup (data stream). */
export class SubgroupWriter {
  private writer: WritableStreamDefaultWriter<Uint8Array>;
  private hasProperties: boolean;

  constructor(writer: WritableStreamDefaultWriter<Uint8Array>, hasProperties: boolean) {
    this.writer = writer;
    this.hasProperties = hasProperties;
    openStreams++;
    console.log(`[stream] opened (${openStreams} open)`);
  }

  /** Write an object payload. ObjectHeader is generated internally. */
  async writeObject(payload: Uint8Array, properties?: Uint8Array): Promise<void> {
    const header = encodeObjectHeader({
      objectIdDelta: 0,
      payloadLength: payload.length,
    });

    if (this.hasProperties) {
      const propsData = properties ?? new Uint8Array(0);
      const propsLen = encodeVarint(propsData.length);
      const buf = new Uint8Array(header.length + propsLen.length + propsData.length + payload.length);
      let offset = 0;
      buf.set(header, offset); offset += header.length;
      buf.set(propsLen, offset); offset += propsLen.length;
      buf.set(propsData, offset); offset += propsData.length;
      buf.set(payload, offset);
      await this.writer.write(buf);
    } else {
      const buf = new Uint8Array(header.length + payload.length);
      buf.set(header, 0);
      buf.set(payload, header.length);
      await this.writer.write(buf);
    }
  }

  /** Finish the stream (send FIN). */
  async finish(): Promise<void> {
    await this.writer.close();
    openStreams--;
    console.log(`[stream] closed (${openStreams} open)`);
  }
}

/** A readable subgroup (data stream). */
export class SubgroupReader {
  readonly header: SubgroupHeader;
  private reader: StreamReader;

  constructor(header: SubgroupHeader, reader: StreamReader) {
    this.header = header;
    this.reader = reader;
  }

  get trackAlias(): number {
    return this.header.trackAlias;
  }

  get groupId(): number {
    return this.header.groupId;
  }

  /** Cancel the stream (sends STOP_SENDING). */
  async cancel(): Promise<void> {
    await this.reader.cancel();
  }

  /** Read the next object. Returns null on stream end.
   *  If the subgroup has properties, returns { payload, properties }.
   *  Otherwise returns { payload, properties: null }.
   */
  async readObject(): Promise<{ payload: Uint8Array; properties: Uint8Array | null } | null> {
    const result = await this.reader.tryReadVarint();
    if (result === null) return null;

    const [_objectIdDelta, _deltaBytes] = result;
    const [payloadLength, _lenBytes] = await this.reader.readVarint();

    let properties: Uint8Array | null = null;
    if (this.header.hasProperties) {
      const [propsLen, _propsLenBytes] = await this.reader.readVarint();
      if (propsLen > 0) {
        properties = await this.reader.readExact(propsLen);
      }
    }

    const payload = payloadLength === 0
      ? new Uint8Array(0)
      : await this.reader.readExact(payloadLength);

    return { payload, properties };
  }
}

/** An incoming SUBSCRIBE request (publisher side). */
export class SubscribeRequest {
  readonly message: SubscribeMessage;
  private sendWriter: WritableStreamDefaultWriter<Uint8Array>;

  constructor(
    message: SubscribeMessage,
    sendWriter: WritableStreamDefaultWriter<Uint8Array>
  ) {
    this.message = message;
    this.sendWriter = sendWriter;
  }

  /** Accept the subscription with the given track alias. */
  async accept(trackAlias: number): Promise<void> {
    const frame = encodeSubscribeOk({
      trackAlias,
      parameters: [],
      trackPropertiesRaw: new Uint8Array(0),
    });
    await this.sendWriter.write(frame);
  }

  /** Send PUBLISH_DONE to signal end of publishing. */
  async sendPublishDone(streamCount: number): Promise<void> {
    const frame = encodePublishDone({
      statusCode: 0,
      streamCount,
      reasonPhrase: "",
    });
    await this.sendWriter.write(frame);
  }
}

/** An established subscription (subscriber side). */
export class Subscription {
  readonly trackAlias: number;
  /** The full SUBSCRIBE_OK message (for extracting LARGEST_OBJECT etc.). */
  readonly subscribeOk: SubscribeOkMessage;
  /** The SUBSCRIBE request ID used for this subscription. */
  readonly requestId: number;
  private recvReader: StreamReader;

  constructor(requestId: number, ok: SubscribeOkMessage, recvReader: StreamReader) {
    this.requestId = requestId;
    this.trackAlias = ok.trackAlias;
    this.subscribeOk = ok;
    this.recvReader = recvReader;
  }

  /** Wait for PUBLISH_DONE from the publisher. */
  async recvPublishDone(): Promise<PublishDoneMessage> {
    const frame = await this.recvReader.readMessageFrame();
    return decodePublishDone(frame);
  }
}

/** A fetched object from a FETCH_HEADER stream. */
export interface FetchedObject {
  groupId: number;
  objectId: number;
  subgroupId: number;
  payload: Uint8Array;
}

/** Reader for a FETCH_HEADER unidirectional stream. */
export class FetchStreamReader {
  private reader: StreamReader;
  readonly requestId: number;
  private prevGroup = 0;
  private prevSubgroup = 0;
  private prevObject = 0;
  private prevPriority = 0;

  constructor(reader: StreamReader, requestId: number) {
    this.reader = reader;
    this.requestId = requestId;
  }

  /** Read the next fetched object. Returns null on stream end. */
  async readObject(): Promise<FetchedObject | null> {
    const result = await this.reader.tryReadVarint();
    if (result === null) return null;
    const [flags] = result;

    const hasGroupId = (flags & 0x08) !== 0;
    const hasObjectId = (flags & 0x04) !== 0;
    const hasPriority = (flags & 0x10) !== 0;
    const subgroupMode = flags & 0x03;

    const groupId = hasGroupId
      ? (await this.reader.readVarint())[0]
      : this.prevGroup;

    let subgroupId: number;
    if (subgroupMode === 0x03) {
      subgroupId = (await this.reader.readVarint())[0];
    } else if (subgroupMode === 0x00) {
      subgroupId = 0;
    } else if (subgroupMode === 0x01) {
      subgroupId = this.prevSubgroup;
    } else {
      subgroupId = this.prevSubgroup + 1;
    }

    const objectId = hasObjectId
      ? (await this.reader.readVarint())[0]
      : this.prevObject + 1;

    if (hasPriority) {
      const buf = await this.reader.readExact(1);
      this.prevPriority = buf[0];
    }

    // Properties (flag 0x20) - skip if present
    if ((flags & 0x20) !== 0) {
      const [propsLen] = await this.reader.readVarint();
      if (propsLen > 0) await this.reader.readExact(propsLen);
    }

    const [payloadLen] = await this.reader.readVarint();
    const payload = payloadLen > 0
      ? await this.reader.readExact(payloadLen)
      : new Uint8Array(0);

    this.prevGroup = groupId;
    this.prevSubgroup = subgroupId;
    this.prevObject = objectId;

    return { groupId, objectId, subgroupId, payload };
  }
}

export type SessionEvent =
  | { type: "subscribe"; request: SubscribeRequest }
  | { type: "dataStream"; reader: SubgroupReader };

/** A MOQT session over WebTransport. */
export class MoqtSession {
  private transport: WebTransport;
  private nextRequestId = 0;
  private publishedNamespaces: {
    ns: TrackNamespace;
    writer: WritableStreamDefaultWriter<Uint8Array>;
    reader: StreamReader;
  }[] = [];

  private constructor(transport: WebTransport) {
    this.transport = transport;
  }

  /** Connect to a relay and perform SETUP exchange.
   * @param certHash - SHA-256 hash of the server certificate (hex with colons, e.g. "ab:cd:...")
   *                   Required for self-signed certificates.
   */
  static async connect(url: string): Promise<MoqtSession> {
    console.log(`[session] connecting to ${url}...`);
    const transport = new WebTransport(url, { protocols: ['moqt-17'] });

    transport.closed.then(
      (info) => console.log("[session] transport closed:", info),
      (err) => console.error("[session] transport closed with error:", err),
    );

    console.log("[session] waiting for transport.ready...");
    await transport.ready;
    console.log("[session] transport ready");

    const session = new MoqtSession(transport);
    await session.setupExchange();
    console.log("[session] SETUP exchange complete");
    return session;
  }

  private async setupExchange(): Promise<void> {
    // Send SETUP on a new unidirectional stream
    console.log("[setup] creating unidirectional stream...");
    const sendStream = await this.transport.createUnidirectionalStream();
    const writer = sendStream.getWriter();
    // WebTransport: PATH/AUTHORITY are in the HTTP/3 CONNECT request, not SETUP
    const setupMsg = clientSetup();
    const encoded = encodeSetup(setupMsg);
    console.log("[setup] sending SETUP:", Array.from(encoded).map(b => b.toString(16).padStart(2, '0')).join(' '));
    await writer.write(encoded);
    console.log("[setup] SETUP sent");
    // Don't close the control stream (must stay open per spec)

    // Read relay's SETUP from an incoming unidirectional stream
    console.log("[setup] waiting for incoming unidirectional stream...");
    const uniReader = this.transport.incomingUnidirectionalStreams.getReader();
    const { value: recvStream } = await uniReader.read();
    uniReader.releaseLock();
    if (!recvStream) throw new Error("no incoming control stream");

    console.log("[setup] received incoming stream, reading SETUP...");
    const reader = new StreamReader(recvStream);
    const frame = await reader.readMessageFrame();
    console.log("[setup] server SETUP frame:", Array.from(frame).map(b => b.toString(16).padStart(2, '0')).join(' '));
    const _serverSetup = decodeSetup(frame);
  }

  private allocateRequestId(): number {
    const id = this.nextRequestId;
    this.nextRequestId += 2; // even IDs for client
    return id;
  }

  /** Register a namespace with the relay. */
  async publishNamespace(namespace: string[]): Promise<void> {
    const bidi = await this.transport.createBidirectionalStream();
    const writer = bidi.writable.getWriter();
    const reader = new StreamReader(bidi.readable);

    const msg: PublishNamespaceMessage = {
      requestId: this.allocateRequestId(),
      requiredRequestIdDelta: 0,
      trackNamespace: trackNamespaceFrom(namespace),
      parameters: [],
    };
    await writer.write(encodePublishNamespace(msg));

    const frame = await reader.readMessageFrame();
    const { msgType } = decodeMessage(frame, 0);
    if (msgType === MSG_REQUEST_ERROR) {
      throw new Error("PUBLISH_NAMESPACE rejected");
    }
    if (msgType !== MSG_REQUEST_OK) {
      throw new Error(`unexpected response: 0x${msgType.toString(16)}`);
    }
    this.publishedNamespaces.push({
      ns: trackNamespaceFrom(namespace),
      writer,
      reader,
    });
  }

  /** Subscribe to a track. */
  async subscribe(
    namespace: string[],
    trackName: string,
    params: MessageParameter[] = [subscriptionFilterNextGroupStart()]
  ): Promise<Subscription> {
    const bidi = await this.transport.createBidirectionalStream();
    const writer = bidi.writable.getWriter();
    const reader = new StreamReader(bidi.readable);

    const msg: SubscribeMessage = {
      requestId: this.allocateRequestId(),
      requiredRequestIdDelta: 0,
      trackNamespace: trackNamespaceFrom(namespace),
      trackName: new TextEncoder().encode(trackName),
      parameters: params,
    };
    const encoded = encodeSubscribe(msg);
    console.log(`[subscribe] sending SUBSCRIBE: reqId=${msg.requestId} ns=${formatNs(msg.trackNamespace)} track="${trackName}" raw=${Array.from(encoded).map(b => b.toString(16).padStart(2, '0')).join(' ')}`);
    await writer.write(encoded);
    console.log("[subscribe] SUBSCRIBE sent, waiting for response...");

    const frame = await reader.readMessageFrame();
    console.log(`[subscribe] response: ${Array.from(frame).map(b => b.toString(16).padStart(2, '0')).join(' ')}`);
    const { msgType } = decodeMessage(frame, 0);
    if (msgType === MSG_REQUEST_ERROR) {
      console.log("[subscribe] SUBSCRIBE rejected with REQUEST_ERROR");
      throw new Error("SUBSCRIBE rejected");
    }
    if (msgType !== MSG_SUBSCRIBE_OK) {
      throw new Error(`unexpected response: 0x${msgType.toString(16)}`);
    }
    const ok = decodeSubscribeOk(frame);
    return new Subscription(msg.requestId, ok, reader);
  }

  /** Send a Relative Joining FETCH and wait for FETCH_OK. */
  async fetch(
    joiningRequestId: number,
    joiningStart: number,
  ): Promise<FetchOkMessage> {
    const bidi = await this.transport.createBidirectionalStream();
    const writer = bidi.writable.getWriter();
    const reader = new StreamReader(bidi.readable);

    const msg: FetchMessage = {
      requestId: this.allocateRequestId(),
      requiredRequestIdDelta: 0,
      fetchType: FETCH_TYPE_RELATIVE_JOINING,
      joiningRequestId,
      joiningStart,
      parameters: [],
    };
    console.log(`[fetch] sending FETCH: reqId=${msg.requestId} joiningReqId=${joiningRequestId} joiningStart=${joiningStart}`);
    await writer.write(encodeFetch(msg));

    const frame = await reader.readMessageFrame();
    const { msgType } = decodeMessage(frame, 0);
    if (msgType === MSG_REQUEST_ERROR) {
      throw new Error("FETCH rejected");
    }
    if (msgType !== MSG_FETCH_OK) {
      throw new Error(`unexpected FETCH response: 0x${msgType.toString(16)}`);
    }
    const ok = decodeFetchOk(frame);
    console.log(`[fetch] FETCH_OK: endGroup=${ok.endGroup} endObject=${ok.endObject} endOfTrack=${ok.endOfTrack}`);
    return ok;
  }

  /** Accept a FETCH_HEADER unidirectional stream and return a reader.
   *  Call this after fetch() to read the fetched objects.
   */
  async acceptFetchStream(): Promise<FetchStreamReader> {
    const uniReader = this.transport.incomingUnidirectionalStreams.getReader();
    const { value: recvStream } = await uniReader.read();
    uniReader.releaseLock();
    if (!recvStream) throw new Error("no incoming FETCH stream");

    const reader = new StreamReader(recvStream);
    // Read FETCH_HEADER: Type (0x05) + Request ID
    const [headerType] = await reader.readVarint();
    if (headerType !== 0x05) {
      throw new Error(`expected FETCH_HEADER (0x05), got 0x${headerType.toString(16)}`);
    }
    const [requestId] = await reader.readVarint();
    console.log(`[fetch] FETCH_HEADER stream: requestId=${requestId}`);
    return new FetchStreamReader(reader, requestId);
  }

  /** Wait for the next incoming event (SUBSCRIBE or data stream). */
  async nextEvent(): Promise<SessionEvent> {
    // Race between incoming bidi and uni streams
    const bidiReader = this.transport.incomingBidirectionalStreams.getReader();
    const uniReader = this.transport.incomingUnidirectionalStreams.getReader();

    try {
      const result = await Promise.race([
        bidiReader.read().then((r) => ({ kind: "bidi" as const, ...r })),
        uniReader.read().then((r) => ({ kind: "uni" as const, ...r })),
      ]);

      if (result.kind === "bidi" && result.value) {
        bidiReader.releaseLock();
        uniReader.releaseLock();

        const stream = result.value;
        const reader = new StreamReader(stream.readable);
        const frame = await reader.readMessageFrame();
        const { msgType } = decodeMessage(frame, 0);

        if (msgType === MSG_SUBSCRIBE) {
          const sub = decodeSubscribe(frame);
          const writer = stream.writable.getWriter();
          return { type: "subscribe", request: new SubscribeRequest(sub, writer) };
        }
        if (msgType === MSG_PUBLISH_NAMESPACE) {
          const pubNs = decodePublishNamespace(frame);
          const writer = stream.writable.getWriter();
          await writer.write(encodeRequestError({
            errorCode: ERROR_UNINTERESTED,
            retryInterval: 0,
            reasonPhrase: "",
          }));
          console.log(`[session] received PUBLISH_NAMESPACE: reqId=${pubNs.requestId} ns=${formatNs(pubNs.trackNamespace)}, sent UNINTERESTED`);
          return this.nextEvent();
        }
        if (msgType === MSG_SUBSCRIBE_NAMESPACE) {
          const { payload } = decodeMessage(frame, 0);
          let pos = 0;
          const { value: reqId, bytesRead: r1 } = decodeVarint(payload, pos); pos += r1;
          const { value: reqIdDelta, bytesRead: r2 } = decodeVarint(payload, pos); pos += r2;
          const { ns: nsPrefix, bytesRead: r3 } = decodeTrackNamespace(payload, pos); pos += r3;
          const { value: subOpts, bytesRead: r4 } = decodeVarint(payload, pos); pos += r4;

          const writer = stream.writable.getWriter();
          await writer.write(encodeRequestOk({ parameters: [] }));
          console.log(`[session] received SUBSCRIBE_NAMESPACE: reqId=${reqId} prefix=${formatNs(nsPrefix)} opts=${subOpts}, sent REQUEST_OK`);
          return this.nextEvent();
        }
        console.warn(`[session] unexpected bidi message: type=0x${msgType.toString(16)}, raw=${Array.from(frame).map(b => b.toString(16).padStart(2, '0')).join(' ')}`);
        throw new Error(`unexpected bidi message: 0x${msgType.toString(16)}`);
      }

      if (result.kind === "uni" && result.value) {
        bidiReader.releaseLock();
        uniReader.releaseLock();

        const reader = new StreamReader(result.value);
        // Read SubgroupHeader
        const [streamType, typeBytes] = await reader.readVarint();
        const [trackAlias, aliasBytes] = await reader.readVarint();
        const [groupId, groupBytes] = await reader.readVarint();

        // Reconstruct raw bytes for decoding
        const raw: number[] = [...typeBytes, ...aliasBytes, ...groupBytes];

        // Optional Subgroup ID
        const subgroupIdMode = (streamType >> 1) & 0x03;
        if (subgroupIdMode === 0x02) {
          const [_sid, sidBytes] = await reader.readVarint();
          raw.push(...sidBytes);
        }

        // Optional Publisher Priority
        if ((streamType & 0x20) === 0) {
          const priorityByte = await reader.readExact(1);
          raw.push(priorityByte[0]);
        }

        const { header } = decodeSubgroupHeader(new Uint8Array(raw), 0);
        return { type: "dataStream", reader: new SubgroupReader(header, reader) };
      }

      throw new Error("no incoming stream");
    } finally {
      // Ensure locks are released if not already
      try { bidiReader.releaseLock(); } catch {}
      try { uniReader.releaseLock(); } catch {}
    }
  }

  /** Open a subgroup for writing objects. */
  async openSubgroup(
    trackAlias: number,
    groupId: number,
    subgroupId: number,
    hasProperties = false,
  ): Promise<SubgroupWriter> {
    const stream = await this.transport.createUnidirectionalStream();
    const writer = stream.getWriter();

    const header = encodeSubgroupHeader({
      trackAlias,
      groupId,
      hasProperties,
      endOfGroup: true,
      subgroupId,
      publisherPriority: null,
    });
    await writer.write(header);

    return new SubgroupWriter(writer, hasProperties);
  }

  /** Close the session. */
  close(): void {
    this.transport.close();
  }
}
