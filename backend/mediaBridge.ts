/** Audio-only mediasoup bridge between browser WebRTC and Asterisk ExternalMedia RTP.
 * Browsers negotiate PCMU with mediasoup; a DirectTransport forwards raw RTP on loopback. */
import { EventEmitter } from "node:events";
import dgram from "node:dgram";
import { randomUUID } from "node:crypto";
import * as mediasoup from "mediasoup";
import type {
  Consumer,
  DirectTransport,
  DtlsParameters,
  Producer,
  Router,
  RtpCapabilities,
  RtpParameters,
  WebRtcTransport,
  Worker
} from "mediasoup/types";
import type { AriClient } from "./ariClient.js";

interface MediaSession {
  callId: string;
  clientId: string;
  hostname: string;
  externalChannelId: string;
  socket: dgram.Socket;
  direct: DirectTransport;
  transports: Map<string, WebRtcTransport>;
  browserProducer?: Producer;
  browserToAsterisk?: Consumer;
  asteriskProducer?: Producer;
  browserConsumer?: Consumer;
  sendTransport?: WebRtcTransport;
  receiveTransport?: WebRtcTransport;
  browserCapabilities?: RtpCapabilities;
  asteriskAddress?: string;
  asteriskPort?: number;
  readyEmitted: boolean;
}

interface MediaEnvelope {
  requestId?: string;
  transportId?: string;
  direction?: "send" | "recv";
  dtlsParameters?: DtlsParameters;
  kind?: "audio";
  rtpParameters?: RtpParameters;
  rtpCapabilities?: RtpCapabilities;
  consumerId?: string;
}

const pcmuCodec = {
  kind: "audio" as const,
  mimeType: "audio/PCMU",
  preferredPayloadType: 0,
  clockRate: 8000,
  channels: 1,
  parameters: {},
  rtcpFeedback: []
};

/** Read one browser audio port setting, failing startup unless it is a usable UDP port. */
function audioPort(name: string, fallback: number) {
  const raw = process.env[name]?.trim();
  const port = raw ? Number(raw) : fallback;
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error(`${name} must be a UDP port between 1 and 65535`);
  return port;
}

export class MediaBridge extends EventEmitter {
  private worker!: Worker;
  private router!: Router;
  private readonly sessions = new Map<string, MediaSession>();
  private micPort = 0;
  private speakerPort = 0;

  constructor(private readonly ari: AriClient) {
    super();
  }

  /** Start one mediasoup worker/router before accepting calls.
   * Only one call is ever active, so the owning browser needs exactly two fixed ports:
   * one for its microphone audio into the gateway and one for caller audio to its speaker. */
  async start() {
    this.micPort = audioPort("WEBRTC_MIC_PORT", 40000);
    this.speakerPort = audioPort("WEBRTC_SPEAKER_PORT", 40001);
    if (this.micPort === this.speakerPort) throw new Error("WEBRTC_MIC_PORT and WEBRTC_SPEAKER_PORT must differ");
    this.worker = await mediasoup.createWorker();
    this.worker.on("died", (error) => {
      console.error("mediasoup worker died", error);
      process.exitCode = 1;
    });
    this.router = await this.worker.createRouter({ mediaCodecs: [pcmuCodec] });
  }

  /** Create the loopback RTP and ExternalMedia side, then invite one browser to negotiate. */
  async begin(callId: string, clientId: string, hostname: string) {
    await this.close(callId);
    const socket = dgram.createSocket("udp4");
    await new Promise<void>((resolve, reject) => {
      socket.once("error", reject);
      socket.bind(0, "127.0.0.1", () => {
        socket.off("error", reject);
        resolve();
      });
    });
    const address = socket.address();
    if (typeof address === "string") throw new Error("Expected an IPv4 media socket");
    const direct = await this.router.createDirectTransport();
    const externalChannelId = `relay-media-${callId}`;
    const session: MediaSession = {
      callId,
      clientId,
      hostname,
      externalChannelId,
      socket,
      direct,
      transports: new Map(),
      readyEmitted: false
    };
    this.sessions.set(callId, session);
    socket.on("message", (packet) => void this.receiveAsteriskRtp(session, packet));
    await this.ari.externalMedia(externalChannelId, `127.0.0.1:${address.port}`);
    session.asteriskAddress = await this.readVariable(externalChannelId, "UNICASTRTP_LOCAL_ADDRESS");
    session.asteriskPort = Number(await this.readVariable(externalChannelId, "UNICASTRTP_LOCAL_PORT"));
    this.emit("signal", clientId, {
      type: "media",
      action: "start",
      callId,
      data: { rtpCapabilities: this.router.rtpCapabilities, hostname }
    });
    return externalChannelId;
  }

  /** Process one mediasoup negotiation command and return correlated results to the browser. */
  async handle(callId: string, clientId: string, action: string, value: unknown) {
    const session = this.sessions.get(callId);
    if (!session || session.clientId !== clientId) throw new Error("Media session is not owned by this browser");
    const data = (value && typeof value === "object" ? value : {}) as MediaEnvelope;
    try {
      let result: unknown;
      if (action === "createTransport") result = await this.createTransport(session, data.direction);
      else if (action === "connectTransport") result = await this.connectTransport(session, data.transportId, data.dtlsParameters);
      else if (action === "produce") result = await this.produce(session, data.transportId, data.kind, data.rtpParameters);
      else if (action === "setReceiveCapabilities") result = await this.setReceiveCapabilities(session, data.transportId, data.rtpCapabilities);
      else if (action === "resumeConsumer") result = await this.resumeConsumer(session, data.consumerId);
      else throw new Error("Unknown media action");
      this.respond(session.clientId, callId, data.requestId, result);
    } catch (error) {
      this.respond(session.clientId, callId, data.requestId, undefined, error instanceof Error ? error.message : "Media operation failed");
    }
  }

  /** Return the ExternalMedia channel once a claimed call is ready to bridge. */
  externalChannel(callId: string) {
    return this.sessions.get(callId)?.externalChannelId;
  }

  /** Drop one call's MediaSession: WebRTC transports, DirectTransport, loopback UDP, ExternalMedia.
   * The worker/router stay up; only this call's path is released so fixed ports can be rebound. */
  async close(callId: string) {
    const session = this.sessions.get(callId);
    if (!session) return;
    this.sessions.delete(callId);
    for (const transport of session.transports.values()) transport.close();
    session.direct.close();
    session.socket.close();
    await this.ari.hangup(session.externalChannelId).catch(() => {});
  }

  /** Create a browser ICE/DTLS transport advertised at its same-origin hostname.
   * The browser's send transport carries its microphone and binds the mic port; its receive
   * transport carries caller audio and binds the speaker port. A retried direction first
   * closes its previous transport so the fixed port is free to bind again. */
  private async createTransport(session: MediaSession, direction: "send" | "recv" | undefined) {
    if (direction !== "send" && direction !== "recv") throw new Error("Transport direction is required");
    const previous = direction === "send" ? session.sendTransport : session.receiveTransport;
    if (previous) {
      previous.close();
      session.transports.delete(previous.id);
      if (direction === "send") {
        session.browserProducer = undefined;
        session.browserToAsterisk = undefined;
      } else {
        session.browserConsumer = undefined;
      }
    }
    const announcedAddress = process.env.WEBRTC_ANNOUNCED_ADDRESS || session.hostname;
    const transport = await this.router.createWebRtcTransport({
      listenInfos: [{
        protocol: "udp",
        ip: "0.0.0.0",
        announcedAddress,
        port: direction === "send" ? this.micPort : this.speakerPort
      }],
      enableUdp: true,
      enableTcp: false,
      preferUdp: true
    });
    session.transports.set(transport.id, transport);
    if (direction === "send") session.sendTransport = transport;
    else session.receiveTransport = transport;
    transport.on("dtlsstatechange", (state) => {
      if (state === "closed" || state === "failed") transport.close();
    });
    return {
      id: transport.id,
      iceParameters: transport.iceParameters,
      iceCandidates: transport.iceCandidates,
      dtlsParameters: transport.dtlsParameters
    };
  }

  /** Apply browser DTLS parameters to its matching transport. */
  private async connectTransport(session: MediaSession, transportId?: string, dtlsParameters?: DtlsParameters) {
    const transport = transportId ? session.transports.get(transportId) : undefined;
    if (!transport || !dtlsParameters) throw new Error("Unknown transport or missing DTLS parameters");
    await transport.connect({ dtlsParameters });
    this.checkReady(session);
    return { connected: true };
  }

  /** Register the browser microphone producer and start forwarding it to Asterisk. */
  private async produce(session: MediaSession, transportId?: string, kind?: "audio", rtpParameters?: RtpParameters) {
    const transport = transportId ? session.transports.get(transportId) : undefined;
    if (!transport || kind !== "audio" || !rtpParameters) throw new Error("Invalid audio producer");
    session.browserProducer?.close();
    session.browserProducer = await transport.produce({ kind, rtpParameters });
    session.browserToAsterisk = await session.direct.consume({
      producerId: session.browserProducer.id,
      rtpCapabilities: this.router.rtpCapabilities
    });
    session.browserToAsterisk.on("rtp", (packet) => {
      if (session.asteriskAddress && session.asteriskPort) {
        session.socket.send(packet, session.asteriskPort, session.asteriskAddress);
      }
    });
    this.checkReady(session);
    return { id: session.browserProducer.id };
  }

  /** Store browser receive capabilities and create a consumer once Asterisk sends RTP. */
  private async setReceiveCapabilities(session: MediaSession, transportId?: string, capabilities?: RtpCapabilities) {
    const transport = transportId ? session.transports.get(transportId) : undefined;
    if (!transport || !capabilities) throw new Error("Invalid receive transport");
    session.receiveTransport = transport;
    session.browserCapabilities = capabilities;
    await this.createBrowserConsumer(session);
    this.checkReady(session);
    return { accepted: true };
  }

  /** Resume a browser consumer after mediasoup-client has attached its MediaStreamTrack. */
  private async resumeConsumer(session: MediaSession, consumerId?: string) {
    if (!session.browserConsumer || session.browserConsumer.id !== consumerId) throw new Error("Unknown consumer");
    await session.browserConsumer.resume();
    return { resumed: true };
  }

  /** Learn Asterisk's SSRC from its first packet, then inject all RTP into mediasoup. */
  private async receiveAsteriskRtp(session: MediaSession, packet: Buffer) {
    if (packet.length < 12 || (packet[0] >> 6) !== 2) return;
    if (!session.asteriskProducer) {
      const ssrc = packet.readUInt32BE(8);
      session.asteriskProducer = await session.direct.produce({
        kind: "audio",
        rtpParameters: {
          codecs: [{
            mimeType: "audio/PCMU",
            payloadType: packet[1] & 0x7f,
            clockRate: 8000,
            channels: 1,
            parameters: {},
            rtcpFeedback: []
          }],
          encodings: [{ ssrc }]
        }
      });
      await this.createBrowserConsumer(session);
    }
    session.asteriskProducer.send(packet);
  }

  /** Create the paused browser consumer and signal its negotiated RTP parameters. */
  private async createBrowserConsumer(session: MediaSession) {
    if (session.browserConsumer || !session.asteriskProducer || !session.receiveTransport || !session.browserCapabilities) return;
    if (!this.router.canConsume({ producerId: session.asteriskProducer.id, rtpCapabilities: session.browserCapabilities })) {
      throw new Error("Browser cannot consume PCMU audio");
    }
    session.browserConsumer = await session.receiveTransport.consume({
      producerId: session.asteriskProducer.id,
      rtpCapabilities: session.browserCapabilities,
      paused: true
    });
    this.emit("signal", session.clientId, {
      type: "media",
      action: "consumer",
      callId: session.callId,
      data: {
        id: session.browserConsumer.id,
        producerId: session.asteriskProducer.id,
        kind: session.browserConsumer.kind,
        rtpParameters: session.browserConsumer.rtpParameters
      }
    });
  }

  /** Announce readiness once microphone production and a receive transport both exist. */
  private checkReady(session: MediaSession) {
    if (session.readyEmitted || !session.browserProducer || !session.receiveTransport) return;
    session.readyEmitted = true;
    this.emit("ready", session.callId, session.clientId);
  }

  /** Retry briefly because UnicastRTP variables may appear just after channel creation. */
  private async readVariable(channelId: string, variable: string) {
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const value = await this.ari.channelVariable(channelId, variable).catch(() => "");
      if (value) return value;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    throw new Error(`Asterisk did not publish ${variable}`);
  }

  /** Send one correlated media result through the socket hub. */
  private respond(clientId: string, callId: string, requestId: string | undefined, data?: unknown, error?: string) {
    this.emit("signal", clientId, { type: "media", action: "response", callId, requestId: requestId || randomUUID(), data, error });
  }
}
