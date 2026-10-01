/** Authoritative call and message controller for every connected dashboard.
 * It owns Asterisk operations, first-answer arbitration, and persisted outcomes. */
import { EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";
import type { AmiClient } from "./amiClient.js";
import type { AriClient, AriEvent } from "./ariClient.js";
import type { BrowserClient, BrowserCommand } from "./browserSocket.js";
import type { MediaBridge } from "./mediaBridge.js";

export type CallPhase = "idle" | "incoming" | "claiming" | "calling" | "active" | "held" | "ended";

export interface CallSnapshot {
  phase: CallPhase;
  callId?: string;
  direction?: "incoming" | "outgoing";
  peer?: string;
  name?: string;
  owned?: boolean;
  startedAt?: string;
  answeredAt?: string;
}

export interface CallRecord {
  id: string;
  peer: string;
  name?: string;
  direction: "in" | "out" | "missed";
  time: string;
  duration?: number;
  outcome?: "answered" | "declined" | "missed" | "handled";
}

export interface MessageRecord {
  id: string;
  peer: string;
  body: string;
  direction: "in" | "out";
  time: string;
  status: "sent" | "failed";
}

interface ActiveCall {
  id: string;
  phase: Exclude<CallPhase, "idle" | "ended">;
  direction: "incoming" | "outgoing";
  peer: string;
  name?: string;
  startedAt: number;
  answeredAt?: number;
  upstreamChannelId: string;
  ownerId?: string;
  hostname?: string;
  bridgeId?: string;
  externalChannelId?: string;
  claimTimer?: NodeJS.Timeout;
  dismissed: Set<string>;
  finishing: boolean;
}

interface ControllerHooks {
  saveCall: (record: CallRecord) => Promise<void>;
  saveMessage: (record: MessageRecord) => Promise<void>;
  pushIncoming: (peer: string, name?: string) => Promise<void>;
  pushEnded: () => Promise<void>;
}

export class CallController extends EventEmitter {
  private call: ActiveCall | null = null;

  constructor(
    private readonly ari: AriClient,
    private readonly ami: AmiClient,
    private readonly media: MediaBridge,
    private readonly hooks: ControllerHooks
  ) {
    super();
    ari.on("event", (event: AriEvent) => void this.handleAriEvent(event));
    ami.on("event", (event: Record<string, string>) => void this.handleAmiEvent(event));
    media.on("signal", (clientId: string, event: object) => this.emit("clientEvent", clientId, event));
    media.on("ready", (callId: string, clientId: string) => void this.mediaReady(callId, clientId));
  }

  /** Start internal Asterisk control connections. */
  start() {
    this.ari.start();
    this.ami.start();
  }

  /** Mark owned=true only for this clientId; omit foreign owner ids. */
  snapshot(clientId?: string): CallSnapshot {
    if (!this.call) return { phase: "idle" };
    return {
      phase: this.call.phase,
      callId: this.call.id,
      direction: this.call.direction,
      peer: this.call.peer,
      name: this.call.name,
      owned: Boolean(clientId && this.call.ownerId === clientId),
      startedAt: new Date(this.call.startedAt).toISOString(),
      answeredAt: this.call.answeredAt ? new Date(this.call.answeredAt).toISOString() : undefined
    };
  }

  /** Route one validated browser command to the authoritative state transition. */
  async command(client: BrowserClient, command: BrowserCommand) {
    if (command.type === "call") await this.startOutgoing(client, command.target);
    else if (command.type === "answer") await this.claim(client, command.callId);
    else if (command.type === "decline") this.decline(client.id, command.callId);
    else if (command.type === "hangup") await this.hangup(client.id, command.callId);
    else if (command.type === "hold") await this.setHold(client.id, command.callId, command.held);
    else if (command.type === "dtmf") await this.sendDtmf(client.id, command.callId, command.digit);
    else if (command.type === "message") await this.sendMessage(client.id, command);
    else if (command.type === "media") await this.media.handle(command.callId, client.id, command.action, command.data);
  }

  /** Release a pre-answer claim or hang up an active call whose owner disappeared. */
  async disconnected(clientId: string) {
    if (!this.call || this.call.ownerId !== clientId) return;
    if (this.call.phase === "claiming") {
      await this.releaseClaim("The answering browser disconnected.");
    } else if (this.call.phase === "calling" || this.call.phase === "active" || this.call.phase === "held") {
      await this.ari.hangup(this.call.upstreamChannelId);
      await this.finish("handled");
    }
  }

  /** Handle Stasis lifecycle and upstream channel state changes. */
  private async handleAriEvent(event: AriEvent) {
    const channelId = event.channel?.id;
    if (event.type === "StasisStart" && channelId) {
      const [kind, suppliedId, suppliedPeer] = event.args || [];
      if (kind === "relay-media") return;
      if (kind === "incoming") {
        // Dialplan Busy() is the normal gate; this covers a race where two INVITEs both passed.
        if (this.call) {
          await this.ari.hangup(channelId, "busy");
          return;
        }
        const peer = suppliedId || event.channel?.caller?.number || "Unknown";
        // Trust upstream CALLERID(name) verbatim; consumers decide how to truncate for display.
        const name = event.channel?.caller?.name || undefined;
        this.call = {
          id: channelId,
          phase: "incoming",
          direction: "incoming",
          peer,
          name,
          startedAt: Date.now(),
          upstreamChannelId: channelId,
          dismissed: new Set(),
          finishing: false
        };
        await this.ari.ring(channelId).catch(() => {});
        this.broadcastState();
        await this.hooks.pushIncoming(peer, name);
      } else if (kind === "outbound" && this.call?.id === suppliedId) {
        this.call.upstreamChannelId = channelId;
        if (suppliedPeer) this.call.peer = suppliedPeer;
      }
      return;
    }
    if (!this.call || channelId !== this.call.upstreamChannelId) return;
    if (event.type === "ChannelStateChange" && event.channel?.state === "Up" && this.call.direction === "outgoing") {
      this.call.phase = "active";
      this.call.answeredAt ||= Date.now();
      this.broadcastState();
    }
    if (event.type === "StasisEnd" || event.type === "ChannelDestroyed") {
      await this.finish(this.call.answeredAt ? "answered" : this.call.direction === "incoming" ? "missed" : "handled");
    }
  }

  /** Persist an inbound dialplan UserEvent before publishing its server record. */
  private async handleAmiEvent(event: Record<string, string>) {
    if (event.Event !== "UserEvent" || event.UserEvent !== "RelayMessage") return;
    try {
      const body = Buffer.from(event.Base64Body || "", "base64").toString("utf8");
      const message: MessageRecord = {
        id: randomUUID(),
        peer: event.Peer || "Unknown",
        body,
        direction: "in",
        time: new Date().toISOString(),
        status: "sent"
      };
      await this.hooks.saveMessage(message);
      this.emit("broadcast", { type: "message-created", record: message });
    } catch (error) {
      console.error("Could not persist inbound SIP MESSAGE", error);
    }
  }

  /** Atomically reserve an incoming call for the first answering browser. */
  private async claim(client: BrowserClient, callId: string) {
    if (!this.call || this.call.id !== callId || this.call.phase !== "incoming") throw new Error("Call is no longer available");
    if (this.call.dismissed.has(client.id)) throw new Error("This browser declined the call");
    this.call.phase = "claiming";
    this.call.ownerId = client.id;
    this.call.hostname = client.hostname;
    this.emit("broadcastExcept", client.id, { type: "answered_elsewhere", callId });
    this.broadcastState();
    try {
      this.call.externalChannelId = await this.media.begin(callId, client.id, client.hostname);
      this.call.claimTimer = setTimeout(() => void this.releaseClaim("Media setup timed out."), 10_000);
    } catch (error) {
      await this.releaseClaim(error instanceof Error ? error.message : "Could not prepare media");
      throw error;
    }
  }

  /** Make an incoming call claimable again after winner media failure. */
  private async releaseClaim(message: string) {
    if (!this.call || this.call.phase !== "claiming") return;
    if (this.call.claimTimer) clearTimeout(this.call.claimTimer);
    const previousOwner = this.call.ownerId;
    await this.media.close(this.call.id);
    this.call.phase = "incoming";
    this.call.ownerId = undefined;
    this.call.hostname = undefined;
    this.call.externalChannelId = undefined;
    if (previousOwner) this.emit("clientEvent", previousOwner, { type: "error", message });
    this.broadcastState();
  }

  /** Answer and bridge an incoming call, or attach media to an outgoing call. */
  private async mediaReady(callId: string, clientId: string) {
    const call = this.call;
    if (!call || call.id !== callId || call.ownerId !== clientId) return;
    if (call.claimTimer) clearTimeout(call.claimTimer);
    call.claimTimer = undefined;
    call.externalChannelId = this.media.externalChannel(callId);
    if (!call.externalChannelId || !call.upstreamChannelId) return;
    call.bridgeId = `relay-bridge-${call.id}`;
    await this.ari.createBridge(call.bridgeId);
    await this.ari.addChannels(call.bridgeId, [call.upstreamChannelId, call.externalChannelId]);
    if (call.direction === "incoming") {
      await this.ari.answer(call.upstreamChannelId);
      call.answeredAt = Date.now();
      call.phase = "active";
    }
    this.broadcastState();
  }

  /** Start one server-owned outbound call for the requesting browser. */
  private async startOutgoing(client: BrowserClient, rawTarget: string) {
    if (this.call) throw new Error("Another call is already in progress");
    const target = this.target(rawTarget);
    if (!/^[0-9A-Za-z_+*#@.-]{1,160}$/.test(target)) throw new Error("Invalid call destination");
    const id = randomUUID();
    const upstreamChannelId = `relay-upstream-${id}`;
    this.call = {
      id,
      phase: "calling",
      direction: "outgoing",
      peer: target,
      startedAt: Date.now(),
      upstreamChannelId,
      ownerId: client.id,
      hostname: client.hostname,
      dismissed: new Set(),
      finishing: false
    };
    this.broadcastState();
    try {
      await this.ari.originate(`PJSIP/${target}@upstream`, upstreamChannelId, `outbound,${id},${target}`);
      this.call.externalChannelId = await this.media.begin(id, client.id, client.hostname);
    } catch (error) {
      await this.finish("handled");
      throw error;
    }
  }

  /** Dismiss an incoming call in only one browser. */
  private decline(clientId: string, callId: string) {
    if (!this.call || this.call.id !== callId || this.call.phase !== "incoming") return;
    this.call.dismissed.add(clientId);
    this.emit("clientEvent", clientId, { type: "declined", callId });
  }

  /** Permit only the owner to end a claimed/active call. */
  private async hangup(clientId: string, callId: string) {
    if (!this.call || this.call.id !== callId || this.call.ownerId !== clientId) throw new Error("This browser does not own the call");
    await this.ari.hangup(this.call.upstreamChannelId);
    await this.finish(this.call.answeredAt ? "answered" : "handled");
  }

  /** Apply hold state to the upstream channel and broadcast the result. */
  private async setHold(clientId: string, callId: string, held: boolean) {
    if (!this.call || this.call.id !== callId || this.call.ownerId !== clientId || !this.call.answeredAt) throw new Error("No owned active call");
    if (held) await this.ari.hold(this.call.upstreamChannelId);
    else await this.ari.unhold(this.call.upstreamChannelId);
    this.call.phase = held ? "held" : "active";
    this.broadcastState();
  }

  /** Send one keypad digit from the owner to the upstream channel. */
  private async sendDtmf(clientId: string, callId: string, digit: string) {
    if (!this.call || this.call.id !== callId || this.call.ownerId !== clientId || !/^[0-9A-D#*]$/.test(digit)) throw new Error("Invalid DTMF command");
    await this.ari.sendDtmf(this.call.upstreamChannelId, digit);
  }

  /** Submit a SIP MESSAGE, persist its final result, and publish one shared record. */
  private async sendMessage(clientId: string, command: Extract<BrowserCommand, { type: "message" }>) {
    const target = this.target(command.target);
    const body = command.body;
    if (!/^[0-9A-Za-z_+*#@.-]{1,160}$/.test(target) || !body || body.length > 8_000) throw new Error("Invalid message");
    const from = `sip:${process.env.PBX_USERNAME || "relay"}@${process.env.PBX_HOST || "localhost"}`;
    let status: MessageRecord["status"] = "sent";
    try {
      await this.ami.sendMessage(target, from, body);
    } catch {
      status = "failed";
    }
    const message: MessageRecord = {
      id: randomUUID(),
      peer: target,
      body,
      direction: "out",
      time: new Date().toISOString(),
      status
    };
    await this.hooks.saveMessage(message);
    this.emit("broadcast", {
      type: "message-created",
      record: message,
      requestId: command.requestId,
      senderId: clientId
    });
  }

  /** Persist one final server-owned record and release all call resources. */
  private async finish(outcome: "answered" | "missed" | "handled") {
    const call = this.call;
    if (!call || call.finishing) return;
    call.finishing = true;
    if (call.claimTimer) clearTimeout(call.claimTimer);
    const endedAt = Date.now();
    await this.media.close(call.id);
    if (call.bridgeId) await this.ari.destroyBridge(call.bridgeId);
    const record: CallRecord = {
      id: `gateway-${call.id}`,
      peer: call.peer,
      name: call.name,
      direction: call.direction === "outgoing" ? "out" : outcome === "missed" ? "missed" : "in",
      time: new Date(call.startedAt).toISOString(),
      outcome,
      duration: call.answeredAt ? Math.max(0, Math.round((endedAt - call.answeredAt) / 1000)) : 0
    };
    this.call = null;
    await this.hooks.saveCall(record);
    this.emit("broadcast", { type: "state", call: { phase: "ended", callId: call.id, peer: call.peer, name: call.name, direction: call.direction } });
    this.emit("broadcast", { type: "state", call: { phase: "idle" } });
    await this.hooks.pushEnded();
  }

  /** Broadcast the public state; clients derive ownership from their own snapshot events. */
  private broadcastState() {
    this.emit("broadcastState");
  }

  /** Convert a SIP-looking UI address into the upstream endpoint's destination user. */
  private target(value: string) {
    return value.trim().replace(/^sips?:/i, "").split("@")[0];
  }
}
