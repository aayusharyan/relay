/** Minimal Asterisk REST Interface client used for server-owned call control.
 * REST methods mutate channels/bridges while the event WebSocket drives state. */
import { EventEmitter } from "node:events";
import WebSocket from "ws";

export interface AriEvent {
  type: string;
  application?: string;
  args?: string[];
  channel?: { id: string; name: string; state: string; caller?: { number?: string } };
  bridge?: { id: string };
}

export interface AriChannel {
  id: string;
  name: string;
  state: string;
}

export class AriClient extends EventEmitter {
  private socket: WebSocket | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private stopped = false;

  constructor(
    private readonly baseUrl = "http://127.0.0.1:8088/ari",
    private readonly username = "relay",
    private readonly password = process.env.INTERNAL_ARI_PASSWORD || "",
    readonly app = "relay"
  ) {
    super();
  }

  /** Connect to ARI events and keep reconnecting until stop is called. */
  start() {
    this.stopped = false;
    this.connectEvents();
  }

  /** Stop event delivery and cancel reconnect work. */
  stop() {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.socket?.close();
    this.socket = null;
  }

  /** Execute one authenticated ARI request and parse a JSON response when present. */
  private async request<T = void>(method: string, resource: string, query: Record<string, string | number | undefined> = {}) {
    const url = new URL(`${this.baseUrl}${resource}`);
    for (const [key, value] of Object.entries(query)) if (value !== undefined) url.searchParams.set(key, String(value));
    const authorization = `Basic ${Buffer.from(`${this.username}:${this.password}`).toString("base64")}`;
    const response = await fetch(url, { method, headers: { authorization } });
    if (!response.ok) throw new Error(`ARI ${method} ${resource} failed: ${response.status} ${await response.text()}`);
    const body = await response.text();
    return (body ? JSON.parse(body) : undefined) as T;
  }

  /** Open the ARI event stream; malformed events are ignored without dropping calls. */
  private connectEvents() {
    if (this.stopped || this.socket?.readyState === WebSocket.OPEN) return;
    const url = new URL(this.baseUrl.replace(/^http/, "ws") + "/events");
    url.searchParams.set("app", this.app);
    url.searchParams.set("api_key", `${this.username}:${this.password}`);
    url.searchParams.set("subscribeAll", "true");
    const socket = new WebSocket(url);
    this.socket = socket;
    socket.on("open", () => this.emit("connected"));
    socket.on("message", (data) => {
      try {
        this.emit("event", JSON.parse(data.toString()) as AriEvent);
      } catch {
        console.warn("Ignored malformed ARI event");
      }
    });
    socket.on("error", (error) => console.error("ARI event connection failed", error));
    socket.on("close", () => {
      if (this.socket === socket) this.socket = null;
      this.emit("disconnected");
      if (!this.stopped) this.reconnectTimer = setTimeout(() => this.connectEvents(), 1_000);
    });
  }

  /** Answer an incoming channel after the winning browser media is ready. */
  answer(channelId: string) {
    return this.request("POST", `/channels/${encodeURIComponent(channelId)}/answer`);
  }

  /** Ask Asterisk to send ringing indication without answering the channel. */
  ring(channelId: string) {
    return this.request("POST", `/channels/${encodeURIComponent(channelId)}/ring`);
  }

  /** Hang up a channel; optional ARI reason (for example busy) maps to the SIP cause.
   * A missing channel is harmless during idempotent teardown. */
  async hangup(channelId: string, reason?: string) {
    try {
      await this.request("DELETE", `/channels/${encodeURIComponent(channelId)}`, reason ? { reason } : {});
    } catch (error) {
      if (!String(error).includes("404")) throw error;
    }
  }

  /** Set a channel variable or writable dialplan function such as GROUP(). */
  setVariable(channelId: string, variable: string, value: string) {
    return this.request("POST", `/channels/${encodeURIComponent(channelId)}/variable`, { variable, value });
  }

  /** Create an upstream Stasis channel, mark it in the single-call group, then dial. */
  async originate(endpoint: string, channelId: string, appArgs: string) {
    const channel = await this.request<AriChannel>("POST", "/channels/create", {
      endpoint,
      channelId,
      app: this.app,
      appArgs
    });
    // Same group the dialplan checks so inbound Busy() works during an outbound call.
    await this.setVariable(channelId, "GROUP(activecall)", "1");
    await this.request("POST", `/channels/${encodeURIComponent(channelId)}/dial`, { timeout: 60 });
    return channel;
  }

  /** Create a mixing bridge for one upstream channel and one media channel. */
  createBridge(bridgeId: string) {
    return this.request("POST", `/bridges/${encodeURIComponent(bridgeId)}`, {
      type: "mixing,proxy_media,dtmf_events",
      name: bridgeId
    });
  }

  /** Add one or more channels to a bridge. */
  addChannels(bridgeId: string, channelIds: string[]) {
    return this.request("POST", `/bridges/${encodeURIComponent(bridgeId)}/addChannel`, {
      channel: channelIds.join(",")
    });
  }

  /** Destroy a bridge; a missing bridge is harmless during teardown. */
  async destroyBridge(bridgeId: string) {
    try {
      await this.request("DELETE", `/bridges/${encodeURIComponent(bridgeId)}`);
    } catch (error) {
      if (!String(error).includes("404")) throw error;
    }
  }

  /** Create a bidirectional ulaw ExternalMedia channel targeting a local UDP socket. */
  externalMedia(channelId: string, host: string) {
    return this.request<AriChannel>("POST", "/channels/externalMedia", {
      app: this.app,
      channelId,
      external_host: host,
      format: "ulaw",
      encapsulation: "rtp",
      transport: "udp",
      connection_type: "client",
      direction: "both",
      data: "relay-media"
    });
  }

  /** Read a channel variable such as the RTP address allocated by UnicastRTP. */
  async channelVariable(channelId: string, variable: string) {
    const result = await this.request<{ value?: string }>(
      "GET",
      `/channels/${encodeURIComponent(channelId)}/variable`,
      { variable }
    );
    return result.value || "";
  }

  /** Put the upstream channel on hold using Asterisk's channel operation. */
  hold(channelId: string) {
    return this.request("POST", `/channels/${encodeURIComponent(channelId)}/hold`);
  }

  /** Remove hold from the upstream channel. */
  unhold(channelId: string) {
    return this.request("DELETE", `/channels/${encodeURIComponent(channelId)}/hold`);
  }

  /** Send one validated DTMF digit toward the upstream channel. */
  sendDtmf(channelId: string, digit: string) {
    return this.request("POST", `/channels/${encodeURIComponent(channelId)}/dtmf`, {
      dtmf: digit,
      duration: 160,
      between: 100
    });
  }
}
