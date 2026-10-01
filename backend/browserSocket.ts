/** Same-origin JSON WebSocket hub for dashboard presence, call control, and media signaling.
 * It validates envelope shape and leaves command-specific checks to the call controller. */
import { EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import { WebSocketServer, WebSocket } from "ws";

export type BrowserCommand =
  | { type: "call"; target: string }
  | { type: "answer"; callId: string }
  | { type: "decline"; callId: string }
  | { type: "hangup"; callId: string }
  | { type: "hold"; callId: string; held: boolean }
  | { type: "dtmf"; callId: string; digit: string }
  | { type: "message"; target: string; body: string; requestId: string }
  | { type: "media"; callId: string; action: string; data?: unknown };

export interface BrowserClient {
  id: string;
  hostname: string;
}

interface ClientRecord extends BrowserClient {
  socket: WebSocket;
}

export class BrowserSocketHub extends EventEmitter {
  private readonly server = new WebSocketServer({ noServer: true, maxPayload: 256 * 1024 });
  private readonly clients = new Map<string, ClientRecord>();

  /** Accept only the application /ws path and bind it to a generated client identity. */
  handleUpgrade(request: IncomingMessage, socket: Duplex, head: Buffer) {
    const path = new URL(request.url || "/", "http://localhost").pathname;
    if (path !== "/ws") {
      socket.destroy();
      return;
    }
    this.server.handleUpgrade(request, socket, head, (webSocket) => {
      const id = randomUUID();
      const hostname = this.hostname(request);
      const client: ClientRecord = { id, hostname, socket: webSocket };
      this.clients.set(id, client);
      webSocket.on("message", (data) => this.receive(client, data.toString()));
      webSocket.on("close", () => {
        this.clients.delete(id);
        this.emit("disconnected", { id, hostname } satisfies BrowserClient);
      });
      webSocket.on("error", (error) => console.error("Browser WebSocket failed", error));
      this.send(id, { type: "connected", clientId: id });
      this.emit("connected", { id, hostname } satisfies BrowserClient);
    });
  }

  /** Send an event to one browser if it is still connected. */
  send(clientId: string, event: object) {
    const socket = this.clients.get(clientId)?.socket;
    if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify(event));
  }

  /** Broadcast an event, optionally excluding one client. */
  broadcast(event: object, exceptClientId?: string) {
    const payload = JSON.stringify(event);
    for (const client of this.clients.values()) {
      if (client.id !== exceptClientId && client.socket.readyState === WebSocket.OPEN) client.socket.send(payload);
    }
  }

  /** Return true while a browser identity still has an open socket. */
  has(clientId: string) {
    return this.clients.has(clientId);
  }

  /** Return connected browser IDs for per-browser decline accounting. */
  clientIds() {
    return [...this.clients.keys()];
  }

  /** Parse a command envelope and reject malformed messages without closing the socket. */
  private receive(client: ClientRecord, raw: string) {
    try {
      const command = JSON.parse(raw) as BrowserCommand;
      if (!command || typeof command !== "object" || typeof command.type !== "string") throw new Error("Invalid command");
      this.emit("command", { client: { id: client.id, hostname: client.hostname }, command });
    } catch {
      this.send(client.id, { type: "error", message: "Invalid WebSocket command." });
    }
  }

  /** Prefer an explicit media override, then the proxy-preserved host name. */
  private hostname(request: IncomingMessage) {
    const configured = process.env.WEBRTC_ANNOUNCED_ADDRESS?.trim();
    if (configured) return configured;
    const forwarded = String(request.headers["x-forwarded-host"] || "").split(",")[0].trim();
    const host = forwarded || String(request.headers.host || "localhost");
    const hostname = host.startsWith("[") ? host.slice(1, host.indexOf("]")) : host.replace(/:\d+$/, "");
    if (!/^[A-Za-z0-9.-]+$/.test(hostname)) return "localhost";
    return hostname;
  }
}
