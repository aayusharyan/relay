/** Small Asterisk Manager Interface client for login, correlated actions, and UserEvent parsing (SIP MESSAGE). */
import { EventEmitter } from "node:events";
import net from "node:net";
import { randomUUID } from "node:crypto";

type AmiHeaders = Record<string, string>;

export class AmiClient extends EventEmitter {
  private socket: net.Socket | null = null;
  private buffer = "";
  private stopped = false;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private readonly pending = new Map<string, {
    resolve: (headers: AmiHeaders) => void;
    reject: (error: Error) => void;
    timer: NodeJS.Timeout;
  }>();

  constructor(
    private readonly host = "127.0.0.1",
    private readonly port = 5038,
    private readonly username = "relay",
    private readonly password = process.env.INTERNAL_AMI_PASSWORD || ""
  ) {
    super();
  }

  /** Connect and authenticate; reconnect after transport failure. */
  start() {
    this.stopped = false;
    this.connect();
  }

  /** Close AMI and reject commands that can no longer receive responses. */
  stop() {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.socket?.destroy();
    this.socket = null;
    this.rejectPending(new Error("AMI stopped"));
  }

  /** Send one SIP MESSAGE through Asterisk using a newline-safe base64 body. */
  sendMessage(destination: string, from: string, body: string) {
    return this.action({
      Action: "MessageSend",
      Destination: `pjsip:PJSIP/${destination}@upstream`,
      To: `sip:${destination}`,
      From: from,
      Base64Body: Buffer.from(body, "utf8").toString("base64"),
      Variable: "Content-Type=text/plain"
    });
  }

  /** Connect a TCP stream and log in with events enabled. */
  private connect() {
    if (this.stopped || this.socket) return;
    const socket = net.createConnection({ host: this.host, port: this.port });
    this.socket = socket;
    socket.setEncoding("utf8");
    socket.on("connect", () => {
      void this.action({
        Action: "Login",
        Username: this.username,
        Secret: this.password,
        Events: "on"
      }).then(() => this.emit("connected")).catch((error) => {
        console.error("AMI login failed", error);
        socket.destroy();
      });
    });
    socket.on("data", (chunk: string) => this.consume(chunk));
    socket.on("error", (error) => console.error("AMI connection failed", error));
    socket.on("close", () => {
      if (this.socket === socket) this.socket = null;
      this.rejectPending(new Error("AMI disconnected"));
      this.emit("disconnected");
      if (!this.stopped) this.reconnectTimer = setTimeout(() => this.connect(), 1_000);
    });
  }

  /** Send one AMI action and resolve its matching ActionID response. */
  private action(headers: AmiHeaders) {
    if (!this.socket?.writable) return Promise.reject(new Error("AMI is not connected"));
    const actionId = randomUUID();
    const payload = { ...headers, ActionID: actionId };
    return new Promise<AmiHeaders>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(actionId);
        reject(new Error(`AMI ${headers.Action} timed out`));
      }, 5_000);
      this.pending.set(actionId, { resolve, reject, timer });
      this.socket?.write(`${Object.entries(payload).map(([key, value]) => `${key}: ${value}`).join("\r\n")}\r\n\r\n`);
    });
  }

  /** Parse complete blank-line-delimited AMI frames from the TCP stream. */
  private consume(chunk: string) {
    this.buffer += chunk;
    let boundary = this.buffer.indexOf("\r\n\r\n");
    while (boundary >= 0) {
      const frame = this.buffer.slice(0, boundary);
      this.buffer = this.buffer.slice(boundary + 4);
      this.handleFrame(frame);
      boundary = this.buffer.indexOf("\r\n\r\n");
    }
  }

  /** Resolve action responses or emit unsolicited manager events. */
  private handleFrame(frame: string) {
    const headers: AmiHeaders = {};
    for (const line of frame.split("\r\n")) {
      const separator = line.indexOf(":");
      if (separator > 0) headers[line.slice(0, separator)] = line.slice(separator + 1).trimStart();
    }
    const actionId = headers.ActionID;
    if (actionId && this.pending.has(actionId)) {
      const pending = this.pending.get(actionId)!;
      clearTimeout(pending.timer);
      this.pending.delete(actionId);
      if (headers.Response === "Error") pending.reject(new Error(headers.Message || "AMI action failed"));
      else pending.resolve(headers);
      return;
    }
    if (headers.Event) this.emit("event", headers);
  }

  /** Reject every waiting action after a connection-level failure. */
  private rejectPending(error: Error) {
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
  }
}
