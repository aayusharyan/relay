/** SQLite persistence for shared calls, messages, favourites, and Web Push subscriptions.
 * Startup creates the database, tables, and indexes idempotently. */
import { mkdirSync } from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import type { PushSubscription } from "web-push";
import type { CallRecord, FavoriteRecord, MessageRecord } from "./callController.js";

export const FAVOURITE_COUNT_LIMIT = 12;

/** SQLite calls SELECT shape before mapCall; nullable columns stay null here. */
interface CallRow {
  id: string;
  peer: string;
  name: string | null;
  direction: CallRecord["direction"];
  time: string;
  duration: number | null;
  outcome: CallRecord["outcome"] | null;
}

/** SQLite messages SELECT shape; columns match MessageRecord directly. */
interface MessageRow {
  id: string;
  peer: string;
  body: string;
  direction: MessageRecord["direction"];
  time: string;
  status: MessageRecord["status"];
}

/** SQLite favourites SELECT shape before mapFavorite; nullable columns stay null here. */
interface FavoriteRow {
  id: string;
  peer: string;
  name: string | null;
  position: number;
}

export class Store {
  private readonly database: DatabaseSync;

  /** Open relay.sqlite and create the complete schema when it is absent. */
  constructor(dataDir: string) {
    mkdirSync(dataDir, { recursive: true });
    this.database = new DatabaseSync(path.join(dataDir, "relay.sqlite"));
    this.database.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA foreign_keys = ON;

      CREATE TABLE IF NOT EXISTS calls (
        id TEXT PRIMARY KEY,
        peer TEXT NOT NULL,
        name TEXT,
        direction TEXT NOT NULL CHECK (direction IN ('in', 'out', 'missed')),
        time TEXT NOT NULL,
        duration INTEGER,
        outcome TEXT CHECK (outcome IN ('answered', 'declined', 'missed', 'handled'))
      );

      CREATE INDEX IF NOT EXISTS calls_time_idx ON calls(time DESC);

      CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        peer TEXT NOT NULL,
        body TEXT NOT NULL,
        direction TEXT NOT NULL CHECK (direction IN ('in', 'out')),
        time TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('sent', 'failed'))
      );

      CREATE INDEX IF NOT EXISTS messages_time_idx ON messages(time ASC);

      CREATE TABLE IF NOT EXISTS favorites (
        id TEXT PRIMARY KEY,
        peer TEXT NOT NULL UNIQUE,
        name TEXT,
        position INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS favorites_position_idx ON favorites(position ASC, rowid ASC);

      CREATE TABLE IF NOT EXISTS push_subscriptions (
        endpoint TEXT PRIMARY KEY,
        subscription_json TEXT NOT NULL
      );
    `);
  }

  /** Return shared calls newest first, preserving optional fields. */
  listCalls(): CallRecord[] {
    const rows = this.database.prepare(`
      SELECT id, peer, name, direction, time, duration, outcome
      FROM calls
      ORDER BY time DESC, rowid DESC
    `).all() as unknown as CallRow[];
    return rows.map((row) => this.mapCall(row));
  }

  /** Return one call by ID, or undefined when it does not exist. */
  getCall(id: string): CallRecord | undefined {
    const row = this.database.prepare(`
      SELECT id, peer, name, direction, time, duration, outcome FROM calls WHERE id = ?
    `).get(id) as unknown as CallRow | undefined;
    return row ? this.mapCall(row) : undefined;
  }

  /**
   * Return the newest nonempty Asterisk display name previously stored for this peer.
   * Used when a later call from the same number arrives without CALLERID(name).
   */
  latestCallerName(peer: string) {
    const row = this.database.prepare(`
      SELECT name FROM calls
      WHERE peer = ? AND name IS NOT NULL AND TRIM(name) != ''
      ORDER BY time DESC, rowid DESC
      LIMIT 1
    `).get(peer) as unknown as { name: string } | undefined;
    return row?.name;
  }

  /** Insert a call once and report whether this invocation created it. */
  insertCall(call: CallRecord) {
    const result = this.database.prepare(`
      INSERT OR IGNORE INTO calls (id, peer, name, direction, time, duration, outcome)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(call.id, call.peer, call.name ?? null, call.direction, call.time, call.duration ?? null, call.outcome ?? null);
    return Number(result.changes) > 0;
  }

  /** Map one SQLite call row into the shared CallRecord shape. */
  private mapCall(row: CallRow): CallRecord {
    return {
      id: row.id,
      peer: row.peer,
      ...(row.name ? { name: row.name } : {}),
      direction: row.direction,
      time: row.time,
      ...(row.duration === null ? {} : { duration: row.duration }),
      ...(row.outcome === null ? {} : { outcome: row.outcome })
    };
  }

  /** Delete one call and report whether a row was removed. */
  deleteCall(id: string) {
    return Number(this.database.prepare("DELETE FROM calls WHERE id = ?").run(id).changes) > 0;
  }

  /** Delete every call and return the number removed. */
  clearCalls() {
    return Number(this.database.prepare("DELETE FROM calls").run().changes);
  }

  /** Return shared messages oldest first for conversation rendering. */
  listMessages(): MessageRecord[] {
    return this.database.prepare(`
      SELECT id, peer, body, direction, time, status
      FROM messages
      ORDER BY time ASC, rowid ASC
    `).all() as unknown as MessageRow[];
  }

  /** Insert a message once and report whether this invocation created it. */
  insertMessage(message: MessageRecord) {
    const result = this.database.prepare(`
      INSERT OR IGNORE INTO messages (id, peer, body, direction, time, status)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(message.id, message.peer, message.body, message.direction, message.time, message.status);
    return Number(result.changes) > 0;
  }

  /** Return shared favourites in quick-dial order. */
  listFavorites(): FavoriteRecord[] {
    const rows = this.database.prepare(`
      SELECT id, peer, name, position
      FROM favorites
      ORDER BY position ASC, rowid ASC
    `).all() as unknown as FavoriteRow[];
    return rows.map((row) => this.mapFavorite(row));
  }

  /** Return one favourite by ID, or undefined when it does not exist. */
  getFavorite(id: string): FavoriteRecord | undefined {
    const row = this.database.prepare(`
      SELECT id, peer, name, position FROM favorites WHERE id = ?
    `).get(id) as unknown as FavoriteRow | undefined;
    return row ? this.mapFavorite(row) : undefined;
  }

  /** Return the favourite pinned to this peer, if any. */
  getFavoriteByPeer(peer: string): FavoriteRecord | undefined {
    const row = this.database.prepare(`
      SELECT id, peer, name, position FROM favorites WHERE peer = ?
    `).get(peer) as unknown as FavoriteRow | undefined;
    return row ? this.mapFavorite(row) : undefined;
  }

  /** Return the current number of shared favourites. */
  favoriteCount() {
    const row = this.database.prepare("SELECT COUNT(*) AS count FROM favorites").get() as unknown as { count: number };
    return Number(row.count);
  }

  /** Insert one favourite at the next position and return the stored record. */
  insertFavorite(favorite: Omit<FavoriteRecord, "position">): FavoriteRecord {
    const row = this.database.prepare(`
      SELECT COALESCE(MAX(position), -1) + 1 AS next_position FROM favorites
    `).get() as unknown as { next_position: number };
    const position = Number(row.next_position);
    this.database.prepare(`
      INSERT INTO favorites (id, peer, name, position) VALUES (?, ?, ?, ?)
    `).run(favorite.id, favorite.peer, favorite.name ?? null, position);
    return this.mapFavorite({
      id: favorite.id,
      peer: favorite.peer,
      name: favorite.name ?? null,
      position
    });
  }

  /** Map one SQLite favourite row into the shared FavoriteRecord shape. */
  private mapFavorite(row: FavoriteRow): FavoriteRecord {
    return {
      id: row.id,
      peer: row.peer,
      ...(row.name ? { name: row.name } : {}),
      position: row.position
    };
  }

  /** Delete one favourite and report whether a row was removed. */
  deleteFavorite(id: string) {
    return Number(this.database.prepare("DELETE FROM favorites WHERE id = ?").run(id).changes) > 0;
  }

  /** Return every stored push subscription. */
  listSubscriptions(): PushSubscription[] {
    const rows = this.database.prepare(`
      SELECT subscription_json FROM push_subscriptions ORDER BY endpoint
    `).all() as unknown as Array<{ subscription_json: string }>;
    return rows.map(({ subscription_json }) => JSON.parse(subscription_json) as PushSubscription);
  }

  /** Insert or replace one subscription using its endpoint as identity. */
  upsertSubscription(subscription: PushSubscription) {
    this.database.prepare(`
      INSERT INTO push_subscriptions (endpoint, subscription_json)
      VALUES (?, ?)
      ON CONFLICT(endpoint) DO UPDATE SET subscription_json = excluded.subscription_json
    `).run(subscription.endpoint, JSON.stringify(subscription));
  }

  /** Delete one subscription endpoint and report whether it existed. */
  deleteSubscription(endpoint: string) {
    return Number(this.database.prepare("DELETE FROM push_subscriptions WHERE endpoint = ?").run(endpoint).changes) > 0;
  }

  /** Delete multiple expired endpoints atomically. */
  deleteSubscriptions(endpoints: Iterable<string>) {
    const remove = this.database.prepare("DELETE FROM push_subscriptions WHERE endpoint = ?");
    this.database.exec("BEGIN IMMEDIATE");
    try {
      for (const endpoint of endpoints) remove.run(endpoint);
      this.database.exec("COMMIT");
    } catch (error) {
      this.database.exec("ROLLBACK");
      throw error;
    }
  }

  /** Return the current number of stored push endpoints. */
  subscriptionCount() {
    const row = this.database.prepare("SELECT COUNT(*) AS count FROM push_subscriptions").get() as unknown as { count: number };
    return Number(row.count);
  }
}
