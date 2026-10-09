import {
  MatrixClient,
  MatrixHttpApi,
  MemoryStore,
  ClientEvent,
  Direction,
  EventType,
  MsgType,
  RoomEvent,
  SyncState,
  Method,
  type Room,
  type MatrixEvent,
  type IRoomTimelineData,
} from "matrix-js-sdk";
import type { RoomMessageEventContent } from "matrix-js-sdk/lib/@types/events.js";
import type { Config } from "./config.js";
import type { WebhookItem } from "./webhook.js";
// Raw events keep bounded history independent of SDK timeline aggregation/thread processing.
export type Event = {
  event_id?: string;
  room_id?: string;
  sender?: string;
  origin_server_ts?: number;
  type?: string;
  content?: Record<string, any>;
  unsigned?: Record<string, any>;
  redacts?: string;
  state_key?: string;
};
export const FIELD = 16000,
  TOTAL = 64000,
  DISCOVERY = 100;
const clip = (s: unknown, max = FIELD) =>
  typeof s === "string" ? s.slice(0, max) : "";
function isRedacted(event: Event, page: Event[]) {
  return (
    !!event.unsigned?.redacted_because ||
    page.some(
      (e) =>
        e.type === "m.room.redaction" &&
        (e.redacts ?? e.content?.redacts) === event.event_id,
    )
  );
}

export function record(event: Event, page: Event[] = []) {
  const original = event.content ?? {};
  if (
    event.type !== "m.room.message" ||
    original["m.relates_to"]?.rel_type === "m.replace"
  )
    return null;
  const redacted = isRedacted(event, page);
  if (
    !redacted &&
    (original.msgtype !== "m.text" || typeof original.body !== "string")
  )
    return null;
  const bundled = event.unsigned?.["m.relations"]?.["m.replace"];
  const edits = [...page, ...(bundled ? [bundled] : [])]
    .filter(
      (e) =>
        e.type === "m.room.message" &&
        e.state_key === undefined &&
        event.state_key === undefined &&
        (!e.room_id || !event.room_id || e.room_id === event.room_id) &&
        e.sender === event.sender &&
        e.content?.["m.relates_to"]?.rel_type === "m.replace" &&
        e.content?.["m.relates_to"]?.event_id === event.event_id &&
        e.content?.["m.new_content"]?.msgtype === "m.text" &&
        typeof e.content?.["m.new_content"]?.body === "string" &&
        !isRedacted(e, page),
    )
    .sort(
      (a, b) =>
        (a.origin_server_ts ?? 0) - (b.origin_server_ts ?? 0) ||
        ((a.event_id ?? "") === (b.event_id ?? "")
          ? 0
          : (a.event_id ?? "") < (b.event_id ?? "")
            ? -1
            : 1),
    );
  const content = edits.at(-1)?.content?.["m.new_content"] ?? original;
  const mentions = Array.isArray(content["m.mentions"]?.user_ids)
    ? content["m.mentions"].user_ids
        .filter((s: unknown) => typeof s === "string")
        .slice(0, 100)
        .map((s: string) => clip(s, 255))
    : [];
  const reply = original["m.relates_to"]?.["m.in_reply_to"]?.event_id;
  return {
    event_id: clip(event.event_id, 255),
    sender_id: clip(event.sender, 255),
    timestamp: event.origin_server_ts ?? 0,
    ...(redacted ? {} : { body: clip(content.body) }),
    mentions,
    ...(typeof reply === "string"
      ? { reply_to_event_id: clip(reply, 255) }
      : {}),
    edited: edits.length > 0,
    redacted,
    incomplete: !redacted,
    truncated:
      !redacted &&
      ((typeof content.body === "string" && content.body.length > FIELD) ||
        (content["m.mentions"]?.user_ids?.length ?? 0) > 100),
  };
}
export class Matrix {
  identity!: { user_id: string; device_id?: string };
  private replyHttp?: MatrixClient["http"];
  private stopped = false;
  constructor(readonly client: MatrixClient) {}
  static async connect(c: Config) {
    // Suppress SDK diagnostics: SDK errors can contain request URLs/headers or event content.
    const quiet = {
      trace() {},
      debug() {},
      info() {},
      warn() {},
      error() {},
      getChild() {
        return quiet;
      },
    };
    // Avoid createClient: its default scheduler retains send retry timers after stop.
    const client = new MatrixClient({
      store: new MemoryStore(),
      baseUrl: c.homeserver,
      accessToken: c.token,
      localTimeoutMs: 10000,
      logger: quiet,
    });
    const matrix = new Matrix(client);
    try {
      matrix.identity = await client.whoami();
      client.credentials.userId = matrix.identity.user_id;
      return matrix;
    } catch {
      client.stopClient();
      client.http.abort();
      throw new Error("Matrix identity lookup failed");
    }
  }
  room(id: string): Room {
    const r = this.client.getRoom(id);
    if (
      !r ||
      r.getMyMembership() !== "join" ||
      r.currentState.getStateEvents(EventType.RoomEncryption, "")
    )
      throw new Error("Room unavailable or encrypted");
    return r;
  }
  rooms() {
    const rooms = this.client
      .getRooms()
      .filter(
        (r) =>
          r.getMyMembership() === "join" &&
          !r.currentState.getStateEvents(EventType.RoomEncryption, ""),
      );
    return {
      rooms: rooms
        .slice(0, DISCOVERY)
        .map((r) => ({ room_id: r.roomId, name: clip(r.name, 255) })),
      truncated: rooms.length > DISCOVERY,
    };
  }
  async members(id: string) {
    const room = this.room(id);
    await room.loadMembersIfNeeded();
    this.room(id);
    const members = room.getJoinedMembers();
    return {
      members: members
        .slice(0, DISCOVERY)
        .map((m) => ({ user_id: m.userId, display_name: clip(m.name, 255) })),
      truncated: members.length > DISCOVERY,
    };
  }
  async send(
    id: string,
    body: string,
    reply?: string,
    mentions: string[] = [],
  ) {
    this.room(id);
    if (reply) {
      const e = await this.client.fetchRoomEvent(id, reply);
      if (e.event_id !== reply || (e.room_id && e.room_id !== id))
        throw new Error("Reply target unavailable");
    }
    this.room(id);
    const content: RoomMessageEventContent = {
      msgtype: MsgType.Text,
      body,
      "m.mentions": { user_ids: mentions },
      ...(reply
        ? { "m.relates_to": { "m.in_reply_to": { event_id: reply } } }
        : {}),
    };
    try {
      const result = await this.client.sendMessage(id, content);
      if (!result.event_id) throw new Error("Missing acknowledgment");
      return { event_id: result.event_id };
    } catch {
      throw new Error(
        "Send acknowledgment unknown; message may have been sent. Do not blindly resend.",
      );
    }
  }
  async read(id: string, limit: number, before?: string) {
    this.room(id);
    const page = await this.client.createMessagesRequest(
      id,
      before ?? null,
      limit,
      Direction.Backward,
    );
    const room = this.room(id);
    const events = (page.chunk.slice(0, limit) as Event[]).map((e) => ({
      ...e,
      room_id: e.room_id ?? id,
    }));
    let size = 0,
      truncated = false;
    const messages = events
      .map((e) => {
        const message = record(e, events);
        return message
          ? {
              ...message,
              sender_display_name: clip(
                room.getMember(e.sender ?? "")?.name,
                255,
              ),
            }
          : null;
      })
      .filter((e) => e !== null)
      .reverse()
      .filter((e) => {
        const n = JSON.stringify(e).length;
        if (size + n > TOTAL) {
          truncated = true;
          return false;
        }
        size += n;
        return true;
      });
    return {
      messages,
      next_batch: page.end && page.end !== page.start ? page.end : null,
      truncated: truncated || messages.some((e) => e.truncated),
      incomplete: true,
    };
  }
  private async replySender(roomId: string, eventId: string, signal: AbortSignal) {
    const room = this.room(roomId);
    let target: unknown = room.findEventById(eventId)?.event;
    if (!target) {
      try {
        // Only reply lookup uses this SDK HTTP adapter. Its inherited account
        // options authenticate with the shared client; sync/MCP transport stays intact.
        // The SDK detaches abort listeners at headers, so consume the body before
        // returning to keep its request abort/timeout active through JSON transfer.
        this.replyHttp ??= new MatrixHttpApi(this.client, {
          ...this.client.http.opts,
          fetchFn: (async (url: URL | RequestInfo, options?: RequestInit) => {
            const response = await this.client.http.fetch(
              url instanceof Request ? url.url : url,
              options,
            );
            return new Response(await response.arrayBuffer(), {
              status: response.status,
              statusText: response.statusText,
              headers: response.headers,
            });
          }) as typeof fetch,
        });
        target = await this.replyHttp.authedRequest(
          Method.Get,
          `/rooms/${encodeURIComponent(roomId)}/event/${encodeURIComponent(eventId)}`,
          {},
          undefined,
          { abortSignal: signal, localTimeoutMs: 2000 },
        );
      } catch {
        return undefined;
      }
    }
    if (!target || typeof target !== "object") return undefined;
    const event = target as Event;
    return (
      event.event_id === eventId &&
      (event.room_id === undefined || event.room_id === roomId) &&
      event.type === "m.room.message" &&
      event.state_key === undefined &&
      event.content !== null &&
      typeof event.content === "object" &&
      !Array.isArray(event.content) &&
      event.content?.["m.relates_to"]?.rel_type !== "m.replace" &&
      typeof event.sender === "string" &&
      event.sender.length <= 255 &&
      /^@[^\s:]+:[^\s]+$/u.test(event.sender)
    )
      ? event.sender
      : undefined;
  }
  observe(deliver: (value: WebhookItem, available: () => boolean) => void) {
    let live = false;
    const abort = new AbortController();
    const onSync = (
      state: SyncState,
      _prev: SyncState | null,
      data?: { catchingUp?: boolean },
    ) => {
      live = state === SyncState.Syncing && data?.catchingUp === false;
    };
    const onTimeline = (
      e: MatrixEvent,
      room: Room | undefined,
      toStart: boolean | undefined,
      removed: boolean,
      data: IRoomTimelineData,
    ) => {
      if (
        !live ||
        !room ||
        toStart ||
        removed ||
        !data?.liveEvent ||
        e.getSender() === this.identity.user_id
      )
        return;
      try {
        this.room(room.roomId);
      } catch {
        return;
      }
      const message = record(e.event as Event);
      if (!message || message.redacted) return;
      const {
        edited: _edited,
        redacted: _redacted,
        incomplete: _incomplete,
        ...fields
      } = message;
      const value = {
        type: "message",
        room_id: room.roomId,
        sender_display_name: clip(
          room.getMember(e.getSender() ?? "")?.name,
          255,
        ),
        ...fields,
      };
      const originalReply = e.event.content?.["m.relates_to"]?.["m.in_reply_to"]?.event_id;
      // Only queue work here; SDK sync remains free to apply later room state.
      const available = () => {
        if (abort.signal.aborted || this.stopped) return false;
        try { this.room(room.roomId); return true; } catch { return false; }
      };
      deliver(async (signal) => {
        const combined = AbortSignal.any([signal, abort.signal]);
        if (combined.aborted || this.stopped) return null;
        try {
          this.room(room.roomId);
          const sender =
            fields.reply_to_event_id && originalReply === fields.reply_to_event_id
              ? await this.replySender(
                  room.roomId,
                  fields.reply_to_event_id,
                  AbortSignal.any([combined, AbortSignal.timeout(2000)]),
                )
              : undefined;
          if (combined.aborted || this.stopped) return null;
          this.room(room.roomId);
          return { ...value, ...(sender ? { reply_to_sender_id: sender } : {}) };
        } catch {
          return null;
        }
      }, available);
    };
    this.client.on(ClientEvent.Sync, onSync);
    this.client.on(RoomEvent.Timeline, onTimeline);
    return () => {
      abort.abort();
      this.client.removeListener(ClientEvent.Sync, onSync);
      this.client.removeListener(RoomEvent.Timeline, onTimeline);
    };
  }
  stop() {
    this.stopped = true;
    this.replyHttp?.abort();
    this.client.stopClient();
    this.client.http.abort();
  }
}
