import { test, expect } from "bun:test";
import { createServer, type ServerResponse } from "node:http";
import { mkdir, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { resolve, join } from "node:path";
import { ClientEvent, SyncState } from "matrix-js-sdk";
import { Matrix } from "../src/matrix.js";
import { Webhook } from "../src/webhook.js";

const roomId = "!room:test";
const until = async (fn: () => boolean) => {
  for (let i = 0; i < 400; i++) { if (fn()) return; await Bun.sleep(10); }
  throw new Error("DM fixture timeout");
};
const event = (id: string, sender = "@allowed:test", content: any = {}) => ({
  type: "m.room.message", event_id: id, sender, origin_server_ts: 1700000000000,
  content: { msgtype: "m.text", body: `ordinary text ${id}`, ...content },
});
async function fixture(identity = "@self:test", initialDirect: unknown = { "@allowed:test": [roomId] }) {
  const pending: ServerResponse[] = [], held: ServerResponse[] = [], raw: string[] = [];
  let sequence = 0, lookup = false, retry = false;
  const batch = (events: any[], direct?: unknown, state: any[] = []) => ({
    next_batch: String(++sequence),
    ...(direct === undefined ? {} : { account_data: { events: [{ type: "m.direct", content: direct }] } }),
    rooms: { join: { [roomId]: { state: { events: state },
      timeline: { events, limited: false, prev_batch: "old" },
      ephemeral: { events: [] }, account_data: { events: [] } } } },
  });
  const send = (res: ServerResponse, value: unknown) =>
    res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(value));
  const home = createServer((req, res) => {
    const path = new URL(req.url!, "http://localhost").pathname;
    if (path.endsWith("/versions")) send(res, { versions: ["v1.11"], unstable_features: {} });
    else if (path.endsWith("/whoami")) send(res, { user_id: identity, device_id: "TEST" });
    else if (path.endsWith("/pushrules/")) send(res, { global: { override: [], content: [], room: [], sender: [], underride: [] } });
    else if (path.endsWith("/filter")) send(res, { filter_id: "dm-fixture" });
    else if (path.endsWith("/capabilities")) send(res, { capabilities: {} });
    else if (path.endsWith("/sync")) {
      if (!sequence) send(res, batch([event("$startup"), event("$parent", identity)], initialDirect,
        [identity, "@allowed:test", "@denied:test"].map((user, i) => ({
          type: "m.room.member", state_key: user, sender: user, event_id: `$member${i}`,
          content: { membership: "join", displayname: user === "@allowed:test" ? "Allowed" : user },
        }))));
      else pending.push(res);
    } else if (path.includes("/event/")) { lookup = true; held.push(res); }
    else send(res, {});
  });
  const receiver = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    raw.push(Buffer.concat(chunks).toString("utf8"));
    res.writeHead(retry ? 503 : 204).end(); retry = false;
  });
  for (const server of [home, receiver]) await new Promise<void>(r => server.listen(0, "127.0.0.1", r));
  const url = (s: typeof home) => `http://127.0.0.1:${(s.address() as any).port}`;
  const matrix = await Matrix.connect({ homeserver: url(home), token: "synthetic-dm-token" });
  const webhook = new Webhook(url(receiver), undefined, () => {}, { capacity: 100, timeout: 1000, attempts: 3, delay: 100 });
  const unobserve = matrix.observe((v, available) => webhook.push(v, available));
  let live = false;
  matrix.client.on(ClientEvent.Sync, s => { if (s === SyncState.Syncing) live = true; });
  const close = async () => {
    unobserve(); matrix.stop(); await webhook.stop();
    for (const s of [home, receiver]) {
      s.closeAllConnections(); await new Promise<void>(r => s.close(() => r()));
    }
  };
  try { await matrix.start(); await until(() => live && pending.length > 0); }
  catch (error) { await close(); throw error; }
  return { matrix, raw, held, close, retry() { retry = true; }, lookup: () => lookup,
    async leave() {
      await until(() => pending.length > 0);
      send(pending.shift()!, { next_batch: String(++sequence), rooms: { leave: { [roomId]: {
        state: { events: [{ type: "m.room.member", event_id: "$leave", sender: identity,
          state_key: identity, content: { membership: "leave" } }] }, timeline: { events: [], limited: false },
      } } } });
      await until(() => pending.length > 0);
    },
    async emit(events: any[], direct?: unknown, state: any[] = []) {
      await until(() => pending.length > 0); send(pending.shift()!, batch(events, direct, state));
      await until(() => pending.length > 0);
    },
  };
}

test("SDK direct facts use initial and same-sync account data, ignore heuristics and isolate identities", async () => {
  const a = await fixture(), b = await fixture("@second:test", {});
  try {
    expect(a.raw).toEqual([]); expect(b.raw).toEqual([]);
    expect(a.matrix.room(roomId).getJoinedMembers().length).toBe(3);
    await a.emit([event("$allowed"), event("$denied", "@denied:test", {
      body: "DENIED_DM_SENTINEL agent", "m.mentions": { user_ids: ["@self:test"] },
      "m.relates_to": { "m.in_reply_to": { event_id: "$parent" } },
    })]);
    await b.emit([event("$allowed")]);
    await until(() => a.raw.length === 2 && b.raw.length === 1);
    await a.emit([event("$removed")], {});
    await a.emit([event("$marked")], { "@allowed:test": [roomId] });
    await until(() => a.raw.length === 4);
    expect(a.raw.map(s => JSON.parse(s).conversation_type)).toEqual(["dm", "dm", "room", "dm"]);
    expect(JSON.parse(b.raw[0]).conversation_type).toBe("room");
    expect(JSON.parse(a.raw[0])).toMatchObject({ sender_display_name: "Allowed", body: "ordinary text $allowed", timestamp: 1700000000000, mentions: [], truncated: false });
    for (const direct of [{ "@allowed:test": roomId }, { "@allowed:test": null }, [], "bad", null]) {
      const count = a.raw.length;
      await a.emit([event(`$malformed${count}`)], direct);
      await until(() => a.raw.length > count);
      expect(JSON.parse(a.raw.at(-1)!).conversation_type).toBe("room");
    }
    if (process.env.MFA_DM_EXPORT_DIR) {
      const directory = resolve(process.env.MFA_DM_EXPORT_DIR);
      await mkdir(directory, { recursive: true });
      const cases = [];
      for (const [name, body, expected] of [
        ["allowed-dm", a.raw[0], "dm"], ["denied-addressed-dm", a.raw[1], "dm"],
        ["unmarked-room", a.raw[2], "room"], ["other-identity-room", b.raw[0], "room"],
      ] as const) {
        await writeFile(join(directory, `${name}.json`), body);
        cases.push({ name, file: `${name}.json`, bytes: Buffer.byteLength(body), sha256: createHash("sha256").update(body).digest("hex"),
          receiver_identity: name === "other-identity-room" ? "@second:test" : "@self:test", conversation_type: expected,
          sender_id: JSON.parse(body).sender_id, event_id: JSON.parse(body).event_id, room_id: roomId });
      }
      await writeFile(join(directory, "manifest.json"), JSON.stringify({ spec: 3664, receiver_allow_list: ["@allowed:test"],
        provenance: "actual matrix-js-sdk fake-homeserver sync -> Matrix.observe -> Webhook -> owned HTTP receiver; original UTF-8 request bodies", cases }, null, 2));
      console.log(`DM_FIXTURES=${directory}`);
    }
  } finally { await a.close(); await b.close(); }
});

test("queued reply enrichment and retry bytes retain admission-time direct facts", async () => {
  const f = await fixture();
  try {
    f.retry();
    await f.emit([event("$delayed", "@allowed:test", { "m.relates_to": { "m.in_reply_to": { event_id: "$cold" } } }), event("$queued")]);
    await until(f.lookup);
    await f.emit([], {});
    f.held[0].end(JSON.stringify(event("$cold", "@self:test")));
    await until(() => f.raw.length === 1);
    await f.emit([event("$later")]);
    await until(() => f.raw.length === 4);
    expect(f.raw[0]).toBe(f.raw[1]);
    expect(f.raw.map(s => JSON.parse(s).conversation_type)).toEqual(["dm", "dm", "dm", "room"]);
    expect(JSON.parse(f.raw[0]).reply_to_sender_id).toBe("@self:test");
  } finally { await f.close(); }
});

test("direct marking cannot bypass self/media/edit/encryption or joined-room admission", async () => {
  const f = await fixture();
  try {
    await f.emit([event("$self", "@self:test"), event("$media", "@allowed:test", { msgtype: "m.image" }),
      event("$edit", "@allowed:test", { "m.relates_to": { rel_type: "m.replace", event_id: "$old" } })]);
    expect(f.raw).toEqual([]);
    await f.leave();
    expect(f.matrix.rooms().rooms).toEqual([]);
    expect(() => f.matrix.room(roomId)).toThrow("Room unavailable or encrypted");
    expect(f.raw).toEqual([]);
    await f.emit([event("$encrypted")], undefined, [{ type: "m.room.member", event_id: "$join", sender: "@self:test",
      state_key: "@self:test", content: { membership: "join" } }, { type: "m.room.encryption", state_key: "", sender: "@self:test",
      event_id: "$encryption", content: { algorithm: "m.megolm.v1.aes-sha2" } }]);
    expect(f.raw).toEqual([]);
  } finally { await f.close(); }
});
