import { test, expect } from "bun:test";
import { createServer, type ServerResponse } from "node:http";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { ClientEvent, SyncState } from "matrix-js-sdk";
import { Matrix } from "../src/matrix.js";
import { Webhook } from "../src/webhook.js";

const until = async (fn: () => boolean) => {
  for (let n = 0; n < 400; n++) {
    if (fn()) return;
    await Bun.sleep(10);
  }
  throw new Error("fixture timeout");
};
const roomId = "!room:test";
const event = (id: string, sender = "@other:test", content: any = {}) => ({
  type: "m.room.message", event_id: id, sender, origin_server_ts: 1700000000000,
  content: { msgtype: "m.text", body: "ordinary reply", ...content },
});
const reply = (id: string, target: string) => event(id, "@other:test", {
  "m.relates_to": { "m.in_reply_to": { event_id: target } },
});
async function fixture() {
  const pending: ServerResponse[] = [], lookups: string[] = [],
    lookupAuth: string[] = [], raw: string[] = [], receiverAuth: string[] = [],
    diagnostics: string[] = [], closed: string[] = [];
  const targets = new Map<string, unknown>();
  const signals = new Map<string, AbortSignal>();
  const held = new Map<string, ServerResponse>();
  const heldPosts: ServerResponse[] = [];
  let syncs = 0, retryFirst = false, holdPost = false;
  const batch = (events: any[] = [], state: any[] = []) => ({
    next_batch: String(++syncs), rooms: { join: { [roomId]: {
      state: { events: state }, timeline: { events, limited: false, prev_batch: "old" },
      ephemeral: { events: [] }, account_data: { events: [] },
    } } },
  });
  const send = (res: ServerResponse, value: unknown, status = 200) =>
    res.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(value));
  const home = createServer((req, res) => {
    const path = new URL(req.url!, "http://localhost").pathname;
    if (path.endsWith("/versions")) send(res, { versions: ["v1.11"], unstable_features: {} });
    else if (path.endsWith("/whoami")) send(res, { user_id: "@self:test", device_id: "TEST" });
    else if (path.endsWith("/pushrules/"))
      send(res, { global: { override: [], content: [], room: [], sender: [], underride: [] } });
    else if (path.endsWith("/filter")) send(res, { filter_id: "fixture" });
    else if (path.endsWith("/capabilities")) send(res, { capabilities: {} });
    else if (path.endsWith("/sync")) {
      if (syncs === 0) send(res, batch([
        event("$own", "@self:test"), event("$nonown", "@third:test"),
        event("$redacted", "@self:test", {}),
        reply("$startup", "$cold-startup"),
      ], [{ type: "m.room.member", event_id: "$member", sender: "@self:test",
        state_key: "@self:test", content: { membership: "join" } }]));
      else pending.push(res);
    } else if (path.includes("/event/")) {
      const id = decodeURIComponent(path.split("/").at(-1)!);
      lookups.push(id); lookupAuth.push(req.headers.authorization ?? "");
      res.on("close", () => closed.push(id));
      if (id.startsWith("$hang")) {
        held.set(id, res);
        if (id === "$hang-body") {
          res.writeHead(200, { "content-type": "application/json" });
          res.write('{"sender":');
        }
      } else if (targets.has(id)) send(res, targets.get(id));
      else send(res, { errcode: "M_NOT_FOUND", error: "fake-account-token secret upstream diagnostic" }, 404);
    } else send(res, {});
  });
  const receiver = createServer(async (req, res) => {
    let body = "";
    for await (const chunk of req) body += chunk;
    raw.push(body); receiverAuth.push(req.headers.authorization ?? "");
    if (holdPost) { heldPosts.push(res); return; }
    res.writeHead(retryFirst && raw.length === 1 ? 503 : 204).end();
  });
  for (const server of [home, receiver])
    await new Promise<void>(r => server.listen(0, "127.0.0.1", r));
  const url = (server: typeof home) => `http://127.0.0.1:${(server.address() as any).port}`;
  const matrix = await Matrix.connect({ homeserver: url(home), token: "fake-account-token", host: "127.0.0.1", port: 0 });
  const fetchFn = matrix.client.http.opts.fetchFn ?? fetch;
  matrix.client.http.opts.fetchFn = (async (url: URL | RequestInfo, options?: RequestInit) => {
    const path = new URL(url instanceof Request ? url.url : url).pathname;
    if (path.includes("/event/") && options?.signal)
      signals.set(decodeURIComponent(path.split("/").at(-1)!), options.signal);
    return fetchFn(url, options);
  }) as typeof fetch;
  const webhook = new Webhook(url(receiver), "fake-receiver-token", s => diagnostics.push(s),
    { capacity: 100, timeout: 5000, attempts: 3, delay: 10 });
  const unobserve = matrix.observe((v, available) => webhook.push(v, available));
  let live = false;
  matrix.client.on(ClientEvent.Sync, state => { if (state === SyncState.Syncing) live = true; });
  const close = async () => {
    unobserve(); matrix.stop(); await webhook.stop();
    for (const server of [home, receiver]) {
      server.closeAllConnections();
      await new Promise<void>(r => server.close(() => r()));
    }
  };
  try {
    await matrix.client.startClient({ initialSyncLimit: 0 });
    await until(() => live && pending.length > 0);
  } catch (error) { await close(); throw error; }
  const respond = async (value: unknown) => {
    await until(() => pending.length > 0);
    send(pending.shift()!, value);
    await until(() => pending.length > 0);
  };
  return {
    matrix, webhook, raw, lookups, lookupAuth, receiverAuth, diagnostics, closed, held, targets, signals, heldPosts,
    retry() { retryFirst = true; },
    holdDelivery() { holdPost = true; },
    emit: async (events: any[], state: any[] = []) => respond(batch(events, state)),
    leave: async () => respond({ next_batch: String(++syncs), rooms: { leave: { [roomId]: {
      state: { events: [{ type: "m.room.member", event_id: "$leave", sender: "@self:test", state_key: "@self:test", content: { membership: "leave" } }] },
      timeline: { events: [], limited: false },
    } } } }),
    async stop() { unobserve(); matrix.stop(); await webhook.stop(); },
    close,
  };
}

test("real SDK verifies cached/cold reply facts, freezes retry bytes and exports actual envelopes", async () => {
  const f = await fixture();
  try {
    expect(f.raw).toEqual([]); expect(f.lookups).toEqual([]);
    f.retry();
    f.targets.set("$cold-own", { ...event("$cold-own", "@self:test"), room_id: roomId });
    f.targets.set("$cold-nonown", event("$cold-nonown", "@third:test"));
    await f.emit([reply("$reply-own", "$own"), reply("$reply-nonown", "$nonown"),
      reply("$reply-unknown", "$missing"), reply("$reply-cold-own", "$cold-own"),
      reply("$reply-cold-nonown", "$cold-nonown")]);
    await until(() => f.raw.length === 6);
    const values = f.raw.map(s => JSON.parse(s));
    expect(f.raw[0]).toBe(f.raw[1]);
    expect(values.map(v => v.reply_to_sender_id)).toEqual([
      "@self:test", "@self:test", "@third:test", undefined, "@self:test", "@third:test",
    ]);
    expect(f.lookups).toEqual(["$missing", "$cold-own", "$cold-nonown"]);
    expect(f.lookupAuth.every(v => v === "Bearer fake-account-token")).toBe(true);
    expect(f.receiverAuth.every(v => v === "Bearer fake-receiver-token")).toBe(true);
    expect(f.raw.join("")).not.toContain("token");
    expect(f.raw.join("")).not.toContain("diagnostic");
    if (process.env.MFA_EXPORT_REPLY_FIXTURES === "1") {
      const directory = await mkdtemp(join(tmpdir(), "matrix-reply-wake-"));
      const cases = [];
      for (const [name, index, wake] of [["own", 0, true], ["nonown", 2, false], ["unknown", 3, false]] as const) {
        const body = f.raw[index];
        await writeFile(join(directory, `${name}.json`), body);
        cases.push({ name, file: `${name}.json`, sha256: createHash("sha256").update(body).digest("hex"),
          bytes: Buffer.byteLength(body), expected_reply_wake: wake, event_id: values[index].event_id,
          room_id: values[index].room_id, timestamp: values[index].timestamp });
      }
      await writeFile(join(directory, "manifest.json"), JSON.stringify({ spec: 3609,
        receiver_identity: "@self:test", provenance: "real matrix-js-sdk sync -> Matrix.observe -> Webhook -> test-owned local HTTP captured bytes", cases }, null, 2));
      console.log(`REPLY_FIXTURES=${directory}`);
    }
  } finally { await f.close(); }
});

test("real SDK omits invalid facts and preserves redacted target authors", async () => {
  const f = await fixture();
  try {
    const invalid = [null, "malformed", { ...event("$wrong-id", "@self:test") },
      { ...event("$target3", "@self:test"), room_id: "!other:test" },
      { ...event("$target4"), type: "m.room.member" },
      event("$target5", "@self:test", { "m.relates_to": { rel_type: "m.replace" } }),
      event("$target6", "@has space:test"), event("$target7", `@${"x".repeat(255)}:test`),
      { ...event("$target8"), room_id: null }, event("$target9", "display name"),
      { ...event("$target10", "@self:test"), content: undefined },
      { ...event("$target11", "@self:test"), content: null },
      { ...event("$target12", "@self:test"), content: "malformed" },
      { ...event("$target13", "@self:test"), content: [] }];
    invalid.forEach((v, i) => f.targets.set(`$target${i}`, v));
    f.targets.set("$valid-redacted", { ...event("$valid-redacted", "@self:test"), content: {}, unsigned: { redacted_because: {} } });
    await f.emit([...invalid.map((_, i) => reply(`$invalid${i}`, `$target${i}`)),
      reply("$reply-redacted", "$valid-redacted"), reply("$empty-target", ""),
      reply("$clipped-target", "$".repeat(256))]);
    await until(() => f.raw.length === invalid.length + 3);
    const values = f.raw.map(s => JSON.parse(s));
    expect(values.slice(10, 14).map(v => v.reply_to_sender_id)).toEqual(Array(4).fill(undefined));
    expect(values.slice(0, invalid.length).every(v => !("reply_to_sender_id" in v))).toBe(true);
    expect(values.at(-3).reply_to_sender_id).toBe("@self:test");
    expect(values.at(-2)).not.toHaveProperty("reply_to_sender_id");
    expect(values.at(-1)).not.toHaveProperty("reply_to_sender_id");
    expect(f.lookups).not.toContain("");
  } finally { await f.close(); }
});

test("real SDK enrichment is serial, bounded and nonblocking during timeout/overflow", async () => {
  const f = await fixture();
  try {
    await f.emit([reply("$slow", "$hang-timeout")]);
    await until(() => f.lookups.length === 1);
    const started = Date.now();
    await f.emit(Array.from({ length: 101 }, (_, i) => reply(`$queued${i}`, "$own")));
    expect(Date.now() - started).toBeLessThan(1000);
    expect(f.lookups).toEqual(["$hang-timeout"]);
    await until(() => f.raw.length === 100);
    expect(f.raw.map(s => JSON.parse(s).event_id)).toEqual(["$slow", ...Array.from({ length: 99 }, (_, i) => `$queued${i}`)]);
    expect(JSON.parse(f.raw[0])).not.toHaveProperty("reply_to_sender_id");
    expect(f.signals.get("$hang-timeout")?.aborted).toBe(true);
    if (!process.versions.bun) await until(() => f.closed.includes("$hang-timeout"));
    expect(f.diagnostics).toEqual(Array(2).fill("Webhook queue overflow: newest message dropped"));
  } finally { await f.close(); }
}, 8000);

test("real SDK room admission also prevents retries after leave", async () => {
  const f = await fixture();
  try {
    f.holdDelivery();
    await f.emit([reply("$posted", "$own"), reply("$pending", "$own")]);
    await until(() => f.heldPosts.length === 1);
    await f.leave();
    f.heldPosts[0].writeHead(503).end();
    await Bun.sleep(100);
    expect(f.raw.length).toBe(1);
    expect(f.diagnostics).toEqual([]);
  } finally { await f.close(); }
});

for (const transition of ["encryption", "leave", "stop", "body-stop", "body-timeout"] as const) {
  test(`real SDK cancels or suppresses delayed enrichment: ${transition}`, async () => {
    const f = await fixture();
    try {
      const target = transition.startsWith("body") ? "$hang-body" : "$hang-state";
      await f.emit([reply("$delayed", target), reply("$later", "$own")]);
      await until(() => f.held.has(target));
      if (transition === "body-timeout") {
        await until(() => f.raw.length === 2);
        expect(f.signals.get(target)?.aborted).toBe(true);
        if (!process.versions.bun) await until(() => f.closed.includes(target));
        expect(JSON.parse(f.raw[0])).not.toHaveProperty("reply_to_sender_id");
      } else if (transition === "stop" || transition === "body-stop") {
        await f.stop();
        expect(f.signals.get(target)?.aborted).toBe(true);
        if (!process.versions.bun) await until(() => f.closed.includes(target));
        await Bun.sleep(100);
        expect(f.raw).toEqual([]);
      } else {
        if (transition === "leave") await f.leave();
        else await f.emit([], [{ type: "m.room.encryption", state_key: "", event_id: "$encrypted", sender: "@self:test", content: { algorithm: "m.megolm.v1.aes-sha2" } }]);
        const response = f.held.get(target)!;
        response.end(JSON.stringify(event(target, "@self:test")));
        await until(() => f.closed.includes(target));
        await Bun.sleep(100);
        expect(f.raw).toEqual([]);
      }
    } finally { await f.close(); }
  }, 8000);
}
