import { test, expect } from "bun:test";
import { createServer, type ServerResponse } from "node:http";
import { ClientEvent, SyncState, MsgType } from "matrix-js-sdk";
import { Matrix } from "../src/matrix.js";
import { Webhook } from "../src/webhook.js";
const wait = async (fn: () => boolean) => {
  for (let n = 0; n < 400; n++) {
    if (fn()) return;
    await Bun.sleep(10);
  }
  throw new Error("sync timeout");
};
test("real Matrix SDK startup/reconnect catch-up never reaches local webhook", async () => {
  const received: any[] = [],
    auth: string[] = [],
    pending: ServerResponse[] = [];
  let syncs = 0;
  const state = [
    {
      type: "m.room.member",
      state_key: "@self:test",
      sender: "@self:test",
      event_id: "$member",
      content: { membership: "join" },
    },
    {
      type: "m.room.member",
      state_key: "@other:test",
      sender: "@other:test",
      event_id: "$other",
      content: { membership: "join", displayname: "Other" },
    },
    {
      type: "m.room.create",
      state_key: "",
      sender: "@self:test",
      event_id: "$create",
      content: { creator: "@self:test", room_version: "10" },
    },
  ];
  const event = (
    id: string,
    sender = "@other:test",
    content: any = { msgtype: "m.text", body: id },
  ) => ({
    type: "m.room.message",
    event_id: id,
    sender,
    origin_server_ts: Date.now(),
    content,
  });
  const batch = (id: string, initial = false) => ({
    next_batch: id,
    rooms: {
      join: {
        "!room:test": {
          state: { events: initial ? state : [] },
          timeline: {
            events: [event(id)],
            prev_batch: "history",
            limited: false,
          },
          ephemeral: { events: [] },
          account_data: { events: [] },
        },
      },
    },
  });
  const home = createServer(async (req, res) => {
    const u = new URL(req.url!, "http://localhost");
    const send = (v: unknown, status = 200) => {
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify(v));
    };
    if (u.pathname.endsWith("/versions"))
      send({ versions: ["v1.11"], unstable_features: {} });
    else if (u.pathname.endsWith("/whoami"))
      send({ user_id: "@self:test", device_id: "TEST" });
    else if (u.pathname.endsWith("/pushrules/"))
      send({
        global: {
          override: [],
          content: [],
          room: [],
          sender: [],
          underride: [],
        },
      });
    else if (u.pathname.endsWith("/filter")) send({ filter_id: "test-filter" });
    else if (u.pathname.endsWith("/capabilities")) send({ capabilities: {} });
    else if (u.pathname.endsWith("/sync")) {
      syncs++;
      if (syncs === 1) send(batch("$startup", true));
      else pending.push(res);
    } else if (u.pathname.endsWith("/messages"))
      send({ start: "history", end: "end", chunk: [event("$history")] });
    else send({});
  });
  const receiver = createServer(async (req, res) => {
    let body = "";
    for await (const c of req) body += c;
    auth.push(req.headers.authorization ?? "");
    received.push(JSON.parse(body));
    res.writeHead(204).end();
  });
  for (const server of [home, receiver])
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const url = (s: typeof home) =>
    `http://127.0.0.1:${(s.address() as any).port}`;
  const matrix = await Matrix.connect({
    homeserver: url(home),
    token: "fake-account-token",
    host: "127.0.0.1",
    port: 0,
  });
  const webhook = new Webhook(url(receiver));
  const unobserve = matrix.observe((v) => webhook.push(v));
  const states: string[] = [];
  matrix.client.on(ClientEvent.Sync, (s) => states.push(s));
  const respond = async (value: unknown, status = 200) => {
    await wait(() => pending.length > 0);
    const r = pending.shift()!;
    r.writeHead(status, { "content-type": "application/json" });
    r.end(JSON.stringify(value));
  };
  try {
    await matrix.client.startClient({ initialSyncLimit: 0 });
    await wait(() => states.includes(SyncState.Syncing));
    expect(received).toEqual([]);
    await respond(batch("$live"));
    await wait(() => received.length === 1);
    expect(received[0].event_id).toBe("$live");
    expect(auth).toEqual([""]);
    await matrix.read("!room:test", 20);
    expect(received.length).toBe(1);
    await respond({ errcode: "M_UNKNOWN", error: "fake outage" }, 500);
    await wait(() => states.includes(SyncState.Reconnecting));
    matrix.client.retryImmediately();
    await respond(batch("$offline"));
    await wait(() => pending.length > 0);
    expect(received.length).toBe(1);
    const live = batch("$after");
    live.rooms.join["!room:test"].timeline.events.push(
      event("$self", "@self:test"),
      event("$thread", "@other:test", {
        msgtype: "m.text",
        body: "thread text",
        "m.relates_to": { rel_type: "m.thread", event_id: "$live" },
      }),
      event("$edit", "@other:test", {
        msgtype: "m.text",
        body: "* edited",
        "m.relates_to": { rel_type: "m.replace", event_id: "$live" },
        "m.new_content": { msgtype: "m.text", body: "edited" },
      }),
      event("$media", "@other:test", { msgtype: "m.image", body: "image" }),
    );
    await respond(live);
    await wait(() => received.length === 3);
    expect(received.map((e) => e.event_id)).toEqual([
      "$live",
      "$after",
      "$thread",
    ]);
    expect(received[2]).not.toHaveProperty("thread_id");
    const encrypted = batch("$private");
    encrypted.rooms.join["!room:test"].state.events.push({
      type: "m.room.encryption",
      state_key: "",
      sender: "@self:test",
      event_id: "$encryption",
      content: { algorithm: "m.megolm.v1.aes-sha2" },
    } as any);
    await respond(encrypted);
    await wait(() => pending.length > 0);
    expect(received.length).toBe(3);
    expect(matrix.rooms().rooms).toEqual([]);
  } finally {
    unobserve();
    matrix.stop();
    await webhook.stop();
    for (const s of [home, receiver]) {
      s.closeAllConnections();
      await new Promise<void>((r) => s.close(() => r()));
    }
  }
}, 15000);

test("real Matrix SDK never retries a failed send after stop", async () => {
  let sends = 0;
  let firstResponse!: () => void;
  const responded = new Promise<void>((resolve) => {
    firstResponse = resolve;
  });
  const home = createServer((req, res) => {
    res.setHeader("content-type", "application/json");
    if (req.url?.endsWith("/whoami")) {
      res.end(JSON.stringify({ user_id: "@self:test", device_id: "TEST" }));
    } else if (
      req.method === "PUT" &&
      req.url?.includes("/send/m.room.message/")
    ) {
      sends++;
      if (sends === 1) {
        res.writeHead(503, { "retry-after": "1" });
        res.end(
          JSON.stringify({ errcode: "M_UNKNOWN", error: "temporary failure" }),
          firstResponse,
        );
      } else {
        res.end(JSON.stringify({ event_id: "$late" }));
      }
    } else {
      res.writeHead(404).end(JSON.stringify({ errcode: "M_NOT_FOUND" }));
    }
  });
  await new Promise<void>((resolve) => home.listen(0, "127.0.0.1", resolve));
  const matrix = await Matrix.connect({
    homeserver: `http://127.0.0.1:${(home.address() as any).port}`,
    token: "fake-account-token",
    host: "127.0.0.1",
    port: 0,
  });
  // Exercise the real SDK send path without starting an unrelated sync loop.
  const sending = matrix.client
    .sendMessage("!room:test", { msgtype: MsgType.Text, body: "hello" })
    .then(
      () => "acknowledged",
      () => "failed",
    );
  try {
    await responded;
    // Let the SDK consume the 503 and enter retry backoff when enabled.
    await Bun.sleep(50);
    matrix.stop();
    const sendsAtStop = sends;
    await Bun.sleep(2300);
    expect(sendsAtStop).toBe(1);
    expect(sends).toBe(sendsAtStop);
    expect(await sending).toBe("failed");
  } finally {
    matrix.stop();
    home.closeAllConnections();
    await new Promise<void>((resolve) => home.close(() => resolve()));
  }
}, 5000);
