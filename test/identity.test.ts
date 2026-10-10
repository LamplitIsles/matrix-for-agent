import { expect, test } from "bun:test";
import {
  createServer,
  type ServerResponse,
  type RequestListener,
} from "node:http";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  ClientEvent,
  SyncState,
  TypedEventEmitter,
  type MatrixClient,
} from "matrix-js-sdk";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { config, parseWebhooks, loadWebhooks, bearer } from "../src/config.js";
import { Matrix } from "../src/matrix.js";
import { Registry, authenticate } from "../src/registry.js";
import { listen } from "../src/mcp.js";
async function endpoint(handler: RequestListener) {
  const server = createServer(handler);
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  return {
    server,
    url: `http://127.0.0.1:${(server.address() as any).port}`,
    async close() {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}
async function until(fn: () => boolean) {
  for (let n = 0; n < 300; n++) {
    if (fn()) return;
    await Bun.sleep(5);
  }
  throw new Error("fixture timeout");
}
const bounds = {
  capacity: 16,
  admission: 16,
  idle: 300000,
  sweep: 30000,
  readiness: 1000,
  auth: 1000,
};
const entry = {
  user_id: "@a:test",
  access_token: "a",
  url: "http://127.0.0.1:1",
  bearer_token: "receiver-only",
};
test("closed bounded private JSON config and bearer parsing", async () => {
  expect(
    config({ MATRIX_HOMESERVER_URL: "https://matrix.test" }).webhookConfig,
  ).toBeUndefined();
  expect(parseWebhooks("[]")).toEqual([]);
  expect(parseWebhooks(JSON.stringify([entry]))).toEqual([
    { ...entry, url: entry.url + "/" },
  ]);
  for (const bad of [
    {},
    [null],
    [{ ...entry, extra: true }],
    [{ ...entry, user_id: "a" }],
    [{ ...entry, user_id: "@" + "a".repeat(256) + ":test" }],
    [{ ...entry, url: "http://external.test" }],
    [{ ...entry, url: "https://u:p@test" }],
    [{ ...entry, url: "https://test/#x" }],
    [{ ...entry, access_token: " " }],
    [{ ...entry, access_token: "x".repeat(16385) }],
    [{ ...entry, bearer_token: "\nsecret" }],
    [{ ...entry, bearer_token: "" }],
    [{ ...entry, access_token: "é" }],
    [entry, { ...entry, access_token: "b" }],
    [entry, { ...entry, user_id: "@b:test" }],
    Array(17).fill(entry),
  ])
    expect(() => parseWebhooks(JSON.stringify(bad))).toThrow(
      "Invalid webhook configuration",
    );
  expect(() => parseWebhooks(" ".repeat(1048577))).toThrow();
  for (const header of [
    undefined,
    "Basic a",
    "Bearer a b",
    "Bearer a,Bearer b",
    "Bearer  a",
    "Bearer ",
    "Bearer é",
    "Bearer " + "a".repeat(16385),
  ])
    expect(bearer(header)).toBeUndefined();
  expect(bearer("bearer a")).toBe("a");
  const dir = await mkdtemp(join(tmpdir(), "mfa-config-"));
  try {
    const path = join(dir, "config.json");
    expect(await loadWebhooks()).toEqual([]);
    await writeFile(path, JSON.stringify([entry]), { mode: 0o600 });
    expect(await loadWebhooks(path)).toHaveLength(1);
    for (const value of [
      Buffer.from([0xff]),
      Buffer.alloc(1048577, 32),
      Buffer.from("secret-malformed"),
    ]) {
      await writeFile(path, value);
      await expect(loadWebhooks(path)).rejects.toThrow(
        "Invalid or unreadable webhook configuration",
      );
    }
    await expect(loadWebhooks(join(dir, "absent"))).rejects.toThrow(
      "Invalid or unreadable webhook configuration",
    );
  } finally {
    await rm(dir, { recursive: true });
  }
});
// Actual SDK sync/history/send and public MCP requests, all token-scoped by an
// owned fake homeserver. Tokens c and d deliberately have the same Matrix user.
async function home() {
  const users: Record<string, string> = {
    a: "@a:test",
    b: "@b:test",
    c: "@same:test",
    d: "@same:test",
  };
  const revoked = new Set<string>();
  const pending = new Map<string, ServerResponse[]>();
  const initial = new Map<string, number>();
  const sends: { token: string; room: string; content: unknown }[] = [];
  let outage = false,
    holdAuth = false,
    holdInitial = false,
    holdVersions = false,
    requests = 0,
    auths = 0;
  const versionPending: ServerResponse[] = [];
  const authPending: ServerResponse[] = [];
  const event = (id: string, token: string, reply = false) => ({
    type: "m.room.message",
    event_id: id,
    sender: "@other:test",
    origin_server_ts: 1,
    content: {
      msgtype: "m.text",
      body: token,
      "m.mentions": { user_ids: [] },
      ...(reply
        ? { "m.relates_to": { "m.in_reply_to": { event_id: "$parent" } } }
        : {}),
    },
  });
  const batch = (token: string, id: string, first = false, reply = false) => ({
    next_batch: id,
    rooms: {
      join: {
        [`!${token}:test`]: {
          state: {
            events: first
              ? [
                  {
                    type: "m.room.create",
                    state_key: "",
                    sender: users[token],
                    event_id: "$create",
                    content: { creator: users[token], room_version: "10" },
                  },
                  ...[users[token], "@other:test"].map((user, i) => ({
                    type: "m.room.member",
                    state_key: user,
                    sender: user,
                    event_id: "$m" + i,
                    content: { membership: "join", displayname: user },
                  })),
                ]
              : [],
          },
          timeline: {
            events: [event(id, token, reply)],
            limited: false,
            prev_batch: "history",
          },
          ephemeral: { events: [] },
          account_data: { events: [] },
        },
      },
    },
  });
  const e = await endpoint(async (req, res) => {
    requests++;
    const token = req.headers.authorization?.slice(7) ?? "";
    const u = new URL(req.url!, "http://local");
    const send = (value: unknown, status = 200) => {
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify(value));
    };
    if (u.pathname.endsWith("/whoami")) {
      auths++;
      if (holdAuth) {
        authPending.push(res);
        return;
      }
      if (outage)
        send(
          { errcode: "M_UNKNOWN", error: "synthetic-private-upstream" },
          503,
        );
      else if (!users[token] || revoked.has(token))
        send(
          { errcode: "M_UNKNOWN_TOKEN", error: "synthetic-private-upstream" },
          401,
        );
      else send({ user_id: users[token], device_id: token.toUpperCase() });
    } else if (u.pathname.endsWith("/versions")) {
      if (holdVersions) {
        res.writeHead(200, { "content-type": "application/json" });
        res.write('{"versions":');
        versionPending.push(res);
      } else send({ versions: ["v1.11"], unstable_features: {} });
    } else if (u.pathname.endsWith("/pushrules/"))
      send({
        global: {
          override: [],
          content: [],
          room: [],
          sender: [],
          underride: [],
        },
      });
    else if (u.pathname.endsWith("/filter")) send({ filter_id: token });
    else if (u.pathname.endsWith("/capabilities")) send({ capabilities: {} });
    else if (u.pathname.endsWith("/sync")) {
      if (!u.searchParams.has("since") && !holdInitial) {
        initial.set(token, (initial.get(token) ?? 0) + 1);
        send(batch(token, "$startup", true));
      } else {
        const list = pending.get(token) ?? [];
        list.push(res);
        pending.set(token, list);
      }
    } else if (u.pathname.endsWith("/messages"))
      send({
        start: "history",
        end: "end",
        chunk: [event("$history-" + token, token)],
      });
    else if (
      u.pathname.endsWith("/event/%24parent") ||
      u.pathname.endsWith("/event/$parent")
    )
      send({
        event_id: "$parent",
        room_id: `!${token}:test`,
        type: "m.room.message",
        sender: users[token],
        content: { msgtype: "m.text", body: "private-parent" },
      });
    else if (u.pathname.includes("/send/m.room.message/")) {
      let body = "";
      for await (const chunk of req) body += chunk;
      sends.push({
        token,
        room: decodeURIComponent(
          u.pathname.split("/rooms/")[1].split("/send/")[0],
        ),
        content: JSON.parse(body),
      });
      send({ event_id: "$sent-" + token });
    } else send({});
  });
  return {
    ...e,
    users,
    revoked,
    initial,
    sends,
    pending,
    authPending,
    versionPending,
    get requests() {
      return requests;
    },
    get holdVersions() {
      return holdVersions;
    },
    set holdVersions(v: boolean) {
      holdVersions = v;
    },
    get auths() {
      return auths;
    },
    get outage() {
      return outage;
    },
    set outage(v: boolean) {
      outage = v;
    },
    get holdAuth() {
      return holdAuth;
    },
    set holdAuth(v: boolean) {
      holdAuth = v;
    },
    get holdInitial() {
      return holdInitial;
    },
    set holdInitial(v: boolean) {
      holdInitial = v;
    },
    async emit(token: string, id: string, reply = false) {
      await until(
        () => (pending.get(token)?.filter((r) => !r.destroyed).length ?? 0) > 0,
      );
      const list = pending.get(token)!;
      let res = list.shift()!;
      while (res.destroyed) res = list.shift()!;
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(batch(token, id, false, reply)));
    },
  };
}
async function caller(url: URL, token: string) {
  const client = new Client({ name: "owned-test", version: "1" });
  await client.connect(
    new StreamableHTTPClientTransport(url, {
      requestInit: { headers: { authorization: `Bearer ${token}` } },
    }),
  );
  return client;
}
async function call(client: Client, name: string, args = {}) {
  const result = await client.callTool({ name, arguments: args });
  return JSON.parse((result.content as any)[0].text);
}
test("public SDK callers isolate tokens, coordinate cold sync, validate every request and retire revoked clients", async () => {
  const h = await home(),
    registry = new Registry(h.url, bounds);
  const gateway = await listen(
    { homeserver: h.url, host: "127.0.0.1", port: 0 },
    registry,
  );
  const url = new URL(
    `http://127.0.0.1:${(gateway.http.address() as any).port}/mcp`,
  );
  const clients: Client[] = [];
  try {
    const [a, a2, b, c, d] = await Promise.all(
      ["a", "a", "b", "c", "d"].map((t) => caller(url, t)),
    );
    clients.push(a, a2, b, c, d);
    expect(h.initial.get("a")).toBe(1);
    expect((await call(a, "whoami")).user_id).toBe("@a:test");
    expect((await call(b, "whoami")).user_id).toBe("@b:test");
    expect((await call(c, "whoami")).user_id).toBe(
      (await call(d, "whoami")).user_id,
    );
    for (const [client, token] of [
      [a, "a"],
      [b, "b"],
      [c, "c"],
      [d, "d"],
    ] as const) {
      expect(
        (await call(client, "list_rooms")).rooms.map((r: any) => r.room_id),
      ).toEqual([`!${token}:test`]);
      expect(
        (await call(client, "read_messages", { room_id: `!${token}:test` }))
          .messages[0].body,
      ).toBe(token);
      expect(
        (
          await call(client, "send_message", {
            room_id: `!${token}:test`,
            body: "send-" + token,
          })
        ).event_id,
      ).toBe("$sent-" + token);
    }
    expect(h.sends.map((s) => [s.token, s.room])).toEqual(
      ["a", "b", "c", "d"].map((t) => [t, `!${t}:test`]),
    );
    const cross = await a.callTool({
      name: "read_messages",
      arguments: { room_id: "!b:test" },
    });
    expect(cross.isError).toBe(true);
    const before = h.auths;
    await a.listTools();
    expect(h.auths).toBe(before + 1);
    h.revoked.add("a");
    const response = await fetch(url, {
      method: "POST",
      headers: { authorization: "Bearer a" },
      body: "not-json",
    });
    expect(response.status).toBe(401);
    expect(await response.text()).not.toContain("synthetic-private");
    await expect(call(a, "whoami")).rejects.toThrow();
    expect((await call(b, "whoami")).user_id).toBe("@b:test");
    h.revoked.delete("a");
    expect((await call(a2, "list_rooms")).rooms[0].room_id).toBe("!a:test");
    expect(h.initial.get("a")).toBe(2);
    h.outage = true;
    const failed = await fetch(url, {
      method: "POST",
      headers: { authorization: "Bearer b" },
      body: "invalid",
    });
    expect(failed.status).toBe(503);
    expect(await failed.text()).not.toContain("synthetic-private");
    h.outage = false;
    const status = async (target: URL, options: RequestInit) => {
      const response = await fetch(target, options);
      await response.text();
      return response.status;
    };
    for (const method of ["GET", "DELETE"])
      expect(
        await status(url, { method, headers: { authorization: "Bearer b" } }),
      ).toBe(405);
    expect(
      await status(new URL("/else", url), {
        headers: { authorization: "Bearer b" },
      }),
    ).toBe(404);
    h.users.e = "@e:test";
    expect(
      await status(new URL("/else", url), {
        method: "POST",
        headers: { authorization: "Bearer e" },
        body: "invalid",
      }),
    ).toBe(404);
    expect(await status(url, { headers: { authorization: "Bearer e" } })).toBe(
      405,
    );
    expect(h.initial.has("e")).toBe(false);
    const count = h.auths;
    for (const header of ["Bearer b b", "Bearer b,Bearer c"]) {
      expect(
        await status(url, {
          method: "POST",
          headers: { authorization: header },
          body: "{}",
          signal: AbortSignal.timeout(1000),
        }),
      ).toBe(401);
    }
    expect(h.auths).toBe(count);
    expect(
      await status(url, {
        method: "POST",
        headers: { authorization: "Bearer b" },
        body: "x".repeat(128001),
        signal: AbortSignal.timeout(1000),
      }),
    ).toBe(413);
  } finally {
    await Promise.all(clients.map((c) => c.close()));
    await Promise.all([gateway.stop(), registry.stop()]);
    await h.close();
  }
}, 10000);

test("two configured real SDK accounts notify independently, reuse exact credential, and dynamic tools never enroll", async () => {
  const h = await home();
  const received: { bytes: string; auth?: string }[][] = [[], []];
  let failFirst = true;
  let failingResponse: ServerResponse | undefined;
  const receivers = await Promise.all(
    received.map((list, i) =>
      endpoint(async (req, res) => {
        let bytes = "";
        for await (const c of req) bytes += c;
        list.push({ bytes, auth: req.headers.authorization });
        if (i === 0 && failFirst) {
          failingResponse = res;
          return;
        }
        res.writeHead(204).end();
      }),
    ),
  );
  const registry = new Registry(h.url, { ...bounds, idle: 20, sweep: 5 });
  let client: Client | undefined, dynamic: Client | undefined;
  let gateway: Awaited<ReturnType<typeof listen>> | undefined;
  try {
    await registry.start([
      { ...entry, url: receivers[0].url },
      {
        user_id: "@b:test",
        access_token: "b",
        url: receivers[1].url,
        bearer_token: "receiver-b",
      },
    ]);
    expect(received).toEqual([[], []]);
    await h.emit("a", "$reply-a", true);
    await h.emit("b", "$reply-b", true);
    await until(() => received[0].length > 0 && received[1].length > 0);
    expect(received[0]).toHaveLength(1); // b delivered while a waits for retry
    for (const [i, token] of [
      [0, "a"],
      [1, "b"],
    ] as const) {
      expect(JSON.parse(received[i][0].bytes)).toEqual({
        type: "message",
        conversation_type: "room",
        event_id: "$reply-" + token,
        room_id: `!${token}:test`,
        sender_id: "@other:test",
        sender_display_name: "@other:test",
        timestamp: 1,
        body: token,
        mentions: [],
        reply_to_event_id: "$parent",
        reply_to_sender_id: `@${token}:test`,
        truncated: false,
      });
      expect(received[i][0].auth).toBe(
        "Bearer " + (i ? "receiver-b" : "receiver-only"),
      );
      expect(received[i][0].bytes).not.toContain("private-parent");
    }
    failFirst = false;
    failingResponse!.writeHead(503).end();
    await until(() => received[0].length === 2);
    expect(received[0][1].bytes).toBe(received[0][0].bytes);
    gateway = await listen(
      { homeserver: h.url, host: "127.0.0.1", port: 0 },
      registry,
    );
    const url = new URL(
      `http://127.0.0.1:${(gateway.http.address() as any).port}/mcp`,
    );
    client = await caller(url, "a");
    dynamic = await caller(url, "c");
    expect(h.initial.get("a")).toBe(1);
    await h.emit("c", "$dynamic");
    await h.emit("a", "$only-once");
    await until(() => received[0].length === 3);
    await Bun.sleep(30);
    expect(received.map((l) => l.length)).toEqual([3, 1]);
    // SDK reconnect ERROR boundary and catch-up remain the existing behavior.
    await until(() => !!h.pending.get("b")?.length);
    const pending = h.pending.get("b")!.shift()!;
    pending
      .writeHead(500, { "content-type": "application/json" })
      .end(JSON.stringify({ errcode: "M_UNKNOWN" }));
    const lease = await registry.request("b", new AbortController().signal);
    await until(
      () => lease.matrix!.client.getSyncState() === SyncState.Reconnecting,
    );
    lease.matrix!.client.retryImmediately();
    lease.release();
    await h.emit("b", "$catchup");
    await h.emit("b", "$after-reconnect");
    await until(() => received[1].length === 2);
    expect(received[1].map((v) => JSON.parse(v.bytes).event_id)).toEqual([
      "$reply-b",
      "$after-reconnect",
    ]);
  } finally {
    await Promise.all([client?.close(), dynamic?.close()]);
    await Promise.all([gateway?.stop(), registry.stop()]);
    await Promise.all([h.close(), ...receivers.map((r) => r.close())]);
  }
}, 10000);

// Short controlled lifecycle timing injected into the registry, with real SDK
// authentication against owned HTTP and a controllable client at the sync seam.
function factory() {
  const made: {
    matrix: Matrix;
    token: string;
    stopped: number;
    ready(): void;
  }[] = [];
  let auto = true;
  const create = (
    c: { homeserver: string; token: string },
    identity: Matrix["identity"],
  ) => {
    const client = new TypedEventEmitter() as any;
    let record: (typeof made)[number];
    Object.assign(client, {
      startClient: async () => {
        if (auto)
          queueMicrotask(() =>
            client.emit(ClientEvent.Sync, SyncState.Prepared),
          );
      },
      stopClient: () => {
        record.stopped++;
      },
      http: { abort() {} },
    });
    const matrix = new Matrix(client as MatrixClient);
    matrix.identity = identity;
    record = {
      matrix,
      token: c.token,
      stopped: 0,
      ready() {
        client.emit(ClientEvent.Sync, SyncState.Prepared);
      },
    };
    made.push(record);
    return matrix;
  };
  return {
    made,
    create,
    set auto(v: boolean) {
      auto = v;
    },
  };
}
test("registry bounds resident/pending clients, protects active owners, expires idle and recovers failures", async () => {
  const h = await home(),
    f = factory();
  const registry = new Registry(
    h.url,
    { ...bounds, capacity: 2, idle: 40, sweep: 5, readiness: 50 },
    f.create,
  );
  const signal = new AbortController().signal;
  try {
    const a = await registry.request("a", signal),
      b = await registry.request("b", signal);
    await expect(registry.request("c", signal)).rejects.toMatchObject({
      status: 503,
    });
    await Bun.sleep(60);
    expect(f.made.every((m) => m.stopped === 0)).toBe(true);
    a.release();
    a.release();
    await until(() => f.made[0].stopped > 0);
    const c = await registry.request("c", signal);
    c.release();
    b.release();
    await until(() => f.made.every((m) => m.stopped > 0));
    f.auto = false;
    await expect(registry.request("d", signal)).rejects.toMatchObject({
      status: 503,
    });
    expect(f.made.at(-1)!.stopped).toBeGreaterThan(0);
    f.auto = true;
    const recovered = await registry.request("d", signal);
    recovered.release();
    expect(f.made.filter((m) => m.token === "d")).toHaveLength(2);
  } finally {
    await registry.stop();
    await h.close();
  }
});

test("shared pending initialization disconnects only its waiter; last waiter and shutdown cancel creation", async () => {
  const h = await home(),
    f = factory();
  f.auto = false;
  const registry = new Registry(h.url, { ...bounds, capacity: 1 }, f.create);
  const first = new AbortController(),
    second = new AbortController();
  try {
    const a = registry.request("a", first.signal).catch((e) => e);
    const a2 = registry.request("a", second.signal);
    await until(() => f.made.length === 1 && h.auths === 2);
    await Bun.sleep(10);
    await expect(
      registry.request("b", new AbortController().signal),
    ).rejects.toMatchObject({ status: 503 });
    first.abort();
    expect((await a).status).toBe(503);
    expect(f.made[0].stopped).toBe(0);
    f.made[0].ready();
    const lease = await a2;
    lease.release();
    const b = registry.request("a", new AbortController().signal);
    (await b).release();
    await registry.stop();
    expect(f.made[0].stopped).toBeGreaterThan(0);
  } finally {
    await registry.stop();
    await h.close();
  }
  const h2 = await home(),
    f2 = factory();
  f2.auto = false;
  const r2 = new Registry(h2.url, bounds, f2.create);
  try {
    const controller = new AbortController();
    const pending = r2.request("a", controller.signal).catch((e) => e);
    await until(() => f2.made.length === 1);
    controller.abort();
    expect((await pending).status).toBe(503);
    await until(() => f2.made[0].stopped > 0);
    f2.auto = true;
    const retry = await r2.request("a", new AbortController().signal);
    retry.release();
    f2.auto = false;
    const stopped = r2
      .request("b", new AbortController().signal)
      .catch((e) => e);
    await until(() => f2.made.length === 3);
    await r2.stop();
    expect((await stopped).status).toBe(503);
    f2.made[2].ready();
    await expect(
      r2.request("b", new AbortController().signal),
    ).rejects.toMatchObject({ status: 503 });
  } finally {
    await r2.stop();
    await h2.close();
  }
});

test("finite auth admission, stalled-body timeout, and startup identity mismatch are safe", async () => {
  const h = await home(),
    f = factory();
  const registry = new Registry(
    h.url,
    { ...bounds, admission: 2, auth: 70 },
    f.create,
  );
  try {
    h.holdAuth = true;
    const pending = ["a", "b"].map((t) =>
      registry.request(t, new AbortController().signal).catch((e) => e),
    );
    await until(() => h.authPending.length === 2);
    await expect(
      registry.request("c", new AbortController().signal),
    ).rejects.toMatchObject({ status: 503 });
    expect(h.auths).toBe(2);
    expect(f.made).toHaveLength(0);
    // Headers succeed but body never finishes. Timeout must cover body transfer.
    h.authPending[0].writeHead(200, { "content-type": "application/json" });
    h.authPending[0].write('{"user_id":');
    expect((await Promise.all(pending)).map((e) => e.status)).toEqual([
      503, 503,
    ]);
    h.holdAuth = false;
    const retry = await registry.request("a", new AbortController().signal);
    retry.release();
    const old = f.made[0];
    h.users.a = "@changed:test";
    await expect(
      registry.request("a", new AbortController().signal),
    ).rejects.toMatchObject({ status: 503 });
    expect(old.stopped).toBeGreaterThan(0);
  } finally {
    await registry.stop();
    await h.close();
  }
  const h2 = await home(),
    r2 = new Registry(h2.url, bounds);
  try {
    await expect(
      r2.start([
        { ...entry, url: "http://127.0.0.1:1" },
        { ...entry, access_token: "b", user_id: "@wrong:test" },
      ]),
    ).rejects.toMatchObject({ status: 503 });
    await expect(
      r2.request("a", new AbortController().signal),
    ).rejects.toMatchObject({ status: 503 });
  } finally {
    await r2.stop();
    await h2.close();
  }
});

test("real SDK cold-sync timeout retries cleanly and stop aborts startup before late publication", async () => {
  const h = await home(),
    registry = new Registry(h.url, { ...bounds, readiness: 80 });
  try {
    h.holdInitial = true;
    await expect(
      registry.request("a", new AbortController().signal),
    ).rejects.toMatchObject({ status: 503 });
    h.holdInitial = false;
    const lease = await registry.request("a", new AbortController().signal);
    expect(lease.matrix!.rooms().rooms[0].room_id).toBe("!a:test");
    lease.release();
    h.holdInitial = true;
    const pending = registry
      .request("b", new AbortController().signal)
      .catch((e) => e);
    await until(() => !!h.pending.get("b")?.length);
    await registry.stop();
    expect((await pending).status).toBe(503);
  } finally {
    await registry.stop();
    await h.close();
  }
});

test("SDK auth distinguishes unknown/unauthorized credentials from upstream failures", async () => {
  let status = 401,
    body = "not-json";
  const home = await endpoint((_req, res) => {
    res.writeHead(status, { "content-type": "application/json" });
    res.end(body);
  });
  try {
    const auth = () =>
      authenticate(
        home.url,
        "synthetic-token",
        new AbortController().signal,
        100,
      );
    await expect(auth()).rejects.toMatchObject({ status: 401 });
    status = 403;
    body = JSON.stringify({
      errcode: "M_UNAUTHORIZED",
      error: "synthetic-private",
    });
    await expect(auth()).rejects.toMatchObject({ status: 401 });
    status = 500;
    body = JSON.stringify({ errcode: "M_UNKNOWN", error: "synthetic-private" });
    await expect(auth()).rejects.toMatchObject({ status: 503 });
    status = 200;
    body = JSON.stringify({ user_id: "invalid-user" });
    await expect(auth()).rejects.toMatchObject({ status: 503 });
  } finally {
    await home.close();
  }
});

test("stop cancels real SDK startup body transfer and authentication without later upstream requests", async () => {
  const h = await home(),
    registry = new Registry(h.url, bounds);
  try {
    h.holdVersions = true;
    const pending = registry
      .request("a", new AbortController().signal)
      .catch((e) => e);
    await until(() => h.versionPending.length > 0);
    await registry.stop();
    expect((await pending).status).toBe(503);
    const after = h.requests;
    await Bun.sleep(50);
    expect(h.requests).toBe(after);
  } finally {
    await registry.stop();
    await h.close();
  }
  const h2 = await home(),
    registry2 = new Registry(h2.url, bounds);
  try {
    h2.holdAuth = true;
    const pending = registry2
      .request("a", new AbortController().signal)
      .catch((e) => e);
    await until(() => h2.authPending.length > 0);
    await registry2.stop();
    expect((await pending).status).toBe(503);
  } finally {
    await registry2.stop();
    await h2.close();
  }
});
