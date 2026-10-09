import { test, expect } from "bun:test";
import { createServer, type RequestListener } from "node:http";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { Matrix, record, TOTAL, type Event } from "../src/matrix.js";
import { config } from "../src/config.js";
import { listen } from "../src/mcp.js";
import { Webhook } from "../src/webhook.js";
import type { MatrixClient } from "matrix-js-sdk";
const base = {
  homeserver: "http://127.0.0.1",
  token: "test-token",
  host: "127.0.0.1",
  port: 0,
};
const text = (
  id: string,
  body = "hello",
  extra: Partial<Event> = {},
): Event => ({
  event_id: id,
  type: "m.room.message",
  sender: "@other:test",
  origin_server_ts: 1,
  content: { msgtype: "m.text", body },
  ...extra,
});
async function endpoint(handler: RequestListener) {
  const server = createServer(handler);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    url: `http://127.0.0.1:${(server.address() as any).port}`,
    async close() {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}
async function until(fn: () => boolean) {
  for (let i = 0; i < 200; i++) {
    if (fn()) return;
    await Bun.sleep(10);
  }
  throw new Error("timed out");
}
function fake() {
  let membership = "join",
    encrypted = false,
    content: unknown,
    chunk: Event[] | undefined,
    count = 1,
    memberLoads = 0,
    uncertain = false,
    lookup: () => void = () => {};
  const names = new Map<string, string | undefined>([["@other:test", "Other"]]);
  const room = {
    roomId: "!room:test",
    name: "Room",
    getMember: (id: string) =>
      names.has(id) ? { name: names.get(id) } : null,
    getMyMembership: () => membership,
    currentState: { getStateEvents: () => (encrypted ? {} : null) },
    loadMembersIfNeeded: async () => {
      memberLoads++;
    },
    getJoinedMembers: () =>
      Array.from({ length: count }, () => ({
        userId: "@other:test",
        name: "Other",
      })),
  };
  const client = {
    getRoom: () => room,
    getRooms: () => Array.from({ length: count }, () => room),
    fetchRoomEvent: async (_r: string, e: string) => {
      lookup();
      return { event_id: e, room_id: "!room:test" };
    },
    sendMessage: async (_r: string, c: unknown) => {
      content = c;
      if (uncertain) throw new Error("test-token upstream headers");
      return { event_id: "$sent" };
    },
    createMessagesRequest: async (_r: string, from: string) => ({
      start: from ?? "latest",
      end: from ? "end" : "older",
      chunk:
        chunk ??
        (from
          ? [text("$old")]
          : [text("$new"), text("$self", "mine", { sender: "@self:test" })]),
    }),
  };
  const matrix = new Matrix(client as unknown as MatrixClient);
  matrix.identity = { user_id: "@self:test", device_id: "TEST" };
  return {
    matrix,
    names,
    get memberLoads() {
      return memberLoads;
    },
    set chunk(v: Event[]) {
      chunk = v;
    },
    set count(v: number) {
      count = v;
    },
    set uncertain(v: boolean) {
      uncertain = v;
    },
    set encrypted(v: boolean) {
      encrypted = v;
    },
    set membership(v: string) {
      membership = v;
    },
    set lookup(v: () => void) {
      lookup = v;
    },
    get content() {
      return content;
    },
  };
}
test("config rejects unsafe endpoints and accepts short Matrix tokens", () => {
  expect(
    config({
      MATRIX_HOMESERVER_URL: "http://localhost:8008",
      MATRIX_ACCESS_TOKEN: "x",
    }).token,
  ).toBe("x");
  for (const patch of [
    { MATRIX_MCP_LISTEN: "0.0.0.0:8766" },
    { MATRIX_WEBHOOK_URL: "http://example.com" },
    { MATRIX_HOMESERVER_URL: "https://u:p@example.com" },
    { MATRIX_WEBHOOK_BEARER_TOKEN: "receiver" },
  ])
    expect(() =>
      config({
        MATRIX_HOMESERVER_URL: "https://example.com",
        MATRIX_ACCESS_TOKEN: "x",
        ...patch,
      }),
    ).toThrow();
});
test("public stateless HTTP MCP tools, auth, scope, sends and history", async () => {
  const f = fake(),
    gateway = await listen(base, f.matrix);
  const url = new URL(
    `http://127.0.0.1:${(gateway.http.address() as any).port}/mcp`,
  );
  const clients: Client[] = [];
  try {
    for (const method of ["GET", "DELETE", "POST"])
      expect((await fetch(url, { method })).status).toBe(401);
    for (const method of ["GET", "DELETE"])
      expect(
        (
          await fetch(url, {
            method,
            headers: { authorization: "Bearer test-token" },
          })
        ).status,
      ).toBe(405);
    for (let i = 0; i < 2; i++) {
      const c = new Client({ name: "test", version: "1" });
      await c.connect(
        new StreamableHTTPClientTransport(url, {
          requestInit: { headers: { authorization: "Bearer test-token" } },
        }),
      );
      clients.push(c);
    }
    expect(
      (await clients[0].listTools()).tools.map((t) => t.name).sort(),
    ).toEqual(
      [
        "whoami",
        "list_rooms",
        "list_room_members",
        "read_messages",
        "send_message",
      ].sort(),
    );
    const call = async (name: string, args: Record<string, unknown> = {}) => {
      const r = await clients[0].callTool({ name, arguments: args });
      return {
        error: r.isError,
        value: JSON.parse(
          (r.content as any)[0].text.startsWith("{")
            ? (r.content as any)[0].text
            : "{}",
        ),
        raw: r,
      };
    };
    expect((await call("whoami")).value.user_id).toBe("@self:test");
    expect((await call("list_rooms")).value.rooms[0].room_id).toBe(
      "!room:test",
    );
    expect(
      (await call("list_room_members", { room_id: "!room:test" })).value
        .members[0].user_id,
    ).toBe("@other:test");
    const history = (await call("read_messages", { room_id: "!room:test" }))
      .value;
    expect(history.messages.map((m: any) => m.event_id)).toEqual([
      "$self", "$new",
    ]);
    expect(
      history.messages.map((m: any) => [m.sender_id, m.sender_display_name]),
    ).toEqual([["@self:test", ""], ["@other:test", "Other"]]);
    expect(
      (await call("read_messages", { room_id: "!room:test", before: "older" }))
        .value.messages[0].event_id,
    ).toBe("$old");
    expect(
      (
        await call("send_message", {
          room_id: "!room:test",
          body: "unchanged",
          reply_to_event_id: "$parent",
          mentions: ["@other:test"],
        })
      ).value.event_id,
    ).toBe("$sent");
    expect(f.content).toEqual({
      msgtype: "m.text",
      body: "unchanged",
      "m.mentions": { user_ids: ["@other:test"] },
      "m.relates_to": { "m.in_reply_to": { event_id: "$parent" } },
    });
    expect(
      (
        await clients[0].callTool({
          name: "send_message",
          arguments: { room_id: "!room:test", body: " " },
        })
      ).isError,
    ).toBe(true);
    f.count = 101;
    expect((await call("list_rooms")).value.truncated).toBe(true);
    expect(
      (await call("list_room_members", { room_id: "!room:test" })).value.members
        .length,
    ).toBe(100);
    f.chunk = Array.from({ length: 100 }, (_, i) =>
      text(`$large${i}`, "x".repeat(16000)),
    );
    const bounded = (
      await call("read_messages", { room_id: "!room:test", limit: 100 })
    ).value;
    expect(bounded.truncated).toBe(true);
    expect(JSON.stringify(bounded.messages).length).toBeLessThan(64100);
    expect(bounded.next_batch).toBe("older");
    f.chunk = [
      text("$redacted", "former body", { unsigned: { redacted_because: {} } }),
    ];
    f.names.set("@other:test", "😀".repeat(130));
    const redacted = (await call("read_messages", { room_id: "!room:test" }))
      .value.messages[0];
    expect(redacted).not.toHaveProperty("body");
    expect(redacted.sender_display_name).toBe("😀".repeat(130).slice(0, 255));
    expect(redacted.redacted).toBe(true);
    f.uncertain = true;
    const uncertain = await call("send_message", {
      room_id: "!room:test",
      body: "test",
    });
    expect(uncertain.error).toBe(true);
    expect(JSON.stringify(uncertain.raw)).toContain("may have been sent");
    expect(JSON.stringify(uncertain.raw)).not.toContain("test-token");
    f.lookup = () => {
      f.encrypted = true;
    };
    expect(
      (
        await call("send_message", {
          room_id: "!room:test",
          body: "secret",
          reply_to_event_id: "$parent",
        })
      ).error,
    ).toBe(true);
    expect((await call("list_rooms")).value.rooms).toEqual([]);
    expect((await call("read_messages", { room_id: "!room:test" })).error).toBe(
      true,
    );
    f.encrypted = false;
    f.membership = "leave";
    expect(
      (await call("list_room_members", { room_id: "!room:test" })).error,
    ).toBe(true);
  } finally {
    await Promise.all(clients.map((c) => c.close()));
    await gateway.stop();
  }
});
test("history uses current names without loading members and budgets enriched records", async () => {
  const f = fake();
  f.chunk = Array.from({ length: 4 }, (_, i) =>
    text(`$budget${i}`, "x".repeat(15800)),
  );
  f.names.set("@other:test", undefined);
  const unnamed = await f.matrix.read("!room:test", 4, "older");
  expect(unnamed.messages).toHaveLength(4);
  expect(unnamed.messages.every((m) => m.sender_display_name === "")).toBe(true);
  expect(unnamed.truncated).toBe(false);
  f.names.set("@other:test", "n".repeat(255));
  const named = await f.matrix.read("!room:test", 4, "older");
  expect(named.messages.map((m) => m.event_id)).toEqual([
    "$budget3", "$budget2", "$budget1",
  ]);
  expect(
    named.messages.every((m) => m.sender_display_name === "n".repeat(255)),
  ).toBe(true);
  expect(
    named.messages.reduce((size, m) => size + JSON.stringify(m).length, 0),
  ).toBeLessThanOrEqual(TOTAL);
  expect(named.truncated).toBe(true);
  expect(named.incomplete).toBe(true);
  expect(named.next_batch).toBe("end");
  expect(f.memberLoads).toBe(0);
});
test("bounded edit/redaction resolution, ordinary replies and thread text", () => {
  const original = text("$a", "old");
  const edit = text("$edit", "* new", {
    content: {
      msgtype: "m.text",
      body: "* new",
      "m.relates_to": { rel_type: "m.replace", event_id: "$a" },
      "m.new_content": {
        msgtype: "m.text",
        body: "new",
        "m.mentions": { user_ids: ["@self:test"] },
      },
    },
  });
  expect(record(original, [original, edit])?.body).toBe("new");
  const reply = {
    ...original,
    content: {
      ...original.content,
      "m.relates_to": { "m.in_reply_to": { event_id: "$parent" } },
    },
  };
  expect(record(reply, [edit])?.reply_to_event_id).toBe("$parent");
  expect(
    record({ ...original, room_id: "!room:test" }, [
      { ...edit, room_id: "!elsewhere:test" },
    ])?.body,
  ).toBe("old");
  expect(record(original, [{ ...edit, state_key: "" }])?.body).toBe("old");
  const later = {
    ...edit,
    event_id: "$zzz",
    content: {
      ...edit.content,
      "m.new_content": { msgtype: "m.text", body: "last" },
    },
  };
  expect(record(original, [later, edit])?.body).toBe("last");
  expect(record(edit)).toBeNull();
  expect(record(original, [{ ...edit, sender: "@impostor:test" }])?.body).toBe(
    "old",
  );
  expect(
    record({ ...original, unsigned: { "m.relations": { "m.replace": edit } } })
      ?.edited,
  ).toBe(true);
  expect(
    record(original, [{ type: "m.room.redaction", redacts: "$a" }])?.body,
  ).toBeUndefined();
  expect(
    record({ ...original, unsigned: { redacted_because: {} } })?.redacted,
  ).toBe(true);
  const thread = text("$thread", "thread", {
    content: {
      msgtype: "m.text",
      body: "thread",
      "m.relates_to": {
        rel_type: "m.thread",
        event_id: "$root",
        "m.in_reply_to": { event_id: "$reply" },
      },
    },
  });
  expect(record(thread)?.reply_to_event_id).toBe("$reply");
  expect(record(thread)).not.toHaveProperty("thread_id");
  expect(record(text("$long", "x".repeat(17000)))?.truncated).toBe(true);
});
test("webhook ordered retry, overflow, exhaustion, redirects, timeout and shutdown discard", async () => {
  const bodies: any[] = [],
    headers: any[] = [],
    diagnostics: string[] = [];
  let requests = 0;
  const receiver = await endpoint(async (req, res) => {
    let body = "";
    for await (const c of req) body += c;
    headers.push(req.headers);
    bodies.push(JSON.parse(body));
    requests++;
    if (requests === 1) {
      res.writeHead(503).end();
    } else res.writeHead(204).end();
  });
  const w = new Webhook(
    receiver.url,
    "receiver-token",
    (s) => diagnostics.push(s),
    { capacity: 2, attempts: 2, timeout: 100, delay: 5 },
  );
  try {
    w.push({ event_id: "1" });
    w.push({ event_id: "2" });
    w.push({ event_id: "3" });
    await until(() => bodies.length === 3);
    expect(bodies.map((b) => b.event_id)).toEqual(["1", "1", "2"]);
    expect(headers[0].authorization).toBe("Bearer receiver-token");
    expect(diagnostics[0]).toContain("overflow");
  } finally {
    await w.stop();
    await receiver.close();
  }
  let redirects = 0;
  const failure = await endpoint((_q, r) => {
    redirects++;
    r.writeHead(302, { location: "/redirect" }).end();
  });
  const fail = new Webhook(failure.url, undefined, (s) => diagnostics.push(s), {
    capacity: 2,
    attempts: 2,
    timeout: 50,
    delay: 5,
  });
  try {
    fail.push({ event_id: "4" });
    await until(() => diagnostics.some((s) => s.includes("exhausted")));
    expect(redirects).toBe(2);
  } finally {
    await fail.stop();
    await failure.close();
  }
  let hangs = 0;
  const hanging = await endpoint(() => {
    hangs++;
  });
  const stopped = new Webhook(
    hanging.url,
    undefined,
    (s) => diagnostics.push(s),
    { capacity: 2, attempts: 2, timeout: 20, delay: 5 },
  );
  try {
    stopped.push({ event_id: "5" });
    await until(() => hangs === 2);
    await stopped.stop();
    stopped.push({ event_id: "6" });
    expect(hangs).toBe(2);
  } finally {
    await stopped.stop();
    await hanging.close();
  }
});

test("webhook resumes when a message arrives as suppressed enrichment finishes", async () => {
  let posts = 0;
  const receiver = await endpoint((_req, res) => { posts++; res.writeHead(204).end(); });
  const webhook = new Webhook(receiver.url);
  try {
    webhook.push(async () => null);
    await Promise.resolve();
    webhook.push({ event_id: "$later" });
    await until(() => posts === 1);
    expect(posts).toBe(1);
  } finally { await webhook.stop(); await receiver.close(); }
});
