import { createServer, type ServerResponse } from "node:http";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import { authorized, type Config } from "./config.js";
import { Matrix } from "./matrix.js";
const id = z.string().min(1).max(255);
export function tools(matrix: Matrix) {
  const server = new McpServer({ name: "matrix", version: "0.1.0" });
  const run = (fn: () => Promise<unknown> | unknown) => async () => {
    try {
      const value = await fn();
      return {
        content: [{ type: "text" as const, text: JSON.stringify(value) }],
      };
    } catch (e) {
      const message =
        e instanceof Error &&
        [
          "Room unavailable or encrypted",
          "Reply target unavailable",
          "Send acknowledgment unknown; message may have been sent. Do not blindly resend.",
        ].includes(e.message)
          ? e.message
          : "Matrix operation failed";
      return {
        isError: true,
        content: [{ type: "text" as const, text: message }],
      };
    }
  };
  server.registerTool(
    "whoami",
    { inputSchema: {} },
    run(() => matrix.identity),
  );
  server.registerTool(
    "list_rooms",
    { inputSchema: {} },
    run(() => matrix.rooms()),
  );
  server.registerTool(
    "list_room_members",
    { inputSchema: { room_id: id } },
    ({ room_id }) => run(() => matrix.members(room_id))(),
  );
  server.registerTool(
    "send_message",
    {
      inputSchema: {
        room_id: id,
        body: z
          .string()
          .min(1)
          .max(16000)
          .refine((s) => s.trim().length > 0),
        reply_to_event_id: id.optional(),
        mentions: z
          .array(
            z
              .string()
              .max(255)
              .regex(/^@[^\s:]+:[^\s]+$/),
          )
          .max(100)
          .optional(),
      },
    },
    ({ room_id, body, reply_to_event_id, mentions }) =>
      run(() => matrix.send(room_id, body, reply_to_event_id, mentions))(),
  );
  server.registerTool(
    "read_messages",
    {
      inputSchema: {
        room_id: id,
        limit: z.number().int().min(1).max(100).default(20),
        before: z.string().min(1).max(4096).optional(),
      },
    },
    ({ room_id, limit, before }) =>
      run(() => matrix.read(room_id, limit, before))(),
  );
  return server;
}
export async function listen(c: Config, matrix: Matrix) {
  const active = new Set<{
    server: McpServer;
    transport: StreamableHTTPServerTransport;
  }>();
  const json = (res: ServerResponse, status: number, message: string) => {
    res.writeHead(status, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        jsonrpc: "2.0",
        id: null,
        error: { code: -32000, message },
      }),
    );
  };
  const http = createServer(async (req, res) => {
    if (!authorized(req.headers.authorization, c.token)) {
      json(res, 401, "Unauthorized");
      return;
    }
    if (req.url !== "/mcp") {
      json(res, 404, "Not found");
      return;
    }
    if (req.method !== "POST") {
      res.setHeader("Allow", "POST");
      json(res, 405, "Method not allowed");
      return;
    }
    let entry:
      | { server: McpServer; transport: StreamableHTTPServerTransport }
      | undefined;
    try {
      const chunks: Buffer[] = [];
      let bytes = 0;
      for await (const chunk of req) {
        const buffer = Buffer.from(chunk);
        bytes += buffer.length;
        if (bytes > 128000) {
          json(res, 413, "Request too large");
          return;
        }
        chunks.push(buffer);
      }
      const body = Buffer.concat(chunks).toString("utf8");
      let parsed;
      try {
        parsed = JSON.parse(body);
      } catch {
        json(res, 400, "Invalid JSON");
        return;
      }
      const server = tools(matrix),
        transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: undefined,
          enableJsonResponse: true,
        });
      entry = { server, transport };
      active.add(entry);
      const current = entry;
      res.on("close", () => {
        active.delete(current);
        void current.server.close();
      });
      await server.connect(transport);
      await transport.handleRequest(req, res, parsed);
    } catch {
      if (!res.headersSent) json(res, 500, "Request failed");
      if (entry) {
        active.delete(entry);
        await entry.server.close();
      }
    }
  });
  http.requestTimeout = 15000;
  await new Promise<void>((resolve, reject) => {
    http.once("error", reject);
    http.listen(c.port, c.host, () => {
      http.removeListener("error", reject);
      resolve();
    });
  });
  return {
    http,
    async stop() {
      const closed = new Promise<void>((resolve) =>
        http.close(() => resolve()),
      );
      await Promise.all([...active].map((e) => e.server.close()));
      http.closeAllConnections();
      await closed;
    },
  };
}
