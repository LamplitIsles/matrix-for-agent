import { open } from "node:fs/promises";
export interface WebhookConfig {
  user_id: string;
  access_token: string;
  url: string;
  bearer_token?: string;
}
export interface Config {
  homeserver: string;
  host: string;
  port: number;
  webhookConfig?: string;
}
export function credential(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length <= 16384 &&
    /^[\x21-\x7e]+$/.test(value)
  );
}
export function userId(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length <= 255 &&
    /^@[^\s:]+:[^\s]+$/u.test(value)
  );
}
export function bearer(header: string | undefined) {
  const value = header?.match(/^Bearer ([\x21-\x7e]+)$/i)?.[1];
  return credential(value) ? value : undefined;
}
export function loopback(host: string) {
  return ["127.0.0.1", "localhost", "[::1]", "::1"].includes(host);
}
function url(value: string, webhook = false) {
  const u = new URL(value);
  if (
    u.username ||
    u.password ||
    u.hash ||
    !["https:", "http:"].includes(u.protocol) ||
    (webhook && u.protocol === "http:" && !loopback(u.hostname))
  )
    throw new Error("Invalid endpoint URL");
  return u.toString();
}
export function config(env: NodeJS.ProcessEnv): Config {
  for (const key of [
    "MATRIX_ACCESS_TOKEN",
    "MATRIX_WEBHOOK_URL",
    "MATRIX_WEBHOOK_BEARER_TOKEN",
  ])
    if (env[key] !== undefined)
      throw new Error(
        `${key} is obsolete; use caller bearer and MATRIX_WEBHOOK_CONFIG`,
      );
  if (!env.MATRIX_HOMESERVER_URL)
    throw new Error("Matrix homeserver URL required");
  const listen = new URL(`http://${env.MATRIX_MCP_LISTEN ?? "127.0.0.1:8766"}`);
  if (
    !loopback(listen.hostname) ||
    listen.username ||
    listen.password ||
    listen.pathname !== "/" ||
    listen.search ||
    listen.hash
  )
    throw new Error("Listener must be loopback host:port");
  const port = Number(listen.port || 80);
  if (!Number.isInteger(port) || port < 1 || port > 65535)
    throw new Error("Invalid listener port");
  if (env.MATRIX_WEBHOOK_CONFIG !== undefined && !env.MATRIX_WEBHOOK_CONFIG)
    throw new Error("Invalid webhook configuration");
  return {
    homeserver: url(env.MATRIX_HOMESERVER_URL),
    host: listen.hostname.replace(/[\[\]]/g, ""),
    port,
    webhookConfig: env.MATRIX_WEBHOOK_CONFIG,
  };
}
export function parseWebhooks(value: string): WebhookConfig[] {
  try {
    if (Buffer.byteLength(value) > 1048576) throw new Error();
    const entries: unknown = JSON.parse(value);
    if (!Array.isArray(entries) || entries.length > 16) throw new Error();
    const users = new Set<string>(),
      tokens = new Set<string>();
    return entries.map((entry) => {
      if (
        !entry ||
        typeof entry !== "object" ||
        Array.isArray(entry) ||
        Object.keys(entry).some(
          (k) =>
            !["user_id", "access_token", "url", "bearer_token"].includes(k),
        ) ||
        !userId(entry.user_id) ||
        !credential(entry.access_token) ||
        typeof entry.url !== "string" ||
        !entry.url ||
        ("bearer_token" in entry && !credential(entry.bearer_token)) ||
        users.has(entry.user_id) ||
        tokens.has(entry.access_token)
      )
        throw new Error();
      users.add(entry.user_id);
      tokens.add(entry.access_token);
      return {
        user_id: entry.user_id,
        access_token: entry.access_token,
        url: url(entry.url, true),
        ...(entry.bearer_token === undefined
          ? {}
          : { bearer_token: entry.bearer_token }),
      };
    });
  } catch {
    throw new Error("Invalid webhook configuration");
  }
}
export async function loadWebhooks(path?: string): Promise<WebhookConfig[]> {
  if (path === undefined) return [];
  try {
    const file = await open(path, "r");
    try {
      const buffer = Buffer.alloc(1048577);
      let total = 0;
      while (total < buffer.length) {
        const { bytesRead } = await file.read(
          buffer,
          total,
          buffer.length - total,
          null,
        );
        if (!bytesRead) break;
        total += bytesRead;
      }
      if (total > 1048576) throw new Error();
      return parseWebhooks(
        new TextDecoder("utf-8", { fatal: true }).decode(
          buffer.subarray(0, total),
        ),
      );
    } finally {
      await file.close();
    }
  } catch {
    throw new Error("Invalid or unreadable webhook configuration");
  }
}
