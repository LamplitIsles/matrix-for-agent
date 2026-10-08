import { timingSafeEqual, createHash } from "node:crypto";
export interface Config {
  homeserver: string;
  token: string;
  host: string;
  port: number;
  webhook?: string;
  webhookToken?: string;
}
export function loopback(host: string) {
  return (
    host === "127.0.0.1" ||
    host === "localhost" ||
    host === "[::1]" ||
    host === "::1"
  );
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
  if (
    !env.MATRIX_HOMESERVER_URL ||
    !env.MATRIX_ACCESS_TOKEN ||
    /\s/.test(env.MATRIX_ACCESS_TOKEN)
  )
    throw new Error("Matrix URL and nonempty bearer token required");
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
  if (
    env.MATRIX_WEBHOOK_BEARER_TOKEN &&
    (!env.MATRIX_WEBHOOK_URL || /[\r\n]/.test(env.MATRIX_WEBHOOK_BEARER_TOKEN))
  )
    throw new Error("Webhook bearer requires URL and valid header");
  return {
    homeserver: url(env.MATRIX_HOMESERVER_URL),
    token: env.MATRIX_ACCESS_TOKEN,
    host: listen.hostname.replace(/[\[\]]/g, ""),
    port,
    webhook: env.MATRIX_WEBHOOK_URL
      ? url(env.MATRIX_WEBHOOK_URL, true)
      : undefined,
    webhookToken: env.MATRIX_WEBHOOK_BEARER_TOKEN,
  };
}
export function authorized(header: string | undefined, token: string) {
  const hash = (s: string) => createHash("sha256").update(s).digest();
  return timingSafeEqual(hash(header ?? ""), hash(`Bearer ${token}`));
}
