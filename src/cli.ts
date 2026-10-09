#!/usr/bin/env node
import { config } from "./config.js";
import { Matrix } from "./matrix.js";
import { listen } from "./mcp.js";
import { Webhook } from "./webhook.js";
if (process.argv.includes("--help")) {
  console.log(
    "matrix-for-agent\nStart: MATRIX_HOMESERVER_URL and MATRIX_ACCESS_TOKEN required.\nOptional: MATRIX_MCP_LISTEN (127.0.0.1:8766), MATRIX_WEBHOOK_URL, MATRIX_WEBHOOK_BEARER_TOKEN.\nStateless loopback HTTP MCP at /mcp. See README.md for contracts.",
  );
} else {
  let matrix: Matrix | undefined;
  let webhook: Webhook | undefined;
  let unobserve: (() => void) | undefined;
  try {
    const c = config(process.env);
    matrix = await Matrix.connect(c);
    webhook = c.webhook ? new Webhook(c.webhook, c.webhookToken) : undefined;
    unobserve = webhook
      ? matrix.observe((value, available) => webhook!.push(value, available))
      : undefined;
    await matrix.client.startClient({ initialSyncLimit: 0 });
    const gateway = await listen(c, matrix);
    let stopping = false;
    const stop = async () => {
      if (stopping) return;
      stopping = true;
      unobserve?.();
      matrix!.stop();
      await Promise.all([webhook?.stop(), gateway.stop()]);
    };
    process.once("SIGINT", () => void stop());
    process.once("SIGTERM", () => void stop());
    console.log("Matrix gateway listening on loopback /mcp");
  } catch {
    unobserve?.();
    matrix?.stop();
    await webhook?.stop();
    console.error(
      "Gateway startup failed; check configuration and Matrix connectivity",
    );
    process.exitCode = 1;
  }
}
