#!/usr/bin/env node
import { config, loadWebhooks } from "./config.js";
import { Registry } from "./registry.js";
import { listen } from "./mcp.js";
if (process.argv.includes("--help")) {
  console.log(
    "matrix-for-agent\nStart: MATRIX_HOMESERVER_URL required.\nOptional: MATRIX_MCP_LISTEN (127.0.0.1:8766), MATRIX_WEBHOOK_CONFIG (private JSON file).\nCallers authenticate with their own Matrix bearer tokens at /mcp.\nMATRIX_ACCESS_TOKEN, MATRIX_WEBHOOK_URL and MATRIX_WEBHOOK_BEARER_TOKEN are obsolete.\nSee README.md for contracts and shutdown guidance.",
  );
} else {
  let registry: Registry | undefined;
  let gateway: Awaited<ReturnType<typeof listen>> | undefined;
  let stopping = false;
  const stop = async () => {
    stopping = true;
    await Promise.all([gateway?.stop(), registry?.stop()]);
  };
  process.once("SIGINT", () => void stop());
  process.once("SIGTERM", () => void stop());
  try {
    const c = config(process.env);
    const webhooks = await loadWebhooks(c.webhookConfig);
    if (!stopping) {
      registry = new Registry(c.homeserver);
      await registry.start(webhooks);
      if (!stopping) {
        gateway = await listen(c, registry);
        if (stopping) await gateway.stop();
        else console.log("Matrix gateway listening on loopback /mcp");
      }
    }
  } catch (error) {
    await stop();
    const obsolete =
      error instanceof Error &&
      /^MATRIX_(ACCESS_TOKEN|WEBHOOK_URL|WEBHOOK_BEARER_TOKEN) is obsolete;/.test(
        error.message,
      );
    console.error(
      obsolete
        ? error.message
        : "Gateway startup failed; check configuration and Matrix connectivity",
    );
    process.exitCode = 1;
  }
}
