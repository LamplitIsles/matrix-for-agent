import { test, expect } from "bun:test";
import { createServer, request } from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

test("owned Node CLI validates bearer/header bounds and closes HTTP on SIGTERM", async () => {
  const dir = await mkdtemp(join(tmpdir(), "mfa-cli-"));
  const cli = join(dir, "cli.js");
  const home = Bun.spawn(
    [
      "node",
      "--input-type=module",
      "-e",
      `
    import { createServer } from "node:http";
    const server = createServer({ maxHeaderSize: 32768 }, (_req, res) => {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ user_id: "@owned:test" }));
    });
    server.listen(0, "127.0.0.1", () => console.log(server.address().port));
  `,
    ],
    { env: { PATH: process.env.PATH }, stdout: "pipe", stderr: "pipe" },
  );
  const reserved = createServer();
  let child: ReturnType<typeof Bun.spawn> | undefined;
  try {
    const build = Bun.spawn(
      ["bun", "build", "src/cli.ts", "--target=node", `--outfile=${cli}`],
      { stdout: "pipe", stderr: "pipe" },
    );
    expect(await build.exited).toBe(0);
    const help = Bun.spawn(["node", cli, "--help"], {
      env: { PATH: process.env.PATH },
      stdout: "pipe",
      stderr: "pipe",
    });
    expect(await help.exited).toBe(0);
    expect(await new Response(help.stdout).text()).toContain(
      "MATRIX_WEBHOOK_CONFIG",
    );
    const obsolete = Bun.spawn(["node", cli], {
      env: {
        PATH: process.env.PATH,
        MATRIX_HOMESERVER_URL: "http://127.0.0.1:1",
        MATRIX_ACCESS_TOKEN: "synthetic-secret",
      },
      stdout: "pipe",
      stderr: "pipe",
    });
    expect(await obsolete.exited).toBe(1);
    const diagnostic = await new Response(obsolete.stderr).text();
    expect(diagnostic).toContain("MATRIX_ACCESS_TOKEN is obsolete");
    expect(diagnostic).not.toContain("synthetic-secret");
    const homeReader = home.stdout.getReader();
    const homePort = Number(
      new TextDecoder().decode((await homeReader.read()).value).trim(),
    );
    homeReader.releaseLock();
    await new Promise<void>((r) => reserved.listen(0, "127.0.0.1", r));
    const port = (reserved.address() as any).port;
    await new Promise<void>((r) => reserved.close(() => r()));
    const running = Bun.spawn(["node", cli], {
      env: {
        PATH: process.env.PATH,
        MATRIX_HOMESERVER_URL: `http://127.0.0.1:${homePort}`,
        MATRIX_MCP_LISTEN: `127.0.0.1:${port}`,
      },
      stdout: "pipe",
      stderr: "pipe",
    });
    child = running;
    const reader = running.stdout.getReader();
    const first = await reader.read();
    expect(new TextDecoder().decode(first.value)).toContain("listening");
    reader.releaseLock();
    const url = `http://127.0.0.1:${port}/mcp`;
    const status = async (header?: string) => {
      const response = await fetch(url, {
        headers: header ? { authorization: header } : {},
        signal: AbortSignal.timeout(1000),
      });
      await response.text();
      return response.status;
    };
    expect(await status()).toBe(401);
    expect(await status("Bearer a")).toBe(405);
    expect(await status("Bearer " + "a".repeat(16384))).toBe(405);
    expect(await status("Bearer " + "a".repeat(16385))).toBe(401);
    const duplicate = await new Promise<number>((resolve, reject) => {
      const req = request(
        url,
        { headers: ["Authorization", "Bearer a", "Authorization", "Bearer b"] },
        (res) => {
          res.resume();
          res.once("end", () => resolve(res.statusCode!));
        },
      );
      req.on("error", reject);
      req.end();
    });
    expect(duplicate).toBe(401);
    child.kill("SIGTERM");
    expect(
      await Promise.race([child.exited, Bun.sleep(1500).then(() => "timeout")]),
    ).toBe(0);
    await expect(
      fetch(url, { signal: AbortSignal.timeout(500) }),
    ).rejects.toThrow();
  } finally {
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    home.kill("SIGTERM");
    await home.exited;
    reserved.closeAllConnections();
    await new Promise<void>((r) => reserved.close(() => r()));
    await rm(dir, { recursive: true });
  }
}, 15000);
