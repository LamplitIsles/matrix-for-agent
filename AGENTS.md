# Repository workflow

Use Bun for dependencies and the scripts in `package.json`. After runtime changes,
run typecheck, full tests, a Node-target build and Node executable help smoke.
When `dist/cli.js` is a live installed artifact, redirect the equivalent Bun build
command to an owned `.scratch/` outfile and smoke-test that output. Verify the
installed artifact's before/after hash unchanged; pack only copied owned output in
an isolated staging directory. The distributed runtime targets Node.js.

Keep one Matrix client per credential shared by stateless HTTP MCP requests.
Validate each request against the fixed homeserver; isolate distinct credentials
even for the same user ID. Configured webhook runtimes own independent observers
and queues and outlive MCP callers; dynamic tool access never enrolls a webhook.
Room admission uses current joined/plaintext state for both tools and incoming
notifications. Preserve the conservative startup/reconnect live boundary; test
SDK ordering against the local fake homeserver when changing it.

Tests own their clients, endpoints and temporary state. Never use real Matrix
accounts, credentials, configuration directories or receivers in tests. Safe
errors omit upstream diagnostics and authorization data. Keep `.scratch/` local.

Read `CONTEXT.md` when changing domain terminology. Operator-facing contracts and
limits belong in `README.md`; update them with observable behavior changes.
