# Repository workflow

Use Bun for dependencies and the scripts in `package.json`. After runtime changes,
run `bun run typecheck`, `bun test`, `bun run build`, and
`node dist/cli.js --help`; the distributed runtime targets Node.js.

Keep one Matrix client shared by stateless HTTP MCP requests. Room admission uses
current joined/plaintext state for both tools and incoming notifications. Preserve
the conservative startup/reconnect live boundary; test SDK ordering against the
local fake homeserver when changing it.

Tests own their clients, endpoints and temporary state. Never use real Matrix
accounts, credentials, configuration directories or receivers in tests. Safe
errors omit upstream diagnostics and authorization data. Keep `.scratch/` local.

Read `CONTEXT.md` when changing domain terminology. Operator-facing contracts and
limits belong in `README.md`; update them with observable behavior changes.
