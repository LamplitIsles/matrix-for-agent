# Matrix for Agent

A small plaintext Matrix gateway for any agent host. One process owns one Matrix
user connection, exposes five tools over loopback HTTP MCP, and optionally posts
new incoming text to one webhook receiver. The receiver decides how to route
messages and whether an agent responds.

## Run

Install Bun for dependency management and a current Node.js LTS runtime (Node 22
or later). Use an existing Matrix account and access token; this package does not
log in, create accounts, join rooms, or configure devices.

```sh
bun install --frozen-lockfile
bun run typecheck
bun test
bun run build
node dist/cli.js --help

export MATRIX_HOMESERVER_URL=https://matrix.example.org
export MATRIX_ACCESS_TOKEN='<existing Matrix access token>'
node dist/cli.js
```

The build creates a Node-compatible executable at `dist/cli.js`. `bun run start`
also starts it. SIGINT/SIGTERM close the listener, Matrix requests, event listeners,
webhook requests and timers. All state is ephemeral; no database or crypto store
is created. The process discovers its user/device IDs through Matrix `whoami`.

## Configuration and MCP connection

| Environment variable          | Meaning                                                                                 |
| ----------------------------- | --------------------------------------------------------------------------------------- |
| `MATRIX_HOMESERVER_URL`       | Required HTTPS or intentional HTTP homeserver URL, without URL credentials or fragment. |
| `MATRIX_ACCESS_TOKEN`         | Required nonempty Matrix-issued bearer token. No minimum length rule.                   |
| `MATRIX_MCP_LISTEN`           | Loopback host:port, default `127.0.0.1:8766`; `localhost` and `[::1]` also accepted.    |
| `MATRIX_WEBHOOK_URL`          | Optional single receiver: HTTPS, or loopback HTTP; no URL credentials or fragment.      |
| `MATRIX_WEBHOOK_BEARER_TOKEN` | Optional receiver-issued bearer, requires webhook URL.                                  |

Configure your host's Streamable HTTP MCP client with URL
`http://127.0.0.1:8766/mcp` and header `Authorization: Bearer <MATRIX_ACCESS_TOKEN>`.
Every request method requires authorization. Authenticated GET/DELETE return 405;
there is no persistent SSE stream or session ID. Independent concurrent clients
share the same Matrix connection. POST bodies are capped at 128,000 bytes.

Local MCP callers knowingly hold the **full Matrix account credential**. The
listener accepts only loopback connections. The Matrix credential is never
attached to a webhook; only the separate receiver bearer is used there. Errors
and diagnostics omit credentials, headers and server error details.

## Tools

Tool results contain a JSON object in MCP text content. Operation errors use
`isError: true`. All room operations require current membership `join` and no
`m.room.encryption` state. Scope follows sync membership/encryption changes.

| Tool                | Input                                                          | Successful output                                                     |
| ------------------- | -------------------------------------------------------------- | --------------------------------------------------------------------- |
| `whoami`            | `{}`                                                           | `{user_id, device_id?}`                                               |
| `list_rooms`        | `{}`                                                           | `{rooms: [{room_id, name}], truncated}`                               |
| `list_room_members` | `{room_id}`                                                    | `{members: [{user_id, display_name}], truncated}`                     |
| `read_messages`     | `{room_id, limit?: 1..100, before?: string}`; default limit 20 | `{messages, next_batch: string \| null, truncated, incomplete: true}` |
| `send_message`      | `{room_id, body, reply_to_event_id?, mentions?: string[]}`     | `{event_id}` only on acknowledged success                             |

IDs are raw Matrix IDs, bounded to 255 characters in tool inputs. `before` is an
opaque homeserver pagination cursor, capped at 4096 characters. Discovery returns
at most 100 rooms/members, with names capped at 255 characters and explicit
`truncated`. Sending accepts unchanged nonblank plaintext of 1–16,000 characters,
and at most 100 full `@user:server` mention IDs. Replies validate an event in the
same available room and revalidate scope after lookup. Sends emit `m.text`, native
`m.mentions`, and optional ordinary `m.in_reply_to`, without HTML formatting.

An uncertain send acknowledgment reports that the message **may have been sent**.
Do not blindly resend: recover by reading history. There is no gateway retry layer
for sends. The optional SDK send scheduler is disabled, so failed sends are not
queued or retried and cannot resume after shutdown. Requests retain their finite
HTTP timeout; uncertain outcomes require history recovery.

### Reading

Without `before`, read from the newest room history. Pass returned `next_batch`
as `before` to go further backward; null denotes the end. Each page is in
chronological homeserver timeline order. `limit` bounds scanned **events**, so
filtering media/edits can produce an empty or underfilled message page even when
`next_batch` remains available. No read markers, receipts or notifications are sent.

A message contains `event_id`, `sender_id`, `timestamp` (milliseconds), `body`,
`mentions` (user IDs), optional `reply_to_event_id`, and `edited`, `redacted`,
`incomplete`, `truncated` flags. Known redactions omit `body`; a redacted message
whose former type is unknown may appear as a bodyless placeholder. Bodies are
capped at 16,000 characters, mentions at 100 IDs of 255 characters each, and the
serialized message array at 64,000 characters of records. Dropped/truncated
content is disclosed. Pagination follows the scanned page, including dropped
records; ask for smaller limits when encountering output truncation.

Only valid same-original-author replacements available in the server's bundled
relations or current page are applied, choosing the newest timestamp (event ID
breaks ties). Original reply relationships are retained across edits. Edit events
are never independent text records. Cross-author, malformed or known-redacted
edits are ignored. Known redactions in the page or event unsigned data suppress
former bodies. `incomplete: true` means current edits/redactions are **not proven
exhaustive**; there is no relation recovery scan. Server history can omit changes.
Thread relations receive no special handling or metadata; text remains ordinary
text and ordinary reply relations are preserved. Media, encrypted content and
HTML are not read/rendered. Reading includes the account's own text.

## Incoming webhook

The optional receiver gets POST JSON for all non-self new `m.text` in available
rooms, regardless of whether the message addresses the account:

```json
{
  "type": "message",
  "event_id": "$event",
  "room_id": "!room:example.org",
  "sender_id": "@alice:example.org",
  "sender_display_name": "Alice",
  "timestamp": 1700000000000,
  "body": "Hello",
  "mentions": [],
  "reply_to_event_id": "$parent",
  "truncated": false
}
```

`reply_to_event_id` is optional. Display names are bounded to 255 characters;
body/mentions use reading's field bounds. Own echoes, edits, redactions, reactions,
media, encrypted rooms and history pagination do not notify. Thread metadata is
omitted. Redaction cannot erase a copy already received by the receiver.

Delivery is serial and ordered through an in-memory queue of at most 100 items
(including the active item). Overflow drops the newest incoming item with a safe
diagnostic. Each item gets at most three attempts, each with a five-second timeout,
with 250ms between attempts. Redirects are disabled; any 2xx acknowledges receipt.
Exhaustion drops the item with a diagnostic and continues. The receiver should
deduplicate by `room_id` + `event_id`, because retries can deliver duplicates.
Shutdown/restart discards pending items.

**Every startup and reconnect suppresses initial/catch-up history.** Delivery opens
only after the Matrix SDK finishes a non-catch-up sync; messages around that
boundary can be missed deliberately. There is no stored sync position, durable
queue, offline replay or delivery guarantee. After gateway/receiver downtime, the
agent owns recovery by `read_messages`; the receiver owns routing/response policy.

## Verification and boundaries

`bun test` uses test-owned fake clients and local endpoints. Real MCP SDK clients
exercise HTTP initialize/list/call. A real Matrix SDK connected to a local fake
homeserver verifies timeline-before-sync ordering and startup/reconnect suppression
through a local webhook. Typecheck, tests, build and Node executable help smoke
require no real Matrix account. Actual homeserver interoperability remains
unverified. E2EE, account/room management, thread APIs, media, read markers,
edit/redaction writes, stdio, replay and deployment are outside this package.

See [CONTEXT.md](CONTEXT.md) for domain vocabulary.
