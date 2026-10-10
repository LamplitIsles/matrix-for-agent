# Matrix for Agent

A small plaintext Matrix gateway for any agent host. One process connects to one
fixed Matrix homeserver and exposes five tools over loopback HTTP MCP. Each caller
uses its own Matrix access token. Independently configured identities synchronize
continuously and post new incoming text to their own webhook receivers. Each
receiver decides how to route messages and whether an agent responds.

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
node dist/cli.js
```

The build creates a Node-compatible executable at `dist/cli.js`. `bun run start`
also starts it. SIGINT/SIGTERM close the listener, Matrix requests, event listeners,
webhook requests and gateway timers. All state is ephemeral; no database or crypto
store is created. The gateway verifies each credential's user/device IDs through
Matrix `whoami`. The SDK may retain baseline Node process resources after SIGTERM
even after HTTP closes; operators should use a finite service stop timeout and
forced termination for a process that remains. No delayed gateway sends or webhook
POSTs survive shutdown.

If `dist/cli.js` is an installed live artifact, build and smoke-test an owned output
instead of replacing it:

```sh
mkdir -p .scratch/multi-identity-gateway
sha256sum dist/cli.js
bun build src/cli.ts --target=node --outfile=.scratch/multi-identity-gateway/cli.js
node .scratch/multi-identity-gateway/cli.js --help
sha256sum dist/cli.js
```

The before/after live artifact hashes must match. Package only the copied owned
output in an isolated staging directory when verifying a release artifact.

## Configuration and MCP connection

| Environment variable    | Meaning                                                                                       |
| ----------------------- | --------------------------------------------------------------------------------------------- |
| `MATRIX_HOMESERVER_URL` | Required fixed HTTPS or intentional HTTP homeserver URL, without URL credentials or fragment. |
| `MATRIX_MCP_LISTEN`     | Loopback host:port, default `127.0.0.1:8766`; `localhost` and `[::1]` also accepted.          |
| `MATRIX_WEBHOOK_CONFIG` | Optional path to a private UTF-8 JSON webhook configuration file; absent means MCP-only.      |

`MATRIX_ACCESS_TOKEN`, `MATRIX_WEBHOOK_URL` and `MATRIX_WEBHOOK_BEARER_TOKEN` are
obsolete. Supplying any of them, even an empty value, fails startup. Remove them
when converting a future service configuration. Existing deployed artifacts and
secret files are not converted by this source change.

Configure your host's Streamable HTTP MCP client with URL
`http://127.0.0.1:8766/mcp` and header
`Authorization: Bearer <that caller's existing Matrix access token>`. There is no
identity argument, registration step or caller-selected homeserver. Every request
method validates the credential with the fixed homeserver's `whoami`, including
access to a resident client. Missing/malformed or upstream-invalid credentials
return 401; upstream failures/timeouts, identity changes, cold synchronization
failure or capacity exhaustion return a sanitized 503. A prior successful request
never authorizes a later request by itself. This custom Matrix bearer scheme is
**not standard MCP OAuth** and introduces no separate gateway token.

Authenticated off-path requests return 404 and GET/DELETE on `/mcp` return 405;
these validations do not create persistent dynamic clients. There is no persistent
SSE stream or session ID. POST bodies are capped at 128,000 bytes; oversized bodies
return 413 and malformed JSON returns 400 after authorization. The Authorization
header must occur exactly once and contain one Bearer credential, with no extra
whitespace; tokens are 1–16,384 visible ASCII characters without whitespace.

Concurrent callers with the exact same credential share initialization and one
Matrix client. Distinct credentials have separate clients, room state and
operations, even when their full Matrix user IDs match. On first use, initial usable
sync is awaited for at most ten seconds before tools can access rooms. Each
`whoami` validation is bounded to ten seconds including response-body transfer.
There are at most 16 concurrent authentication/initialization admissions and at
most 16 initializing or resident dynamic clients. Overflow returns 503. Dynamic
clients expire after five idle minutes, with cleanup at most 30 seconds later;
in-use requests protect their client until the last request releases it. Expired
idle clients may be removed to admit a new client at capacity. Failed creation is
removed so a later request can try again. A disconnected waiter releases only its
ownership; creation is cancelled when its last waiter leaves. Configured background
clients have separate capacity and remain active without MCP callers.

Local MCP callers knowingly hold the **full Matrix account credential**. The
listener accepts only loopback connections. Credentials are never logged or
persisted from requests. Errors and diagnostics omit credentials, headers and
upstream error details. The Matrix credential is never attached to a webhook;
only its separate receiver bearer is sent there.

### Configured webhook identities

`MATRIX_WEBHOOK_CONFIG` reads one file once at startup: a top-level JSON array of
at most 16 closed objects. The file is at most 1 MiB and must be valid UTF-8 JSON;
an empty array is valid. Each entry has exactly these fields:

| Field          | Contract                                                                                            |
| -------------- | --------------------------------------------------------------------------------------------------- |
| `user_id`      | Required exact full Matrix user ID, at most 255 UTF-16 code units, matching `/^@[^\s:]+:[^\s]+$/u`. |
| `access_token` | Required Matrix token: 1–16,384 visible ASCII characters without whitespace.                        |
| `url`          | Required receiver URL: HTTPS, or loopback HTTP; no URL credentials or fragment.                     |
| `bearer_token` | Optional receiver-issued credential: 1–16,384 visible ASCII characters without whitespace.          |

Unknown fields, duplicate user IDs or duplicate access tokens, malformed endpoints,
and unreadable/invalid files fail startup with sanitized diagnostics. Store this
file with private permissions (for example 0600) outside the source tree. Example
with placeholders only:

```json
[
  {
    "user_id": "@shio:example.org",
    "access_token": "<Shio-Matrix-token>",
    "url": "https://shio-receiver.example.org/matrix",
    "bearer_token": "<Shio-receiver-token>"
  },
  {
    "user_id": "@serein:example.org",
    "access_token": "<Serein-Matrix-token>",
    "url": "https://serein-receiver.example.org/matrix"
  }
]
```

For a future conversion, place each former identity's Matrix token and receiver
settings into its own object in a new private file, remove the three obsolete
variables, set `MATRIX_WEBHOOK_CONFIG=/private/path/webhooks.json`, and keep the
fixed homeserver and loopback listener. Conversion and deployment are separate
operator work; there is no migration, reload, login or credential refresh API.

Every entry must authenticate, match its exact configured `user_id`, and reach
initial usable sync before the listener opens. Startup is all-or-nothing: failure
stops all already-created observers, clients and queues. Observation starts before
sync to preserve the conservative no-replay boundary. Each configured runtime owns
its independent webhook queue and receiver credential; one receiver's retries do
not block another. An MCP caller with the exact configured Matrix token reuses
that client without another observer or changing delivery ownership. A different
valid token only enables tools and never enrolls an identity in webhook delivery.

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

A message contains `event_id`, `sender_id`, `sender_display_name`, `timestamp`
(milliseconds), `body`,
`mentions` (user IDs), optional `reply_to_event_id`, and `edited`, `redacted`,
`incomplete`, `truncated` flags. Known redactions omit `body`; a redacted message
whose former type is unknown may appear as a bodyless placeholder. Bodies are
capped at 16,000 characters, mentions at 100 IDs of 255 characters each, and the
serialized message array at 64,000 characters of records. Dropped/truncated
content is disclosed. Pagination follows the scanned page, including dropped
records; ask for smaller limits when encountering output truncation.

`sender_display_name` is the best-effort current synced room member name for the
original sender, capped at 255 UTF-16 code units; unknown members or names yield
an empty string. It is included on redacted messages and counts toward the total
response budget. It is not a historical nickname snapshot or an identity assertion.
Reading does not fetch profiles or load additional members to resolve names.

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

Each configured receiver gets POST JSON for all non-self new `m.text` in rooms
available to its configured Matrix identity, regardless of whether the message
addresses that identity:

```json
{
  "type": "message",
  "conversation_type": "dm",
  "event_id": "$event",
  "room_id": "!room:example.org",
  "sender_id": "@alice:example.org",
  "sender_display_name": "Alice",
  "timestamp": 1700000000000,
  "body": "Hello",
  "mentions": [],
  "reply_to_event_id": "$parent",
  "reply_to_sender_id": "@agent:example.org",
  "truncated": false
}
```

`reply_to_event_id` and `reply_to_sender_id` are optional. The author field is
present only with a nonempty target ID and a verified original `m.room.message`
target in the same room. It is the exact full Matrix user ID, at most 255 UTF-16
code units, matching `/^@[^\s:]+:[^\s]+$/u`; it is never clipped or inferred from
quoted text or names. Redacted targets may retain their author; their content is
not forwarded. An unavailable, malformed or mismatched target omits the author,
meaning unknown. Receivers must reject invalid/orphan author fields and decide
wake policy themselves: matching their own full identity permits reply wake;
other/unknown targets do not. Mention takes precedence over reply, then configured
alias for ordinary rooms. Self echoes and blank text must never wake.
History/MCP records do not gain these notification facts.

`conversation_type` is always exactly `"dm"` or `"room"`. A DM is a room ID
in an array in the configured account's synchronized `m.direct` account data.
Malformed entries are ignored; absent or unusable markings mean `"room"`.
Classification uses neither room names nor participant counts. A marked DM may
have more than two members, and two configured identities can classify the same
room differently. The fact is captured at live timeline admission, before queued
reply enrichment; marking changes affect subsequent events and never retry bytes.

Operators must manually accept/join an invitation and mark the intended plaintext
room as direct in their Matrix client **for the configured gateway account**.
The gateway does not join invitations or write account data. Unmarked rooms retain
receiver group rules; the DM allow list is not universal two-person-room access
control. Encryption and media remain excluded.

Each receiver owns its exact full Matrix sender-ID allow list: Chat/Hosted
`matrix.dmAllowList`, CFL `[matrix].dm_allow_list`. Missing/empty lists deny all
marked DMs. Lists allow at most 64 unique full IDs of at most 255 UTF-16 units,
without trimming, case folding, wildcards or display-name matching. Allowed
nonself, nonblank DM text automatically wakes; denied DM bodies must be rejected
before context, queue, model input, history or original-payload retention, even
when they mention, reply to or alias the agent. The gateway still sends both
allowed and denied senders' notifications: it reports facts, not receiver policy.
Generic MCP authorization and tool reads/sends remain independent; dynamic MCP
callers never acquire webhooks. Receiver rollout must support this field before
new producer notifications are used. No deployment or historical purge is implied.

Display names are bounded to 255 characters;
body/mentions use reading's field bounds. Own echoes, edits, redactions, reactions,
media, encrypted rooms and history pagination do not notify. Thread metadata is
omitted. Redaction cannot erase a copy already received by the receiver.

Delivery is serial and ordered through an in-memory queue of at most 100 items
(including the active item) per configured runtime. Overflow drops the newest incoming item with a safe
diagnostic. Enrichment runs serially inside this same queue without blocking SDK
sync. It uses the room's SDK timeline first, otherwise one authenticated SDK event
request, bounded to two seconds including response-body transfer and aborted on
timeout/shutdown. No profile lookup or separate author cache is used. Room
membership/encryption is checked again after lookup and before each POST;
unavailable rooms discard pending delivery. Enrichment and serialization occur
once before retries, so
retry bodies are identical. With webhook disabled no enrichment runs.
Each item gets at most three attempts, each with a five-second timeout,
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
exercise HTTP initialize/list/call. Real Matrix SDK clients connected to owned local fake
homeservers verify credential isolation, readiness and timeline-before-sync
ordering, plus independent webhook delivery and startup/reconnect suppression.
Short injected registry timing verifies admission/capacity, idle ownership, failed
creation, disconnect and shutdown cleanup. Typecheck, tests, build and Node executable help smoke
require no real Matrix account. Actual homeserver interoperability remains
unverified. E2EE, account/room management, thread APIs, media, read markers,
edit/redaction writes, stdio, replay and deployment are outside this package.

See [CONTEXT.md](CONTEXT.md) for domain vocabulary.
