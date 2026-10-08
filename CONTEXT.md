# Matrix for Agent

Glossary of Matrix identities, rooms, events and agent integration terms.

## Language

**Matrix identity**:
The Matrix user represented by a full Matrix user ID. One identity may participate through multiple devices.
_Avoid_: Agent session, device

**Matrix device**:
A client installation associated with a Matrix identity, identified by a device ID and, when encryption is enabled, its device keys.
_Avoid_: Matrix identity, agent

**Room**:
A Matrix conversation space identified by a room ID. Its display name may change and is not its identity.
_Avoid_: Channel, destination name

**Event**:
A Matrix record identified by an event ID within a room. Messages, edits, reactions, and room-state changes are distinct event types.
_Avoid_: Message when referring to all event types

**Thread**:
A conversation rooted at a room event. The gateway treats its text as ordinary text and does not expose thread metadata. An ordinary reply does not by itself establish a thread.
_Avoid_: Room, ordinary reply

**Available room**:
A plaintext Matrix room that the gateway's Matrix identity currently belongs to. All such rooms are available to the agent integration.
_Avoid_: Allowed room, configured room

**Incoming message**:
A newly observed text message authored by another Matrix identity in an available room, eligible for notification regardless of whether it addresses the agent. Historical messages, edits, and redactions are not incoming messages.
_Avoid_: Triggering message

**Integration receiver**:
The external recipient of incoming Matrix notifications that decides how they contribute to agent context and whether an agent responds.
_Avoid_: DSH Companion, Matrix device

**Message edit**:
A replacement of a message's content by its original author, related to the original message event.
_Avoid_: New message

**Redaction**:
A Matrix operation that removes content from a target event according to Matrix rules. It does not erase the event's identity or guarantee deletion of copies already received.
_Avoid_: Event deletion
