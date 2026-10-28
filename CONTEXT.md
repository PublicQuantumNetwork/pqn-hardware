# PQN Hardware Network

The messaging layer that lets consumers on one machine operate lab instruments attached to another machine in a Public Quantum Network node.

## Language

### Network elements

**Router**:
The hub that every other network element registers with and that forwards **Packets** between them.
_Avoid_: broker, server

**Provider**:
A network element that owns real **Instruments** and executes requests against them. (Class: `InstrumentProvider`.)
_Avoid_: instrument server, node

**Client**:
A network element that sends requests to **Providers** through a **Router** and waits for replies.

**Proxy Instrument**:
A client-side stand-in for an **Instrument** on a remote **Provider**. Reading or calling it turns into **Packets** on the wire.

**Identity**:
The ZMQ routing id of a network element. It always equals the element's name, and the **Router** rejects any **Packet** whose source does not match the identity it arrived on.

**Heartbeat**:
The periodic re-registration a **Provider** sends to its **Router**. It lets the **Provider** re-register after a **Router** restart, and it refreshes the **Provider**'s entry in the **Directory**.
_Avoid_: beat, keepalive

**Last seen**:
The time of the most recent **Heartbeat** the **Router** received from a **Provider**. It is a hint only, because a **Provider** busy with a long **Operation** sends no **Heartbeats**.

**Directory**:
The **Router**'s record of registered **Providers**: name, **Last seen**, and the **Instruments** they reported. It is used for diagnostics and `GET_PROVIDERS`, never for delivery. Entries are never removed while the **Router** runs.
_Avoid_: registry, routing table

### Instruments

**Instrument**:
A piece of lab hardware (rotator, time tagger, polarimeter, …) driven by a driver class on a **Provider**, and addressed by a unique name.

**Operation**:
A named action an **Instrument** can perform, taking keyword arguments and returning a value (e.g. `move_to`, `count_singles`).
_Avoid_: method, command

**Parameter**:
A named, readable and optionally writable value on an **Instrument** (e.g. `degrees`).
_Avoid_: attribute, property, setting

**Device structure**:
The description a **Provider** publishes of one **Instrument**: its **Operations** and **Parameters**, with JSON Schemas for their types.

### Messages

**Packet**:
The single message envelope on the wire. It carries a protocol version, an **Intent**, a **Request**, source and destination names, a hop count, an id (echoed as `reply_to` in the reply), and a **Payload**.
_Avoid_: message, frame

**Request**:
A verb from a fixed set (e.g. `CALL_OPERATION`, `GET_PARAMETER`) naming what a **Packet** asks for. Each **Request** has exactly one **Payload** shape.

**Intent**:
The category of a **Packet** (data, control, registration, ping, error, …), which decides how the receiver handles it.

**Payload**:
The intent-specific body of a **Packet**. It must be representable as JSON.

**Error code**:
A stable, machine-readable identifier in an error **Payload** (e.g. `UNKNOWN_DESTINATION`, `INSTRUMENT_ERROR`). Peers branch on the code, never on the message text.
_Avoid_: error string, error type

**Origin**:
The name of the network element that raised an error, which is not necessarily the **Packet**'s source.

**Wire protocol**:
The encoding of **Packets** as bytes on ZMQ sockets. It is JSON (see ADR-0001).

## Relationships

- A **Provider** registers with exactly one **Router**; a **Client** registers with exactly one **Router**.
- A **Provider** owns one or more **Instruments**; a **Proxy Instrument** mirrors exactly one of them.
- Every **Packet** has exactly one **Intent** and at most one **Payload**.
- A **Router** delivers to whichever element is currently connected with the destination's **Identity**, whether or not it appears in the **Directory**.
- A **Router** only delivers to elements registered with itself. Router-to-router forwarding is out of scope: a **Packet** for an unknown destination gets an error reply.

## Flagged ambiguities

- None yet.
