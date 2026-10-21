# JSON wire protocol, no pickle

pqn-hardware is being adopted by a second quantum network with a larger team, and its network elements will need to talk to peers that are not written in Python. We are replacing pickle with JSON as the only wire encoding for **Packets**, with the message shapes defined as pydantic models. There is no pickle fallback or "serialize any Python object" escape hatch: every **Payload** must be expressible in JSON and described by a schema that non-Python peers can implement against.

## Consequences

- All Router, Provider, and Client peers must upgrade together. A pickle peer and a JSON peer cannot talk to each other.
- Payloads that relied on Python-only types (enums, tuples, sets, class objects, dataclasses, driver-specific result objects) must get explicit JSON representations.
- The wire protocol is pinned by `PROTOCOL_VERSION` (starting at 2). The Router rejects any Packet whose version does not match exactly.
- The JSON Schema of the protocol (the envelope, Request Payloads, error Payload, enums) is committed as `protocol/schema.json`. That file is the spec for non-Python peers. CI fails a PR if the committed schema is stale, or if it differs from `master` without a `PROTOCOL_VERSION` bump. Any schema change requires a bump, docstring-derived descriptions included. Per-driver Operation and Parameter types are outside the protocol: they are discovered at runtime through the Device structure, and changing them never requires a bump.
