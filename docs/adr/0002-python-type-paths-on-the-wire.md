# Python type paths on the wire, plain dataclasses, no import allowlist

Operation arguments, return values, and Parameter values are typed by the driver's ordinary type hints. Any type that pydantic's `TypeAdapter` can handle is allowed, including plain stdlib `@dataclass`es. Researchers writing drivers do not have to learn about or subclass pydantic `BaseModel`.

When a value's type is not JSON-native, the device structure tells the Client the type's Python import path (`__module__` + `__qualname__`). The Python `ProxyInstrument` imports that path and rebuilds the typed object, so both ends work with the same class. Non-Python peers ignore the path and use the JSON Schema. If the import fails, the proxy returns the plain JSON data and logs a warning.

## Considered options

- **A registry of stable wire names** (a decorator on each model). Rejected because it is too much ceremony for the non-developer researchers who write drivers.
- **An allowlist of importable module prefixes.** Rejected because every deployment runs on a private VPN and the ports are not reachable from outside it. We accept the risk that the network can make a Client import any installed module.
- **Requiring `BaseModel` for all complex types.** Rejected because researchers find it hard to understand, and plain dataclasses already validate and serialize through `TypeAdapter`.

## Consequences

- A driver method whose type hints cannot be turned into a JSON Schema is a configuration error when the Provider starts, not at call time.
- If the VPN assumption ever stops holding, revisit the allowlist.
