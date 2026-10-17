# Changelog

All notable changes to this project will be documented in this file, formatted according to [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Additional protocols for the valentines demo and qkd

## [0.3.0] - 2026-10-17

### Changed

- The wire protocol between Router, Providers and Clients is JSON (pydantic models, protocol version 2), replacing pickle. A v1 peer and a v2 peer cannot talk to each other: upgrade the Router, all Providers and all Clients together.
- Requests are fixed verbs with typed payloads (`CALL_OPERATION`, `GET_PARAMETER`, ...); `GET_DEVICES` returns per-instrument summaries carrying the driver's import path.
- Errors are error payloads with stable, machine-readable codes (`UNKNOWN_DESTINATION`, `MALFORMED_PACKET`, ...). The Router and Providers answer bad input with an error instead of crashing or dropping it.
- Every request carries a correlation `id`, echoed as `reply_to`, so a late reply can no longer be returned as the answer to the next request.

### Added

- `protocol/schema.json`, the committed JSON Schema of the wire protocol, generated from the models with `uv run scripts/protocol_schema generate`. CI fails a PR whose committed schema is stale, or that changes it without a `PROTOCOL_VERSION` bump.
- `GET_PROVIDERS`, answered by the Router: each Provider's name, last seen time and devices.

## [0.1.0] - 2025-02-05

### Added

- Existing functionality for CHSH bell tests.
- Device drivers for polarimeters, rotation mounts, and timetaggers.
- Inter-node communication.
- This CHANGELOG file to track notable changes in the project.

[unreleased]: https://github.com/PublicQuantumNetwork/pqn-stack/compare/0.3.0...HEAD
[0.3.0]: https://github.com/PublicQuantumNetwork/pqn-stack/compare/0.1.0...0.3.0
[0.1.0]: https://github.com/PublicQuantumNetwork/pqn-stack/releases/tag/0.1.0
