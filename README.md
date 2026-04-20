# pqn-hardware

**Hardware control library for the Public Quantum Network (PQN)**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)

`pqn-hardware` provides the instrument drivers, network messaging, and protocols used to operate hardware at PQN nodes. It is consumed as a library by the FastAPI-based Node service that lives in [pqn-node](https://github.com/PublicQuantumNetwork/pqn-node), and can also be used standalone for bench experiments or scripts.

This package was split off from [pqn-stack](https://github.com/PublicQuantumNetwork/pqn-stack) (now `pqn-node`) to let hardware work and app work evolve independently.

## What's in here

- `pqn_hardware.base` — `Instrument` protocols and common abstractions (rotator, timetagger, polarimeter, …).
- `pqn_hardware.network` — ZMQ-based router, instrument provider, and client for cross-machine messaging between hardware and consumers.
- `pqn_hardware.pqn.drivers` — concrete instrument drivers (Thorlabs rotators, TimeTagger, QKD, polarimeter, …).
- `pqn_hardware.pqn.protocols` — quantum protocols (CHSH, QKD, tomography, visibility).
- `pqn_hardware.constants` — shared polarization/Bell-state definitions used by both protocols and downstream consumers.
- `pqn-hw` CLI — start an `InstrumentProvider` or a `Router`.

## Install

```bash
git clone https://github.com/PublicQuantumNetwork/pqn-hardware.git
cd pqn-hardware
uv sync
```

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

## Quick start

Start a router:

```bash
uv run pqn-hw start-router --config configs/config_messaging_example.toml
```

Start an instrument provider:

```bash
uv run pqn-hw start-provider --config configs/config_messaging_example.toml
```

Or pass config via CLI flags:

```bash
uv run pqn-hw start-provider \
  --name provider1 \
  --router-name router1 \
  --instruments '{"dummy1": {"import": "pqn_hardware.pqn.drivers.dummies.DummyInstrument", "desc": "Test Instrument", "hw_address": "123456"}}'
```

See [`configs/config_messaging_example.toml`](configs/config_example.toml) for the full config shape.

## Consuming from pqn-node

`pqn-node` depends on this package via a git URL pinned to a tag or commit:

```toml
dependencies = [
    "pqn-hardware @ git+https://github.com/PublicQuantumNetwork/pqn-hardware.git@<tag-or-sha>",
]
```

## Acknowledgements

The Public Quantum Network is supported in part by NSF Quantum Leap Challenge Institute HQAN under Award No. 2016136, Illinois Computes, and by the DOE Grant No. 712869, "Advanced Quantum Networks for Science Discovery."

## Contact

Reach the PQN team at publicquantumnetwork@gmail.com.
