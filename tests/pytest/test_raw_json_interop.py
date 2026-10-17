"""Raw-JSON interop test: a peer with no pqn_hardware or pydantic imports talks the wire protocol.

The peer code below builds every message as a hand-written dict, sends it with plain zmq, and
validates it against protocol/schema.json with jsonschema, the way a non-Python peer would
(ADR-0001). If it passes, the committed spec matches the code. The only pqn_hardware code in
this file is the interop_port fixture, which starts the system under test (the real Router and
Provider) in-process; the peer never touches it.

Covered: REGISTER/ACK, PING/PONG to the Router, GET_PROVIDERS,
GET_DEVICES, GET_DEVICE_STRUCTURE, CALL_OPERATION, GET_PARAMETER, SET_PARAMETER, GET_INFO and
one ERROR reply.
"""

import json
import threading
import uuid
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
import zmq
from jsonschema import validate

# The system under test: the real Router and Provider, started in-process with the existing
# test helpers.
from pqn_hardware.network.instrument_provider import InstrumentProvider
from pqn_hardware.network.router import Router
from tests.pytest.test_internal_network import HOST
from tests.pytest.test_internal_network import PROVIDER_NAME
from tests.pytest.test_internal_network import ROUTER_NAME
from tests.pytest.test_internal_network import _free_port
from tests.pytest.test_internal_network import _wait_until

INTEROP_PROVIDER_NAME = "pqn_hardware-interop-provider"
INTEROP_CLIENT_NAME = "pqn_hardware-interop-client"
REPLY_TIMEOUT_MS = 2_000

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "protocol" / "schema.json"
SCHEMA: dict[str, Any] = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))

# Each request verb's Payload (None means the wire carries null). Each test
# passes the reply's Payload model explicitly: an ERROR reply can answer any request. The
# GET_DEVICE_STRUCTURE reply carries the Device structure, whose shape is per-driver and outside
# the protocol, so only its envelope is validated.
REQUEST_PAYLOADS: dict[str, str | None] = {
    "REGISTER": "RegisterPayload",
    "GET_DEVICE_STRUCTURE": "GetDeviceStructurePayload",
    "CALL_OPERATION": "CallOperationPayload",
    "GET_PARAMETER": "GetParameterPayload",
    "SET_PARAMETER": "SetParameterPayload",
    "GET_INFO": "GetInfoPayload",
    "PING": None,
    "GET_PROVIDERS": None,
    "GET_DEVICES": None,
}


def envelope(intent: str, request: str, source: str, destination: str, payload: dict[str, Any] | None = None) -> dict:
    """Build a wire envelope by hand: the peer knows the protocol only from the schema."""
    return {
        "version": SCHEMA["protocol_version"],
        "id": uuid.uuid4().hex,
        "reply_to": None,
        "intent": intent,
        "request": request,
        "source": source,
        "destination": destination,
        "hops": 0,
        "payload": payload,
    }


def validate_message(message: dict[str, Any], payload_model: str | None) -> None:
    """Validate one wire message: its envelope, and its Payload when the request fixes one."""
    validate(instance=message, schema=SCHEMA["envelope"])
    if payload_model is not None:
        validate(instance=message["payload"], schema=SCHEMA["payloads"][payload_model])


class RawDealerPeer:
    """A Provider-like peer: a DEALER socket sending [identity, data]."""

    def __init__(self, port: int, name: str) -> None:
        self.name = name
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.RCVTIMEO, REPLY_TIMEOUT_MS)
        self.socket.setsockopt_string(zmq.IDENTITY, name)
        self.socket.connect(f"tcp://{HOST}:{port}")

    def send(self, message: dict[str, Any], payload_model: str | None) -> None:
        validate_message(message, payload_model)
        self.socket.send(json.dumps(message).encode("utf-8"))

    def receive(self, payload_model: str | None = None) -> dict[str, Any]:
        # A Router sends [delimiter, data] to a DEALER peer.
        _, data = self.socket.recv_multipart()
        message = json.loads(data)
        validate_message(message, payload_model)
        return message

    def close(self) -> None:
        self.socket.close(linger=0)
        self.context.term()


class RawReqPeer:
    """A Client-like peer: a REQ socket sending [identity, "", data]."""

    def __init__(self, port: int, name: str) -> None:
        self.name = name
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.setsockopt(zmq.RCVTIMEO, REPLY_TIMEOUT_MS)
        self.socket.setsockopt_string(zmq.IDENTITY, name)
        self.socket.connect(f"tcp://{HOST}:{port}")

    def ask(self, message: dict[str, Any], reply_payload_model: str | None) -> dict[str, Any]:
        request_payload_model = REQUEST_PAYLOADS[message["request"]]
        validate_message(message, request_payload_model)
        self.socket.send(json.dumps(message).encode("utf-8"))
        reply = json.loads(self.socket.recv())
        validate_message(reply, reply_payload_model)
        return reply

    def close(self) -> None:
        self.socket.close(linger=0)
        self.context.term()


@pytest.fixture(scope="module")
def interop_port() -> int:
    """Start the real Router and an InstrumentProvider in daemon threads; return the port they talk on."""
    port = _free_port()

    router = Router(ROUTER_NAME, HOST, port)
    threading.Thread(target=router.start, daemon=True).start()
    _wait_until(lambda: router.running, 10, "Router did not start.")

    provider = InstrumentProvider(
        PROVIDER_NAME,
        HOST,
        port,
        ROUTER_NAME,
        dummy1={
            "import": "pqn_hardware.drivers.dummies.DummyInstrument",
            "desc": "Dummy instrument1 for testing purposes",
            "hw_address": "1234",
        },
        dummy2={
            "import": "pqn_hardware.drivers.dummies.DummyInstrument",
            "desc": "Dummy instrument2 for testing purposes",
            "hw_address": "5678",
        },
    )
    threading.Thread(target=provider.start, daemon=True).start()
    _wait_until(lambda: PROVIDER_NAME in router.directory, 15, "InstrumentProvider did not register with the Router.")

    return port


@pytest.fixture(scope="module")
def dealer_peer(interop_port: int) -> Generator[RawDealerPeer, Any, None]:
    """Provide a registered Provider-like DEALER peer, so GET_PROVIDERS lists it deterministically."""
    peer = RawDealerPeer(interop_port, INTEROP_PROVIDER_NAME)
    registration = envelope(
        "REGISTRATION",
        "REGISTER",
        INTEROP_PROVIDER_NAME,
        ROUTER_NAME,
        payload={
            "kind": "PROVIDER",
            "devices": {
                "interop-dummy": {
                    "type": "raw.peer.DummyInstrument",
                    "desc": "a peer-reported device",
                    "hw_address": "7777",
                }
            },
        },
    )
    peer.send(registration, "RegisterPayload")
    peer.receive()  # The registration acknowledgement; asserted in full by the REGISTER test below.
    yield peer
    peer.close()


@pytest.fixture(scope="module")
def client_peer(interop_port: int) -> Generator[RawReqPeer, Any, None]:
    """Provide a registered Client-like REQ peer, one for the whole module like the Router's real Clients."""
    peer = RawReqPeer(interop_port, INTEROP_CLIENT_NAME)
    registration = envelope("REGISTRATION", "REGISTER", INTEROP_CLIENT_NAME, ROUTER_NAME, payload={"kind": "CLIENT"})
    peer.ask(registration, None)
    yield peer
    peer.close()


def test_register_gets_acknowledged(dealer_peer: RawDealerPeer) -> None:
    # A Provider-like peer registers (a repeated REGISTER is a Heartbeat) and gets the
    # acknowledgement: intent REGISTRATION_ACK, request ACKNOWLEDGE, payload null, echoing the
    # registration's id as reply_to.
    registration = envelope(
        "REGISTRATION",
        "REGISTER",
        INTEROP_PROVIDER_NAME,
        ROUTER_NAME,
        payload={
            "kind": "PROVIDER",
            "devices": {
                "interop-dummy": {
                    "type": "raw.peer.DummyInstrument",
                    "desc": "a peer-reported device",
                    "hw_address": "7777",
                }
            },
        },
    )

    dealer_peer.send(registration, "RegisterPayload")
    ack = dealer_peer.receive()

    assert ack["intent"] == "REGISTRATION_ACK"
    assert ack["request"] == "ACKNOWLEDGE"
    assert ack["source"] == ROUTER_NAME
    assert ack["destination"] == INTEROP_PROVIDER_NAME
    assert ack["reply_to"] == registration["id"]
    assert ack["id"] != registration["id"]
    assert ack["payload"] is None


def test_ping_to_the_router_gets_pong(client_peer: RawReqPeer) -> None:
    ping = envelope("PING", "PING", INTEROP_CLIENT_NAME, ROUTER_NAME)

    pong = client_peer.ask(ping, None)

    assert pong["intent"] == "PING"
    assert pong["request"] == "PONG"
    assert pong["source"] == ROUTER_NAME
    assert pong["destination"] == INTEROP_CLIENT_NAME
    assert pong["reply_to"] == ping["id"]
    assert pong["payload"] is None


@pytest.mark.usefixtures("dealer_peer")  # The Dealer peer must be registered before this test runs.
def test_get_providers_lists_the_registered_providers(client_peer: RawReqPeer) -> None:
    request = envelope("DATA", "GET_PROVIDERS", INTEROP_CLIENT_NAME, ROUTER_NAME)

    reply = client_peer.ask(request, "ProvidersPayload")

    providers = {provider["name"]: provider for provider in reply["payload"]["providers"]}
    # The in-process Provider reports its Instruments at registration.
    assert set(providers[PROVIDER_NAME]["devices"]) == {"dummy1", "dummy2"}
    # The raw Dealer peer registered as a Provider too, reporting its own device.
    assert providers[INTEROP_PROVIDER_NAME]["devices"]["interop-dummy"]["hw_address"] == "7777"
    # Last seen travels as an ISO 8601 string in UTC.
    assert isinstance(providers[PROVIDER_NAME]["last_seen"], str)


def test_get_devices_lists_the_providers_instruments(client_peer: RawReqPeer) -> None:
    request = envelope("DATA", "GET_DEVICES", INTEROP_CLIENT_NAME, PROVIDER_NAME)

    reply = client_peer.ask(request, "DevicesPayload")

    devices = reply["payload"]["devices"]
    assert set(devices) == {"dummy1", "dummy2"}
    assert devices["dummy1"]["type"] == "pqn_hardware.drivers.dummies.DummyInstrument"
    assert devices["dummy1"]["desc"] == "Dummy instrument1 for testing purposes"
    assert devices["dummy1"]["hw_address"] == "1234"


def test_get_device_structure_describes_the_instrument(client_peer: RawReqPeer) -> None:
    request = envelope(
        "DATA", "GET_DEVICE_STRUCTURE", INTEROP_CLIENT_NAME, PROVIDER_NAME, payload={"instrument": "dummy1"}
    )

    reply = client_peer.ask(request, None)

    # The Device structure's shape is per-driver and outside the protocol, so only
    # the envelope is schema-validated; the structure itself is asserted by hand.
    structure = reply["payload"]
    assert structure["name"] == "dummy1"
    assert structure["type"] == "pqn_hardware.drivers.dummies.DummyInstrument"
    assert structure["info"]["type"] == "pqn_hardware.drivers.dummies.DummyInfo"
    assert set(structure["operations"]) >= {"double_int", "set_half_input_int", "fail"}
    assert set(structure["parameters"]) == {"param_int", "param_str", "param_bool"}


def test_call_operation_runs_the_operation(client_peer: RawReqPeer) -> None:
    # set_half_input_int halves its argument, so the reply is deterministic whatever dummy1's
    # state the earlier tests left behind.
    request = envelope(
        "CONTROL",
        "CALL_OPERATION",
        INTEROP_CLIENT_NAME,
        PROVIDER_NAME,
        payload={"instrument": "dummy1", "operation": "set_half_input_int", "kwargs": {"value": 36}},
    )

    reply = client_peer.ask(request, "ValuePayload")

    assert reply["intent"] == "CONTROL"
    assert reply["request"] == "CALL_OPERATION"
    assert reply["source"] == PROVIDER_NAME
    assert reply["reply_to"] == request["id"]
    assert reply["payload"] == {"value": 18}


def test_set_then_get_parameter_round_trips(client_peer: RawReqPeer) -> None:
    set_request = envelope(
        "CONTROL",
        "SET_PARAMETER",
        INTEROP_CLIENT_NAME,
        PROVIDER_NAME,
        payload={"instrument": "dummy2", "parameter": "param_int", "value": 7},
    )
    set_reply = client_peer.ask(set_request, None)
    assert set_reply["payload"] is None

    get_request = envelope(
        "CONTROL",
        "GET_PARAMETER",
        INTEROP_CLIENT_NAME,
        PROVIDER_NAME,
        payload={"instrument": "dummy2", "parameter": "param_int"},
    )
    get_reply = client_peer.ask(get_request, "ValuePayload")
    assert get_reply["payload"] == {"value": 7}


def test_get_info_returns_the_info_object(client_peer: RawReqPeer) -> None:
    request = envelope("CONTROL", "GET_INFO", INTEROP_CLIENT_NAME, PROVIDER_NAME, payload={"instrument": "dummy2"})

    reply = client_peer.ask(request, "ValuePayload")

    info = reply["payload"]["value"]
    assert info["name"] == "dummy2"
    assert info["desc"] == "Dummy instrument2 for testing purposes"
    assert info["hw_address"] == "5678"
    assert set(info) == {"name", "desc", "hw_address", "hw_status", "param_int", "param_str", "param_bool"}


def test_error_reply_carries_the_error_payload(client_peer: RawReqPeer) -> None:
    # A PING to a destination that never registered: the Router answers with an ERROR Packet
    # whose ErrorPayload carries a stable code, never a free-text string.
    request = envelope("PING", "PING", INTEROP_CLIENT_NAME, "no-such-destination")

    reply = client_peer.ask(request, "ErrorPayload")

    assert reply["intent"] == "ERROR"
    assert reply["request"] == "ERROR"
    assert reply["source"] == ROUTER_NAME
    assert reply["reply_to"] == request["id"]
    error = reply["payload"]
    assert error["code"] == "UNKNOWN_DESTINATION"
    assert error["origin"] == ROUTER_NAME
    assert isinstance(error["message"], str)
    assert error["details"] == {}
