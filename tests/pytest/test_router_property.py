"""Property-based Router test: generated envelopes over every Intent and Request.

Example-based tests leave narrow input slices untested (an Intent, a hops value, a spoofed
source). This test sends generated envelopes to one running Router from raw zmq sockets and
asserts the outcome the Router's rules prescribe for each incoming message:

1. A wrong frame count gets MALFORMED_PACKET.
2. Bytes that are not valid JSON or not a valid envelope get MALFORMED_PACKET.
3. A version other than PROTOCOL_VERSION gets UNSUPPORTED_VERSION.
4. A source that does not match the identity frame gets MALFORMED_PACKET.
5. A Packet addressed to the Router itself: REGISTER updates the Directory and is ACKed, PING
   gets PONG, GET_PROVIDERS lists the Providers; anything else gets UNSUPPORTED_REQUEST.
6. Any other Packet is forwarded to its destination's identity with one more hop. If that
   fails, the sender gets UNKNOWN_DESTINATION, or DESTINATION_UNREACHABLE with Last seen when
   the destination is in the Directory.

An ERROR Packet is itself a reply and is never answered: one addressed to the Router (step 5)
or one the Router cannot deliver (step 6) is logged and dropped.
"""

import json
import logging
import threading
from collections.abc import Generator
from typing import Any

import pytest
import zmq
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from pqn_hardware.network.packet import PROTOCOL_VERSION
from pqn_hardware.network.packet import DeviceSummary
from pqn_hardware.network.packet import ErrorCode
from pqn_hardware.network.packet import ErrorPayload
from pqn_hardware.network.packet import NetworkElementClass
from pqn_hardware.network.packet import Packet
from pqn_hardware.network.packet import PacketIntent
from pqn_hardware.network.packet import RegisterPayload
from pqn_hardware.network.packet import Request
from pqn_hardware.network.packet import create_registration_packet
from pqn_hardware.network.packet import decode
from pqn_hardware.network.packet import encode
from pqn_hardware.network.router import Router
from tests.pytest.test_internal_network import HOST
from tests.pytest.test_internal_network import ROUTER_NAME
from tests.pytest.test_internal_network import _free_port
from tests.pytest.test_internal_network import _stop_router
from tests.pytest.test_internal_network import _wait_until

logger = logging.getLogger(__name__)

# The sender's identity: the generated source equals it or does not.
SENDER_NAME = "property-client"
# A Dealer standing in for a registered, connected Provider.
REGISTERED_PROVIDER = "property-provider"
# A connected peer that never registers: delivery is by identity, not by registration.
LURKER = "property-lurker"
# A name nothing ever connects or registers with.
UNKNOWN_NAME = "no-such-destination"

DESTINATIONS = (ROUTER_NAME, REGISTERED_PROVIDER, LURKER, UNKNOWN_NAME)
CONNECTED_PEERS = (REGISTERED_PROVIDER, LURKER)

# Every reply must arrive well inside this bound; the receive timeout fails a missing reply
# fast. Replies on localhost take milliseconds, so a short bound still tells reply from silence.
REPLY_TIMEOUT_MS = 150


class _PropertyRouter:
    """One running Router plus the raw sockets the property test sends from and receives on."""

    def __init__(self) -> None:
        self.port = _free_port()
        self.router = Router(ROUTER_NAME, HOST, self.port)
        threading.Thread(target=self.router.start, daemon=True).start()
        _wait_until(lambda: self.router.running, 10, "Router did not start.")

        self.context = zmq.Context()
        self.peers: dict[str, zmq.Socket[bytes]] = {}
        for name in (REGISTERED_PROVIDER, LURKER, SENDER_NAME):
            peer = self.context.socket(zmq.DEALER)
            peer.setsockopt(zmq.RCVTIMEO, REPLY_TIMEOUT_MS)
            peer.setsockopt_string(zmq.IDENTITY, name)
            peer.connect(f"tcp://{HOST}:{self.port}")
            self.peers[name] = peer

        # Register the Provider stand-in so the Directory holds it; the lurker only connects.
        registration = create_registration_packet(
            source=REGISTERED_PROVIDER,
            destination=ROUTER_NAME,
            kind=NetworkElementClass.PROVIDER,
            devices={"prop1": DeviceSummary(type="property.Driver", desc="a device", hw_address="1")},
        )
        self.peers[REGISTERED_PROVIDER].send(encode(registration))
        _, ack_data = self.peers[REGISTERED_PROVIDER].recv_multipart()
        assert decode(ack_data).intent == PacketIntent.REGISTRATION_ACK, "Provider stand-in did not register."

    def send(self, wire: bytes) -> None:
        self.peers[SENDER_NAME].send(wire)

    def receive(self, name: str) -> Packet | None:
        """Receive one reply on a peer's socket, or None when nothing arrives in time."""
        try:
            _, data = self.peers[name].recv_multipart()
        except zmq.error.Again:
            return None
        return decode(data)

    def close(self) -> None:
        try:
            _stop_router(self.router)
        finally:
            for peer in self.peers.values():
                peer.close()
            self.context.term()


@pytest.fixture(scope="module")
def property_router() -> Generator[_PropertyRouter, None, None]:
    harness = _PropertyRouter()
    yield harness
    harness.close()


def _assert_error_reply(reply: Packet, request: Packet, code: ErrorCode) -> ErrorPayload:
    """Assert a reply is the Router's error reply for the request, echoing its id.

    The reply always answers the sender's identity frame, never a spoofed source.
    """
    assert reply.intent == PacketIntent.ERROR
    assert reply.request == Request.ERROR
    assert reply.source == ROUTER_NAME
    assert reply.destination == SENDER_NAME
    assert reply.reply_to == request.id
    payload = ErrorPayload.model_validate(reply.payload)
    assert payload.code == code
    assert payload.origin == ROUTER_NAME
    return payload


def _assert_still_running(property_router: _PropertyRouter) -> None:
    """Assert the Router keeps running: a follow-up PING to it still gets its PONG."""
    probe = Packet(intent=PacketIntent.PING, request=Request.PING, source=SENDER_NAME, destination=ROUTER_NAME)
    property_router.send(encode(probe))
    reply = property_router.receive(SENDER_NAME)
    assert reply is not None
    assert reply.request == Request.PONG
    assert reply.reply_to == probe.id


def _assert_request_to_router(property_router: _PropertyRouter, packet: Packet) -> None:
    """Assert the outcome step 5 prescribes for a request to the Router."""
    if packet.intent == PacketIntent.ERROR:
        # An ERROR Packet addressed to the Router is logged, never answered.
        assert property_router.receive(SENDER_NAME) is None
        _assert_still_running(property_router)
        return
    match packet.request:
        case Request.REGISTER:
            # Step 5: handle the REGISTER (a Client registers too) and ACK.
            reply = property_router.receive(SENDER_NAME)
            assert reply is not None, "a REGISTER must get its acknowledgement"
            assert reply.intent == PacketIntent.REGISTRATION_ACK
            assert reply.request == Request.ACKNOWLEDGE
            assert reply.source == ROUTER_NAME
            assert reply.reply_to == packet.id
        case Request.PING:
            reply = property_router.receive(SENDER_NAME)
            assert reply is not None, "a PING to the Router must get a PONG"
            assert reply.intent == PacketIntent.PING
            assert reply.request == Request.PONG
            assert reply.source == ROUTER_NAME
            assert reply.reply_to == packet.id
        case Request.GET_PROVIDERS:
            reply = property_router.receive(SENDER_NAME)
            assert reply is not None, "a GET_PROVIDERS must get its reply"
            assert reply.intent == PacketIntent.DATA
            assert reply.request == Request.GET_PROVIDERS
            assert reply.reply_to == packet.id
            listed = {provider["name"] for provider in reply.payload["providers"]}
            assert REGISTERED_PROVIDER in listed
        case _:
            # Step 5: anything else addressed to the Router is unsupported.
            reply = property_router.receive(SENDER_NAME)
            assert reply is not None, "an unsupported request must get a fast reply"
            _assert_error_reply(reply, packet, ErrorCode.UNSUPPORTED_REQUEST)


def _assert_forwarding(property_router: _PropertyRouter, packet: Packet) -> None:
    """Assert the outcome step 6 prescribes for a Packet to another peer."""
    if packet.request == Request.REGISTER:
        # A REGISTER not addressed to the Router is malformed.
        reply = property_router.receive(SENDER_NAME)
        assert reply is not None, "a misaddressed REGISTER must get a fast reply"
        _assert_error_reply(reply, packet, ErrorCode.MALFORMED_PACKET)
        return

    if packet.destination in CONNECTED_PEERS:
        # Step 6: forward to the destination's identity with one more hop, registered or not.
        forwarded = property_router.receive(packet.destination)
        assert forwarded is not None, "a Packet to a connected peer must be forwarded"
        assert forwarded == packet.model_copy(update={"hops": packet.hops + 1})
        assert property_router.receive(SENDER_NAME) is None
        return

    if packet.intent == PacketIntent.ERROR:
        # An ERROR Packet the Router cannot deliver is dropped with a warning, never
        # answered: the sender never answers an ERROR either, so it would only log the answer.
        assert property_router.receive(SENDER_NAME) is None
        _assert_still_running(property_router)
        return

    # An unknown destination: not in the Directory and not connected.
    reply = property_router.receive(SENDER_NAME)
    assert reply is not None, "a Packet to an unknown destination must get a fast reply"
    error = _assert_error_reply(reply, packet, ErrorCode.UNKNOWN_DESTINATION)
    assert f"Destination '{UNKNOWN_NAME}' is not registered with {ROUTER_NAME}" in error.message
    assert REGISTERED_PROVIDER in error.message


@settings(max_examples=120, deadline=None)
@given(
    intent=st.sampled_from(PacketIntent),
    request_verb=st.sampled_from(Request),
    version=st.integers(min_value=0, max_value=4),
    hops=st.integers(min_value=0, max_value=2**31),
    spoofed=st.booleans(),
    destination=st.sampled_from(DESTINATIONS),
)
def test_router_outcomes(  # noqa: PLR0913 - the fixture plus one parameter per generated facet
    property_router: _PropertyRouter,
    intent: PacketIntent,
    request_verb: Request,
    version: int,
    hops: int,
    spoofed: bool,  # noqa: FBT001 - a hypothesis-generated facet, not a flag
    destination: str,
) -> None:
    """The Router's reply (or silence, or the forward) is exactly what steps 1-6 prescribe."""
    source = "pretend-to-be-someone" if spoofed else SENDER_NAME
    # The Payload shape is fixed by the request verb; only REGISTER carries one the Router validates.
    payload: dict[str, Any] | None = (
        RegisterPayload(
            kind=NetworkElementClass.CLIENT, devices={"d": DeviceSummary(type="t", desc="d", hw_address="a")}
        ).model_dump()
        if request_verb == Request.REGISTER
        else None
    )
    packet = Packet(
        intent=intent, request=request_verb, source=source, destination=destination, hops=hops, payload=payload
    )
    wire = json.loads(encode(packet))
    wire["version"] = version

    property_router.send(json.dumps(wire).encode("utf-8"))

    if version != PROTOCOL_VERSION:
        # Step 3: the version check precedes everything else, whatever the source.
        reply = property_router.receive(SENDER_NAME)
        assert reply is not None, "a wrong version must get a fast reply"
        error = _assert_error_reply(reply, packet, ErrorCode.UNSUPPORTED_VERSION)
        assert error.details == {"expected": PROTOCOL_VERSION, "received": version}
    elif spoofed:
        # Step 4: the source must match the identity frame the Packet arrived on.
        reply = property_router.receive(SENDER_NAME)
        assert reply is not None, "a spoofed source must get a fast reply"
        _assert_error_reply(reply, packet, ErrorCode.MALFORMED_PACKET)
    elif destination == ROUTER_NAME:
        _assert_request_to_router(property_router, packet)
    else:
        _assert_forwarding(property_router, packet)
