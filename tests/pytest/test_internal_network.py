"""Network tests running the Router and an InstrumentProvider in-process, on an ephemeral port.

The CLI smoke test in test_cli_smoke.py covers starting the same elements as subprocesses.
"""

import datetime
import json
import logging
import socket
import threading
import time
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

import pytest
import zmq
from pydantic import ValidationError

from pqn_hardware.drivers.dummies import DummyInfo
from pqn_hardware.drivers.dummies import DummyInstrument
from pqn_hardware.errors import InvalidInstrumentsConfigurationError
from pqn_hardware.errors import PacketError
from pqn_hardware.instrument import InstrumentInfo
from pqn_hardware.network.client import Client
from pqn_hardware.network.client import ProxyInstrument
from pqn_hardware.network.client import _dump_argument
from pqn_hardware.network.instrument_provider import InstrumentProvider
from pqn_hardware.network.packet import PROTOCOL_VERSION
from pqn_hardware.network.packet import CallOperationPayload
from pqn_hardware.network.packet import DeviceSummary
from pqn_hardware.network.packet import ErrorCode
from pqn_hardware.network.packet import ErrorPayload
from pqn_hardware.network.packet import GetDeviceStructurePayload
from pqn_hardware.network.packet import GetInfoPayload
from pqn_hardware.network.packet import GetParameterPayload
from pqn_hardware.network.packet import NetworkElementClass
from pqn_hardware.network.packet import Packet
from pqn_hardware.network.packet import PacketIntent
from pqn_hardware.network.packet import RegisterPayload
from pqn_hardware.network.packet import Request
from pqn_hardware.network.packet import SetParameterPayload
from pqn_hardware.network.packet import create_registration_packet
from pqn_hardware.network.packet import decode
from pqn_hardware.network.packet import encode
from pqn_hardware.network.router import Router

logger = logging.getLogger(__name__)

ROUTER_NAME = "pqn_hardware-router"
PROVIDER_NAME = "pqn_hardware-provider"
RAW_PROVIDER_NAME = "pqn_hardware-raw-provider"
HOST = "127.0.0.1"
# Error replies must arrive well inside a Client's timeout; the socket timeout fails a missing reply fast.
ERROR_TIMEOUT_MS = 2_000


def _free_port() -> int:
    """Grab an ephemeral TCP port that is free right now."""
    with socket.socket() as s:
        s.bind((HOST, 0))
        return int(s.getsockname()[1])


def _wait_until(condition: Any, timeout_s: float, failure_msg: str) -> None:
    deadline = time.monotonic() + timeout_s
    while not condition():
        if time.monotonic() > deadline:
            raise TimeoutError(failure_msg)
        time.sleep(0.01)


@pytest.fixture(scope="module")
def network_port() -> int:
    """Start the Router and the InstrumentProvider in daemon threads; return the port they talk on."""
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
    _wait_until(lambda: PROVIDER_NAME in router.providers, 15, "InstrumentProvider did not register with the Router.")

    logger.debug("Router and InstrumentProvider are up on port %s", port)
    return port


def _client(network_port: int) -> Client:
    return Client(host=HOST, port=network_port, router_name=ROUTER_NAME, timeout=10_000)


class _RawClient:
    """A REQ socket speaking the wire protocol directly.

    The Client class raises PacketError on an ERROR reply before a test can read the ErrorPayload;
    this socket returns the reply Packet itself.
    """

    def __init__(self, port: int, name: str) -> None:
        self.name = name
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
        self.socket.setsockopt_string(zmq.IDENTITY, name)
        self.socket.connect(f"tcp://{HOST}:{port}")
        self.ask(create_registration_packet(source=name, destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT))

    def ask(self, packet: Packet) -> Packet:
        """Send a Packet and return the reply Packet.

        The short socket timeout is what makes every test using it assert a fast reply, not a timeout.
        """
        self.socket.send(encode(packet))
        return decode(self.socket.recv())

    def close(self) -> None:
        self.socket.close()
        self.context.term()


@pytest.fixture
def raw_client(network_port: int) -> Generator[_RawClient, Any, None]:
    client = _RawClient(network_port, "raw-test-client")
    yield client
    client.close()


class _RawRouter:
    """A plain ZMQ ROUTER socket standing in for the Router.

    It lets tests deliver Packets the real Router would never forward to a Provider: misaddressed
    Packets, undecodable bytes and wrong frame counts.
    """

    def __init__(self, provider_name: str) -> None:
        self.provider_name = provider_name
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
        self.port = _free_port()
        self.socket.bind(f"tcp://{HOST}:{self.port}")

        # A long beat period keeps registration Heartbeats from arriving between test messages.
        provider = InstrumentProvider(
            provider_name,
            HOST,
            self.port,
            ROUTER_NAME,
            beat_period=60_000,
            dummy1={
                "import": "pqn_hardware.drivers.dummies.DummyInstrument",
                "desc": "Dummy instrument on the raw router",
                "hw_address": "1234",
            },
            measuring1={
                "import": "tests.pytest.test_internal_network._MeasuringInstrument",
                "desc": "Instrument whose operations return dataclasses and lists",
                "hw_address": "5678",
            },
            shapes1={
                "import": "tests.pytest.test_internal_network._ShapesInstrument",
                "desc": "Instrument exercising Device-structure shapes",
                "hw_address": "1111",
            },
            raising1={
                "import": "tests.pytest.test_internal_network._RaisingInstrument",
                "desc": "Instrument whose driver access raises",
                "hw_address": "2222",
            },
        )
        threading.Thread(target=provider.start, daemon=True).start()

        # Answer the provider's registration Heartbeat with an acknowledgement.
        self.provider_identity, reg_data = self.socket.recv_multipart()
        registration = decode(reg_data)
        assert registration.request == Request.REGISTER
        assert registration.source == provider_name
        self.socket.send_multipart(
            [
                self.provider_identity,
                b"",
                encode(
                    Packet(
                        intent=PacketIntent.REGISTRATION_ACK,
                        request=Request.ACKNOWLEDGE,
                        source=ROUTER_NAME,
                        destination=provider_name,
                    )
                ),
            ]
        )

    def ask(self, packet: Packet) -> Packet:
        """Deliver a Packet to the Provider and return its reply Packet."""
        self.socket.send_multipart([self.provider_identity, b"", encode(packet)])
        _, data = self.socket.recv_multipart()
        return decode(data)

    def send_bytes(self, data: bytes, *extra: bytes) -> None:
        """Deliver raw frames to the Provider, after the delimiter the Router sends."""
        self.socket.send_multipart([self.provider_identity, b"", data, *extra])

    def receive(self) -> Packet:
        """Receive the Provider's reply Packet."""
        _, data = self.socket.recv_multipart()
        return decode(data)

    def close(self) -> None:
        self.socket.close()
        self.context.term()


@pytest.fixture(scope="module")
def raw_router() -> Generator[_RawRouter, Any, None]:
    harness = _RawRouter(RAW_PROVIDER_NAME)
    yield harness
    harness.close()


MEASURING_PROVIDER_NAME = "pqn_hardware-measuring-provider"


@pytest.fixture(scope="module")
def measuring_network_port() -> int:
    """Start a Router and a Provider whose Instrument returns a dataclass; return the port."""
    port = _free_port()

    router = Router(ROUTER_NAME, HOST, port)
    threading.Thread(target=router.start, daemon=True).start()
    _wait_until(lambda: router.running, 10, "Router did not start.")

    provider = InstrumentProvider(
        MEASURING_PROVIDER_NAME,
        HOST,
        port,
        ROUTER_NAME,
        beat_period=60_000,
        measuring1={
            "import": "tests.pytest.test_internal_network._MeasuringInstrument",
            "desc": "Instrument whose operation returns a dataclass",
            "hw_address": "1234",
        },
        no_info1={
            "import": "tests.pytest.test_internal_network._NoInfoInstrument",
            "desc": "Instrument whose info is None",
            "hw_address": "5678",
        },
    )
    threading.Thread(target=provider.start, daemon=True).start()
    _wait_until(lambda: MEASURING_PROVIDER_NAME in router.providers, 15, "Measuring provider did not register.")

    return port


def _assert_error_reply(reply: Packet, request: Packet, code: ErrorCode, origin: str) -> ErrorPayload:
    """Assert a reply is an error reply with a code and Origin, echoing the request id."""
    assert reply.intent == PacketIntent.ERROR
    assert reply.request == Request.ERROR
    assert reply.destination == request.source
    assert reply.reply_to == request.id
    payload = ErrorPayload.model_validate(reply.payload)
    assert payload.code == code
    assert payload.origin == origin
    return payload


def _assert_registration_ack(ack: Packet, registration: Packet) -> None:
    """Assert a reply is the REGISTRATION_ACK answering a registration Packet."""
    assert ack.intent == PacketIntent.REGISTRATION_ACK
    assert ack.request == Request.ACKNOWLEDGE
    assert ack.payload is None
    assert ack.reply_to == registration.id


def test_client_ping(network_port: int) -> None:
    client = _client(network_port)
    response = client.ping(PROVIDER_NAME)

    assert isinstance(response, Packet)
    assert response.intent == PacketIntent.PING
    assert response.source == PROVIDER_NAME
    assert response.destination == client.name
    assert response.request == Request.PONG


def test_getting_all_instruments(network_port: int) -> None:
    client = _client(network_port)

    response = client.get_available_devices(PROVIDER_NAME)

    assert list(response) == ["dummy1", "dummy2"]
    assert isinstance(response["dummy1"], DeviceSummary)
    assert response["dummy1"].type == "pqn_hardware.drivers.dummies.DummyInstrument"
    assert response["dummy1"].desc == "Dummy instrument1 for testing purposes"
    assert response["dummy1"].hw_address == "1234"
    assert isinstance(response["dummy2"], DeviceSummary)
    assert response["dummy2"].type == "pqn_hardware.drivers.dummies.DummyInstrument"
    assert response["dummy2"].hw_address == "5678"


def test_proxy_instrument(network_port: int) -> None:
    client = _client(network_port)

    proxy_instrument = client.get_device(PROVIDER_NAME, "dummy1")
    assert isinstance(proxy_instrument, ProxyInstrument)

    base_int = 2
    double_int = base_int * 2
    arbitrary_int = 12

    assert proxy_instrument.name == "dummy1"
    assert proxy_instrument.param_int == base_int
    assert proxy_instrument.param_str == "hello"

    assert proxy_instrument.double_int() == double_int
    assert proxy_instrument.param_int == double_int

    proxy_instrument.param_int = arbitrary_int
    assert proxy_instrument.param_int == arbitrary_int

    assert proxy_instrument.uppercase_str() == "HELLO"
    assert proxy_instrument.param_str == "HELLO"

    # Make sure you cannot add attributes to the proxy instrument
    fail_flag = False
    try:
        proxy_instrument.new_attr = 42
    except AttributeError:
        fail_flag = True

    assert fail_flag, "Should not be able to set new attributes on the ProxyInstrument"


def test_proxy_instrument_operation_with_keyword_arguments(network_port: int) -> None:
    client = _client(network_port)

    # Uses dummy2 so the test does not depend on the state test_proxy_instrument leaves on dummy1.
    proxy_instrument = client.get_device(PROVIDER_NAME, "dummy2")

    # Operation arguments go on the wire by keyword only.
    input_value = 36
    half_input_value = input_value // 2
    assert proxy_instrument.set_half_input_int(value=input_value) == half_input_value
    assert proxy_instrument.param_int == half_input_value


def test_proxy_instrument_binds_positional_arguments_to_names(network_port: int) -> None:
    client = _client(network_port)

    proxy_instrument = client.get_device(PROVIDER_NAME, "dummy2")

    # Positional arguments are bound to their names from the Device structure.
    input_value = 10
    half_input_value = input_value // 2
    assert proxy_instrument.set_half_input_int(input_value) == half_input_value
    assert proxy_instrument.param_int == half_input_value

    # More positional arguments than the Operation takes is a local TypeError.
    with pytest.raises(TypeError, match="at most 1 positional argument"):
        proxy_instrument.set_half_input_int(1, 2)

    # A positional argument repeated by keyword is a local TypeError.
    with pytest.raises(TypeError, match="multiple values for argument 'value'"):
        proxy_instrument.set_half_input_int(1, value=2)


def test_proxy_instrument_rejects_setting_a_parameter_with_more_than_one_value(network_port: int) -> None:
    client = _client(network_port)

    proxy_instrument = client.get_device(PROVIDER_NAME, "dummy2")

    with pytest.raises(TypeError, match="exactly one value"):
        proxy_instrument.client.trigger_parameter("param_int", 1, 2)


def test_proxy_instrument_get_info(network_port: int) -> None:
    client = _client(network_port)

    # Uses dummy2 so the test does not depend on the state test_proxy_instrument leaves on dummy1.
    proxy_instrument = client.get_device(PROVIDER_NAME, "dummy2")

    info = proxy_instrument.info

    # The info is rebuilt from the type path in the Device structure, as the same class.
    assert isinstance(info, DummyInfo)
    assert info.name == "dummy2"
    assert info.desc == "Dummy instrument2 for testing purposes"
    assert info.hw_address == "5678"
    # Order-safe under pytest-randomly: compare against a live read of the same parameters.
    assert info.param_str == proxy_instrument.param_str
    assert info.param_int == proxy_instrument.param_int


def test_client_survives_a_timeout_and_the_next_request_succeeds(network_port: int) -> None:
    # The Router drops a Packet addressed to itself, so the ping gets no reply and times out.
    client = Client(host=HOST, port=network_port, router_name=ROUTER_NAME, timeout=500)

    with pytest.raises(TimeoutError):
        client.ping(ROUTER_NAME)

    # Without REQ_RELAXED this request's send would raise EFSM; the next request must succeed.
    response = client.ping(PROVIDER_NAME)
    assert response is not None
    assert response.source == PROVIDER_NAME
    assert response.request == Request.PONG


def test_late_reply_is_dropped_not_returned_to_the_next_request(network_port: int) -> None:
    client = _client(network_port)

    # toggle_bool sleeps ~1.4 s on the Provider; a proxy with a shorter timeout misses the reply.
    proxy_instrument = client.get_device(PROVIDER_NAME, "dummy2", timeout_ms=500)
    param_bool_before = proxy_instrument.param_bool

    with pytest.raises(TimeoutError):
        proxy_instrument.toggle_bool()

    # The late reply arrives while no request is outstanding; it must be dropped, not queued.
    time.sleep(2)
    assert proxy_instrument.param_bool == (not param_bool_before)

    # The next request gets its own reply, never the late reply to toggle_bool.
    input_value = 36
    half_input_value = input_value // 2
    assert proxy_instrument.set_half_input_int(input_value) == half_input_value


def test_error_reply_raises_packet_error_with_the_error_payload(network_port: int) -> None:
    client = _client(network_port)
    try:
        with pytest.raises(PacketError) as exc_info:
            client.ping("no-such-destination")
    finally:
        # Close the socket while the Client is alive: a Client that only dies by garbage
        # collection can block another thread's garbage collection in zmq Context.term().
        client.disconnect()

    error = exc_info.value
    assert error.code == ErrorCode.UNKNOWN_DESTINATION
    assert error.origin == ROUTER_NAME
    assert error.details == {}
    assert error.message


def test_mismatched_reply_to_is_dropped_and_ask_raises_timeout(caplog: pytest.LogCaptureFixture) -> None:
    # A reply that passes the socket's request-id filter but answers no request of ours: only a
    # peer that breaks the protocol sends one, so this harness stands in for the Router.
    port = _free_port()
    context = zmq.Context()
    server = context.socket(zmq.ROUTER)
    server.setsockopt(zmq.ROUTER_HANDOVER, 1)
    server.bind(f"tcp://{HOST}:{port}")

    seen: dict[str, str] = {}

    def reply(identity: bytes, frame: bytes, request: Packet, reply_to: str | None) -> None:
        server.send_multipart(
            [
                identity,
                frame,
                b"",
                encode(
                    Packet(
                        intent=PacketIntent.PING,
                        request=Request.PONG,
                        source=PROVIDER_NAME,
                        destination=request.source,
                        reply_to=reply_to,
                    )
                ),
            ]
        )

    def serve() -> None:
        # Registration: echo the request id, with the captured request-id frame.
        identity, frame, _, data = server.recv_multipart()
        registration = decode(data)
        server.send_multipart(
            [
                identity,
                frame,
                b"",
                encode(
                    Packet(
                        intent=PacketIntent.REGISTRATION_ACK,
                        request=Request.ACKNOWLEDGE,
                        source=ROUTER_NAME,
                        destination=registration.source,
                        reply_to=registration.id,
                    )
                ),
            ]
        )

        # A reply with a null reply_to answers no request either.
        identity, frame, _, data = server.recv_multipart()
        reply(identity, frame, decode(data), None)

        # An ERROR reply whose reply_to matches no request of ours: it is dropped like any
        # other mismatched reply, never raised as the answer to this request.
        identity, frame, _, data = server.recv_multipart()
        request = decode(data)
        seen["ping"] = request.id
        server.send_multipart(
            [
                identity,
                frame,
                b"",
                encode(
                    Packet(
                        intent=PacketIntent.ERROR,
                        request=Request.ERROR,
                        source=ROUTER_NAME,
                        destination=request.source,
                        reply_to="not-the-request-id",
                        payload=ErrorPayload(
                            code=ErrorCode.UNKNOWN_DESTINATION, message="a stale error", origin=ROUTER_NAME
                        ).model_dump(),
                    )
                ),
            ]
        )

        # The next request gets a reply that answers it.
        identity, frame, _, data = server.recv_multipart()
        request = decode(data)
        reply(identity, frame, request, request.id)

    threading.Thread(target=serve, daemon=True).start()

    client = Client(host=HOST, port=port, router_name=ROUTER_NAME, timeout=1000)
    try:
        with (
            caplog.at_level(logging.WARNING, logger="pqn_hardware.network.client"),
            pytest.raises(TimeoutError),
        ):
            client.ping(PROVIDER_NAME)

        with pytest.raises(TimeoutError):
            client.ping(PROVIDER_NAME)

        # Each drop warning names the reply's reply_to and the request's id, in that order,
        # and every drop is a warning, never a louder level.
        expected = f"its reply_to not-the-request-id does not match the request id {seen['ping']}"
        assert any(expected in record.message for record in caplog.records)
        drop_records = [record for record in caplog.records if "does not match the request id" in record.message]
        assert all(record.levelname == "WARNING" for record in drop_records)

        # The Client survives the dropped replies: the next request succeeds on the same socket.
        reply_packet = client.ping(PROVIDER_NAME)
        assert reply_packet is not None
        assert reply_packet.request == Request.PONG
        assert reply_packet.reply_to is not None
    finally:
        # Close the socket while the Client is alive, so a garbage-collected context can never
        # block another thread's garbage collection in zmq Context.term().
        client.disconnect()

    server.close()
    context.term()


def test_error_reply_with_a_null_payload_raises(caplog: pytest.LogCaptureFixture) -> None:
    # An ERROR reply with a null payload is not a valid error reply: validating it raises, and
    # the reply is never returned to the caller as a success.
    port = _free_port()
    context = zmq.Context()
    server = context.socket(zmq.ROUTER)
    server.setsockopt(zmq.ROUTER_HANDOVER, 1)
    server.bind(f"tcp://{HOST}:{port}")

    def serve() -> None:
        # Registration: echo the request id, with the captured request-id frame.
        identity, frame, _, data = server.recv_multipart()
        registration = decode(data)
        server.send_multipart(
            [
                identity,
                frame,
                b"",
                encode(
                    Packet(
                        intent=PacketIntent.REGISTRATION_ACK,
                        request=Request.ACKNOWLEDGE,
                        source=ROUTER_NAME,
                        destination=registration.source,
                        reply_to=registration.id,
                    )
                ),
            ]
        )

        # An ERROR Packet that answers the request, but carries no ErrorPayload.
        identity, frame, _, data = server.recv_multipart()
        request = decode(data)
        server.send_multipart(
            [
                identity,
                frame,
                b"",
                encode(
                    Packet(
                        intent=PacketIntent.ERROR,
                        request=Request.ERROR,
                        source=PROVIDER_NAME,
                        destination=request.source,
                        reply_to=request.id,
                        payload=None,
                    )
                ),
            ]
        )

    threading.Thread(target=serve, daemon=True).start()

    client = Client(host=HOST, port=port, router_name=ROUTER_NAME, timeout=1000)
    try:
        with (
            caplog.at_level(logging.WARNING, logger="pqn_hardware.network.client"),
            pytest.raises(ValidationError),
        ):
            client.ping(PROVIDER_NAME)
    finally:
        client.disconnect()

    server.close()
    context.term()


def test_instrument_error_raises_packet_error_with_the_error_payload(network_port: int) -> None:
    client = _client(network_port)

    proxy_instrument = client.get_device(PROVIDER_NAME, "dummy2")
    try:
        with pytest.raises(PacketError) as exc_info:
            proxy_instrument.fail()
    finally:
        proxy_instrument.close()
        client.disconnect()

    error = exc_info.value
    # The ErrorPayload's code, origin and details are attributes of the exception.
    assert error.code == ErrorCode.INSTRUMENT_ERROR
    assert error.origin == PROVIDER_NAME
    assert error.details["exception_type"] == "RuntimeError"
    assert "dummy instrument failure" in error.details["exception"]
    # The exception carries the ErrorPayload itself, and its message is the Payload's message.
    assert isinstance(error.payload, ErrorPayload)
    assert error.payload.code == ErrorCode.INSTRUMENT_ERROR
    assert error.payload.origin == PROVIDER_NAME
    assert error.payload.details == error.details
    assert str(error) == error.payload.message


def test_packet_error_without_a_payload_has_no_code_origin_or_details() -> None:
    error = PacketError("a client-side error")

    assert error.message == "a client-side error"
    assert error.payload is None
    assert error.code is None
    assert error.origin is None
    assert error.details == {}


def test_router_sends_a_stale_reply_without_the_request_id_frame(network_port: int) -> None:
    # A reply that no longer answers the Client's latest request goes out without the
    # request-id frame, so the Client's REQ socket drops it; the reply that does answer
    # it is the only one the socket accepts.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.REQ_RELAXED, 1)
    socket.setsockopt(zmq.REQ_CORRELATE, 1)
    socket.setsockopt(zmq.RCVTIMEO, 5000)
    socket.setsockopt_string(zmq.IDENTITY, "correlated-test-client")
    socket.connect(f"tcp://{HOST}:{network_port}")

    registration = create_registration_packet(
        source="correlated-test-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    socket.send(encode(registration))
    ack = decode(socket.recv())
    _assert_registration_ack(ack, registration)
    # The Router sends the acknowledgement: it names itself as the source.
    assert ack.source == ROUTER_NAME
    assert ack.destination == "correlated-test-client"

    # A slow request the client does not wait for, then a second request while it runs.
    toggle = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.CALL_OPERATION,
        source="correlated-test-client",
        destination=PROVIDER_NAME,
        payload=CallOperationPayload(instrument="dummy2", operation="toggle_bool").model_dump(),
    )
    socket.send(encode(toggle))

    ping = Packet(
        intent=PacketIntent.PING, request=Request.PING, source="correlated-test-client", destination=PROVIDER_NAME
    )
    socket.send(encode(ping))

    # The one reply the socket accepts answers the second request.
    reply = decode(socket.recv())
    assert reply.request == Request.PONG
    assert reply.reply_to == ping.id

    socket.close()
    context.term()


def test_registration_for_another_router_replies_malformed_packet(network_port: int) -> None:
    # Registering with a Router of another name is a malformed registration: the Router's own
    # error reply carries the request's id as reply_to, so the Client gets a fast error.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.REQ_RELAXED, 1)
    socket.setsockopt(zmq.REQ_CORRELATE, 1)
    socket.setsockopt(zmq.RCVTIMEO, 2000)
    socket.setsockopt_string(zmq.IDENTITY, "wrong-router-client")
    socket.connect(f"tcp://{HOST}:{network_port}")

    registration = create_registration_packet(
        source="wrong-router-client", destination="another-router", kind=NetworkElementClass.CLIENT
    )
    socket.send(encode(registration))
    try:
        reply = decode(socket.recv())

        assert reply.intent == PacketIntent.ERROR
        assert reply.reply_to == registration.id
        # The Router sends the error reply: it names itself as the source.
        assert reply.source == ROUTER_NAME
        assert reply.destination == "wrong-router-client"
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.MALFORMED_PACKET
        assert payload.origin == ROUTER_NAME
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


def test_wrong_frame_count_gets_malformed_packet_and_the_router_keeps_running(network_port: int) -> None:
    # A message with the wrong frame count gets a fast MALFORMED_PACKET reply, and the Router
    # keeps serving afterwards.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 2000)
    socket.setsockopt_string(zmq.IDENTITY, "bad-frames-client")
    socket.connect(f"tcp://{HOST}:{network_port}")

    # Register first, so the follow-up request below can be routed back to this client.
    registration = create_registration_packet(
        source="bad-frames-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    socket.send(encode(registration))
    ack = decode(socket.recv())
    _assert_registration_ack(ack, registration)
    # The Router sends the acknowledgement: it names itself as the source.
    assert ack.source == ROUTER_NAME
    assert ack.destination == "bad-frames-client"

    try:
        # More frames than any peer sends (the REQ delimiter plus three data frames).
        socket.send_multipart([b"", b"one", b"two", b"three"])
        reply = decode(socket.recv())

        assert reply.intent == PacketIntent.ERROR
        # There is no request id to echo, so the error reply carries a null reply_to.
        assert reply.reply_to is None
        # The Router sends the error reply: it names itself as the source.
        assert reply.source == ROUTER_NAME
        assert reply.destination == "bad-frames-client"
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.MALFORMED_PACKET
        assert payload.origin == ROUTER_NAME

        # The Router keeps serving after the bad input.
        ping = Packet(
            intent=PacketIntent.PING, request=Request.PING, source="bad-frames-client", destination=PROVIDER_NAME
        )
        socket.send(encode(ping))
        pong = decode(socket.recv())
        assert pong.request == Request.PONG
        assert pong.reply_to == ping.id
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


def test_garbage_bytes_get_malformed_packet_and_the_router_keeps_running(network_port: int) -> None:
    # Bytes a pre-v2 pickle peer would send: not JSON at all, so there is no id to echo. The
    # reply still carries the request-id frame the message arrived with, so a REQ_CORRELATE
    # Client's socket accepts the error instead of timing out.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.REQ_RELAXED, 1)
    socket.setsockopt(zmq.REQ_CORRELATE, 1)
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.setsockopt_string(zmq.IDENTITY, "garbage-client")
    socket.connect(f"tcp://{HOST}:{network_port}")

    registration = create_registration_packet(
        source="garbage-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    socket.send(encode(registration))
    registration_ack = decode(socket.recv())
    _assert_registration_ack(registration_ack, registration)

    try:
        socket.send(b"\x80\x04\x95pickle-bytes")
        reply = decode(socket.recv())

        assert reply.intent == PacketIntent.ERROR
        assert reply.request == Request.ERROR
        # The garbage carries no readable id, so the error reply carries a null reply_to.
        assert reply.reply_to is None
        # The Router sends the error reply: it names itself as the source.
        assert reply.source == ROUTER_NAME
        assert reply.destination == "garbage-client"
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.MALFORMED_PACKET
        assert payload.origin == ROUTER_NAME

        # The Router keeps serving after the garbage bytes.
        ping = Packet(
            intent=PacketIntent.PING, request=Request.PING, source="garbage-client", destination=PROVIDER_NAME
        )
        socket.send(encode(ping))
        pong = decode(socket.recv())
        assert pong.request == Request.PONG
        assert pong.reply_to == ping.id
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


@pytest.mark.parametrize("identity", [None, b"\x00\xff\xfe\xfd"], ids=["auto-identity", "explicit-non-utf8"])
def test_garbage_bytes_from_a_dealer_with_a_non_utf8_identity_get_malformed_packet(
    network_port: int, identity: bytes | None
) -> None:
    # A DEALER whose identity frame is not UTF-8 text: libzmq's auto identity when none is
    # set (binary in all but a few random cases), or an explicit binary identity. The Router
    # never crashes on either, and the error reply still arrives on that socket.
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    if identity is not None:
        socket.setsockopt(zmq.IDENTITY, identity)
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.connect(f"tcp://{HOST}:{network_port}")

    # Registering first lets the Router route the follow-up request back to this socket, and
    # exercises the acknowledgement sent to a non-UTF-8 identity frame.
    registration = create_registration_packet(
        source="non-utf8-dealer-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    socket.send(encode(registration))
    _, ack_data = socket.recv_multipart()
    _assert_registration_ack(decode(ack_data), registration)

    try:
        socket.send(b"\x80\x04\x95pickle-bytes")
        _, error_data = socket.recv_multipart()
        reply = decode(error_data)

        assert reply.intent == PacketIntent.ERROR
        # The garbage carries no readable id, so the error reply carries a null reply_to.
        assert reply.reply_to is None
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.MALFORMED_PACKET
        assert payload.origin == ROUTER_NAME

        # The Router keeps serving after garbage from the non-UTF-8 identity.
        ping = Packet(
            intent=PacketIntent.PING,
            request=Request.PING,
            source="non-utf8-dealer-client",
            destination=PROVIDER_NAME,
        )
        socket.send(encode(ping))
        _, pong_data = socket.recv_multipart()
        pong = decode(pong_data)
        assert pong.request == Request.PONG
        assert pong.reply_to == ping.id
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


def test_a_non_utf8_identity_client_receives_the_routers_own_replies(network_port: int) -> None:
    # A REQ_CORRELATE Client whose identity frame is not UTF-8 text still receives the
    # Router's own replies: they carry its request-id frame back, looked up by the identity
    # bytes, not by the reply Packet's destination text. Without the frame the socket drops
    # every reply and the Client would time out instead of getting a fast error.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.REQ_RELAXED, 1)
    socket.setsockopt(zmq.REQ_CORRELATE, 1)
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.setsockopt(zmq.IDENTITY, b"\x00\xff\xfe\xfd")
    socket.connect(f"tcp://{HOST}:{network_port}")

    try:
        # A v2 Packet to an unknown destination gets a fast UNKNOWN_DESTINATION reply that
        # passes the socket's request-id filter.
        request = Packet(
            intent=PacketIntent.DATA,
            request=Request.GET_DEVICES,
            source="non-utf8-req-client",
            destination="no-such-destination",
        )
        socket.send(encode(request))
        reply = decode(socket.recv())

        assert reply.intent == PacketIntent.ERROR
        assert reply.reply_to == request.id
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.UNKNOWN_DESTINATION
        assert payload.origin == ROUTER_NAME

        # Its REGISTRATION_ACK passes the filter too.
        registration = create_registration_packet(
            source="non-utf8-req-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
        )
        socket.send(encode(registration))
        ack = decode(socket.recv())
        _assert_registration_ack(ack, registration)
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


def test_router_error_replies_answer_the_identity_frame_not_the_source(network_port: int) -> None:
    # Every Router error reply answers the identity frame the message arrived on, never the
    # Packet's source: a peer whose identity differs from its source still gets every reply.
    # The identity is deliberately not UTF-8 text, and no Packet names it as source, so any
    # reply addressed to the source instead reaches no peer at all.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.setsockopt(zmq.IDENTITY, b"\x00\xff\xfe\xfd")
    socket.connect(f"tcp://{HOST}:{network_port}")

    try:
        # An invalid REGISTER Payload: the MALFORMED_PACKET reply arrives on this socket.
        bad_register = Packet(
            intent=PacketIntent.REGISTRATION,
            request=Request.REGISTER,
            source="spoofed-source",
            destination=ROUTER_NAME,
            payload={"kind": "NOT_A_KIND"},
        )
        socket.send(encode(bad_register))
        reply = decode(socket.recv())

        assert reply.intent == PacketIntent.ERROR
        assert reply.reply_to == bad_register.id
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.MALFORMED_PACKET
        assert payload.origin == ROUTER_NAME

        # A REGISTER addressed to another Router: its error reply arrives on this socket too.
        wrong_router = create_registration_packet(
            source="spoofed-source", destination="another-router", kind=NetworkElementClass.CLIENT
        )
        socket.send(encode(wrong_router))
        reply = decode(socket.recv())

        assert reply.intent == PacketIntent.ERROR
        assert reply.reply_to == wrong_router.id
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.MALFORMED_PACKET
        assert payload.origin == ROUTER_NAME

        # A wrong-version Packet: its UNSUPPORTED_VERSION reply arrives on this socket too.
        ping = Packet(
            intent=PacketIntent.PING, request=Request.PING, source="spoofed-source", destination=PROVIDER_NAME
        )
        wire = json.loads(encode(ping))
        wire["version"] = 3
        socket.send(json.dumps(wire).encode("utf-8"))
        reply = decode(socket.recv())

        assert reply.intent == PacketIntent.ERROR
        assert reply.reply_to == ping.id
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.UNSUPPORTED_VERSION
        assert payload.origin == ROUTER_NAME
        assert payload.details == {"expected": PROTOCOL_VERSION, "received": 3}
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


def test_wrong_version_error_packet_gets_unsupported_version(
    network_port: int, caplog: pytest.LogCaptureFixture
) -> None:
    # The version check precedes Intent handling, so every Packet gets UNSUPPORTED_VERSION on
    # a wrong version, an ERROR Packet included (an ERROR Packet addressed to the Router
    # is never answered, but the version check comes first). The registered peer the ERROR Packet is
    # addressed to receives nothing: the Provider logs a warning for an ERROR Packet it never
    # asked for, so no such warning means it was never forwarded.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.setsockopt_string(zmq.IDENTITY, "stale-error-client")
    socket.connect(f"tcp://{HOST}:{network_port}")

    registration = create_registration_packet(
        source="stale-error-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    socket.send(encode(registration))
    registration_ack = decode(socket.recv())
    _assert_registration_ack(registration_ack, registration)

    try:
        request = Packet(
            intent=PacketIntent.ERROR,
            request=Request.ERROR,
            source="stale-error-client",
            destination=PROVIDER_NAME,
            payload=ErrorPayload(
                code=ErrorCode.INSTRUMENT_ERROR, message="a stale error", origin="stale-error-client"
            ).model_dump(),
        )
        wire = json.loads(encode(request))
        wire["version"] = 3
        with caplog.at_level(logging.WARNING, logger="pqn_hardware.network.instrument_provider"):
            socket.send(json.dumps(wire).encode("utf-8"))
            reply = decode(socket.recv())

        assert reply.intent == PacketIntent.ERROR
        assert reply.reply_to == request.id
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.UNSUPPORTED_VERSION
        assert payload.origin == ROUTER_NAME
        assert payload.details == {"expected": PROTOCOL_VERSION, "received": 3}

        # The Provider received nothing: it logged no warning during the request.
        assert not [record for record in caplog.records if record.name == "pqn_hardware.network.instrument_provider"]

        # The Router keeps serving after refusing the wrong-version ERROR Packet.
        ping = Packet(
            intent=PacketIntent.PING, request=Request.PING, source="stale-error-client", destination=PROVIDER_NAME
        )
        socket.send(encode(ping))
        pong = decode(socket.recv())
        assert pong.request == Request.PONG
        assert pong.reply_to == ping.id
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


def test_wrong_version_packets_are_refused_never_handled(network_port: int) -> None:
    # A wrong-version Packet is refused and never handled: no second reply follows the
    # UNSUPPORTED_VERSION error, the Provider never receives the Packet, and the sender is
    # never registered. A Dealer receives every reply the Router sends it, so a second
    # reply cannot be filtered away by a socket.
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, b"wrong-version-dealer")
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.connect(f"tcp://{HOST}:{network_port}")

    try:
        # A version-1 REGISTER addressed to the Router: refused with UNSUPPORTED_VERSION.
        registration = create_registration_packet(
            source="wrong-version-dealer", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
        )
        wire = json.loads(encode(registration))
        wire["version"] = 1
        socket.send(json.dumps(wire).encode("utf-8"))
        _, error_data = socket.recv_multipart()
        reply = decode(error_data)

        assert reply.intent == PacketIntent.ERROR
        assert reply.reply_to == registration.id
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.UNSUPPORTED_VERSION
        assert payload.origin == ROUTER_NAME
        assert payload.details == {"expected": PROTOCOL_VERSION, "received": 1}

        # No REGISTRATION_ACK follows the error: the REGISTER was never handled, so the
        # sender was never registered.
        with pytest.raises(zmq.error.Again):
            socket.recv_multipart()

        # A version-1 PING addressed to the Provider: refused, never forwarded, so the
        # Provider never replies.
        ping = Packet(
            intent=PacketIntent.PING, request=Request.PING, source="wrong-version-dealer", destination=PROVIDER_NAME
        )
        wire = json.loads(encode(ping))
        wire["version"] = 1
        socket.send(json.dumps(wire).encode("utf-8"))
        _, error_data = socket.recv_multipart()
        reply = decode(error_data)

        assert reply.intent == PacketIntent.ERROR
        assert reply.reply_to == ping.id
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.UNSUPPORTED_VERSION
        assert payload.origin == ROUTER_NAME

        # No PONG follows the error: the Provider never received the PING.
        with pytest.raises(zmq.error.Again):
            socket.recv_multipart()

        # The sender was never registered: a v2 Packet addressed to its name is refused with
        # UNKNOWN_DESTINATION instead of being routed back to it.
        probe = Packet(
            intent=PacketIntent.PING,
            request=Request.PING,
            source="wrong-version-dealer",
            destination="wrong-version-dealer",
        )
        socket.send(encode(probe))
        _, error_data = socket.recv_multipart()
        reply = decode(error_data)

        assert reply.intent == PacketIntent.ERROR
        assert reply.reply_to == probe.id
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.UNKNOWN_DESTINATION
        assert payload.origin == ROUTER_NAME
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


@pytest.mark.parametrize(
    ("wire", "expected_reply_to"),
    [
        (b"[1, 2, 3]", None),
        (b'{"id": 42}', None),
        (b'"abc"', None),
        (b'{"reply_to": "abc", "intent": 5}', None),
        (b'{"id": "abc"}', "abc"),
        (b'{"ID": "abc"}', None),
        (b'{"Id": "abc"}', None),
        (b'{"id": "ab\xffc"}', None),
    ],
    ids=[
        "json-array",
        "id-not-a-string",
        "json-string",
        "reply-to-but-no-id",
        "id-only-no-intent",
        "upper-case-id-key",
        "mixed-case-id-key",
        "invalid-utf8-in-id",
    ],
)
def test_json_that_is_not_a_usable_envelope_gets_malformed_packet(
    network_port: int, wire: bytes, expected_reply_to: str | None
) -> None:
    # Valid JSON that is not a usable envelope: a JSON array, a JSON object whose id is not
    # a string, a bare JSON string, a JSON object with a reply_to but no id, or bytes that
    # are not UTF-8 and so not JSON at all. The echoed reply_to is the id when the data is a
    # JSON object with a string id under exactly that key: any other key, case or no id
    # at all gives a null reply_to. A follow-up request on the same socket is still answered.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.REQ_RELAXED, 1)
    socket.setsockopt(zmq.REQ_CORRELATE, 1)
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.setsockopt_string(zmq.IDENTITY, "json-envelope-client")
    socket.connect(f"tcp://{HOST}:{network_port}")

    registration = create_registration_packet(
        source="json-envelope-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    socket.send(encode(registration))
    registration_ack = decode(socket.recv())
    _assert_registration_ack(registration_ack, registration)

    try:
        socket.send(wire)
        reply = decode(socket.recv())

        assert reply.intent == PacketIntent.ERROR
        assert reply.reply_to == expected_reply_to
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.MALFORMED_PACKET
        assert payload.origin == ROUTER_NAME

        # A follow-up request on the same socket is still answered.
        ping = Packet(
            intent=PacketIntent.PING, request=Request.PING, source="json-envelope-client", destination=PROVIDER_NAME
        )
        socket.send(encode(ping))
        pong = decode(socket.recv())
        assert pong.request == Request.PONG
        assert pong.reply_to == ping.id
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


@pytest.mark.parametrize("hops", [0, 1], ids=["hops-0", "hops-1"])
@pytest.mark.parametrize("version", [1, 3], ids=["older-version", "newer-version"])
def test_unsupported_version_register_gets_unsupported_version_error(
    network_port: int, version: int, hops: int
) -> None:
    # The version is enforced on REGISTER too, as an exact match: a REGISTER in
    # any other version, older or newer, whatever its hop count, never gets a
    # REGISTRATION_ACK, and a following v2 REGISTER still gets its own acknowledgement.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.setsockopt_string(zmq.IDENTITY, "old-register-client")
    socket.connect(f"tcp://{HOST}:{network_port}")

    try:
        # A hand-written REGISTER speaking another protocol version.
        request = create_registration_packet(
            source="old-register-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
        )
        wire = json.loads(encode(request))
        wire["version"] = version
        wire["hops"] = hops
        socket.send(json.dumps(wire).encode("utf-8"))
        reply = decode(socket.recv())

        assert reply.intent == PacketIntent.ERROR
        assert reply.reply_to == request.id
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.UNSUPPORTED_VERSION
        assert payload.origin == ROUTER_NAME
        assert payload.details == {"expected": PROTOCOL_VERSION, "received": version}

        # A following v2 REGISTER still gets its acknowledgement.
        v2_request = create_registration_packet(
            source="old-register-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
        )
        socket.send(encode(v2_request))
        ack = decode(socket.recv())
        _assert_registration_ack(ack, v2_request)
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


@pytest.mark.parametrize("hops", [0, 1], ids=["hops-0", "hops-1"])
@pytest.mark.parametrize("version", [1, 3], ids=["older-version", "newer-version"])
def test_unsupported_version_gets_unsupported_version_error(network_port: int, version: int, hops: int) -> None:
    # A version this Router does not speak, older or newer, whatever the hop count (an exact
    # match), gets a fast UNSUPPORTED_VERSION reply whose details carry the
    # expected and received versions, and the Router keeps serving afterwards.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.REQ_RELAXED, 1)
    socket.setsockopt(zmq.REQ_CORRELATE, 1)
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.setsockopt_string(zmq.IDENTITY, "old-version-client")
    socket.connect(f"tcp://{HOST}:{network_port}")

    registration = create_registration_packet(
        source="old-version-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    socket.send(encode(registration))
    registration_ack = decode(socket.recv())
    _assert_registration_ack(registration_ack, registration)

    try:
        # A hand-written wire envelope speaking another protocol version.
        request = Packet(
            intent=PacketIntent.PING, request=Request.PING, source="old-version-client", destination=PROVIDER_NAME
        )
        wire = json.loads(encode(request))
        wire["version"] = version
        wire["hops"] = hops
        socket.send(json.dumps(wire).encode("utf-8"))
        reply = decode(socket.recv())

        assert reply.intent == PacketIntent.ERROR
        assert reply.reply_to == request.id
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.UNSUPPORTED_VERSION
        assert payload.origin == ROUTER_NAME
        assert payload.details == {"expected": PROTOCOL_VERSION, "received": version}

        # The Router keeps serving after the unsupported version.
        ping = Packet(
            intent=PacketIntent.PING, request=Request.PING, source="old-version-client", destination=PROVIDER_NAME
        )
        socket.send(encode(ping))
        pong = decode(socket.recv())
        assert pong.request == Request.PONG
        assert pong.reply_to == ping.id
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


def test_missing_version_gets_malformed_packet(network_port: int) -> None:
    # A missing version fails envelope validation: the Router replies MALFORMED_PACKET,
    # echoing the request's id as reply_to, and keeps serving afterwards.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.REQ_RELAXED, 1)
    socket.setsockopt(zmq.REQ_CORRELATE, 1)
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.setsockopt_string(zmq.IDENTITY, "no-version-client")
    socket.connect(f"tcp://{HOST}:{network_port}")

    registration = create_registration_packet(
        source="no-version-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    socket.send(encode(registration))
    registration_ack = decode(socket.recv())
    _assert_registration_ack(registration_ack, registration)

    try:
        # A hand-written wire envelope with no version at all.
        request = Packet(
            intent=PacketIntent.PING, request=Request.PING, source="no-version-client", destination=PROVIDER_NAME
        )
        wire = json.loads(encode(request))
        del wire["version"]
        socket.send(json.dumps(wire).encode("utf-8"))
        reply = decode(socket.recv())

        assert reply.intent == PacketIntent.ERROR
        # The id parses as a JSON object's string id, so it is echoed as reply_to.
        assert reply.reply_to == request.id
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.MALFORMED_PACKET
        assert payload.origin == ROUTER_NAME

        # The Router keeps serving after the missing version.
        ping = Packet(
            intent=PacketIntent.PING, request=Request.PING, source="no-version-client", destination=PROVIDER_NAME
        )
        socket.send(encode(ping))
        pong = decode(socket.recv())
        assert pong.request == Request.PONG
        assert pong.reply_to == ping.id
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


def test_invalid_registration_payload_gets_malformed_packet(network_port: int) -> None:
    # An invalid REGISTER Payload gets a fast MALFORMED_PACKET reply instead of crashing the
    # Router, and the Router keeps serving afterwards.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.setsockopt_string(zmq.IDENTITY, "bad-register-client")
    socket.connect(f"tcp://{HOST}:{network_port}")

    registration = create_registration_packet(
        source="bad-register-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    socket.send(encode(registration))
    registration_ack = decode(socket.recv())
    _assert_registration_ack(registration_ack, registration)

    try:
        # A REGISTER Packet whose Payload fails validation: no valid registration kind.
        request = Packet(
            intent=PacketIntent.REGISTRATION,
            request=Request.REGISTER,
            source="bad-register-client",
            destination=ROUTER_NAME,
            payload={"kind": "NOT_A_KIND"},
        )
        socket.send(encode(request))
        reply = decode(socket.recv())

        assert reply.intent == PacketIntent.ERROR
        assert reply.reply_to == request.id
        payload = ErrorPayload.model_validate(reply.payload)
        assert payload.code == ErrorCode.MALFORMED_PACKET
        assert payload.origin == ROUTER_NAME

        # The Router keeps serving after the invalid registration.
        ping = Packet(
            intent=PacketIntent.PING, request=Request.PING, source="bad-register-client", destination=PROVIDER_NAME
        )
        socket.send(encode(ping))
        pong = decode(socket.recv())
        assert pong.request == Request.PONG
        assert pong.reply_to == ping.id
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


def test_unknown_destination_error_lists_registered_providers(network_port: int) -> None:
    # The UNKNOWN_DESTINATION reply lists the Providers registered with the Router,
    # every one of them, so the message says what went wrong instead of "Routing not
    # implemented yet."
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.setsockopt_string(zmq.IDENTITY, "lost-client")
    socket.connect(f"tcp://{HOST}:{network_port}")

    # A second Provider registers with the Router: announcing itself is enough, no real
    # Provider needs to be behind it.
    dealer = context.socket(zmq.DEALER)
    dealer.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    dealer.setsockopt_string(zmq.IDENTITY, "second-provider")
    dealer.connect(f"tcp://{HOST}:{network_port}")
    second_registration = create_registration_packet(
        source="second-provider", destination=ROUTER_NAME, kind=NetworkElementClass.PROVIDER
    )
    dealer.send(encode(second_registration))
    _, ack_data = dealer.recv_multipart()
    _assert_registration_ack(decode(ack_data), second_registration)

    registration = create_registration_packet(
        source="lost-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    socket.send(encode(registration))
    registration_ack = decode(socket.recv())
    _assert_registration_ack(registration_ack, registration)

    try:
        request = Packet(
            intent=PacketIntent.DATA, request=Request.GET_DEVICES, source="lost-client", destination="no-such-provider"
        )
        socket.send(encode(request))
        reply = decode(socket.recv())

        payload = _assert_error_reply(reply, request, ErrorCode.UNKNOWN_DESTINATION, ROUTER_NAME)
        assert f"Destination 'no-such-provider' is not registered with {ROUTER_NAME}" in payload.message
        assert PROVIDER_NAME in payload.message
        assert "second-provider" in payload.message
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        dealer.close()
        context.term()


def test_destination_names_are_matched_case_sensitively(network_port: int) -> None:
    # A Packet addressed to the Router's name in a different case is not addressed to the
    # Router: names are matched exactly, so the upper-cased name is an ordinary unknown
    # destination and gets a fast UNKNOWN_DESTINATION instead of a silent drop.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.setsockopt_string(zmq.IDENTITY, "upper-case-client")
    socket.connect(f"tcp://{HOST}:{network_port}")

    try:
        request = Packet(
            intent=PacketIntent.PING,
            request=Request.PING,
            source="upper-case-client",
            destination=ROUTER_NAME.upper(),
        )
        socket.send(encode(request))
        reply = decode(socket.recv())

        payload = _assert_error_reply(reply, request, ErrorCode.UNKNOWN_DESTINATION, ROUTER_NAME)
        assert f"Destination '{ROUTER_NAME.upper()}' is not registered with {ROUTER_NAME}" in payload.message
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


@pytest.mark.parametrize("hops", [0, 100], ids=["small-hop-count", "large-hop-count"])
def test_forwarded_packets_gain_exactly_one_hop(network_port: int, hops: int) -> None:
    # The Router forwards a Packet with exactly one more hop, whatever its hop count: there is
    # no limit on hops, so a large hop count is forwarded like any other. A Dealer standing in for the registered Provider receives the forwarded Packet.
    context = zmq.Context()
    dealer = context.socket(zmq.DEALER)
    dealer.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    dealer.setsockopt_string(zmq.IDENTITY, "hop-counting-provider")
    dealer.connect(f"tcp://{HOST}:{network_port}")

    registration = create_registration_packet(
        source="hop-counting-provider", destination=ROUTER_NAME, kind=NetworkElementClass.PROVIDER
    )
    dealer.send(encode(registration))
    _, ack_data = dealer.recv_multipart()
    _assert_registration_ack(decode(ack_data), registration)

    sender = context.socket(zmq.REQ)
    sender.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    sender.setsockopt_string(zmq.IDENTITY, "hop-counting-sender")
    sender.connect(f"tcp://{HOST}:{network_port}")
    sender_registration = create_registration_packet(
        source="hop-counting-sender", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    sender.send(encode(sender_registration))
    _assert_registration_ack(decode(sender.recv()), sender_registration)

    try:
        request = Packet(
            intent=PacketIntent.PING,
            request=Request.PING,
            source="hop-counting-sender",
            destination="hop-counting-provider",
            hops=hops,
        )
        sender.send(encode(request))
        _, data = dealer.recv_multipart()
        forwarded = decode(data)

        # The forwarded Packet is the request with one more hop, and nothing else changed.
        assert forwarded.id == request.id
        assert forwarded.hops == hops + 1
        assert forwarded.intent == PacketIntent.PING
        assert forwarded.request == Request.PING
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        sender.close()
        dealer.close()
        context.term()


def test_error_packet_addressed_to_the_router_is_never_answered(
    network_port: int, caplog: pytest.LogCaptureFixture
) -> None:
    # An ERROR Packet is itself a reply and is never answered, so errors cannot bounce forever:
    # the Router logs a warning, the sender gets no reply, and the Router keeps serving.
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.REQ_RELAXED, 1)
    socket.setsockopt(zmq.RCVTIMEO, ERROR_TIMEOUT_MS)
    socket.setsockopt_string(zmq.IDENTITY, "error-sender-client")
    socket.connect(f"tcp://{HOST}:{network_port}")

    registration = create_registration_packet(
        source="error-sender-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    socket.send(encode(registration))
    registration_ack = decode(socket.recv())
    _assert_registration_ack(registration_ack, registration)

    try:
        error_packet = Packet(
            intent=PacketIntent.ERROR,
            request=Request.ERROR,
            source="error-sender-client",
            destination=ROUTER_NAME,
            payload=ErrorPayload(
                code=ErrorCode.INSTRUMENT_ERROR, message="an error the Router never asked for", origin="error-sender"
            ).model_dump(),
        )
        with caplog.at_level(logging.WARNING, logger="pqn_hardware.network.router"):
            socket.send(encode(error_packet))
            with pytest.raises(zmq.error.Again):
                socket.recv()

        assert any("ERROR Packet" in record.message for record in caplog.records)

        # The Router keeps serving after dropping the ERROR Packet.
        ping = Packet(
            intent=PacketIntent.PING, request=Request.PING, source="error-sender-client", destination=PROVIDER_NAME
        )
        socket.send(encode(ping))
        pong = decode(socket.recv())
        assert pong.request == Request.PONG
        assert pong.reply_to == ping.id
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        socket.close()
        context.term()


def test_unsolicited_packets_to_a_waiting_client_are_dropped(network_port: int) -> None:
    # A Packet with no reply_to that the Router forwards to a waiting Client carries no
    # request-id frame, so the Client's REQ socket drops it: only a reply to its own request
    # is accepted.
    context = zmq.Context()
    waiter = context.socket(zmq.REQ)
    waiter.setsockopt(zmq.REQ_RELAXED, 1)
    waiter.setsockopt(zmq.REQ_CORRELATE, 1)
    waiter.setsockopt(zmq.RCVTIMEO, 5000)
    waiter.setsockopt_string(zmq.IDENTITY, "waiting-client")
    waiter.connect(f"tcp://{HOST}:{network_port}")

    registration = create_registration_packet(
        source="waiting-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    waiter.send(encode(registration))
    _assert_registration_ack(decode(waiter.recv()), registration)

    # A slow request the waiting client sits on.
    toggle = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.CALL_OPERATION,
        source="waiting-client",
        destination=PROVIDER_NAME,
        payload=CallOperationPayload(instrument="dummy2", operation="toggle_bool").model_dump(),
    )
    waiter.send(encode(toggle))

    # While it waits, another client sends a request addressed to the waiting client.
    sender = context.socket(zmq.REQ)
    sender.setsockopt(zmq.RCVTIMEO, 2000)
    sender.setsockopt_string(zmq.IDENTITY, "sending-client")
    sender.connect(f"tcp://{HOST}:{network_port}")
    sender_registration = create_registration_packet(
        source="sending-client", destination=ROUTER_NAME, kind=NetworkElementClass.CLIENT
    )
    sender.send(encode(sender_registration))
    _assert_registration_ack(decode(sender.recv()), sender_registration)
    unsolicited = Packet(
        intent=PacketIntent.DATA, request=Request.GET_DEVICES, source="sending-client", destination="waiting-client"
    )
    sender.send(encode(unsolicited))

    try:
        # The only reply the waiting client accepts answers its own slow request.
        reply = decode(waiter.recv())
        assert reply.reply_to == toggle.id
        assert reply.request == Request.CALL_OPERATION
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        waiter.close()
        sender.close()
        context.term()


def test_dataclass_return_value_is_rebuilt_as_its_class(measuring_network_port: int) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")

    ret = proxy_instrument.measure()

    assert isinstance(ret, _Measurement)
    assert ret == _Measurement(counts=3)


def test_unimportable_type_path_falls_back_to_dict(
    measuring_network_port: int, caplog: pytest.LogCaptureFixture
) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")
    proxy_instrument.client.device_structure["operations"]["measure"]["returns_type"] = "no.such.module.NoSuchType"

    with caplog.at_level(logging.WARNING, logger="pqn_hardware.network.client"):
        ret = proxy_instrument.measure()

    assert ret == {"counts": 3, "label": "counts"}
    assert any("no.such.module.NoSuchType" in record.message for record in caplog.records)


def test_dataclass_argument_is_dumped_to_json(measuring_network_port: int) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")
    try:
        # The dataclass argument is dumped to JSON on the wire and rebuilt on the Provider; only
        # a rebuilt dataclass carries the counts attribute the Operation reads.
        counts = 5
        assert proxy_instrument.record_measurement(_Measurement(counts=counts)) == counts

        # A field explicitly set to None travels as None, not dropped for its default.
        assert proxy_instrument.record_note_of(_Note(note=None)) is None
        assert proxy_instrument.record_note_of(_Note(note="kept")) == "kept"
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        proxy_instrument.close()
        client.disconnect()


def test_broken_type_paths_fall_back_to_the_plain_data(
    measuring_network_port: int, caplog: pytest.LogCaptureFixture
) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")

    with caplog.at_level(logging.WARNING, logger="pqn_hardware.network.client"):
        # The module imports but the name is missing: AttributeError.
        proxy_instrument.client.device_structure["operations"]["measure"]["returns_type"] = (
            "pqn_hardware.drivers.dummies.NoSuchType"
        )
        assert proxy_instrument.measure() == {"counts": 3, "label": "counts"}

        # The path imports but the value fails validation against it: ValidationError.
        proxy_instrument.client.device_structure["operations"]["labels"]["returns_type"] = (
            "tests.pytest.test_internal_network._Measurement"
        )
        assert proxy_instrument.labels() == ["a", "b"]

        # A path without a dot: ValueError.
        proxy_instrument.client.device_structure["operations"]["measure"]["returns_type"] = "NoSuchType"
        assert proxy_instrument.measure() == {"counts": 3, "label": "counts"}

    warnings = [record.message for record in caplog.records]
    fallbacks = 3
    assert len(warnings) == fallbacks
    assert all("returning the plain JSON data instead" in warning for warning in warnings)


def test_json_native_return_value_logs_no_rebuild_warning(
    measuring_network_port: int, caplog: pytest.LogCaptureFixture
) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")

    # A JSON-native return value has a null type path: it is handed back as it is, with no
    # rebuild and no rebuild warning.
    with caplog.at_level(logging.WARNING, logger="pqn_hardware.network.client"):
        assert proxy_instrument.labels() == ["a", "b"]

    assert caplog.records == []


def test_none_info_comes_back_as_none(measuring_network_port: int) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "no_info1")
    try:
        # An Instrument whose info is None reports None, not an empty mapping.
        assert proxy_instrument.info is None
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        proxy_instrument.close()
        client.disconnect()


def test_dataclass_parameter_round_trips_through_the_proxy(
    measuring_network_port: int, caplog: pytest.LogCaptureFixture
) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")

    with caplog.at_level(logging.WARNING, logger="pqn_hardware.network.client"):
        # Writing an instance dumps it to JSON; reading rebuilds it as the same class.
        written = _Measurement(counts=7)
        proxy_instrument.measurement_param = written
        read_back = proxy_instrument.measurement_param

    assert isinstance(read_back, _Measurement)
    assert read_back == written
    # Neither the write nor the read logs a rebuild warning: a SET reply carries no payload,
    # and the read rebuilds without a fallback.
    assert caplog.records == []


def test_dump_argument_returns_json_data() -> None:
    stamp = _Stamp(when=datetime.datetime(2026, 10, 16, 12, 0, 0, tzinfo=datetime.UTC))

    # The datetime field is dumped as its JSON data, and default-valued fields are kept.
    assert _dump_argument(stamp) == {"when": "2026-10-16T12:00:00Z", "label": "kept"}
    # A dataclass class is not a value: it passes through untouched.
    assert _dump_argument(_Stamp) is _Stamp
    # Values that are not dataclasses pass through as they are, dicts included.
    plain_value = 7
    assert _dump_argument(plain_value) == plain_value
    plain_mapping = {"a": 1}
    assert _dump_argument(plain_mapping) == plain_mapping


def test_arguments_and_parameter_values_go_through_json_dumping(
    measuring_network_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    dumped: list[Any] = []
    real_dump = _dump_argument

    def recording_dump(value: Any) -> Any:
        dumped.append(value)
        return real_dump(value)

    monkeypatch.setattr("pqn_hardware.network.client._dump_argument", recording_dump)

    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)
    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")

    counts = 5
    assert proxy_instrument.record_measurement(_Measurement(counts=counts)) == counts
    proxy_instrument.measurement_param = _Measurement(counts=7)

    # Both the Operation argument and the Parameter value are dumped to JSON on the wire.
    assert _Measurement(counts=5) in dumped
    assert _Measurement(counts=7) in dumped


def test_none_arguments_and_parameter_values_travel(measuring_network_port: int) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")

    # An explicit None argument is sent, not dropped: the driver receives the None.
    assert proxy_instrument.record_note(None) is None
    assert proxy_instrument.record_note("note") == "note"

    # Setting a Parameter to None sends the value None, and reading it hands None back.
    proxy_instrument.note_param = None
    assert proxy_instrument.note_param is None


def test_dict_argument_travels_unchanged(measuring_network_port: int) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")
    try:
        # A plain dict argument is not a dataclass: it passes through untouched, and the
        # Provider receives it unchanged.
        mapping = {"a": 1}
        assert proxy_instrument.record_mapping(mapping) == mapping
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        proxy_instrument.close()
        client.disconnect()


def test_setting_a_typed_parameter_to_none_logs_no_rebuild_warning(
    measuring_network_port: int, caplog: pytest.LogCaptureFixture
) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")
    try:
        # The Parameter publishes the dataclass type path, and its setter also takes None.
        written = _Measurement(counts=9)
        proxy_instrument.optional_measurement_param = written
        assert proxy_instrument.optional_measurement_param == written

        # Setting it to None sends a SET with a null payload: there is no value to rebuild, so
        # no rebuild warning is logged.
        with caplog.at_level(logging.WARNING, logger="pqn_hardware.network.client"):
            proxy_instrument.optional_measurement_param = None

        assert caplog.records == []
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        proxy_instrument.close()
        client.disconnect()


def test_proxy_instrument_binds_two_positional_arguments_in_order(measuring_network_port: int) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")

    # Each positional argument lands on its own name from the Device structure.
    first = 4
    second = 2
    assert proxy_instrument.add(first, second) == first * 10 + second


def test_positional_binding_with_a_defaulted_argument(measuring_network_port: int) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")

    # Fewer positionals than the Operation takes: the remaining arguments keep their defaults.
    value = 3
    default_factor = 10
    assert proxy_instrument.scale(value) == value * default_factor
    # More positionals than required: each lands on its own name, defaulted or not.
    factor = 2
    assert proxy_instrument.scale(value, factor) == value * factor


def test_mixed_positional_and_keyword_arguments(measuring_network_port: int) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")
    try:
        # A positional argument and a keyword argument each land on their own name.
        first = 1
        second = 2
        assert proxy_instrument.add(first, second=second) == first * 10 + second

        # A positional argument with a defaulted keyword argument, and a keyword-only call.
        value = 3
        factor = 2
        assert proxy_instrument.scale(value, factor=factor) == value * factor
        assert proxy_instrument.scale(value=value, factor=factor) == value * factor

        # An argument passed twice, once positionally and once by keyword, still raises.
        with pytest.raises(TypeError, match="multiple values for argument 'value'"):
            proxy_instrument.scale(value, value=value)
    finally:
        # Close the sockets even when an assert fails: a stored failure keeps the frames, and a
        # context that outlives its socket can block the interpreter at exit in zmq term().
        proxy_instrument.close()
        client.disconnect()


def test_positional_arguments_to_an_operation_missing_from_the_device_structure_raise(network_port: int) -> None:
    client = _client(network_port)

    proxy_instrument = client.get_device(PROVIDER_NAME, "dummy2")
    try:
        with pytest.raises(TypeError, match="no entry in the device structure"):
            proxy_instrument.client.trigger_operation("no_such_operation", 1)
    finally:
        proxy_instrument.close()
        client.disconnect()


def test_zero_argument_call_to_an_operation_missing_from_the_structure_is_sent(network_port: int) -> None:
    client = _client(network_port)

    proxy_instrument = client.get_device(PROVIDER_NAME, "dummy2")
    try:
        # With no positional arguments there is nothing to bind: the request is sent, and the
        # Provider's UNKNOWN_OPERATION error comes back as a PacketError, not a local TypeError.
        with pytest.raises(PacketError) as exc_info:
            proxy_instrument.client.trigger_operation("no_such_operation")
    finally:
        proxy_instrument.close()
        client.disconnect()

    assert exc_info.value.code == ErrorCode.UNKNOWN_OPERATION
    assert exc_info.value.origin == PROVIDER_NAME


def test_proxy_instrument_without_a_device_structure_rejects_positional_arguments(network_port: int) -> None:
    # A ProxyInstrument built with the default (empty) Device structure binds no positional
    # arguments: it raises a clean TypeError instead of sending them.
    proxy_instrument = ProxyInstrument(
        name="dummy2",
        host=HOST,
        port=network_port,
        router_name=ROUTER_NAME,
        provider_name=PROVIDER_NAME,
        instrument_name="dummy2",
        timeout_ms=10_000,
    )
    try:
        with pytest.raises(TypeError, match="no entry in the device structure"):
            proxy_instrument.client.trigger_operation("double_int", 1)
    finally:
        proxy_instrument.close()


def test_error_unknown_instrument(raw_client: _RawClient) -> None:
    request = Packet(
        intent=PacketIntent.DATA,
        request=Request.GET_DEVICE_STRUCTURE,
        source=raw_client.name,
        destination=PROVIDER_NAME,
        payload=GetDeviceStructurePayload(instrument="nonexistent").model_dump(),
    )

    reply = raw_client.ask(request)

    _assert_error_reply(reply, request, ErrorCode.UNKNOWN_INSTRUMENT, PROVIDER_NAME)


def test_error_unknown_operation(raw_client: _RawClient) -> None:
    request = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.CALL_OPERATION,
        source=raw_client.name,
        destination=PROVIDER_NAME,
        payload=CallOperationPayload(instrument="dummy1", operation="nonexistent").model_dump(),
    )

    reply = raw_client.ask(request)

    _assert_error_reply(reply, request, ErrorCode.UNKNOWN_OPERATION, PROVIDER_NAME)


def test_error_unknown_parameter(raw_client: _RawClient) -> None:
    request = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.GET_PARAMETER,
        source=raw_client.name,
        destination=PROVIDER_NAME,
        payload=GetParameterPayload(instrument="dummy1", parameter="nonexistent").model_dump(),
    )

    reply = raw_client.ask(request)

    _assert_error_reply(reply, request, ErrorCode.UNKNOWN_PARAMETER, PROVIDER_NAME)


def test_error_invalid_arguments_for_operation_kwargs(raw_client: _RawClient) -> None:
    request = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.CALL_OPERATION,
        source=raw_client.name,
        destination=PROVIDER_NAME,
        payload=CallOperationPayload(
            instrument="dummy1", operation="set_half_input_int", kwargs={"value": "not a number"}
        ).model_dump(),
    )

    reply = raw_client.ask(request)

    _assert_error_reply(reply, request, ErrorCode.INVALID_ARGUMENTS, PROVIDER_NAME)


def test_error_invalid_arguments_for_parameter_value(raw_client: _RawClient) -> None:
    request = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.SET_PARAMETER,
        source=raw_client.name,
        destination=PROVIDER_NAME,
        payload=SetParameterPayload(instrument="dummy1", parameter="param_int", value="not a number").model_dump(),
    )

    reply = raw_client.ask(request)

    _assert_error_reply(reply, request, ErrorCode.INVALID_ARGUMENTS, PROVIDER_NAME)


def test_error_instrument_error(raw_client: _RawClient) -> None:
    request = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.CALL_OPERATION,
        source=raw_client.name,
        destination=PROVIDER_NAME,
        payload=CallOperationPayload(instrument="dummy1", operation="fail").model_dump(),
    )

    reply = raw_client.ask(request)

    payload = _assert_error_reply(reply, request, ErrorCode.INSTRUMENT_ERROR, PROVIDER_NAME)
    assert payload.details["exception_type"] == "RuntimeError"
    assert "dummy instrument failure" in payload.details["exception"]


def test_error_unsupported_request_for_unknown_data_verb(raw_client: _RawClient) -> None:
    request = Packet(
        intent=PacketIntent.DATA,
        request=Request.GET_PROVIDERS,
        source=raw_client.name,
        destination=PROVIDER_NAME,
    )

    reply = raw_client.ask(request)

    _assert_error_reply(reply, request, ErrorCode.UNSUPPORTED_REQUEST, PROVIDER_NAME)


def test_error_unsupported_request_for_unknown_control_verb(raw_client: _RawClient) -> None:
    request = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.GET_DEVICES,
        source=raw_client.name,
        destination=PROVIDER_NAME,
    )

    reply = raw_client.ask(request)

    _assert_error_reply(reply, request, ErrorCode.UNSUPPORTED_REQUEST, PROVIDER_NAME)


def test_registration_intent_packet_gets_unsupported_request(raw_router: _RawRouter) -> None:
    # A REGISTRATION Packet the Provider never handles; through the raw router, since the real Router
    # answers REGISTRATION Packets itself.
    request = Packet(
        intent=PacketIntent.REGISTRATION,
        request=Request.REGISTER,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=RegisterPayload(kind=NetworkElementClass.CLIENT).model_dump(),
    )

    reply = raw_router.ask(request)

    _assert_error_reply(reply, request, ErrorCode.UNSUPPORTED_REQUEST, RAW_PROVIDER_NAME)


def test_error_intent_packet_gets_no_reply(raw_router: _RawRouter) -> None:
    # An ERROR Packet is itself a reply and is never answered, so errors cannot bounce forever.
    request = Packet(
        intent=PacketIntent.ERROR,
        request=Request.ERROR,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=ErrorPayload(
            code=ErrorCode.INSTRUMENT_ERROR, message="an error the Provider never asked for", origin="some-client"
        ).model_dump(),
    )

    with pytest.raises(zmq.error.Again):
        raw_router.ask(request)

    # The Provider keeps serving after ignoring the ERROR Packet.
    follow_up = Packet(
        intent=PacketIntent.DATA,
        request=Request.GET_DEVICES,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
    )
    follow_up_reply = raw_router.ask(follow_up)
    assert follow_up_reply.reply_to == follow_up.id
    assert "dummy1" in follow_up_reply.payload["devices"]


def test_error_malformed_packet_on_invalid_payload(raw_client: _RawClient) -> None:
    # A GET_DEVICE_STRUCTURE Packet without its required payload.
    request = Packet(
        intent=PacketIntent.DATA,
        request=Request.GET_DEVICE_STRUCTURE,
        source=raw_client.name,
        destination=PROVIDER_NAME,
    )

    reply = raw_client.ask(request)

    _assert_error_reply(reply, request, ErrorCode.MALFORMED_PACKET, PROVIDER_NAME)


def test_error_wrong_destination(raw_router: _RawRouter) -> None:
    # The real Router never forwards a misaddressed Packet to a Provider, so this goes through a raw socket.
    request = Packet(
        intent=PacketIntent.DATA,
        request=Request.GET_DEVICES,
        source="some-client",
        destination="another-provider",
    )

    reply = raw_router.ask(request)

    _assert_error_reply(reply, request, ErrorCode.WRONG_DESTINATION, RAW_PROVIDER_NAME)

    # The Provider keeps serving after replying to a misaddressed Packet.
    follow_up = Packet(
        intent=PacketIntent.DATA,
        request=Request.GET_DEVICES,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
    )
    follow_up_reply = raw_router.ask(follow_up)
    assert follow_up_reply.reply_to == follow_up.id
    assert "dummy1" in follow_up_reply.payload["devices"]


def test_error_malformed_packet_on_undecodable_bytes(raw_router: _RawRouter) -> None:
    # Bytes a pre-v2 pickle peer would send: not JSON at all, so there is no id to echo.
    raw_router.send_bytes(b"\x80\x04\x95pickle-bytes")
    reply = raw_router.receive()

    assert reply.intent == PacketIntent.ERROR
    assert reply.reply_to is None
    assert reply.destination == ROUTER_NAME
    payload = ErrorPayload.model_validate(reply.payload)
    assert payload.code == ErrorCode.MALFORMED_PACKET
    assert payload.origin == RAW_PROVIDER_NAME


def test_error_malformed_packet_on_wrong_frame_count(raw_router: _RawRouter) -> None:
    # One frame too many after the delimiter.
    raw_router.send_bytes(b"{}", b"extra-frame")
    reply = raw_router.receive()

    assert reply.intent == PacketIntent.ERROR
    payload = ErrorPayload.model_validate(reply.payload)
    assert payload.code == ErrorCode.MALFORMED_PACKET
    assert payload.origin == RAW_PROVIDER_NAME

    # The Provider keeps serving after replying to bad input.
    follow_up = Packet(
        intent=PacketIntent.DATA,
        request=Request.GET_DEVICES,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
    )
    follow_up_reply = raw_router.ask(follow_up)
    assert follow_up_reply.reply_to == follow_up.id
    assert "dummy1" in follow_up_reply.payload["devices"]


def test_reply_to_is_echoed_on_every_reply(raw_client: _RawClient) -> None:
    # Uses dummy2 so the mutating requests do not depend on, or disturb, dummy1's state.
    requests = [
        Packet(intent=PacketIntent.PING, request=Request.PING, source=raw_client.name, destination=PROVIDER_NAME),
        Packet(
            intent=PacketIntent.DATA, request=Request.GET_DEVICES, source=raw_client.name, destination=PROVIDER_NAME
        ),
        Packet(
            intent=PacketIntent.DATA,
            request=Request.GET_DEVICE_STRUCTURE,
            source=raw_client.name,
            destination=PROVIDER_NAME,
            payload=GetDeviceStructurePayload(instrument="dummy2").model_dump(),
        ),
        Packet(
            intent=PacketIntent.CONTROL,
            request=Request.CALL_OPERATION,
            source=raw_client.name,
            destination=PROVIDER_NAME,
            payload=CallOperationPayload(instrument="dummy2", operation="double_int").model_dump(),
        ),
        Packet(
            intent=PacketIntent.CONTROL,
            request=Request.GET_PARAMETER,
            source=raw_client.name,
            destination=PROVIDER_NAME,
            payload=GetParameterPayload(instrument="dummy2", parameter="param_int").model_dump(),
        ),
        Packet(
            intent=PacketIntent.CONTROL,
            request=Request.SET_PARAMETER,
            source=raw_client.name,
            destination=PROVIDER_NAME,
            payload=SetParameterPayload(instrument="dummy2", parameter="param_int", value=7).model_dump(),
        ),
        Packet(
            intent=PacketIntent.CONTROL,
            request=Request.GET_INFO,
            source=raw_client.name,
            destination=PROVIDER_NAME,
            payload=GetInfoPayload(instrument="dummy2").model_dump(),
        ),
    ]

    for request in requests:
        reply = raw_client.ask(request)
        assert reply.reply_to == request.id, f"Request {request.request} did not echo its id as reply_to"
        assert reply.destination == raw_client.name


def test_device_structure_has_json_schemas_and_type_paths(raw_client: _RawClient) -> None:
    request = Packet(
        intent=PacketIntent.DATA,
        request=Request.GET_DEVICE_STRUCTURE,
        source=raw_client.name,
        destination=PROVIDER_NAME,
        payload=GetDeviceStructurePayload(instrument="dummy1").model_dump(),
    )

    reply = raw_client.ask(request)

    structure = reply.payload
    assert structure["name"] == "dummy1"
    assert structure["desc"] == "Dummy instrument1 for testing purposes"
    assert structure["hw_address"] == "1234"
    assert structure["type"] == "pqn_hardware.drivers.dummies.DummyInstrument"

    # The info carries the JSON Schema of its type and, not being JSON-native, its Python type path.
    assert structure["info"]["type"] == "pqn_hardware.drivers.dummies.DummyInfo"
    assert structure["info"]["schema"]["type"] == "object"
    assert "param_int" in structure["info"]["schema"]["properties"]

    operations = structure["operations"]
    assert set(operations) == {
        "double_int",
        "lowercase_str",
        "uppercase_str",
        "toggle_bool",
        "set_half_input_int",
        "fail",
    }
    # Keyword arguments validate against a JSON Schema that forbids unknown arguments.
    args_schema = operations["set_half_input_int"]["args_schema"]
    assert args_schema["type"] == "object"
    assert args_schema["additionalProperties"] is False
    assert args_schema["properties"]["value"] == {"title": "Value", "type": "integer"}
    assert args_schema["required"] == ["value"]
    # A JSON-native return type has a schema and no type path.
    assert operations["set_half_input_int"]["returns_schema"] == {"type": "integer"}
    assert operations["set_half_input_int"]["returns_type"] is None
    assert operations["double_int"]["args_schema"]["properties"] == {}
    assert operations["fail"]["returns_schema"] == {"type": "null"}

    parameters = structure["parameters"]
    assert set(parameters) == {"param_int", "param_str", "param_bool"}
    assert parameters["param_int"]["schema"] == {"type": "integer"}
    assert parameters["param_int"]["type"] is None
    assert parameters["param_int"]["writable"] is True
    assert parameters["param_str"]["schema"] == {"type": "string"}
    assert parameters["param_bool"]["schema"] == {"type": "boolean"}


def test_dataclass_return_value_publishes_its_type_path(raw_router: _RawRouter) -> None:
    request = Packet(
        intent=PacketIntent.DATA,
        request=Request.GET_DEVICE_STRUCTURE,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=GetDeviceStructurePayload(instrument="measuring1").model_dump(),
    )

    reply = raw_router.ask(request)
    structure = reply.payload

    # A dataclass return value is not JSON-native: it publishes its Python import path (ADR-0002).
    measure = structure["operations"]["measure"]
    assert measure["returns_type"] == "tests.pytest.test_internal_network._Measurement"
    assert measure["returns_schema"]["type"] == "object"
    assert measure["returns_schema"]["properties"]["counts"] == {"title": "Counts", "type": "integer"}

    # A bare list is JSON-native: a schema, but no type path.
    labels = structure["operations"]["labels"]
    assert labels["returns_type"] is None
    assert labels["returns_schema"] == {"items": {}, "type": "array"}

    # Calling the Operation returns the dataclass serialized via its return annotation.
    call = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.CALL_OPERATION,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=CallOperationPayload(instrument="measuring1", operation="measure").model_dump(),
    )
    call_reply = raw_router.ask(call)
    assert call_reply.payload == {"value": {"counts": 3, "label": "counts"}}


def test_device_structure_parameter_metadata(raw_router: _RawRouter) -> None:
    request = Packet(
        intent=PacketIntent.DATA,
        request=Request.GET_DEVICE_STRUCTURE,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=GetDeviceStructurePayload(instrument="shapes1").model_dump(),
    )

    reply = raw_router.ask(request)
    parameters = reply.payload["parameters"]

    # A getter-only property is a read-only Parameter.
    assert parameters["read_only"]["writable"] is False
    assert parameters["read_only"]["schema"] == {"type": "integer"}
    # A Parameter that is a plain attribute carries no type hints, but is writable.
    assert parameters["recorded"]["writable"] is True
    assert parameters["recorded"]["schema"] == {}
    # A write-only property publishes its setter's type hint as the schema.
    assert parameters["write_only"]["writable"] is True
    assert parameters["write_only"]["schema"] == {"type": "integer"}
    # A dataclass Parameter publishes its Python import path (ADR-0002), also through a string
    # (forward reference) annotation.
    assert parameters["measurement_param"]["type"] == "tests.pytest.test_internal_network._Measurement"
    assert parameters["measurement_param"]["schema"]["type"] == "object"
    assert parameters["forward_ref_param"]["type"] == "tests.pytest.test_internal_network._Measurement"
    assert parameters["forward_ref_param"]["schema"]["type"] == "object"
    # A bare dict is JSON-native: a schema, but no type path.
    assert parameters["dict_param"]["type"] is None
    assert parameters["dict_param"]["schema"] == {"additionalProperties": True, "type": "object"}
    # A Parameter hinted with a base dataclass publishes the hinted type's path.
    assert parameters["wide_param"]["type"] == "pqn_hardware.instrument.InstrumentInfo"

    operations = reply.payload["operations"]
    # Variadic parameters are not Operation arguments on the wire.
    assert set(operations["variadic"]["args_schema"]["properties"]) == {"value"}
    assert operations["variadic"]["args_schema"]["required"] == ["value"]
    # A bare dict return type is JSON-native: a schema, but no type path.
    assert operations["mapping"]["returns_type"] is None
    assert operations["mapping"]["returns_schema"] == {"additionalProperties": True, "type": "object"}


def test_operation_argument_binding(raw_router: _RawRouter) -> None:
    # An argument with a default can be omitted; the defaulted value reaches the driver.
    defaulted = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.CALL_OPERATION,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=CallOperationPayload(instrument="shapes1", operation="record").model_dump(),
    )
    assert raw_router.ask(defaulted).payload == {"value": 7}

    # A string for an int argument is coerced, and the driver receives the validated int.
    coerced = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.CALL_OPERATION,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=CallOperationPayload(instrument="shapes1", operation="record", kwargs={"value": "3"}).model_dump(),
    )
    assert raw_router.ask(coerced).payload == {"value": 3}
    recorded = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.GET_PARAMETER,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=GetParameterPayload(instrument="shapes1", parameter="recorded").model_dump(),
    )
    assert raw_router.ask(recorded).payload == {"value": 3}

    # An Operation with variadic parameters is called with its named arguments only.
    variadic = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.CALL_OPERATION,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=CallOperationPayload(instrument="shapes1", operation="variadic", kwargs={"value": 2}).model_dump(),
    )
    assert raw_router.ask(variadic).payload == {"value": 2}


def test_values_are_serialized_through_their_type_hint(raw_router: _RawRouter) -> None:
    # Each reply carries only the hinted type's fields, not the subclass's extra field.
    hinted_fields = {"name": "wide", "desc": "", "hw_address": "", "hw_status": {}}

    call = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.CALL_OPERATION,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=CallOperationPayload(instrument="shapes1", operation="wide_info").model_dump(),
    )
    assert raw_router.ask(call).payload["value"] == hinted_fields

    read = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.GET_PARAMETER,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=GetParameterPayload(instrument="shapes1", parameter="wide_param").model_dump(),
    )
    assert raw_router.ask(read).payload["value"] == hinted_fields

    info = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.GET_INFO,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=GetInfoPayload(instrument="shapes1").model_dump(),
    )
    assert raw_router.ask(info).payload["value"] == {**hinted_fields, "name": "shapes"}


def test_instrument_error_on_raising_driver_access(raw_router: _RawRouter) -> None:
    broken_read = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.GET_PARAMETER,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=GetParameterPayload(instrument="raising1", parameter="broken_read").model_dump(),
    )
    payload = _assert_error_reply(
        raw_router.ask(broken_read), broken_read, ErrorCode.INSTRUMENT_ERROR, RAW_PROVIDER_NAME
    )
    assert payload.details["exception_type"] == "RuntimeError"
    assert "broken read" in payload.details["exception"]

    broken_write = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.SET_PARAMETER,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=SetParameterPayload(instrument="raising1", parameter="broken_write", value=1).model_dump(),
    )
    payload = _assert_error_reply(
        raw_router.ask(broken_write), broken_write, ErrorCode.INSTRUMENT_ERROR, RAW_PROVIDER_NAME
    )
    assert payload.details["exception_type"] == "RuntimeError"
    assert "broken write" in payload.details["exception"]

    broken_info = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.GET_INFO,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
        payload=GetInfoPayload(instrument="raising1").model_dump(),
    )
    payload = _assert_error_reply(
        raw_router.ask(broken_info), broken_info, ErrorCode.INSTRUMENT_ERROR, RAW_PROVIDER_NAME
    )
    assert payload.details["exception_type"] == "RuntimeError"
    assert "broken info" in payload.details["exception"]

    # The Provider keeps serving after driver errors.
    follow_up = Packet(
        intent=PacketIntent.DATA,
        request=Request.GET_DEVICES,
        source="some-client",
        destination=RAW_PROVIDER_NAME,
    )
    assert "dummy1" in raw_router.ask(follow_up).payload["devices"]


def test_return_values_are_serialized_via_the_return_annotation(raw_client: _RawClient) -> None:
    # Uses dummy2 and sets its state first, so the test does not depend on other tests' order.
    input_value = 30
    half_input_value = input_value // 2
    call = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.CALL_OPERATION,
        source=raw_client.name,
        destination=PROVIDER_NAME,
        payload=CallOperationPayload(
            instrument="dummy2", operation="set_half_input_int", kwargs={"value": input_value}
        ).model_dump(),
    )
    call_reply = raw_client.ask(call)
    assert call_reply.payload == {"value": half_input_value}

    read = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.GET_PARAMETER,
        source=raw_client.name,
        destination=PROVIDER_NAME,
        payload=GetParameterPayload(instrument="dummy2", parameter="param_int").model_dump(),
    )
    read_reply = raw_client.ask(read)
    assert read_reply.payload == {"value": half_input_value}

    # The DummyInfo dataclass is serialized through its return annotation, as a JSON object.
    info = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.GET_INFO,
        source=raw_client.name,
        destination=PROVIDER_NAME,
        payload=GetInfoPayload(instrument="dummy2").model_dump(),
    )
    info_reply = raw_client.ask(info)
    info_value = info_reply.payload["value"]
    assert info_value["name"] == "dummy2"
    assert info_value["param_int"] == half_input_value
    # Order-safe under pytest-randomly: compare against a live read of the same parameter.
    param_str_read = Packet(
        intent=PacketIntent.CONTROL,
        request=Request.GET_PARAMETER,
        source=raw_client.name,
        destination=PROVIDER_NAME,
        payload=GetParameterPayload(instrument="dummy2", parameter="param_str").model_dump(),
    )
    assert info_value["param_str"] == raw_client.ask(param_str_read).payload["value"]


class _Unschemaable:
    """A type pydantic cannot turn into a JSON Schema."""


class _BadOperationInstrument(DummyInstrument):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.operations["bad_operation"] = self.bad_operation

    def bad_operation(self, value: _Unschemaable) -> None: ...


class _BadParameterInstrument(DummyInstrument):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.parameters.add("bad_parameter")

    @property
    def bad_parameter(self) -> _Unschemaable: ...

    @bad_parameter.setter
    def bad_parameter(self, value: _Unschemaable) -> None: ...


class _BadInfoInstrument(DummyInstrument):
    @property
    def info(self) -> _Unschemaable: ...


class _UnresolvableHintInstrument(DummyInstrument):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.operations["unresolvable"] = self.unresolvable

    def unresolvable(self, value: "_NoSuchType") -> None: ...  # noqa: F821 - the hint must stay unresolvable


@dataclass(frozen=True, slots=True)
class _Measurement:
    """A dataclass return value, to exercise non-null type paths (ADR-0002)."""

    counts: int
    label: str = "counts"


@dataclass(frozen=True, slots=True)
class _Note:
    """A dataclass argument with a field that may be explicitly None."""

    note: str | None = "unset"


@dataclass(frozen=True, slots=True)
class _Stamp:
    """A dataclass argument with a field whose python-mode dump is not JSON."""

    when: datetime.datetime
    label: str = "kept"


class _NoInfoInstrument(DummyInstrument):
    """An Instrument whose info is None, to exercise a null GET_INFO value."""

    @property
    def info(self) -> None:
        return None


class _MeasuringInstrument(DummyInstrument):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.operations["measure"] = self.measure
        self.operations["labels"] = self.labels
        self.operations["record_measurement"] = self.record_measurement
        self.operations["record_mapping"] = self.record_mapping
        self.operations["record_note_of"] = self.record_note_of
        self.operations["record_note"] = self.record_note
        self.operations["add"] = self.add
        self.operations["scale"] = self.scale
        self.parameters.update({"measurement_param", "note_param", "optional_measurement_param"})
        self._measurement_param = _Measurement(counts=0)
        self._optional_measurement_param = _Measurement(counts=1)
        self._note: str | None = None

    @property
    def measurement_param(self) -> _Measurement:
        return self._measurement_param

    @measurement_param.setter
    def measurement_param(self, value: _Measurement) -> None:
        self._measurement_param = value

    @property
    def optional_measurement_param(self) -> _Measurement:
        return self._optional_measurement_param

    @optional_measurement_param.setter
    def optional_measurement_param(self, value: _Measurement | None) -> None:
        # The setter takes None while the getter still publishes the dataclass type path.
        self._optional_measurement_param = value

    @property
    def note_param(self) -> str | None:
        return self._note

    @note_param.setter
    def note_param(self, value: str | None) -> None:
        self._note = value

    def measure(self) -> _Measurement:
        return _Measurement(counts=3)

    def labels(self) -> list:
        return ["a", "b"]

    def record_measurement(self, measurement: _Measurement) -> int:
        """Take a dataclass argument; only a rebuilt dataclass has the counts attribute."""
        return measurement.counts

    def record_note_of(self, value: _Note) -> str | None:
        """Hand back the dataclass argument's note field, None included."""
        return value.note

    def record_mapping(self, mapping: dict) -> dict:
        """Take a plain dict argument, and hand it back unchanged."""
        return mapping

    def record_note(self, note: str | None) -> str | None:
        """Take a required argument that may be None, and hand it back."""
        self._note = note
        return note

    def add(self, first: int, second: int) -> int:
        """Take two arguments, so a test can check that each lands on the right name."""
        return first * 10 + second

    def scale(self, value: int, factor: int = 10) -> int:
        """Take a required and a defaulted argument, to exercise partial positional binding."""
        return value * factor


@dataclass(frozen=True, slots=True)
class _WideInfo(InstrumentInfo):
    """An InstrumentInfo subclass with an extra field, to test serialization via the hinted type."""

    extra: str = "extra"


class _ShapesInstrument(DummyInstrument):
    """An Instrument whose Parameters and Operations exercise every Device-structure shape."""

    def __post_init__(self) -> None:
        super().__post_init__()
        # A Parameter that is a plain attribute, not a property: no type hints, but writable.
        self.recorded: int | None = None
        self.parameters.update(
            {
                "read_only",
                "recorded",
                "write_only",
                "measurement_param",
                "dict_param",
                "forward_ref_param",
                "wide_param",
            }
        )
        self.operations.update(
            {"record": self.record, "variadic": self.variadic, "mapping": self.mapping, "wide_info": self.wide_info}
        )

    @property
    def read_only(self) -> int:
        return 1

    def _set_write_only(self, value: int) -> None:
        self.recorded = value

    # A write-only property: reading it is not part of the Parameter's contract.
    write_only = property(fset=_set_write_only)

    @property
    def measurement_param(self) -> _Measurement:
        return _Measurement(counts=1)

    @property
    def dict_param(self) -> dict:
        return {"a": 1}

    @property
    def forward_ref_param(self) -> "_Measurement":
        return _Measurement(counts=2)

    @property
    def wide_param(self) -> InstrumentInfo:
        return _WideInfo(name="wide")

    def record(self, value: int = 7) -> int:
        """Store the validated argument, so the test can read it back."""
        self.recorded = value
        return value

    def variadic(self, value: int, *args: int, **kwargs: int) -> int:
        return value + sum(args) + sum(kwargs.values())

    def mapping(self) -> dict:
        return {"a": 1}

    def wide_info(self) -> InstrumentInfo:
        return _WideInfo(name="wide")

    @property
    def info(self) -> InstrumentInfo:
        return _WideInfo(name="shapes")


class _RaisingInstrument(DummyInstrument):
    """An Instrument whose driver access raises, to test INSTRUMENT_ERROR replies."""

    def __post_init__(self) -> None:
        super().__post_init__()
        self.parameters.update({"broken_read", "broken_write"})

    @property
    def broken_read(self) -> int:
        msg = "broken read"
        raise RuntimeError(msg)

    @property
    def broken_write(self) -> int:
        return 0

    @broken_write.setter
    def broken_write(self, _value: int) -> None:
        msg = "broken write"
        raise RuntimeError(msg)

    @property
    def info(self) -> InstrumentInfo:
        msg = "broken info"
        raise RuntimeError(msg)


class _UnresolvableParameterInstrument(DummyInstrument):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.parameters.add("unresolvable_param")

    @property
    def unresolvable_param(self) -> "_NoSuchType": ...  # noqa: F821 - the hint must stay unresolvable


class _UnresolvableInfoInstrument(DummyInstrument):
    @property
    def info(self) -> "_NoSuchType": ...  # noqa: F821 - the hint must stay unresolvable


def test_startup_fails_when_an_operation_type_hint_cannot_produce_a_json_schema() -> None:
    provider = InstrumentProvider(
        "bad-hints-provider",
        HOST,
        _free_port(),
        ROUTER_NAME,
        bad={
            "import": "tests.pytest.test_internal_network._BadOperationInstrument",
            "desc": "Instrument with an unschemaable operation",
            "hw_address": "1234",
        },
    )

    # The configuration error is raised at startup, before the provider talks to any Router.
    with pytest.raises(InvalidInstrumentsConfigurationError, match="bad_operation"):
        provider.start()


def test_startup_fails_when_a_parameter_type_hint_cannot_produce_a_json_schema() -> None:
    provider = InstrumentProvider(
        "bad-hints-provider",
        HOST,
        _free_port(),
        ROUTER_NAME,
        bad={
            "import": "tests.pytest.test_internal_network._BadParameterInstrument",
            "desc": "Instrument with an unschemaable parameter",
            "hw_address": "1234",
        },
    )

    with pytest.raises(InvalidInstrumentsConfigurationError, match="bad_parameter"):
        provider.start()


def test_startup_fails_when_the_info_type_hint_cannot_produce_a_json_schema() -> None:
    provider = InstrumentProvider(
        "bad-hints-provider",
        HOST,
        _free_port(),
        ROUTER_NAME,
        bad={
            "import": "tests.pytest.test_internal_network._BadInfoInstrument",
            "desc": "Instrument with an unschemaable info",
            "hw_address": "1234",
        },
    )

    with pytest.raises(InvalidInstrumentsConfigurationError, match="info"):
        provider.start()


def test_startup_fails_when_a_type_hint_cannot_be_resolved() -> None:
    provider = InstrumentProvider(
        "bad-hints-provider",
        HOST,
        _free_port(),
        ROUTER_NAME,
        bad={
            "import": "tests.pytest.test_internal_network._UnresolvableHintInstrument",
            "desc": "Instrument with an unresolvable type hint",
            "hw_address": "1234",
        },
    )

    with pytest.raises(InvalidInstrumentsConfigurationError, match="unresolvable"):
        provider.start()


def test_startup_fails_when_a_parameter_hint_cannot_be_resolved() -> None:
    provider = InstrumentProvider(
        "bad-hints-provider",
        HOST,
        _free_port(),
        ROUTER_NAME,
        bad={
            "import": "tests.pytest.test_internal_network._UnresolvableParameterInstrument",
            "desc": "Instrument with an unresolvable parameter hint",
            "hw_address": "1234",
        },
    )

    with pytest.raises(InvalidInstrumentsConfigurationError, match="unresolvable_param"):
        provider.start()


def test_startup_fails_when_the_info_hint_cannot_be_resolved() -> None:
    provider = InstrumentProvider(
        "bad-hints-provider",
        HOST,
        _free_port(),
        ROUTER_NAME,
        bad={
            "import": "tests.pytest.test_internal_network._UnresolvableInfoInstrument",
            "desc": "Instrument with an unresolvable info hint",
            "hw_address": "1234",
        },
    )

    with pytest.raises(InvalidInstrumentsConfigurationError, match="info"):
        provider.start()
