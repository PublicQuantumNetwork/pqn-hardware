"""Network tests running the Router and an InstrumentProvider in-process, on an ephemeral port.

The CLI smoke test in test_cli_smoke.py covers starting the same elements as subprocesses.
"""

import logging
import socket
import threading
import time
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

import pytest
import zmq

from pqn_hardware.drivers.dummies import DummyInfo
from pqn_hardware.drivers.dummies import DummyInstrument
from pqn_hardware.errors import InvalidInstrumentsConfigurationError
from pqn_hardware.errors import PacketError
from pqn_hardware.instrument import InstrumentInfo
from pqn_hardware.network.client import Client
from pqn_hardware.network.client import ProxyInstrument
from pqn_hardware.network.instrument_provider import InstrumentProvider
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

        # A reply with the right request-id frame but a reply_to that matches no request.
        identity, frame, _, data = server.recv_multipart()
        request = decode(data)
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
                        reply_to="not-the-request-id",
                    )
                ),
            ]
        )

        # The next request gets a reply that answers it.
        identity, frame, _, data = server.recv_multipart()
        request = decode(data)
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
                        reply_to=request.id,
                    )
                ),
            ]
        )

    threading.Thread(target=serve, daemon=True).start()

    client = Client(host=HOST, port=port, router_name=ROUTER_NAME, timeout=1000)
    try:
        with (
            caplog.at_level(logging.WARNING, logger="pqn_hardware.network.client"),
            pytest.raises(TimeoutError),
        ):
            client.ping(PROVIDER_NAME)

        # The mismatched reply is dropped with a warning naming the ids.
        assert any("not-the-request-id" in record.message for record in caplog.records)

        # The Client survives the dropped reply: the next request succeeds on the same socket.
        reply = client.ping(PROVIDER_NAME)
        assert reply is not None
        assert reply.request == Request.PONG
        assert reply.reply_to is not None
    finally:
        # Close the socket while the Client is alive, so a garbage-collected context can never
        # block another thread's garbage collection in zmq Context.term().
        client.disconnect()

    server.close()
    context.term()


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
    assert ack.intent == PacketIntent.REGISTRATION_ACK
    assert ack.reply_to == registration.id

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

    # The dataclass argument is dumped to JSON on the wire and rebuilt on the Provider; only a
    # rebuilt dataclass carries the counts attribute the Operation reads.
    counts = 5
    assert proxy_instrument.record_measurement(_Measurement(counts=counts)) == counts


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


def test_proxy_instrument_binds_two_positional_arguments_in_order(measuring_network_port: int) -> None:
    client = Client(host=HOST, port=measuring_network_port, router_name=ROUTER_NAME, timeout=10_000)

    proxy_instrument = client.get_device(MEASURING_PROVIDER_NAME, "measuring1")

    # Each positional argument lands on its own name from the Device structure.
    first = 4
    second = 2
    assert proxy_instrument.add(first, second) == first * 10 + second


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


class _MeasuringInstrument(DummyInstrument):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.operations["measure"] = self.measure
        self.operations["labels"] = self.labels
        self.operations["record_measurement"] = self.record_measurement
        self.operations["add"] = self.add
        self.parameters.add("measurement_param")
        self._measurement_param = _Measurement(counts=0)

    @property
    def measurement_param(self) -> _Measurement:
        return self._measurement_param

    @measurement_param.setter
    def measurement_param(self, value: _Measurement) -> None:
        self._measurement_param = value

    def measure(self) -> _Measurement:
        return _Measurement(counts=3)

    def labels(self) -> list:
        return ["a", "b"]

    def record_measurement(self, measurement: _Measurement) -> int:
        """Take a dataclass argument; only a rebuilt dataclass has the counts attribute."""
        return measurement.counts

    def add(self, first: int, second: int) -> int:
        """Take two arguments, so a test can check that each lands on the right name."""
        return first * 10 + second


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
