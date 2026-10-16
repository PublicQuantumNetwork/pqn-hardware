"""Network tests running the Router and an InstrumentProvider in-process, on an ephemeral port.

The CLI smoke test in test_cli_smoke.py covers starting the same elements as subprocesses.
"""

import logging
import socket
import threading
import time
from typing import Any

import pytest

from pqn_hardware.network.client import Client
from pqn_hardware.network.client import ProxyInstrument
from pqn_hardware.network.instrument_provider import InstrumentProvider
from pqn_hardware.network.packet import DeviceSummary
from pqn_hardware.network.packet import Packet
from pqn_hardware.network.packet import PacketIntent
from pqn_hardware.network.packet import Request
from pqn_hardware.network.router import Router

logger = logging.getLogger(__name__)

ROUTER_NAME = "pqn_hardware-router"
PROVIDER_NAME = "pqn_hardware-provider"
HOST = "127.0.0.1"


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
    _wait_until(
        lambda: PROVIDER_NAME in router.providers, 15, "InstrumentProvider did not register with the Router."
    )

    logger.debug("Router and InstrumentProvider are up on port %s", port)
    return port


def _client(network_port: int) -> Client:
    return Client(host=HOST, port=network_port, router_name=ROUTER_NAME, timeout=10_000)


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


def test_proxy_instrument_rejects_positional_operation_arguments(network_port: int) -> None:
    client = _client(network_port)

    proxy_instrument = client.get_device(PROVIDER_NAME, "dummy2")

    with pytest.raises(TypeError, match="positional arguments"):
        proxy_instrument.set_half_input_int(10)
