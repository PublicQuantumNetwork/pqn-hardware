"""CLI smoke test: start the Router and an InstrumentProvider as subprocesses through the pqn-hw CLI."""

import logging
import shutil
import socket
import subprocess
import time
import tomllib
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from pqn_hardware.network.client import Client

logger = logging.getLogger(__name__)

HOST = "localhost"
FIXED_CONFIG_PORT = 5556


def _free_port() -> int:
    """Grab an ephemeral TCP port that is free right now."""
    with socket.socket() as s:
        s.bind((HOST, 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope="module", autouse=True)
def cli_services(tmp_path_factory: pytest.TempPathFactory) -> Generator[int, Any, None]:
    """Start router and provider services through the CLI, as they are deployed.

    The config is a copy of test_network_config.toml with its fixed port replaced by a free
    ephemeral one, so parallel test runs cannot collide on the port.
    """
    logger.debug("Starting messaging services...")
    # Get the path to the config file relative to this test file, not current working directory
    test_dir = Path(__file__).parent
    config_path = test_dir / "test_network_config.toml"
    uv_path = shutil.which("uv")
    if not uv_path:
        msg = "Could not find 'uv' executable in PATH"
        raise RuntimeError(msg)

    port = _free_port()
    run_config = tmp_path_factory.mktemp("cli_smoke") / "test_network_config.toml"
    run_config.write_text(config_path.read_text().replace(f"port = {FIXED_CONFIG_PORT}", f"port = {port}"))
    # Both the router and the provider section must speak on the ephemeral port.
    with run_config.open("rb") as f:
        config = tomllib.load(f)
    assert config["router"]["port"] == port
    assert config["provider"]["port"] == port

    logger.debug("Using uv path: %s, starting router and provider with config: %s", uv_path, run_config)

    # Start router process
    router_process = subprocess.Popen(  # Noqa: S603 # Subprocess is used for testing purposes, not in production code.
        [uv_path, "run", "pqn-hw", "start-router", "--config", str(run_config)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Give router time to start up
    time.sleep(1)

    # Start provider process
    provider_process = subprocess.Popen(  # Noqa: S603 # Subprocess is used for testing purposes, not in production code.
        [uv_path, "run", "pqn-hw", "start-provider", "--config", str(run_config)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Give provider time to start up and connect to router
    time.sleep(1)

    # Check if provider started successfully
    if provider_process.poll() is not None:
        stdout, stderr = provider_process.communicate()
        msg = f"Provider failed to start. Exit code: {provider_process.returncode}\nStdout: {stdout.decode()}\nStderr: {stderr.decode()}"
        raise RuntimeError(msg)

    logger.debug("Services should be ready for testing")

    try:
        yield port
    finally:
        logger.debug("Cleaning up messaging services...")

        # Terminate provider first (depends on router)
        if provider_process.poll() is None:
            provider_process.terminate()
            try:
                provider_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                provider_process.kill()
                provider_process.wait()

        # Then terminate router
        if router_process.poll() is None:
            router_process.terminate()
            try:
                router_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                router_process.kill()
                router_process.wait()

        logger.debug("All services cleaned up")


def test_cli_smoke(cli_services: int) -> None:
    """A Client can talk to CLI-started Router and InstrumentProvider over the JSON wire protocol."""
    client = Client(host=HOST, port=cli_services, router_name="pqn_hardware-router", timeout=10_000)
    try:
        ping_reply = client.ping("pqn_hardware-provider")
        assert ping_reply is not None
        assert ping_reply.source == "pqn_hardware-provider"

        devices = client.get_available_devices("pqn_hardware-provider")
        assert set(devices) == {"dummy1", "dummy2"}
    finally:
        # Close the socket while the Client is alive: a Client that only dies by garbage
        # collection can block another thread's garbage collection in zmq Context.term().
        client.disconnect()
