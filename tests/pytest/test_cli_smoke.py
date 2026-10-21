"""CLI smoke test: start the Router and an InstrumentProvider as subprocesses through the pqn-hw CLI."""

import logging
import shutil
import subprocess
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from pqn_hardware.network.client import Client

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def cli_services() -> Generator[None, Any, None]:
    """Start router and provider services through the CLI, as they are deployed."""
    logger.debug("Starting messaging services...")
    # Get the path to the config file relative to this test file, not current working directory
    test_dir = Path(__file__).parent
    config_path = test_dir / "test_network_config.toml"
    uv_path = shutil.which("uv")
    if not uv_path:
        msg = "Could not find 'uv' executable in PATH"
        raise RuntimeError(msg)

    logger.debug("Using uv path: %s, starting router and provider with config: %s", uv_path, config_path)

    # Start router process
    router_process = subprocess.Popen(  # Noqa: S603 # Subprocess is used for testing purposes, not in production code.
        [uv_path, "run", "pqn-hw", "start-router", "--config", str(config_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Give router time to start up
    time.sleep(1)

    # Start provider process
    provider_process = subprocess.Popen(  # Noqa: S603 # Subprocess is used for testing purposes, not in production code.
        [uv_path, "run", "pqn-hw", "start-provider", "--config", str(config_path)],
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
        yield
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


def test_cli_smoke() -> None:
    """A Client can talk to CLI-started Router and InstrumentProvider over the JSON wire protocol."""
    client = Client(host="localhost", port=5556, router_name="pqn_hardware-router", timeout=10_000)

    ping_reply = client.ping("pqn_hardware-provider")
    assert ping_reply is not None
    assert ping_reply.source == "pqn_hardware-provider"

    devices = client.get_available_devices("pqn_hardware-provider")
    assert set(devices) == {"dummy1", "dummy2"}
