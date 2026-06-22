from __future__ import annotations

import contextlib
import logging
import socket
import struct
import time
from dataclasses import dataclass
from dataclasses import field

# The Swabian SDK is an optional dependency: only SwabianTimeTagger needs it. Guarding the import
# keeps the UDP-based RabbitTimeTagger (and module import) working on machines without the SDK.
try:
    from TimeTagger import ChannelEdge
    from TimeTagger import Correlation
    from TimeTagger import Counter
    from TimeTagger import TimeTagger
    from TimeTagger import createTimeTaggerNetwork
    from TimeTagger import freeTimeTagger
except ImportError:  # pragma: no cover - exercised only where the SDK is absent
    ChannelEdge = Correlation = Counter = TimeTagger = None
    createTimeTaggerNetwork = freeTimeTagger = None

from pqn_hardware.instrument import TimeTaggerInfo
from pqn_hardware.instrument import TimeTaggerInstrument

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SwabianTimeTagger(TimeTaggerInstrument):
    """Instantiate a SwabianTimeTagger Instrument.

    `hw_address` should be of the form "ip:port"
        e.g.: hw_address = "127.0.0.1:41101".
    """

    _tagger: TimeTagger = field(init=False, repr=False)

    def start(self) -> None:
        """Initialize the connection to the Swabian time tagger hardware and configures channels for potential coincidence counting."""
        logger.info("Creating Swabian Time Tagger instance.")
        self._tagger = createTimeTaggerNetwork(self.hw_address)
        if not self._tagger:
            msg = "Failed to create time tagger. Verify hardware connection."
            logger.error(msg)
            raise RuntimeError(msg)

        hw_channels = self._tagger.getChannelList(ChannelEdge.Rising)
        self.active_channels = [hw_channels[ch - 1] for ch in self.active_channels]

        for ch in self.active_channels:
            self._tagger.setInputDelay(ch, 0)

        logger.info("Swabian Time Tagger device is now READY.")

    def close(self) -> None:
        """Safely closes the connection to the Swabian time tagger hardware."""
        if self._tagger is not None:
            logger.info("Closing Swabian Time Tagger connection.")
            freeTimeTagger(self._tagger)
            self._tagger = None

        logger.info("Swabian Time Tagger device is now OFF.")

    @property
    def info(self) -> TimeTaggerInfo:
        return TimeTaggerInfo(
            name=self.name,
            desc=self.desc,
            hw_address=self.hw_address,
            # hw_status=,
            active_channels=self.active_channels,
            test_signal_enabled=self.test_signal_enabled,
            test_signal_divider=self.test_signal_divider,
        )

    def set_input_delay(self, channel: int, delay_ps: int) -> None:
        self._tagger.setInputDelay(channel, delay_ps)

    def set_test_signal(self, channels: list[int], *, enable: bool = True, divider: int = 1) -> None:
        self._tagger.setTestSignal(channels, enable)
        if enable:
            self._tagger.setTestSignalDivider(divider)

    def count_singles(self, channels: list[int], integration_time_s: float = 1.0) -> list[int]:
        # TODO: use these as kwargs
        _duration_ps = int(integration_time_s * 1e12)
        counter = Counter(self._tagger, channels, _duration_ps, 1)
        counter.startFor(_duration_ps)
        counter.waitUntilFinished()
        return [item[0] for item in counter.getData()]

    def measure_correlation(
        self,
        start_ch: int,
        stop_ch: int,
        integration_time_s: float = 1.0,
        binwidth_ps: int = 1,
        n_bins: int = int(1e5),
    ) -> int:
        # TODO: use these as kwargs
        count_time_ps = int(integration_time_s * 1e12)
        corr = Correlation(self._tagger, start_ch, stop_ch, binwidth_ps, n_bins=n_bins)
        corr.startFor(count_time_ps)
        corr.waitUntilFinished()
        return int(max(corr.getData()))


# --- Rabbit Coincidence Counter (UDP) ----------------------------------------------------------
# Protocol reverse-engineered from the firmware source by A. Stummer (University of Toronto):
#   https://www.physics.utoronto.ca/~astummer/Archives/2008%20Coincidence%20Counter/Rabbit/
#
# The unit listens for single-byte ASCII commands on a UDP port (default 37829). Commands:
#   H              heartbeat -> echoes b"H"
#   R              run / start the counters
#   P              pause the counters
#   X              reset & clear all counters
#   T              runtime since last R -> b"T" + uint32 little-endian milliseconds
#   F              test mode -> returns fixed counter data (same framing as C)
#   C + digit      read counters; digit is ASCII "1".."8" = number of 1026-byte packets to send back.
#   D + 11 bytes   set the delay-line timings.
#
# Counter readout: the device holds 2048 x uint32 little-endian counters. The C command returns
# one or more 1026-byte UDP packets, each: byte 0 = b'C', byte 1 = packet index (0-based),
# bytes 2..1025 = 256 counters (4 bytes each, little-endian). Eight packets cover all 2048
# counters. Each counter is indexed by the bitmask of APD channels that fired coincidentally
# (index 0 = total). The device itself performs the coincidence binning, so reading a channel's
# singles or a pair's coincidences is just a lookup into the slot the hardware already filled in.

RABBIT_DEFAULT_PORT = 37829
RABBIT_COUNTER_COUNT = 2048
RABBIT_COUNTERS_PER_PACKET = 256
RABBIT_PACKET_SIZE = 1026  # 1 cmd byte + 1 packet-index byte + 256 * 4 counter bytes
RABBIT_MAX_PACKETS = RABBIT_COUNTER_COUNT // RABBIT_COUNTERS_PER_PACKET  # 8
RABBIT_RECV_TIMEOUT_S = 2.0


@dataclass(slots=True)
class RabbitTimeTagger(TimeTaggerInstrument):
    """A Rabbit-based coincidence counter spoken to over UDP.

    `hw_address` should be of the form "ip:port", e.g. "192.168.1.134:37829". If the port is
    omitted the firmware default (37829) is used.

    Channels are 1-indexed APD inputs. The hardware bins coincidences into 2048 counters indexed
    by the bitmask of channels that fired together, so the singles count for channel ``ch`` lives
    at counter ``1 << (ch - 1)`` and the coincidence count for channels ``a`` and ``b`` lives at
    counter ``(1 << (a - 1)) | (1 << (b - 1))``.

    The firmware latches the host's (ip, port) the first time it hears from it and keeps sending
    replies there until the unit is power-cycled -- it never clears the latch on loss-of-contact.
    To stay reconnectable across runs we therefore bind to a *fixed* local UDP port rather than a
    random ephemeral one: ``local_port`` defaults to the device port (the symmetric-port
    convention these units use). Set ``local_port=0`` to fall back to an ephemeral port.
    """

    local_port: int | None = None

    _sock: socket.socket | None = field(default=None, init=False, repr=False)
    _addr: tuple[str, int] = field(default=("", RABBIT_DEFAULT_PORT), init=False, repr=False)

    def start(self) -> None:
        """Open the UDP socket and confirm the unit answers a heartbeat."""
        host, _, port = self.hw_address.partition(":")
        self._addr = (host, int(port) if port else RABBIT_DEFAULT_PORT)

        # Bind to a stable local port so the device's latched reply-address stays valid across
        # reconnects (see class docstring). Default to the device port; local_port=0 -> ephemeral.
        bind_port = self._addr[1] if self.local_port is None else self.local_port

        logger.info(
            "Opening UDP socket to Rabbit coincidence counter at %s:%s (local port %s)",
            *self._addr,
            bind_port,
        )
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        with contextlib.suppress(AttributeError, OSError):
            # Not available on every platform; harmless where it is.
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self._sock.bind(("", bind_port))
        self._sock.settimeout(RABBIT_RECV_TIMEOUT_S)

        reply = self._command(b"H", expect_reply=True)
        if reply[:1] != b"H":
            msg = f"Rabbit coincidence counter did not answer heartbeat (got {reply!r})."
            logger.error(msg)
            raise RuntimeError(msg)

        logger.info("Rabbit coincidence counter is now READY.")

    def close(self) -> None:
        """Close the UDP socket."""
        if self._sock is not None:
            logger.info("Closing Rabbit coincidence counter connection.")
            self._sock.close()
            self._sock = None

        logger.info("Rabbit coincidence counter is now OFF.")

    @property
    def info(self) -> TimeTaggerInfo:
        return TimeTaggerInfo(
            name=self.name,
            desc=self.desc,
            hw_address=self.hw_address,
            active_channels=self.active_channels,
            test_signal_enabled=self.test_signal_enabled,
            test_signal_divider=self.test_signal_divider,
        )

    # --- raw protocol commands -----------------------------------------------------------------

    def _command(self, payload: bytes, *, expect_reply: bool = False) -> bytes:
        """Send a UDP command and optionally wait for a single reply datagram."""
        if self._sock is None:
            msg = "Rabbit coincidence counter is not connected; call start() first."
            raise RuntimeError(msg)

        self._sock.sendto(payload, self._addr)
        if not expect_reply:
            return b""
        data, _ = self._sock.recvfrom(RABBIT_PACKET_SIZE)
        return data

    def heartbeat(self) -> bool:
        """Send the heartbeat (H) and report whether the unit echoed it back."""
        return self._command(b"H", expect_reply=True)[:1] == b"H"

    def run(self) -> None:
        """Start the counters (R)."""
        self._command(b"R")

    def pause(self) -> None:
        """Pause the counters (P)."""
        self._command(b"P")

    def reset(self) -> None:
        """Stop and clear all counters (X)."""
        self._command(b"X")

    def runtime_ms(self) -> int:
        """Return milliseconds elapsed since the last run() (T)."""
        reply = self._command(b"T", expect_reply=True)
        if reply[:1] != b"T" or len(reply) < 5:
            msg = f"Unexpected runtime reply from Rabbit coincidence counter: {reply!r}"
            raise RuntimeError(msg)
        return struct.unpack_from("<I", reply, 1)[0]

    def set_delay_lines(self, timings: bytes) -> None:
        """Configure the delay-line timings (D + 11 bytes)."""
        if len(timings) != 11:
            msg = f"Delay-line command requires exactly 11 bytes, got {len(timings)}."
            raise ValueError(msg)
        self._command(b"D" + timings)

    def read_counters(self, *, test: bool = False) -> list[int]:
        """Read all 2048 counters from the device.

        Sends the C command (or F for the fixed test pattern) requesting all 8 packets and
        reassembles the counter array using each packet's embedded packet index.
        """
        cmd = b"F" if test else b"C" + str(RABBIT_MAX_PACKETS).encode("ascii")
        if self._sock is None:
            msg = "Rabbit coincidence counter is not connected; call start() first."
            raise RuntimeError(msg)
        self._sock.sendto(cmd, self._addr)

        counters = [0] * RABBIT_COUNTER_COUNT
        for _ in range(RABBIT_MAX_PACKETS):
            packet, _ = self._sock.recvfrom(RABBIT_PACKET_SIZE)
            if len(packet) != RABBIT_PACKET_SIZE:
                msg = f"Expected {RABBIT_PACKET_SIZE}-byte counter packet, got {len(packet)} bytes."
                raise RuntimeError(msg)
            packet_index = packet[1]
            base = packet_index * RABBIT_COUNTERS_PER_PACKET
            values = struct.unpack_from(f"<{RABBIT_COUNTERS_PER_PACKET}I", packet, 2)
            counters[base : base + RABBIT_COUNTERS_PER_PACKET] = values

        return counters

    # --- TimeTaggerInstrument interface --------------------------------------------------------

    def count_singles(self, channels: list[int], integration_time_s: float = 1.0) -> list[int]:
        """Count over ``integration_time_s`` and return each channel's singles counter."""
        self.reset()
        self.run()
        time.sleep(integration_time_s)
        self.pause()
        counters = self.read_counters()
        return [counters[1 << (ch - 1)] for ch in channels]

    def measure_correlation(
        self,
        start_ch: int,
        stop_ch: int,
        integration_time_s: float = 1.0,
        binwidth_ps: int = 1,  # noqa: ARG002 - unused; the Rabbit has no time-resolved histogram
        n_bins: int = int(1e5),  # noqa: ARG002 - unused; kept for interface compatibility
    ) -> int:
        """Count over ``integration_time_s`` and return the coincidence counter for the pair.

        The Rabbit performs coincidence detection in hardware within its own window, so this
        returns the device's coincidence count directly rather than a correlation histogram.
        """
        self.reset()
        self.run()
        time.sleep(integration_time_s)
        self.pause()
        counters = self.read_counters()
        mask = (1 << (start_ch - 1)) | (1 << (stop_ch - 1))
        return counters[mask]
