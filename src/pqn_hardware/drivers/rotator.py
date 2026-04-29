# University of Illinois Urbana-Champaign
# Public Quantum Network
#
# NCSA/Illinois Computes

import logging
import time
from dataclasses import dataclass
from dataclasses import field
from typing import ClassVar

import serial
from thorlabs_apt_device import KDC101
from thorlabs_apt_device import TDC001

from pqn_hardware.errors import DeviceNotStartedError
from pqn_hardware.instrument import RotatorInfo
from pqn_hardware.instrument import RotatorInstrument

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class APTRotator(RotatorInstrument):
    _degrees: float = field(default=0.0, init=False)
    _device: TDC001 | KDC101 = field(init=False, repr=False)
    _encoder_units_per_degree: float = field(default=86384 / 45, init=False, repr=False)

    def start(self) -> None:
        # Additional setup for APT Rotator
        try:
            self._device = TDC001(serial_number=self.hw_address)
        except RuntimeError:
            self._device = KDC101(self.hw_address)

        offset_eu = round(self.offset_degrees * self._encoder_units_per_degree)

        # NOTE: Velocity units seem to not match position units
        # (Device does not actually move at 1000 deg/s...)
        # 500 is noticeably slower, but more than 1000 doesn't seem faster
        vel = round(1000 * self._encoder_units_per_degree)

        self._device.set_home_params(velocity=vel, offset_distance=offset_eu)
        self._device.set_velocity_params(vel, vel)
        time.sleep(0.5)
        self._wait_for_stop()

    def close(self) -> None:
        if self._device is not None:
            logger.info("Closing APT Rotator")
            self._device.close()

    @property
    def info(self) -> RotatorInfo:
        return RotatorInfo(
            name=self.name,
            desc=self.desc,
            hw_address=self.hw_address,
            hw_status=self._device.status,
            degrees=self.degrees,
            offset_degrees=self.offset_degrees,
        )

    def _wait_for_stop(self) -> None:
        if self._device is None:
            msg = "Start the device before setting parameters"
            raise DeviceNotStartedError(msg)

        try:
            time.sleep(0.5)
            while (
                self._device.status["moving_forward"]
                or self._device.status["moving_reverse"]
                or self._device.status["jogging_forward"]
                or self._device.status["jogging_reverse"]
            ):
                time.sleep(0.1)
        except KeyboardInterrupt:
            self._device.stop(immediate=True)

    @property
    def degrees(self) -> float:
        return self._degrees

    @degrees.setter
    def degrees(self, degrees: float) -> None:
        self._set_degrees_unsafe(degrees)
        self._wait_for_stop()

    def _set_degrees_unsafe(self, degrees: float) -> None:
        self._degrees = degrees
        self._device.move_absolute(int(degrees * self._encoder_units_per_degree))


@dataclass(slots=True)
class SerialRotator(RotatorInstrument):
    _degrees: float = 0.0  # The hardware doesn't support position tracking
    _conn: serial.Serial = field(init=False, repr=False)

    def start(self) -> None:
        self._conn = serial.Serial(self.hw_address, baudrate=115200, timeout=1)
        self._conn.write(b"open_channel")
        self._conn.read(100)
        self._conn.write(b"motor_ready")
        self._conn.read(100)

        self.degrees = self.offset_degrees

    def close(self) -> None:
        self.degrees = 0
        self._conn.close()

    @property
    def info(self) -> RotatorInfo:
        return RotatorInfo(
            name=self.name,
            desc=self.desc,
            hw_address=self.hw_address,
            # hw_status=,
            degrees=self.degrees,
            offset_degrees=self.offset_degrees,
        )

    @property
    def degrees(self) -> float:
        return self._degrees

    @degrees.setter
    def degrees(self, degrees: float) -> None:
        self._conn.write(f"SRA {degrees}".encode())
        self._degrees = degrees
        _ = self._conn.readline().decode()


@dataclass(slots=True)
class EllxRotator(RotatorInstrument):
    """Driver for Thorlabs ELLx-series rotation mounts via the ASCII ELLx serial protocol."""

    _GS_MIN_LEN: ClassVar[int] = 5
    _STATUS_BUSY: ClassVar[int] = 0x09
    _PO_MIN_LEN: ClassVar[int] = 11
    _INT32_MAX: ClassVar[int] = 0x7FFFFFFF
    _UINT32_WRAP: ClassVar[int] = 0x100000000

    bus_address: str = "0"
    _degrees: float = field(default=0.0, init=False)
    _conn: serial.Serial = field(init=False, repr=False)
    _ppr: int = field(default=512000, init=False, repr=False)

    def start(self) -> None:
        self._conn = serial.Serial(
            port=self.hw_address,
            baudrate=9600,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            timeout=1.0,
        )
        self._conn.reset_input_buffer()
        self._conn.reset_output_buffer()
        time.sleep(0.05)

        info_reply = self._query(f"{self.bus_address}in", settle_s=0.1)
        if info_reply:
            self._ppr = self._parse_ppr(info_reply)
            logger.info("EllxRotator PPR: %d", self._ppr)

        # Home the device to establish absolute position reference
        self._send(f"{self.bus_address}ho1")
        home_reply = self._read_until_reply(timeout=15.0)
        logger.info("EllxRotator home reply: %s", home_reply)

        self.degrees = self.offset_degrees

    def close(self) -> None:
        if self._conn is not None and self._conn.is_open:
            logger.info("Closing ELLx Rotator")
            self._conn.close()

    @property
    def info(self) -> RotatorInfo:
        return RotatorInfo(
            name=self.name,
            desc=self.desc,
            hw_address=self.hw_address,
            degrees=self.degrees,
            offset_degrees=self.offset_degrees,
        )

    def _send(self, cmd: str) -> None:
        self._conn.reset_input_buffer()
        self._conn.write(cmd.encode("ascii"))
        self._conn.flush()

    def _read_line(self) -> str:
        raw: bytes = self._conn.readline()
        if not raw:
            return ""
        return raw.decode("ascii", errors="replace").strip()

    def _query(self, cmd: str, settle_s: float = 0.05) -> str:
        self._send(cmd)
        time.sleep(settle_s)
        return self._read_line()

    def _parse_ppr(self, info_reply: str) -> int:
        # Reply format: {addr}IN{type 2}{serial 8}{year 2}{firmware 4}{travel 4}{ppr 8}
        # PPR starts at index 25 (1+2+2+10+2+4+4); serial number is 10 hex chars
        try:
            return int(info_reply[25:33], 16)
        except (ValueError, IndexError):
            logger.warning("Could not parse PPR from: %s; using default %d", info_reply, self._ppr)
            return self._ppr

    def _degrees_to_encoder(self, degrees: float) -> int:
        return round(degrees * self._ppr / 360.0)

    def _encoder_to_degrees(self, encoder: int) -> float:
        return encoder * 360.0 / self._ppr

    def _read_until_reply(self, timeout: float = 5.0) -> str:
        deadline = time.time() + timeout
        while time.time() < deadline:
            raw: bytes = self._conn.readline()
            if raw:
                return raw.decode("ascii", errors="replace").strip()
        return ""

    def _wait_for_stop(self) -> None:
        try:
            while True:
                reply = self._query(f"{self.bus_address}gs", settle_s=0.02)
                if not reply or len(reply) < self._GS_MIN_LEN or reply[1:3].upper() != "GS":
                    break
                try:
                    code = int(reply[3:5], 16)
                except ValueError:
                    break
                if code != self._STATUS_BUSY:
                    break
                time.sleep(0.05)
        except KeyboardInterrupt:
            self._query(f"{self.bus_address}ms")

    @property
    def degrees(self) -> float:
        reply = self._query(f"{self.bus_address}gp", settle_s=0.05)
        if reply and len(reply) >= self._PO_MIN_LEN and reply[1:3].upper() == "PO":
            try:
                raw = int(reply[3:11], 16)
                if raw > self._INT32_MAX:
                    raw -= self._UINT32_WRAP
                self._degrees = self._encoder_to_degrees(raw)
            except ValueError:
                pass
        return self._degrees

    @degrees.setter
    def degrees(self, degrees: float) -> None:
        encoder = self._degrees_to_encoder(degrees) & 0xFFFFFFFF
        self._send(f"{self.bus_address}ma{encoder:08X}")
        self._degrees = degrees
        self._wait_for_stop()
