import datetime
import importlib
import logging
from typing import TYPE_CHECKING
from typing import Any

import zmq

from pqn_hardware.errors import CouldNotConnectToNetworkElementError
from pqn_hardware.errors import InvalidInstrumentsConfigurationError
from pqn_hardware.network.packet import CallOperationPayload
from pqn_hardware.network.packet import DevicesPayload
from pqn_hardware.network.packet import DeviceSummary
from pqn_hardware.network.packet import ErrorCode
from pqn_hardware.network.packet import ErrorPayload
from pqn_hardware.network.packet import GetDeviceStructurePayload
from pqn_hardware.network.packet import GetInfoPayload
from pqn_hardware.network.packet import GetParameterPayload
from pqn_hardware.network.packet import NetworkElementClass
from pqn_hardware.network.packet import Packet
from pqn_hardware.network.packet import PacketIntent
from pqn_hardware.network.packet import Request
from pqn_hardware.network.packet import SetParameterPayload
from pqn_hardware.network.packet import ValuePayload
from pqn_hardware.network.packet import create_registration_packet
from pqn_hardware.network.packet import decode
from pqn_hardware.network.packet import encode

if TYPE_CHECKING:
    from pqn_hardware.instrument import Instrument

logger = logging.getLogger(__name__)


class InstrumentProvider:
    def __init__(
        self,
        name: str,
        host: str = "localhost",
        port: int = 5555,
        router_name: str = "router1",
        beat_period: int = 1000,
        **instruments: dict[str, Any],
    ) -> None:
        """
        InstrumentProvider class for PQN.

        A InstrumentProvider is the class that talks with real hardware and performs experiments. It talks to a
        single `Router` instance through zqm and awaits for instructions from it. Every `beat_interval` milliseconds,
        sends a registration packet to the router.
        This is done so if the router goes offline, the provider can reconnect to the router automatically.

        :param name: Name for the InstrumentProvider.
        :param host: Hostname or IP address of the Router this provider talks to.
        :param port: Port of the name of the Router this provider talks to.
        :param router_name: Name of the Router this provider talks to.
        :param beat_period: Interval in milliseconds to send a beat to the Router.
        :param instruments: Instruments is a Dictionary holding the necessary instructions to initialize any hardware
         the InstrumentProvider talks to. The keys are the names of the instruments, every key has another dictionary as its value
         with all the necessary instructions to initialize the instrument. Inside of the dictionary for the specific
         instrument, a key called 'import' is required holding the import path for that specific instrument.
         Note that the name is not necessary since that is the key of the dictionary.

         e.g.
         ```
         instruments = {
            "rotator_1": {
                "import": "pqn_hardware.pqn.drivers.rotator.Rotator",
                "desc": "Rotator in optical table 1",
                "hw_address": "83860213",
                **extra_kwargs
                }
            }
        """
        self.name = name
        self.host = host
        self.port = port
        self.address = f"tcp://{host}:{port}"
        self.router_name = router_name
        self.beat_period = beat_period
        self._beats_since_reply = 0
        self._last_received_beat: datetime.datetime | None = None
        # How many sent beats with no replies do we start logging warnings for disconnected routers.
        self._disconnected_threshold = 3

        self.context: zmq.Context[zmq.Socket[bytes]] | None = None
        self.socket: zmq.Socket[bytes] | None = None  # Has the instance of the socket talking to the router.

        # Verify that every instrument contains the minimum required keys.
        for ins_name, ins_dict in instruments.items():
            if not isinstance(ins_dict, dict):
                msg = f"{ins_name} is not a dictionary, please provide the necessary instructions for this instrument"
                raise InvalidInstrumentsConfigurationError(msg)

            if "import" not in ins_dict:
                msg = f"{ins_name} is missing its 'import' key, please provide an import path for this instrument"
                raise InvalidInstrumentsConfigurationError(msg)

            if "desc" not in ins_dict:
                msg = f"{ins_name} is missing its 'desc' key, please provide a description for this instrument"
                raise InvalidInstrumentsConfigurationError(msg)

            if "hw_address" not in ins_dict:
                msg = f"{ins_name} is missing its 'hw_address' key, please provide an address for this instrument"
                raise InvalidInstrumentsConfigurationError(msg)

        self.instruments = instruments
        self.instantiated_instruments: dict[str, Instrument] = {}

        self.running = False

    def instantiate_instruments(self) -> None:
        for ins_name, ins_dict in self.instruments.items():
            ins_import = ins_dict.pop("import")
            ins_desc = ins_dict.pop("desc")
            ins_hw_address = ins_dict.pop("hw_address")

            logger.info("Instantiating %s", ins_name)
            try:
                module_name, class_name = ins_import.rsplit(".", 1)
                module = importlib.import_module(module_name)
                class_ = getattr(module, class_name)
            except (ImportError, AttributeError) as e:
                msg = f"Could not import {ins_import}. Please verify the import path for this instrument."
                raise InvalidInstrumentsConfigurationError(msg) from e

            try:
                ins = class_(name=ins_name, desc=ins_desc, hw_address=ins_hw_address, **ins_dict)
                ins.start()
            # FIXME: Figure out what the exception type could be if the instrument cannot be instantiated.
            except Exception as e:
                msg = f"Could not instantiate {ins_import}. Please verify the parameters for this instrument."
                raise InvalidInstrumentsConfigurationError(msg) from e

            self.instantiated_instruments[ins_name] = ins
            logger.info("Successfully instantiated %s", ins_name)

    def start(self) -> None:
        self.instantiate_instruments()

        logger.info("Starting provider %s at %s", self.name, self.address)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt_string(zmq.IDENTITY, self.name)

        # Wait 10 beats for the first check before timing out
        self.socket.setsockopt(zmq.RCVTIMEO, self.beat_period * 10)
        try:
            self._beat()
        except zmq.error.Again as er:
            logger.exception("Could not connect to router at %s", self.address)
            msg = "Could not connect to router."
            raise CouldNotConnectToNetworkElementError(msg) from er

        # Set the beat interval to the normal value.
        self.socket.setsockopt(zmq.RCVTIMEO, self.beat_period)
        self.running = True

        try:
            while self.running:
                try:
                    packet = self._listen()
                except zmq.error.ZMQError:
                    logger.debug("Time interval happened, sending a beat.")
                    self._beat()
                    continue

                match packet.intent:
                    case PacketIntent.PING:
                        response = self._handle_ping(packet)
                        self.socket.send(encode(response))

                    case PacketIntent.REGISTRATION_ACK:
                        self._handle_reg_acknowledge()

                    case PacketIntent.DATA:
                        match packet.request:
                            case Request.GET_DEVICES:
                                response = self._handle_get_devices(packet)
                                self.socket.send(encode(response))

                            case Request.GET_DEVICE_STRUCTURE:
                                response = self._handle_get_device_structure(packet)
                                self.socket.send(encode(response))

                    case PacketIntent.CONTROL:
                        response = self._handle_instrument_control(packet)
                        self.socket.send(encode(response))

        finally:
            self.socket.close()

    def _listen(self) -> Packet:
        # This should never happen, but mypy complains if the check is not done
        if self.socket is None:
            msg = "Socket is None, cannot listen."
            logger.error(msg)
            raise RuntimeError(msg)

        _, data = self.socket.recv_multipart()
        packet = decode(data)
        if packet.destination != self.name:
            # FIXME: This should return an error packet instead of just crashing
            msg = f"Packet intended for {packet.destination} but received by {self.name}. Packet: {packet}"
            raise RuntimeError(msg)

        logger.info("Received packet: %s", packet)

        return packet

    def _beat(self) -> None:
        """
        Execute a single beat to the ROUTER.

         This is the same process as initial registration. If the Router cannot be reached, logs the message and keeps going.

        :return:
        """
        if self._beats_since_reply > self._disconnected_threshold:
            local_time = self._last_received_beat.astimezone() if self._last_received_beat else None
            logger.warning(
                "Router at %s did not reply to heartbeat, router seems offline. Will keep trying to reconnect. \n Beats since reply: %s. Time at last reply %s",
                self.address,
                self._beats_since_reply,
                local_time,
            )

        try:
            # This should never happen, but mypy complains if the check is not done
            if self.socket is None:
                msg = "Socket is None, cannot listen."
                logger.error(msg)
                raise RuntimeError(msg)

            self.socket.connect(self.address)
            reg_packet = create_registration_packet(
                source=self.name, destination=self.router_name, kind=NetworkElementClass.PROVIDER
            )
            self.socket.send(encode(reg_packet))
            logger.info("Sent registration packet to router at %s", self.address)
            self._beats_since_reply += 1

        except zmq.error.Again:
            logger.warning("Error while sending beat to router at %s", self.address)

    def _handle_reg_acknowledge(self) -> None:
        logger.info("InstrumentProvider %s is connected to router at %s", self.name, self.address)
        self.running = True
        self._beats_since_reply = 0
        self._last_received_beat = datetime.datetime.now(tz=datetime.UTC)

    def _handle_ping(self, packet: Packet) -> Packet:
        return Packet(
            intent=PacketIntent.PING,
            request=Request.PONG,
            source=self.name,
            destination=packet.source,
            payload=None,
        )

    def _handle_get_devices(self, packet: Packet) -> Packet:
        devices = {
            name: DeviceSummary(
                type=f"{type(ins).__module__}.{type(ins).__qualname__}",
                desc=ins.desc,
                hw_address=ins.hw_address,
            )
            for name, ins in self.instantiated_instruments.items()
        }
        return Packet(
            intent=PacketIntent.DATA,
            request=Request.GET_DEVICES,
            source=self.name,
            destination=packet.source,
            payload=DevicesPayload(devices=devices).model_dump(),
        )

    def _handle_get_device_structure(self, packet: Packet) -> Packet:
        request_payload = GetDeviceStructurePayload.model_validate(packet.payload)
        ins_name = request_payload.instrument

        if ins_name not in self.instantiated_instruments:
            return self._create_error_packet(packet.source, ErrorCode.UNKNOWN_INSTRUMENT, f"Instrument '{ins_name}' not found.")

        instrument = self.instantiated_instruments[ins_name]

        payload = {
            "name": instrument.name,
            "desc": instrument.desc,
            "hw_address": instrument.hw_address,
            "parameters": sorted(instrument.parameters),
            "operations": sorted(instrument.operations),
        }

        return Packet(
            intent=PacketIntent.DATA,
            request=Request.GET_DEVICE_STRUCTURE,
            source=self.name,
            destination=packet.source,
            payload=payload,
        )

    def _handle_operation_control(self, packet: Packet) -> Packet:
        request_payload = CallOperationPayload.model_validate(packet.payload)

        if request_payload.instrument not in self.instantiated_instruments:
            return self._create_error_packet(
                packet.source, ErrorCode.UNKNOWN_INSTRUMENT, f"Instrument '{request_payload.instrument}' not found."
            )
        instrument = self.instantiated_instruments[request_payload.instrument]

        if request_payload.operation not in instrument.operations:
            return self._create_error_packet(
                packet.source,
                ErrorCode.UNKNOWN_OPERATION,
                f"Operation '{request_payload.operation}' not found in '{instrument.name}'",
            )

        try:
            operation_ret = instrument.operations[request_payload.operation](**request_payload.kwargs)
        # Adding ruff exception due to not know what type of exceptions instruments can raise.
        except Exception as e:  # noqa:BLE001
            msg = f"Error executing operation '{request_payload.operation}' in '{instrument.name}'. Error: {e}"
            return self._create_error_packet(packet.source, ErrorCode.INSTRUMENT_ERROR, msg)

        return self._create_control_packet(
            packet.source, Request.CALL_OPERATION, ValuePayload(value=operation_ret).model_dump()
        )

    def _handle_parameter_read(self, packet: Packet) -> Packet:
        request_payload = GetParameterPayload.model_validate(packet.payload)

        if request_payload.instrument not in self.instantiated_instruments:
            return self._create_error_packet(
                packet.source, ErrorCode.UNKNOWN_INSTRUMENT, f"Instrument '{request_payload.instrument}' not found."
            )
        instrument = self.instantiated_instruments[request_payload.instrument]

        if request_payload.parameter not in instrument.parameters:
            return self._create_error_packet(
                packet.source,
                ErrorCode.UNKNOWN_PARAMETER,
                f"Parameter '{request_payload.parameter}' not found in '{instrument.name}'",
            )

        try:
            parameter_ret = getattr(instrument, request_payload.parameter)
        except AttributeError as e:
            msg = f"Error reading parameter '{request_payload.parameter}' in '{instrument.name}'. Error: {e}"
            return self._create_error_packet(packet.source, ErrorCode.INSTRUMENT_ERROR, msg)

        return self._create_control_packet(
            packet.source, Request.GET_PARAMETER, ValuePayload(value=parameter_ret).model_dump()
        )

    def _handle_parameter_write(self, packet: Packet) -> Packet:
        request_payload = SetParameterPayload.model_validate(packet.payload)

        if request_payload.instrument not in self.instantiated_instruments:
            return self._create_error_packet(
                packet.source, ErrorCode.UNKNOWN_INSTRUMENT, f"Instrument '{request_payload.instrument}' not found."
            )
        instrument = self.instantiated_instruments[request_payload.instrument]

        if request_payload.parameter not in instrument.parameters:
            return self._create_error_packet(
                packet.source,
                ErrorCode.UNKNOWN_PARAMETER,
                f"Parameter '{request_payload.parameter}' not found in '{instrument.name}'",
            )

        try:
            setattr(instrument, request_payload.parameter, request_payload.value)
            # TODO: Double check this exception type, I am not entirely sure this would work.
        except AttributeError as e:
            msg = f"Error setting parameter '{request_payload.parameter}' in '{instrument.name}'. Error: {e}"
            return self._create_error_packet(packet.source, ErrorCode.INSTRUMENT_ERROR, msg)

        return self._create_control_packet(packet.source, Request.SET_PARAMETER, None)

    def _handle_info_control(self, packet: Packet) -> Packet:
        request_payload = GetInfoPayload.model_validate(packet.payload)

        if request_payload.instrument not in self.instantiated_instruments:
            return self._create_error_packet(
                packet.source, ErrorCode.UNKNOWN_INSTRUMENT, f"Instrument '{request_payload.instrument}' not found."
            )
        instrument = self.instantiated_instruments[request_payload.instrument]

        return self._create_control_packet(
            packet.source, Request.GET_INFO, ValuePayload(value=instrument.info).model_dump()
        )

    def _handle_instrument_control(self, packet: Packet) -> Packet:
        match packet.request:
            case Request.CALL_OPERATION:
                return self._handle_operation_control(packet)

            case Request.GET_PARAMETER:
                return self._handle_parameter_read(packet)

            case Request.SET_PARAMETER:
                return self._handle_parameter_write(packet)

            case Request.GET_INFO:
                return self._handle_info_control(packet)

            case _:
                return self._create_error_packet(
                    packet.source, ErrorCode.UNSUPPORTED_REQUEST, f"Unsupported request '{packet.request}'"
                )

    def _create_error_packet(self, destination: str, code: ErrorCode, error_msg: str) -> Packet:
        return Packet(
            intent=PacketIntent.ERROR,
            request=Request.ERROR,
            source=self.name,
            destination=destination,
            payload=ErrorPayload(code=code, message=error_msg, origin=self.name).model_dump(),
        )

    def _create_control_packet(self, destination: str, request: Request, payload: dict[str, Any] | None) -> Packet:
        return Packet(
            intent=PacketIntent.CONTROL,
            request=request,
            source=self.name,
            destination=destination,
            payload=payload,
        )
