import datetime
import importlib
import inspect
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING
from typing import Any

import zmq
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import TypeAdapter
from pydantic import ValidationError
from pydantic import create_model
from pydantic.errors import PydanticUserError

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

# Types JSON represents natively; every other type is published with its Python import path (ADR-0002).
_JSON_NATIVE_TYPES = frozenset({type(None), bool, int, float, str})


def _type_path(annotation: Any) -> str | None:
    """Return the Python import path of a type hint, or None when the type is JSON-native (ADR-0002)."""
    if isinstance(annotation, type) and annotation not in _JSON_NATIVE_TYPES:
        return f"{annotation.__module__}.{annotation.__qualname__}"
    return None


def _annotated(annotation: Any) -> Any:
    """Return an inspect annotation, with the "no annotation" sentinel mapped to Any."""
    return annotation if annotation is not inspect.Parameter.empty else Any


def _arguments_model(operation: Callable[..., Any]) -> type[BaseModel]:
    """Build a model of an Operation's keyword arguments from its type hints.

    This is the same arguments model pydantic's validate_call builds internally for the callable.
    """
    fields: dict[str, Any] = {}
    for name, parameter in inspect.signature(operation, eval_str=True).parameters.items():
        if parameter.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        default = ... if parameter.default is inspect.Parameter.empty else parameter.default
        fields[name] = (_annotated(parameter.annotation), default)
    return create_model("Arguments", __config__=ConfigDict(extra="forbid"), **fields)


def _return_annotation(operation: Callable[..., Any]) -> Any:
    """Return the return type hint of an Operation, or Any when it has none."""
    return _annotated(inspect.signature(operation, eval_str=True).return_annotation)


def _getter_return_annotation(prop: property) -> Any:
    """Return the return type hint of a property's getter, or Any when it has none."""
    getter = prop.fget
    if getter is None:
        return Any
    return _annotated(inspect.signature(getter, eval_str=True).return_annotation)


def _setter_value_annotation(prop: property) -> Any:
    """Return the type hint of the value a property's setter takes, or Any when it has none."""
    setter = prop.fset
    if setter is None:
        return Any
    parameters = list(inspect.signature(setter, eval_str=True).parameters.values())
    # The value is the setter's second parameter, after the instrument itself.
    value_parameter = 1
    if len(parameters) <= value_parameter:
        return Any
    return _annotated(parameters[value_parameter].annotation)


def _parameter_annotations(instrument_class: type, parameter: str) -> tuple[Any, Any | None, bool]:
    """Return the (read, write) type hints and the writability of a Parameter, from its property.

    A Parameter that is not a property carries no type hints; its values pass through unvalidated.
    """
    prop = getattr(instrument_class, parameter, None)
    if not isinstance(prop, property):
        return Any, None, True
    read = _getter_return_annotation(prop) if prop.fget is not None else None
    write = _setter_value_annotation(prop) if prop.fset is not None else None
    if read is None:
        read = write if write is not None else Any
    return read, write, prop.fset is not None


def _info_annotation(instrument_class: type) -> Any:
    """Return the return type hint of an Instrument's info, or Any when it has none."""
    prop = getattr(instrument_class, "info", None)
    if isinstance(prop, property) and prop.fget is not None:
        return _getter_return_annotation(prop)
    return Any


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
        # The Device structure of every Instrument, built at startup (ADR-0002).
        self.device_structures: dict[str, dict[str, Any]] = {}
        # The type machinery used to validate arguments and values and to serialize return values.
        self._operation_types: dict[str, dict[str, tuple[type[BaseModel], TypeAdapter[Any]]]] = {}
        self._parameter_types: dict[str, dict[str, tuple[TypeAdapter[Any], TypeAdapter[Any] | None]]] = {}
        self._info_types: dict[str, TypeAdapter[Any]] = {}

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

    def _build_device_types(self) -> None:
        """Build the Device structure and the type adapters of every Instrument.

        A driver type hint that cannot produce a JSON Schema fails here, at startup, as a configuration
        error instead of failing at call time (ADR-0002).
        """
        for ins_name, instrument in self.instantiated_instruments.items():
            self._build_instrument_types(ins_name, instrument)

    def _build_instrument_types(self, ins_name: str, instrument: "Instrument") -> None:
        """Build the Device structure and the type adapters of one Instrument."""
        operations: dict[str, dict[str, Any]] = {}
        operation_types: dict[str, tuple[type[BaseModel], TypeAdapter[Any]]] = {}
        for op_name, operation in instrument.operations.items():
            returns_annotation = _return_annotation(operation)
            try:
                arguments_model = _arguments_model(operation)
                returns_adapter = TypeAdapter(returns_annotation)
                args_schema = arguments_model.model_json_schema()
                returns_schema = returns_adapter.json_schema()
            except PydanticUserError as e:
                msg = (
                    f"Type hints of operation '{op_name}' in instrument '{ins_name}' cannot produce a JSON Schema: {e}"
                )
                raise InvalidInstrumentsConfigurationError(msg) from e
            operations[op_name] = {
                "args_schema": args_schema,
                "returns_schema": returns_schema,
                "returns_type": _type_path(returns_annotation),
            }
            operation_types[op_name] = (arguments_model, returns_adapter)

        parameters: dict[str, dict[str, Any]] = {}
        parameter_types: dict[str, tuple[TypeAdapter[Any], TypeAdapter[Any] | None]] = {}
        for param_name in instrument.parameters:
            read_annotation, write_annotation, writable = _parameter_annotations(type(instrument), param_name)
            try:
                read_adapter = TypeAdapter(read_annotation)
                write_adapter = TypeAdapter(write_annotation) if write_annotation is not None else None
                schema = read_adapter.json_schema()
            except PydanticUserError as e:
                msg = f"Type hints of parameter '{param_name}' in instrument '{ins_name}' cannot produce a JSON Schema: {e}"
                raise InvalidInstrumentsConfigurationError(msg) from e
            parameters[param_name] = {
                "schema": schema,
                "type": _type_path(read_annotation),
                "writable": writable,
            }
            parameter_types[param_name] = (read_adapter, write_adapter)

        info_annotation = _info_annotation(type(instrument))
        try:
            info_adapter = TypeAdapter(info_annotation)
            info_schema = info_adapter.json_schema()
        except PydanticUserError as e:
            msg = f"The info type hint of instrument '{ins_name}' cannot produce a JSON Schema: {e}"
            raise InvalidInstrumentsConfigurationError(msg) from e

        self.device_structures[ins_name] = {
            "name": instrument.name,
            "desc": instrument.desc,
            "hw_address": instrument.hw_address,
            "type": f"{type(instrument).__module__}.{type(instrument).__qualname__}",
            "info": {"schema": info_schema, "type": _type_path(info_annotation)},
            "operations": operations,
            "parameters": parameters,
        }
        self._operation_types[ins_name] = operation_types
        self._parameter_types[ins_name] = parameter_types
        self._info_types[ins_name] = info_adapter

    def start(self) -> None:
        self.instantiate_instruments()
        self._build_device_types()

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

                if packet is None:
                    # _listen already replied to input it could not handle.
                    continue

                try:
                    response = self._handle_packet(packet)
                except ValidationError as e:
                    response = self._create_error_packet(
                        packet,
                        ErrorCode.MALFORMED_PACKET,
                        f"Payload does not match request '{packet.request}': {e}",
                    )

                if response is not None:
                    self.socket.send(encode(response))

        finally:
            self.socket.close()

    def _handle_packet(self, packet: Packet) -> Packet | None:
        """Dispatch an incoming Packet on its Intent. None means no reply."""
        match packet.intent:
            case PacketIntent.PING:
                return self._handle_ping(packet)

            case PacketIntent.REGISTRATION_ACK:
                self._handle_reg_acknowledge()
                return None

            case PacketIntent.DATA:
                return self._handle_data(packet)

            case PacketIntent.CONTROL:
                return self._handle_instrument_control(packet)

            case _:
                return self._create_error_packet(
                    packet, ErrorCode.UNSUPPORTED_REQUEST, f"Unsupported request '{packet.request}'"
                )

    def _handle_data(self, packet: Packet) -> Packet:
        """Answer a DATA request the Provider handles; any other request verb is unsupported."""
        match packet.request:
            case Request.GET_DEVICES:
                return self._handle_get_devices(packet)

            case Request.GET_DEVICE_STRUCTURE:
                return self._handle_get_device_structure(packet)

            case _:
                return self._create_error_packet(
                    packet, ErrorCode.UNSUPPORTED_REQUEST, f"Unsupported request '{packet.request}'"
                )

    def _listen(self) -> Packet | None:
        """Receive one Packet. Bad input or a misaddressed Packet gets an error reply and returns None."""
        # This should never happen, but mypy complains if the check is not done
        if self.socket is None:
            msg = "Socket is None, cannot listen."
            logger.error(msg)
            raise RuntimeError(msg)

        # The Router sends [delimiter, data] to a DEALER socket.
        dealer_parts = 2
        frames = self.socket.recv_multipart()
        if len(frames) != dealer_parts:
            self.socket.send(
                encode(
                    self._create_error_packet(
                        None, ErrorCode.MALFORMED_PACKET, f"Expected {dealer_parts} frames, got {len(frames)}."
                    )
                )
            )
            return None

        _, data = frames
        try:
            packet = decode(data)
        except ValidationError as e:
            self.socket.send(
                encode(
                    self._create_error_packet(
                        None,
                        ErrorCode.MALFORMED_PACKET,
                        f"Packet is not valid JSON for this protocol (a pre-v2 pickle peer sends non-JSON bytes): {e}",
                    )
                )
            )
            return None

        if packet.destination != self.name:
            self.socket.send(
                encode(
                    self._create_error_packet(
                        packet,
                        ErrorCode.WRONG_DESTINATION,
                        f"Packet addressed to '{packet.destination}' but received by '{self.name}'.",
                    )
                )
            )
            return None

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
            reply_to=packet.id,
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
            reply_to=packet.id,
            payload=DevicesPayload(devices=devices).model_dump(),
        )

    def _handle_get_device_structure(self, packet: Packet) -> Packet:
        request_payload = GetDeviceStructurePayload.model_validate(packet.payload)
        ins_name = request_payload.instrument

        if ins_name not in self.instantiated_instruments:
            return self._create_error_packet(
                packet, ErrorCode.UNKNOWN_INSTRUMENT, f"Instrument '{ins_name}' not found."
            )

        return Packet(
            intent=PacketIntent.DATA,
            request=Request.GET_DEVICE_STRUCTURE,
            source=self.name,
            destination=packet.source,
            reply_to=packet.id,
            payload=self.device_structures[ins_name],
        )

    def _handle_operation_control(self, packet: Packet) -> Packet:
        request_payload = CallOperationPayload.model_validate(packet.payload)

        if request_payload.instrument not in self.instantiated_instruments:
            return self._create_error_packet(
                packet, ErrorCode.UNKNOWN_INSTRUMENT, f"Instrument '{request_payload.instrument}' not found."
            )
        instrument = self.instantiated_instruments[request_payload.instrument]

        if request_payload.operation not in instrument.operations:
            return self._create_error_packet(
                packet,
                ErrorCode.UNKNOWN_OPERATION,
                f"Operation '{request_payload.operation}' not found in '{instrument.name}'",
            )

        arguments_model, returns_adapter = self._operation_types[request_payload.instrument][request_payload.operation]

        try:
            arguments = arguments_model.model_validate(request_payload.kwargs)
            kwargs = {name: getattr(arguments, name) for name in arguments_model.model_fields}
        except ValidationError as e:
            msg = f"Invalid arguments for operation '{request_payload.operation}' in '{instrument.name}'. Error: {e}"
            return self._create_error_packet(packet, ErrorCode.INVALID_ARGUMENTS, msg)

        try:
            operation_ret = instrument.operations[request_payload.operation](**kwargs)
            value = returns_adapter.dump_python(operation_ret, mode="json")
        # Adding ruff exception due to not know what type of exceptions instruments can raise.
        except Exception as e:  # noqa:BLE001
            msg = f"Error executing operation '{request_payload.operation}' in '{instrument.name}'. Error: {e}"
            return self._create_error_packet(
                packet,
                ErrorCode.INSTRUMENT_ERROR,
                msg,
                details={"exception_type": type(e).__name__, "exception": str(e)},
            )

        return self._create_control_packet(packet, Request.CALL_OPERATION, ValuePayload(value=value).model_dump())

    def _handle_parameter_read(self, packet: Packet) -> Packet:
        request_payload = GetParameterPayload.model_validate(packet.payload)

        if request_payload.instrument not in self.instantiated_instruments:
            return self._create_error_packet(
                packet, ErrorCode.UNKNOWN_INSTRUMENT, f"Instrument '{request_payload.instrument}' not found."
            )
        instrument = self.instantiated_instruments[request_payload.instrument]

        if request_payload.parameter not in instrument.parameters:
            return self._create_error_packet(
                packet,
                ErrorCode.UNKNOWN_PARAMETER,
                f"Parameter '{request_payload.parameter}' not found in '{instrument.name}'",
            )

        read_adapter, _ = self._parameter_types[request_payload.instrument][request_payload.parameter]

        try:
            parameter_ret = getattr(instrument, request_payload.parameter)
            value = read_adapter.dump_python(parameter_ret, mode="json")
        # Adding ruff exception due to not know what type of exceptions instruments can raise.
        except Exception as e:  # noqa:BLE001
            msg = f"Error reading parameter '{request_payload.parameter}' in '{instrument.name}'. Error: {e}"
            return self._create_error_packet(
                packet,
                ErrorCode.INSTRUMENT_ERROR,
                msg,
                details={"exception_type": type(e).__name__, "exception": str(e)},
            )

        return self._create_control_packet(packet, Request.GET_PARAMETER, ValuePayload(value=value).model_dump())

    def _handle_parameter_write(self, packet: Packet) -> Packet:
        request_payload = SetParameterPayload.model_validate(packet.payload)

        if request_payload.instrument not in self.instantiated_instruments:
            return self._create_error_packet(
                packet, ErrorCode.UNKNOWN_INSTRUMENT, f"Instrument '{request_payload.instrument}' not found."
            )
        instrument = self.instantiated_instruments[request_payload.instrument]

        if request_payload.parameter not in instrument.parameters:
            return self._create_error_packet(
                packet,
                ErrorCode.UNKNOWN_PARAMETER,
                f"Parameter '{request_payload.parameter}' not found in '{instrument.name}'",
            )

        _, write_adapter = self._parameter_types[request_payload.instrument][request_payload.parameter]

        try:
            value = (
                write_adapter.validate_python(request_payload.value)
                if write_adapter is not None
                else request_payload.value
            )
            setattr(instrument, request_payload.parameter, value)
        except ValidationError as e:
            msg = f"Invalid value for parameter '{request_payload.parameter}' in '{instrument.name}'. Error: {e}"
            return self._create_error_packet(packet, ErrorCode.INVALID_ARGUMENTS, msg)
        # Adding ruff exception due to not know what type of exceptions instruments can raise.
        except Exception as e:  # noqa:BLE001
            msg = f"Error setting parameter '{request_payload.parameter}' in '{instrument.name}'. Error: {e}"
            return self._create_error_packet(
                packet,
                ErrorCode.INSTRUMENT_ERROR,
                msg,
                details={"exception_type": type(e).__name__, "exception": str(e)},
            )

        return self._create_control_packet(packet, Request.SET_PARAMETER, None)

    def _handle_info_control(self, packet: Packet) -> Packet:
        request_payload = GetInfoPayload.model_validate(packet.payload)

        if request_payload.instrument not in self.instantiated_instruments:
            return self._create_error_packet(
                packet, ErrorCode.UNKNOWN_INSTRUMENT, f"Instrument '{request_payload.instrument}' not found."
            )
        instrument = self.instantiated_instruments[request_payload.instrument]

        info_adapter = self._info_types[request_payload.instrument]

        try:
            value = info_adapter.dump_python(instrument.info, mode="json")
        # Adding ruff exception due to not know what type of exceptions instruments can raise.
        except Exception as e:  # noqa:BLE001
            msg = f"Error reading the info of '{instrument.name}'. Error: {e}"
            return self._create_error_packet(
                packet,
                ErrorCode.INSTRUMENT_ERROR,
                msg,
                details={"exception_type": type(e).__name__, "exception": str(e)},
            )

        return self._create_control_packet(packet, Request.GET_INFO, ValuePayload(value=value).model_dump())

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
                    packet, ErrorCode.UNSUPPORTED_REQUEST, f"Unsupported request '{packet.request}'"
                )

    def _create_error_packet(
        self, packet: Packet | None, code: ErrorCode, error_msg: str, details: dict[str, Any] | None = None
    ) -> Packet:
        """Create an error reply for a request Packet, echoing its id as reply_to.

        Without a request Packet (input that could not be decoded) the reply goes to the Router.
        """
        return Packet(
            intent=PacketIntent.ERROR,
            request=Request.ERROR,
            source=self.name,
            destination=packet.source if packet is not None else self.router_name,
            reply_to=packet.id if packet is not None else None,
            payload=ErrorPayload(code=code, message=error_msg, origin=self.name, details=details or {}).model_dump(),
        )

    def _create_control_packet(self, packet: Packet, request: Request, payload: dict[str, Any] | None) -> Packet:
        """Create a CONTROL reply for a request Packet, echoing its id as reply_to."""
        return Packet(
            intent=PacketIntent.CONTROL,
            request=request,
            source=self.name,
            destination=packet.source,
            reply_to=packet.id,
            payload=payload,
        )
