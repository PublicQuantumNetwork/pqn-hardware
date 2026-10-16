import importlib
import logging
import secrets
import string
from dataclasses import dataclass
from dataclasses import field
from dataclasses import is_dataclass
from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Self

import zmq
from pydantic import TypeAdapter
from pydantic import ValidationError

from pqn_hardware.errors import PacketError
from pqn_hardware.instrument import Instrument
from pqn_hardware.network.packet import CallOperationPayload
from pqn_hardware.network.packet import DevicesPayload
from pqn_hardware.network.packet import DeviceSummary
from pqn_hardware.network.packet import ErrorPayload
from pqn_hardware.network.packet import GetDeviceStructurePayload
from pqn_hardware.network.packet import GetInfoPayload
from pqn_hardware.network.packet import GetParameterPayload
from pqn_hardware.network.packet import NetworkElementClass
from pqn_hardware.network.packet import Packet
from pqn_hardware.network.packet import PacketIntent
from pqn_hardware.network.packet import Request
from pqn_hardware.network.packet import SetParameterPayload
from pqn_hardware.network.packet import create_registration_packet
from pqn_hardware.network.packet import decode
from pqn_hardware.network.packet import encode

logger = logging.getLogger(__name__)


def _dump_argument(value: Any) -> Any:
    """Dump a dataclass argument to JSON data; every other value passes through as it is."""
    if is_dataclass(value) and not isinstance(value, type):
        return TypeAdapter(type(value)).dump_python(value, mode="json")
    return value


def _rebuild_value(value: Any, type_path: str | None) -> Any:
    """Rebuild a typed value from its Python import path in the Device structure (ADR-0002).

    A null type path means the value is JSON-native. When the path cannot be imported or the
    value does not validate, the plain JSON data is returned and a warning is logged.
    """
    if type_path is None:
        return value
    try:
        module_name, _, type_name = type_path.rpartition(".")
        type_ = getattr(importlib.import_module(module_name), type_name)
        return TypeAdapter(type_).validate_python(value)
    except (ImportError, AttributeError, ValidationError) as e:
        logger.warning(
            "Could not rebuild the value as %s; returning the plain JSON data instead. Error: %s", type_path, e
        )
        return value


class ClientBase:
    def __init__(
        self,
        name: str = "",
        host: str = "127.0.0.1",
        port: int = 5555,
        router_name: str = "router1",
        timeout: int = 30000,
    ) -> None:
        if name == "":
            name = "".join(
                secrets.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(6)
            )
        self.name = name

        self.host = host
        self.port = port
        self.address = f"tcp://{host}:{port}"
        self.router_name = router_name

        self.timeout = timeout

        self.connected = False
        self.context: zmq.Context[zmq.Socket[bytes]] | None = None
        self.socket: zmq.Socket[bytes] | None = None

        self.connect()

    async def __aenter__(self) -> Self:
        if not self.connected:
            self.connect()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        self.disconnect()

    def connect(self) -> None:
        logger.info("Starting client '%s' Connecting to %s", self.name, self.address)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        # A timeout must not break the socket: with REQ_RELAXED a request can be sent even when a
        # reply is still outstanding, and with REQ_CORRELATE the socket drops replies that do not
        # carry the request-id frame of the latest request.
        self.socket.setsockopt(zmq.REQ_RELAXED, 1)
        self.socket.setsockopt(zmq.REQ_CORRELATE, 1)
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout)
        self.socket.setsockopt_string(zmq.IDENTITY, self.name)
        self.socket.connect(self.address)
        self.connected = True

        reg_packet = create_registration_packet(
            source=self.name, destination=self.router_name, kind=NetworkElementClass.CLIENT
        )
        ret = self.ask(reg_packet)
        if ret is None:
            msg = "Something went wrong with the registration."
            raise RuntimeError(msg)
        if ret.intent != PacketIntent.REGISTRATION_ACK:
            msg = "Registration failed."
            raise RuntimeError(msg)
        logger.info("Acknowledged by server. Client is connected.")

    def disconnect(self) -> None:
        logger.info("Disconnecting from %s", self.address)
        if self.socket is None:
            logger.warning("Socket is already None.")
            self.connected = False
            return

        self.socket.close()
        self.connected = False
        logger.info("Disconnected from %s", self.address)

    def ask(self, packet: Packet) -> Packet:
        if not self.connected:
            msg = "No connection yet."
            logger.error(msg)
            raise RuntimeError(msg)

        if self.socket is None:
            msg = "Socket is None. Cannot ask."
            logger.error(msg)
            raise RuntimeError(msg)

        # try so that if timeout happens, the client remains usable

        self.socket.send(encode(packet))
        try:
            ret = decode(self.socket.recv())
            while ret.reply_to != packet.id:
                # A reply that does not answer this request is dropped, so it is never returned
                # as the answer to the next request.
                logger.warning(
                    "Dropping a reply: its reply_to %s does not match the request id %s.", ret.reply_to, packet.id
                )
                ret = decode(self.socket.recv())
        except zmq.error.Again as e:
            logger.exception("Timeout occurred.")
            raise TimeoutError from e

        logger.debug("Response received.")
        logger.debug("Response: %s", str(ret))
        if ret.intent == PacketIntent.ERROR:
            error_payload = ErrorPayload.model_validate(ret.payload)
            raise PacketError(error_payload.message, payload=error_payload)

        return ret

    def create_control_packet(
        self, destination: str, request: Request, payload: dict[str, Any] | None = None
    ) -> Packet:
        return Packet(
            intent=PacketIntent.CONTROL,
            request=request,
            source=self.name,
            destination=destination,
            payload=payload,
        )

    def create_data_packet(self, destination: str, request: Request, payload: dict[str, Any] | None = None) -> Packet:
        return Packet(
            intent=PacketIntent.DATA,
            request=request,
            source=self.name,
            destination=destination,
            payload=payload,
        )


class InstrumentClientInit(NamedTuple):
    name: str
    host: str
    port: int
    router_name: str
    timeout: int
    instrument_name: str
    provider_name: str
    device_structure: dict[str, Any]


class InstrumentClient(ClientBase):
    def __init__(self, init_args: InstrumentClientInit) -> None:
        super().__init__(
            init_args.name, init_args.host, init_args.port, init_args.router_name, timeout=init_args.timeout
        )

        self.instrument_name = init_args.instrument_name
        self.provider_name = init_args.provider_name
        self.device_structure = init_args.device_structure

    def trigger_operation(self, operation: str, *args: Any, **kwargs: Any) -> Any:
        bound = self._bind_arguments(operation, args, kwargs)
        kwargs = {name: _dump_argument(value) for name, value in bound.items()}

        packet = self.create_control_packet(
            self.provider_name,
            Request.CALL_OPERATION,
            CallOperationPayload(instrument=self.instrument_name, operation=operation, kwargs=kwargs).model_dump(),
        )
        response = self.ask(packet)

        return _rebuild_value(self._response_value(response), self._returns_type(operation))

    def _bind_arguments(self, operation: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
        """Bind positional arguments to their names from the Device structure.

        The argument names come from the args_schema's properties, which follow the Operation's
        signature order.
        """
        if not args:
            return kwargs
        structure = self.device_structure.get("operations", {}).get(operation)
        if structure is None:
            msg = f"Operation '{operation}' has no entry in the device structure, cannot bind positional arguments."
            raise TypeError(msg)
        names = list(structure["args_schema"]["properties"])
        if len(args) > len(names):
            msg = f"Operation '{operation}' takes at most {len(names)} positional arguments, got {len(args)}."
            raise TypeError(msg)
        bound = dict(zip(names, args, strict=False))
        for name in bound:
            if name in kwargs:
                msg = f"Operation '{operation}' got multiple values for argument '{name}'."
                raise TypeError(msg)
        bound.update(kwargs)
        return bound

    def _returns_type(self, operation: str) -> str | None:
        """Return the type path an Operation's return value rebuilds from, or None when JSON-native."""
        returns_type: str | None = self.device_structure.get("operations", {}).get(operation, {}).get("returns_type")
        return returns_type

    def trigger_parameter(self, parameter: str, *args: Any, **kwargs: Any) -> Any:
        values = (*args, *kwargs.values())
        if len(values) > 1:
            msg = f"Setting parameter '{parameter}' takes exactly one value, got {len(values)}."
            raise TypeError(msg)

        if values:
            packet = self.create_control_packet(
                self.provider_name,
                Request.SET_PARAMETER,
                SetParameterPayload(
                    instrument=self.instrument_name, parameter=parameter, value=_dump_argument(values[0])
                ).model_dump(),
            )
        else:
            packet = self.create_control_packet(
                self.provider_name,
                Request.GET_PARAMETER,
                GetParameterPayload(instrument=self.instrument_name, parameter=parameter).model_dump(),
            )

        response = self.ask(packet)

        return _rebuild_value(self._response_value(response), self._parameter_type(parameter))

    def _parameter_type(self, parameter: str) -> str | None:
        """Return the type path a Parameter's value rebuilds from, or None when JSON-native."""
        parameter_type: str | None = self.device_structure.get("parameters", {}).get(parameter, {}).get("type")
        return parameter_type

    def get_info(self) -> Any:
        packet = self.create_control_packet(
            self.provider_name,
            Request.GET_INFO,
            GetInfoPayload(instrument=self.instrument_name).model_dump(),
        )

        response = self.ask(packet)

        return _rebuild_value(self._response_value(response), self._info_type())

    def _info_type(self) -> str | None:
        """Return the type path the Instrument's info rebuilds from, or None when JSON-native."""
        info_type: str | None = self.device_structure.get("info", {}).get("type")
        return info_type

    @staticmethod
    def _response_value(response: Packet) -> Any:
        if response.payload is None:
            return None
        return response.payload["value"]


class ProxyInstrumentInit(NamedTuple):
    name: str
    host: str
    port: int
    router_name: str
    instrument_name: str
    timeout: int
    provider_name: str
    desc: str
    address: str
    parameters: set[str]
    operations: dict[str, dict[str, Any]]
    client_name: str


@dataclass
class ProxyInstrument(Instrument):
    """The address here is the zmq address of the router that the InstrumentClient will talk to."""

    name: str = ""
    desc: str = ""
    hw_address: str = ""
    host: str = "127.0.0.1"
    port: int = 5555
    timeout_ms: int = 15000
    router_name: str = "router1"
    client_name: str = ""
    provider_name: str = "provider1"
    instrument_name: str = "instrument1"

    # The Device structure of the Instrument this proxy mirrors: its Operations' and Parameters'
    # JSON Schemas and type paths (ADR-0002).
    device_structure: dict[str, Any] = field(default_factory=dict)

    # Boolean used to control when new attributes are being set.
    _instantiating: bool = True

    def __post_init__(self) -> None:
        # The client's name is the instrument name with "_client" appended and a random 6 character string appended.
        # This is to avoid any potential conflicts with other clients.
        client_name = (
            self.name
            + "_client_"
            + "".join(secrets.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(6))
        )
        instrument_client_init = InstrumentClientInit(
            name=client_name,
            host=self.host,
            port=self.port,
            router_name=self.router_name,
            timeout=self.timeout_ms,
            instrument_name=self.name,
            provider_name=self.provider_name,
            device_structure=self.device_structure,
        )
        self.client = InstrumentClient(instrument_client_init)

        self._instantiating = False

    def __getattr__(self, name: str) -> Any:
        if name in self.operations:
            return lambda *args, **kwargs: self.client.trigger_operation(name, *args, **kwargs)
        if name in self.parameters:
            return self.client.trigger_parameter(name)
        msg = f"Attribute '{name}' not found."
        raise AttributeError(msg)

    def __setattr__(self, name: str, value: Any) -> None:
        # Catch the first iteration
        if name == "_instantiating" or self._instantiating:
            super().__setattr__(name, value)
            return
        if name in self.parameters:
            self.client.trigger_parameter(name, value)
            return
        msg = "Cannot manually set attributes in a ProxyInstrument"
        raise AttributeError(msg)

    def start(self) -> None:
        pass

    def close(self) -> None:
        self.client.disconnect()

    @property
    def info(self) -> Any:
        return self.client.get_info()


class Client(ClientBase):
    def ping(self, destination: str) -> Packet | None:
        ping_packet = Packet(
            intent=PacketIntent.PING,
            request=Request.PING,
            source=self.name,
            destination=destination,
            hops=0,
            payload=None,
        )
        return self.ask(ping_packet)

    def get_available_devices(self, provider_name: str) -> dict[str, DeviceSummary]:
        packet = self.create_data_packet(provider_name, Request.GET_DEVICES, None)
        response = self.ask(packet)

        devices = DevicesPayload.model_validate(response.payload)

        return devices.devices

    def get_device(self, provider_name: str, device_name: str, timeout_ms: int = 60_000) -> Instrument:
        packet = self.create_data_packet(
            provider_name,
            Request.GET_DEVICE_STRUCTURE,
            GetDeviceStructurePayload(instrument=device_name).model_dump(),
        )

        response = self.ask(packet)

        if not isinstance(response.payload, dict):
            msg = "Payload is not a dictionary."
            raise PacketError(msg)

        if response.payload["name"] != device_name:
            msg = f"No device named {device_name}"
            raise ValueError(msg)

        return ProxyInstrument(
            name=response.payload["name"],
            desc=response.payload["desc"],
            hw_address=response.payload["hw_address"],
            host=self.host,
            port=self.port,
            router_name=self.router_name,
            timeout_ms=timeout_ms,
            instrument_name=response.payload["name"],
            provider_name=provider_name,
            parameters=set(response.payload["parameters"]),
            operations=response.payload["operations"],
            device_structure=response.payload,
        )
