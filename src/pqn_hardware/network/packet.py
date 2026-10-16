"""Pydantic models for the JSON wire protocol (version 2)."""

import uuid
from enum import StrEnum
from typing import Any

from pydantic import BaseModel
from pydantic import Field

PROTOCOL_VERSION = 2


class NetworkElementClass(StrEnum):
    """The kind of network element registering with a Router."""

    PROVIDER = "PROVIDER"
    CLIENT = "CLIENT"


class PacketIntent(StrEnum):
    """The category of a Packet, which decides how the receiver handles it."""

    DATA = "DATA"
    CONTROL = "CONTROL"
    REGISTRATION = "REGISTRATION"
    REGISTRATION_ACK = "REGISTRATION_ACK"
    PING = "PING"
    ERROR = "ERROR"


class Request(StrEnum):
    """The verb naming what a Packet asks for. Each verb has one Payload shape."""

    REGISTER = "REGISTER"
    ACKNOWLEDGE = "ACKNOWLEDGE"
    PING = "PING"
    PONG = "PONG"
    GET_PROVIDERS = "GET_PROVIDERS"
    GET_DEVICES = "GET_DEVICES"
    GET_DEVICE_STRUCTURE = "GET_DEVICE_STRUCTURE"
    CALL_OPERATION = "CALL_OPERATION"
    GET_PARAMETER = "GET_PARAMETER"
    SET_PARAMETER = "SET_PARAMETER"
    GET_INFO = "GET_INFO"
    ERROR = "ERROR"


class ErrorCode(StrEnum):
    """Stable, machine-readable identifier in an error Payload.

    Peers branch on the code, never on the message text.
    """

    UNKNOWN_DESTINATION = "UNKNOWN_DESTINATION"
    DESTINATION_UNREACHABLE = "DESTINATION_UNREACHABLE"
    MALFORMED_PACKET = "MALFORMED_PACKET"
    UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION"
    UNSUPPORTED_REQUEST = "UNSUPPORTED_REQUEST"
    WRONG_DESTINATION = "WRONG_DESTINATION"
    UNKNOWN_INSTRUMENT = "UNKNOWN_INSTRUMENT"
    UNKNOWN_OPERATION = "UNKNOWN_OPERATION"
    UNKNOWN_PARAMETER = "UNKNOWN_PARAMETER"
    INVALID_ARGUMENTS = "INVALID_ARGUMENTS"
    INSTRUMENT_ERROR = "INSTRUMENT_ERROR"


class DeviceSummary(BaseModel):
    """Summary of one Instrument a Provider reports, by its driver's import path."""

    type: str
    desc: str
    hw_address: str


class RegisterPayload(BaseModel):
    """Payload of a REGISTER Packet. Only Providers report devices."""

    kind: NetworkElementClass
    devices: dict[str, DeviceSummary] = Field(default_factory=dict)


class GetDeviceStructurePayload(BaseModel):
    """Payload of a GET_DEVICE_STRUCTURE Packet."""

    instrument: str


class CallOperationPayload(BaseModel):
    """Payload of a CALL_OPERATION Packet. Arguments go by keyword only."""

    instrument: str
    operation: str
    kwargs: dict[str, Any] = Field(default_factory=dict)


class GetParameterPayload(BaseModel):
    """Payload of a GET_PARAMETER Packet."""

    instrument: str
    parameter: str


class SetParameterPayload(BaseModel):
    """Payload of a SET_PARAMETER Packet."""

    instrument: str
    parameter: str
    value: Any


class GetInfoPayload(BaseModel):
    """Payload of a GET_INFO Packet."""

    instrument: str


class ValuePayload(BaseModel):
    """Payload of CALL_OPERATION, GET_PARAMETER and GET_INFO replies."""

    value: Any


class DevicesPayload(BaseModel):
    """Payload of a GET_DEVICES reply."""

    devices: dict[str, DeviceSummary]


class ErrorPayload(BaseModel):
    """Payload of an ERROR Packet. The Origin is the element that raised the error."""

    code: ErrorCode
    message: str
    origin: str
    details: dict[str, Any] = Field(default_factory=dict)


class Packet(BaseModel):
    """The single message envelope on the wire. The Payload shape is fixed by `request`."""

    version: int = PROTOCOL_VERSION
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    reply_to: str | None = None
    intent: PacketIntent
    request: Request
    source: str
    destination: str
    hops: int = 0
    payload: dict[str, Any] | None = None


def encode(packet: Packet) -> bytes:
    """Encode a Packet as UTF-8 JSON bytes for the wire."""
    return packet.model_dump_json().encode("utf-8")


def decode(data: bytes) -> Packet:
    """Decode wire bytes into a Packet. Raises if the bytes are not a valid Packet."""
    return Packet.model_validate_json(data)


def create_registration_packet(*, source: str, destination: str, kind: NetworkElementClass, hops: int = 0) -> Packet:
    """Create the REGISTER Packet a network element sends to its Router."""
    return Packet(
        intent=PacketIntent.REGISTRATION,
        request=Request.REGISTER,
        source=source,
        destination=destination,
        hops=hops,
        payload=RegisterPayload(kind=kind).model_dump(exclude_unset=True),
    )
