"""Round-trip tests for the wire protocol models: every Payload model through JSON, and the envelope through encode/decode."""

from enum import StrEnum
from typing import Any

from hypothesis import given
from hypothesis import strategies as st

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
from pqn_hardware.network.packet import RegisterPayload
from pqn_hardware.network.packet import Request
from pqn_hardware.network.packet import SetParameterPayload
from pqn_hardware.network.packet import ValuePayload
from pqn_hardware.network.packet import decode
from pqn_hardware.network.packet import encode

# JSON-native values only: NaN and Infinity handling is deferred (issue #8).
json_values = st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False, allow_infinity=False) | st.text(),
    lambda children: st.lists(children) | st.dictionaries(st.text(), children),
    max_leaves=25,
)

device_summaries = st.builds(
    DeviceSummary,
    type=st.text(),
    desc=st.text(),
    hw_address=st.text(),
)


def round_trip_json(model: Any) -> Any:
    """Round-trip a payload model through JSON and back."""
    return type(model).model_validate_json(model.model_dump_json())


@given(kind=st.sampled_from(NetworkElementClass), devices=st.dictionaries(st.text(), device_summaries))
def test_register_payload_round_trip(kind: NetworkElementClass, devices: dict[str, DeviceSummary]) -> None:
    payload = RegisterPayload(kind=kind, devices=devices)
    assert round_trip_json(payload) == payload


@given(instrument=st.text())
def test_get_device_structure_payload_round_trip(instrument: str) -> None:
    payload = GetDeviceStructurePayload(instrument=instrument)
    assert round_trip_json(payload) == payload


@given(instrument=st.text(), operation=st.text(), kwargs=st.dictionaries(st.text(), json_values))
def test_call_operation_payload_round_trip(instrument: str, operation: str, kwargs: dict[str, Any]) -> None:
    payload = CallOperationPayload(instrument=instrument, operation=operation, kwargs=kwargs)
    assert round_trip_json(payload) == payload


@given(instrument=st.text(), parameter=st.text())
def test_get_parameter_payload_round_trip(instrument: str, parameter: str) -> None:
    payload = GetParameterPayload(instrument=instrument, parameter=parameter)
    assert round_trip_json(payload) == payload


@given(instrument=st.text(), parameter=st.text(), value=json_values)
def test_set_parameter_payload_round_trip(instrument: str, parameter: str, value: Any) -> None:
    payload = SetParameterPayload(instrument=instrument, parameter=parameter, value=value)
    assert round_trip_json(payload) == payload


@given(instrument=st.text())
def test_get_info_payload_round_trip(instrument: str) -> None:
    payload = GetInfoPayload(instrument=instrument)
    assert round_trip_json(payload) == payload


@given(value=json_values)
def test_value_payload_round_trip(value: Any) -> None:
    payload = ValuePayload(value=value)
    assert round_trip_json(payload) == payload


@given(devices=st.dictionaries(st.text(), device_summaries))
def test_devices_payload_round_trip(devices: dict[str, DeviceSummary]) -> None:
    payload = DevicesPayload(devices=devices)
    assert round_trip_json(payload) == payload


@given(device_summary=device_summaries)
def test_device_summary_round_trip(device_summary: DeviceSummary) -> None:
    assert round_trip_json(device_summary) == device_summary


@given(
    code=st.sampled_from(ErrorCode),
    message=st.text(),
    origin=st.text(),
    details=st.dictionaries(st.text(), json_values),
)
def test_error_payload_round_trip(code: ErrorCode, message: str, origin: str, details: dict[str, Any]) -> None:
    payload = ErrorPayload(code=code, message=message, origin=origin, details=details)
    assert round_trip_json(payload) == payload


packets = st.builds(
    Packet,
    intent=st.sampled_from(PacketIntent),
    request=st.sampled_from(Request),
    source=st.text(),
    destination=st.text(),
    hops=st.integers(min_value=0, max_value=2**16),
    reply_to=st.none() | st.text(),
    payload=st.none() | st.dictionaries(st.text(), json_values),
)


@given(packet=packets)
def test_packet_round_trip(packet: Packet) -> None:
    assert decode(encode(packet)) == packet


def test_enums_serialize_by_name() -> None:
    """The wire carries enum names, and every member round-trips through the Packet model."""
    for enum_cls in (PacketIntent, Request, NetworkElementClass, ErrorCode):
        assert issubclass(enum_cls, StrEnum)
        for member in enum_cls:
            assert member.value == member.name
    assert PacketIntent.DATA.value == "DATA"
    assert Request.CALL_OPERATION.value == "CALL_OPERATION"
    assert ErrorCode.UNKNOWN_DESTINATION.value == "UNKNOWN_DESTINATION"
