"""Round-trip tests for driver return types through pydantic's TypeAdapter (ADR-0002)."""

from typing import Any

from hypothesis import given
from hypothesis import strategies as st
from pydantic import TypeAdapter

from pqn_hardware.drivers.dummies import DummyInfo
from pqn_hardware.drivers.polarimeter import PolarizationMeasurement
from pqn_hardware.instrument import InstrumentInfo
from pqn_hardware.instrument import PolarimeterInfo
from pqn_hardware.instrument import RotatorInfo
from pqn_hardware.instrument import TimeTaggerInfo

# NaN and Infinity handling is deferred (issue #8).
finite_floats = st.floats(allow_nan=False, allow_infinity=False)


def round_trip(cls: type, instance: Any) -> Any:
    """Round-trip a dataclass through a TypeAdapter as JSON."""
    adapter = TypeAdapter(cls)
    return adapter.validate_json(adapter.dump_json(instance))


instrument_infos = st.builds(
    InstrumentInfo,
    name=st.text(),
    desc=st.text(),
    hw_address=st.text(),
    hw_status=st.dictionaries(st.text(), st.integers()),
)

rotator_infos = st.builds(
    RotatorInfo,
    name=st.text(),
    desc=st.text(),
    hw_address=st.text(),
    hw_status=st.dictionaries(st.text(), st.integers()),
    degrees=finite_floats,
    offset_degrees=finite_floats,
)

time_tagger_infos = st.builds(
    TimeTaggerInfo,
    name=st.text(),
    desc=st.text(),
    hw_address=st.text(),
    hw_status=st.dictionaries(st.text(), st.integers()),
    active_channels=st.lists(st.integers(min_value=1, max_value=64)),
    test_signal_enabled=st.booleans(),
    test_signal_divider=st.integers(min_value=1),
)

polarimeter_infos = st.builds(
    PolarimeterInfo,
    name=st.text(),
    desc=st.text(),
    hw_address=st.text(),
)

dummy_infos = st.builds(
    DummyInfo,
    name=st.text(),
    desc=st.text(),
    hw_address=st.text(),
    param_int=st.integers(),
    param_str=st.text(),
    param_bool=st.booleans(),
)

polarization_measurements = st.builds(
    PolarizationMeasurement,
    h=finite_floats,
    v=finite_floats,
    d=finite_floats,
    a=finite_floats,
    _last_theta=finite_floats,
)


@given(info=instrument_infos)
def test_instrument_info_round_trip(info: InstrumentInfo) -> None:
    assert round_trip(InstrumentInfo, info) == info


@given(info=rotator_infos)
def test_rotator_info_round_trip(info: RotatorInfo) -> None:
    assert round_trip(RotatorInfo, info) == info


@given(info=time_tagger_infos)
def test_time_tagger_info_round_trip(info: TimeTaggerInfo) -> None:
    assert round_trip(TimeTaggerInfo, info) == info


@given(info=polarimeter_infos)
def test_polarimeter_info_round_trip(info: PolarimeterInfo) -> None:
    assert round_trip(PolarimeterInfo, info) == info


@given(info=dummy_infos)
def test_dummy_info_round_trip(info: DummyInfo) -> None:
    assert round_trip(DummyInfo, info) == info


@given(measurement=polarization_measurements)
def test_polarization_measurement_round_trip(measurement: PolarizationMeasurement) -> None:
    assert round_trip(PolarizationMeasurement, measurement) == measurement


def test_polarization_measurement_last_theta_survives_the_round_trip() -> None:
    """The private _last_theta field must survive, or theta loses its full 0-360 range."""
    measurement = PolarizationMeasurement(h=1.0, v=1.0, d=1.0, a=1.0, _last_theta=270.0)
    round_tripped = round_trip(PolarizationMeasurement, measurement)
    assert isinstance(round_tripped, PolarizationMeasurement)
    assert round_tripped.theta == measurement.theta


def test_round_tripped_infos_keep_their_class() -> None:
    """A TypeAdapter round-trip rebuilds the same subclass, not just the base InstrumentInfo."""
    for info in (
        RotatorInfo(name="r", degrees=12.5),
        TimeTaggerInfo(name="t", active_channels=[1, 2]),
        DummyInfo(name="d", param_int=3),
    ):
        assert type(round_trip(type(info), info)) is type(info)
