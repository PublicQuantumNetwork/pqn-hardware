"""Tests for scripts/protocol_schema: schema generation, staleness, and the master comparison."""

import importlib.machinery
import importlib.util
import json
from pathlib import Path
from typing import Any

from pqn_hardware.network.packet import PROTOCOL_VERSION

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPO_ROOT / "protocol" / "schema.json"


def _load_script() -> Any:
    """Import the extensionless scripts/protocol_schema as a module."""
    loader = importlib.machinery.SourceFileLoader("protocol_schema", str(REPO_ROOT / "scripts" / "protocol_schema"))
    spec = importlib.util.spec_from_loader("protocol_schema", loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


script = _load_script()


def _write_schema(path: Path, document: dict[str, Any]) -> None:
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_generated_schema_matches_the_committed_file() -> None:
    # Regenerating must not change the committed file: the schema is generated from the models,
    # never hand-written (ADR-0001), and the render is deterministic.
    assert SCHEMA_PATH.read_text(encoding="utf-8") == script.render_schema(script.generate_schema())


def test_the_committed_schema_covers_the_protocol() -> None:
    document = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))

    assert document["protocol_version"] == PROTOCOL_VERSION
    assert sorted(document["payloads"]) == [
        "CallOperationPayload",
        "DevicesPayload",
        "ErrorPayload",
        "GetDeviceStructurePayload",
        "GetInfoPayload",
        "GetParameterPayload",
        "ProvidersPayload",
        "RegisterPayload",
        "SetParameterPayload",
        "ValuePayload",
    ]
    assert sorted(document["enums"]) == ["ErrorCode", "NetworkElementClass", "PacketIntent", "Request"]


def test_the_committed_envelope_requires_version_and_id() -> None:
    # version and id are required on the wire even though the Packet model keeps defaults for
    # local construction.
    document = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    required = document["envelope"]["required"]
    assert "version" in required
    assert "id" in required


def test_check_passes_when_the_committed_file_is_up_to_date(tmp_path: Path) -> None:
    schema = tmp_path / "schema.json"
    _write_schema(schema, script.generate_schema())

    assert script.main(["--schema", str(schema)]) == 0


def test_check_fails_on_a_stale_committed_file(tmp_path: Path) -> None:
    # A committed schema that regeneration would change is stale, and the check fails.
    stale = script.generate_schema()
    stale["envelope"]["description"] = "an old description"
    schema = tmp_path / "schema.json"
    _write_schema(schema, stale)

    assert script.main(["--schema", str(schema)]) == 1


def test_check_fails_when_the_committed_file_is_missing(tmp_path: Path) -> None:
    assert script.main(["--schema", str(tmp_path / "schema.json")]) == 1


def test_check_version_passes_when_the_schema_is_unchanged(tmp_path: Path) -> None:
    new, old = tmp_path / "new.json", tmp_path / "old.json"
    _write_schema(new, script.generate_schema())
    _write_schema(old, script.generate_schema())

    assert script.main(["--schema", str(new), "check-version", "--old-schema", str(old)]) == 0


def test_check_version_fails_when_the_schema_changed_without_a_bump(tmp_path: Path) -> None:
    changed = script.generate_schema()
    changed["envelope"]["description"] = "a changed description"
    new, old = tmp_path / "new.json", tmp_path / "old.json"
    _write_schema(new, changed)
    _write_schema(old, script.generate_schema())

    assert script.main(["--schema", str(new), "check-version", "--old-schema", str(old)]) == 1


def test_check_version_passes_when_the_schema_changed_with_a_bump(tmp_path: Path) -> None:
    old_document = script.generate_schema()
    old_document["protocol_version"] = PROTOCOL_VERSION - 1
    new_document = script.generate_schema()
    new_document["envelope"]["description"] = "a changed description"
    new, old = tmp_path / "new.json", tmp_path / "old.json"
    _write_schema(new, new_document)
    _write_schema(old, old_document)

    assert script.main(["--schema", str(new), "check-version", "--old-schema", str(old)]) == 0


def test_check_version_fails_when_the_version_decreases(tmp_path: Path) -> None:
    # A bump means a greater version, not just a different one.
    old_document = script.generate_schema()
    old_document["protocol_version"] = PROTOCOL_VERSION + 1
    new, old = tmp_path / "new.json", tmp_path / "old.json"
    _write_schema(new, script.generate_schema())
    _write_schema(old, old_document)

    assert script.main(["--schema", str(new), "check-version", "--old-schema", str(old)]) == 1


def test_check_version_passes_when_master_has_no_schema(tmp_path: Path) -> None:
    # Master has no protocol/schema.json yet: there is nothing to compare, and both a
    # missing and an empty old schema file mean that.
    new = tmp_path / "new.json"
    _write_schema(new, script.generate_schema())

    assert script.main(["--schema", str(new), "check-version", "--old-schema", str(tmp_path / "missing.json")]) == 0

    empty = tmp_path / "empty.json"
    empty.write_text("", encoding="utf-8")
    assert script.main(["--schema", str(new), "check-version", "--old-schema", str(empty)]) == 0
