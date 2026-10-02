import orjson
import pytest

from etl.reader import MalformedRecord, parse_envelope


def _line(envelope_id, data) -> bytes:
    return orjson.dumps({"id": envelope_id, "date_updated": "2026-01-01 00:00:00.000 Z", "serialized_data": data})


def test_returns_forager_id_and_serialized_data():
    assert parse_envelope(_line(7, {"forager_id": 7, "name": "Acme"})) == (7, {"forager_id": 7, "name": "Acme"})


@pytest.mark.parametrize("line, error", [
    (b"{not json", "invalid JSON"),
    (_line(7, None), "missing serialized_data"),
    (_line(7, {"forager_id": "7"}), "missing integer serialized_data.forager_id"),
    (_line(8, {"forager_id": 7}), "envelope id 8 does not match serialized_data.forager_id 7"),
])
def test_rejects_malformed_envelopes(line, error):
    with pytest.raises(MalformedRecord, match=error):
        parse_envelope(line)
