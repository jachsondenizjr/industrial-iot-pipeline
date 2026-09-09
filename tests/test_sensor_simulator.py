import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_generator.sensor_simulator import (  # noqa: E402
    generate_sensor_reading,
    save_batch_to_json,
    EQUIPMENT_FLEET,
)


def test_equipment_fleet_not_empty():
    assert len(EQUIPMENT_FLEET) > 0


def test_sensor_reading_has_required_fields():
    equipment = EQUIPMENT_FLEET[0]
    reading = generate_sensor_reading(equipment)
    required = ["event_id", "equipment_id", "timestamp", "rpm",
                "temp_coolant_c", "nox_ppm", "is_anomaly"]
    for field in required:
        assert field in reading


def test_sensor_values_within_range():
    equipment = EQUIPMENT_FLEET[0]
    for _ in range(50):
        reading = generate_sensor_reading(equipment)
        assert reading["rpm"] >= 0
        assert reading["temp_coolant_c"] >= 50
        assert reading["nox_ppm"] >= 0
        assert 0 <= reading["load_pct"] <= 100


def test_event_id_is_unique():
    equipment = EQUIPMENT_FLEET[0]
    ids = [generate_sensor_reading(equipment)["event_id"] for _ in range(100)]
    assert len(set(ids)) == 100


def test_save_batch_creates_file(tmp_path):
    import json
    output = str(tmp_path / "test.json")
    save_batch_to_json(output, num_events=10)
    assert os.path.exists(output)
    with open(output) as f:
        data = json.load(f)
    assert len(data) == 10
