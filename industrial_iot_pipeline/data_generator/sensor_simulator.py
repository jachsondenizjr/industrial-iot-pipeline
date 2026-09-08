"""
Industrial IoT Sensor Data Simulator
Simulates diesel engine and heavy equipment sensor data
Based on real J1939/CAN Bus protocol patterns
Author: Jachson Deniz Junior
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Generator


EQUIPMENT_FLEET = [
    {"equipment_id": "EQ-001", "type": "excavator", "model": "CAT 390F", "year": 2020},
    {"equipment_id": "EQ-002", "type": "bulldozer", "model": "Komatsu D375A", "year": 2019},
    {"equipment_id": "EQ-003", "type": "wheel_loader", "model": "Volvo L350H", "year": 2021},
    {"equipment_id": "EQ-004", "type": "dump_truck", "model": "Volvo A45G", "year": 2022},
    {"equipment_id": "EQ-005", "type": "motor_grader", "model": "John Deere 872G", "year": 2020},
]

SENSOR_RANGES = {
    "excavator":    {"rpm": (600, 1800), "temp_coolant": (75, 105), "temp_oil": (80, 120),
                     "pressure_oil": (2.5, 5.5), "fuel_rate": (15, 45), "nox_ppm": (50, 350),
                     "pm25_mg": (0.5, 8.0), "co2_pct": (3.5, 8.0), "load_pct": (20, 95)},
    "bulldozer":    {"rpm": (700, 1900), "temp_coolant": (80, 108), "temp_oil": (85, 125),
                     "pressure_oil": (3.0, 6.0), "fuel_rate": (20, 55), "nox_ppm": (60, 400),
                     "pm25_mg": (0.8, 10.0), "co2_pct": (4.0, 9.0), "load_pct": (30, 100)},
    "wheel_loader": {"rpm": (650, 1750), "temp_coolant": (75, 102), "temp_oil": (78, 118),
                     "pressure_oil": (2.8, 5.8), "fuel_rate": (12, 40), "nox_ppm": (40, 300),
                     "pm25_mg": (0.4, 7.0), "co2_pct": (3.0, 7.5), "load_pct": (15, 90)},
    "dump_truck":   {"rpm": (600, 2000), "temp_coolant": (80, 110), "temp_oil": (85, 130),
                     "pressure_oil": (3.5, 6.5), "fuel_rate": (25, 65), "nox_ppm": (80, 450),
                     "pm25_mg": (1.0, 12.0), "co2_pct": (4.5, 10.0), "load_pct": (40, 100)},
    "motor_grader": {"rpm": (650, 1800), "temp_coolant": (75, 100), "temp_oil": (80, 115),
                     "pressure_oil": (2.5, 5.5), "fuel_rate": (10, 35), "nox_ppm": (35, 280),
                     "pm25_mg": (0.3, 6.0), "co2_pct": (3.0, 7.0), "load_pct": (10, 85)},
}

ANOMALY_THRESHOLDS = {
    "temp_coolant": 105,
    "temp_oil": 120,
    "pressure_oil": 2.5,
    "nox_ppm": 350,
    "pm25_mg": 8.0,
}


def generate_sensor_reading(equipment: dict) -> dict:
    eq_type = equipment["type"]
    ranges = SENSOR_RANGES[eq_type]
    inject_anomaly = random.random() < 0.05

    reading = {
        "event_id": str(uuid.uuid4()),
        "equipment_id": equipment["equipment_id"],
        "equipment_type": equipment["type"],
        "equipment_model": equipment["model"],
        "equipment_year": equipment["year"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "rpm": round(random.uniform(*ranges["rpm"]) * (1.2 if inject_anomaly else 1.0), 1),
        "temp_coolant_c": round(random.uniform(*ranges["temp_coolant"]) * (1.1 if inject_anomaly else 1.0), 1),
        "temp_oil_c": round(random.uniform(*ranges["temp_oil"]) * (1.15 if inject_anomaly else 1.0), 1),
        "pressure_oil_bar": round(random.uniform(*ranges["pressure_oil"]) * (0.7 if inject_anomaly else 1.0), 2),
        "fuel_rate_lh": round(random.uniform(*ranges["fuel_rate"]), 2),
        "nox_ppm": round(random.uniform(*ranges["nox_ppm"]) * (1.3 if inject_anomaly else 1.0), 1),
        "pm25_mg_m3": round(random.uniform(*ranges["pm25_mg"]) * (1.4 if inject_anomaly else 1.0), 2),
        "co2_pct": round(random.uniform(*ranges["co2_pct"]), 2),
        "load_pct": round(random.uniform(*ranges["load_pct"]), 1),
        "is_anomaly": inject_anomaly,
        "location_lat": round(-23.5505 + random.uniform(-0.5, 0.5), 6),
        "location_lon": round(-46.6333 + random.uniform(-0.5, 0.5), 6),
        "ingestion_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }

    flags = []
    if reading["temp_coolant_c"] > ANOMALY_THRESHOLDS["temp_coolant"]:
        flags.append("HIGH_TEMP_COOLANT")
    if reading["temp_oil_c"] > ANOMALY_THRESHOLDS["temp_oil"]:
        flags.append("HIGH_TEMP_OIL")
    if reading["pressure_oil_bar"] < ANOMALY_THRESHOLDS["pressure_oil"]:
        flags.append("LOW_PRESSURE_OIL")
    if reading["nox_ppm"] > ANOMALY_THRESHOLDS["nox_ppm"]:
        flags.append("HIGH_NOX")
    if reading["pm25_mg_m3"] > ANOMALY_THRESHOLDS["pm25_mg"]:
        flags.append("HIGH_PM25")

    reading["anomaly_flags"] = ",".join(flags) if flags else None
    return reading


def save_batch_to_json(output_path: str, num_events: int = 1000):
    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    events = []
    for i in range(num_events):
        equipment = EQUIPMENT_FLEET[i % len(EQUIPMENT_FLEET)]
        events.append(generate_sensor_reading(equipment))
        if (i + 1) % 100 == 0:
            print(f"Generated {i + 1}/{num_events} events...")

    with open(output_path, "w") as f:
        json.dump(events, f, indent=2)

    anomalies = sum(1 for e in events if e["is_anomaly"])
    print(f"Saved {num_events} events to {output_path}")
    print(f"Anomalies injected: {anomalies} ({anomalies/num_events*100:.1f}%)")


if __name__ == "__main__":
    save_batch_to_json("data/sensor_readings_sample.json", num_events=1000)
