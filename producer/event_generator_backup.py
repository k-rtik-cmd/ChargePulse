
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta


STATIONS = ["ST001", "ST002", "ST003", "ST004"]
CONNECTORS = ["C01", "C02", "C03", "C04"]


def generate_event():
    """Generate one sample EV charging event."""

    return {
        "event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "station_id": random.choice(STATIONS),
        "connector_id": random.choice(CONNECTORS),
        "vehicle_id": f"EV{random.randint(1000, 9999)}",
        "energy_delta_kwh": round(random.uniform(0.1, 2.0), 3),
        "power_kw": round(random.uniform(7.0, 150.0), 2),
        "session_status": random.choice(
            ["charging", "charging", "completed"]
        )
    }


def run_producer(total_events, events_per_second,
                 duplicate_rate, late_rate):

    interval = 1.0 / events_per_second
    start_time = time.perf_counter()

    previous_events = []

    for count in range(total_events):
        # Generate a duplicate by reusing an earlier event.
        if previous_events and random.random() < duplicate_rate:
            event = random.choice(previous_events).copy()

        else:
            event = generate_event()

            # Generate a late event with an older event timestamp.
            if random.random() < late_rate:
                old_time = datetime.now(timezone.utc) - timedelta(
                    seconds=random.randint(10, 120)
                )
                event["event_ts"] = old_time.isoformat()

            previous_events.append(event.copy())

        print(json.dumps(event), flush=True)

        next_event_time = start_time + (count + 1) * interval
        sleep_time = next_event_time - time.perf_counter()

        if sleep_time > 0:
            time.sleep(sleep_time)

    elapsed = time.perf_counter() - start_time

    print(
        f"\nGenerated {total_events} events in {elapsed:.2f} seconds.",
        flush=True
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ChargePulse EV charging event generator"
    )

    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=int, default=50)
    parser.add_argument("--duplicate-rate", type=float, default=0.05)
    parser.add_argument("--late-rate", type=float, default=0.05)

    args = parser.parse_args()

    if args.count <= 0 or args.rate <= 0:
        parser.error("--count and --rate must be positive")

    if not 0 <= args.duplicate_rate <= 1:
        parser.error("--duplicate-rate must be between 0 and 1")

    if not 0 <= args.late_rate <= 1:
        parser.error("--late-rate must be between 0 and 1")

    run_producer(
        args.count,
        args.rate,
        args.duplicate_rate,
        args.late_rate
    )
