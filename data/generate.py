#!/usr/bin/env python3
"""Generate the synthetic test dataset next to this script.

The output is committed and is what scripts/load-dataset.sh uploads, so this
only has to run when the shape of the data changes. It is seeded, and on one
Python version re-running reproduces the file byte for byte - across versions
that is not guaranteed, which is the other reason the CSV is committed rather
than generated at install time.

The columns exist to exercise specific parts of the setup:

    customer_id     hashed by the column mask in the OPA Trino policy
    full_name       hidden outright by that same policy
    email           partially masked by that same policy
    region          the row-filter column
    signup_date     a date, so the Iceberg schema is not all strings
    lifetime_value  a numeric measure for Superset to aggregate
    segment         a low-cardinality dimension for grouping

No real or personal data is involved: every name and address is assembled from
the word lists below.
"""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

ROWS = 2000
SEED = 20260909

FIRST_NAMES = [
    "Alina", "Bruno", "Carla", "Dmitri", "Elif", "Farid", "Greta", "Hugo",
    "Ines", "Jonas", "Kira", "Lars", "Maja", "Nils", "Olga", "Piet",
    "Quinn", "Rosa", "Sven", "Tessa", "Ulrich", "Vera", "Wim", "Yara", "Zoran",
]
LAST_NAMES = [
    "Adler", "Bergmann", "Cordova", "Dahl", "Eriksen", "Falk", "Gruber",
    "Halvorsen", "Ivanov", "Jansen", "Kowalski", "Lindqvist", "Moreau",
    "Novak", "Olsen", "Petrov", "Rossi", "Schneider", "Toivonen", "Vermeer",
]
DOMAINS = ["example.com", "example.net", "example.org"]
REGIONS = ["EMEA", "AMER", "APAC"]
SEGMENTS = ["enterprise", "midmarket", "smb"]

START = date(2021, 1, 1)
DAYS = (date(2026, 1, 1) - START).days


def main() -> None:
    rng = random.Random(SEED)
    out = Path(__file__).with_name("customers.csv")

    with out.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(
            [
                "customer_id",
                "full_name",
                "email",
                "region",
                "signup_date",
                "lifetime_value",
                "segment",
            ]
        )
        for index in range(1, ROWS + 1):
            first = rng.choice(FIRST_NAMES)
            last = rng.choice(LAST_NAMES)
            segment = rng.choice(SEGMENTS)
            # Enterprise customers are worth more, so a GROUP BY in Superset
            # shows a difference rather than three equal bars.
            scale = {"enterprise": 40000, "midmarket": 9000, "smb": 1500}[segment]
            writer.writerow(
                [
                    f"CUST-{index:06d}",
                    f"{first} {last}",
                    f"{first.lower()}.{last.lower()}@{rng.choice(DOMAINS)}",
                    rng.choice(REGIONS),
                    (START + timedelta(days=rng.randrange(DAYS))).isoformat(),
                    f"{rng.uniform(0.2, 2.0) * scale:.2f}",
                    segment,
                ]
            )

    print(f"wrote {ROWS} rows to {out}")


if __name__ == "__main__":
    main()
