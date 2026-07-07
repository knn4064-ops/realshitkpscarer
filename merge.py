#!/usr/bin/env python3
"""Spoji leads.csv iz svih shardova u jednu tabelu (dedup po linku)."""
import csv
import glob

HEADER = ["ime", "link", "broj_ocena", "aktivnih_oglasa", "kategorija"]
seen, rows = set(), []

for path in glob.glob("parts/*/leads.csv"):
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        next(r, None)               # preskoci header
        for row in r:
            if len(row) < 2 or row[1] in seen:
                continue
            seen.add(row[1])
            rows.append(row)

# sortiraj po broju ocena (opadajuce)
rows.sort(key=lambda x: int(x[2]) if len(x) > 2 and x[2].isdigit() else 0, reverse=True)

with open("leads.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(HEADER)
    w.writerows(rows)

print(f"Spojeno: {len(rows)} jedinstvenih prodavaca -> leads.csv")
