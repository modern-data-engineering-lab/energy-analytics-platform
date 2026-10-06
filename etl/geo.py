"""
Feeder geography: approximate service areas for the 18 canonical feeders.

IBEDC's real feeder routes aren't public, and OpenStreetMap has none of Ibadan's 33kV network
(it maps only the 132kV/330kV transmission lines). What *is* recoverable is where each feeder
is: distribution feeders are named after the area they serve, and most of those names resolve
to a real OSM place, road or landmark. reference/feeder_locations.csv records one anchor point
per feeder, the OSM feature it came from, and a confidence level, so every location is
reviewable the same way KNOWN_FEEDER_TYPOS in transforms.py is. Feeders that couldn't be placed
are kept as "unlocated" rather than guessed at.

A service area is a circle of service_radius_km around the anchor: a deliberately simple
stand-in for the real (unknown) feeder footprint, good enough to ask "what's the vegetation,
terrain and weather like where this feeder runs" but not to reason about individual spans.

Pure Python, no pyspark/awsglue/shapely imports, so it's unit-tested directly (tests/) and runs
unchanged inside a Glue Python Shell job.
"""

# Glue Python Shell runs 3.9, where `float | None` in an annotation is a runtime error.
from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from pathlib import Path

EARTH_RADIUS_KM = 6371.0088
CONFIDENCE_LEVELS = ("high", "medium", "low", "unlocated")
DEFAULT_LOCATIONS_PATH = Path(__file__).parent.parent / "reference" / "feeder_locations.csv"


@dataclass(frozen=True)
class FeederLocation:
    feeder_canonical: str
    latitude: float | None
    longitude: float | None
    service_radius_km: float | None
    confidence: str
    setting: str | None
    anchor: str

    @property
    def is_located(self) -> bool:
        return self.confidence != "unlocated"


def load_feeder_locations(path=DEFAULT_LOCATIONS_PATH) -> list[FeederLocation]:
    def num(v):
        return float(v) if v else None

    with open(path, newline="", encoding="utf-8") as f:
        return [
            FeederLocation(
                feeder_canonical=row["feeder_canonical"],
                latitude=num(row["latitude"]),
                longitude=num(row["longitude"]),
                service_radius_km=num(row["service_radius_km"]),
                confidence=row["confidence"],
                setting=row["setting"] or None,
                anchor=row["anchor"],
            )
            for row in csv.DictReader(f)
        ]


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def destination_point(lat: float, lon: float, bearing_deg: float, distance_km: float):
    """(lat, lon) reached by travelling distance_km from (lat, lon) on the given bearing.
    Spherical, not planar: a circle drawn in raw degrees would be ~1% wider than tall at
    Ibadan's latitude, and noticeably distorted anywhere further from the equator."""
    p1, l1 = math.radians(lat), math.radians(lon)
    b, d = math.radians(bearing_deg), distance_km / EARTH_RADIUS_KM
    p2 = math.asin(math.sin(p1) * math.cos(d) + math.cos(p1) * math.sin(d) * math.cos(b))
    l2 = l1 + math.atan2(
        math.sin(b) * math.sin(d) * math.cos(p1), math.cos(d) - math.sin(p1) * math.sin(p2)
    )
    return math.degrees(p2), math.degrees(l2)


def service_area_polygon(lat: float, lon: float, radius_km: float, n_vertices: int = 64):
    """Closed ring of (lon, lat) vertices, lon first per the WKT/GeoJSON axis order."""
    ring = []
    for i in range(n_vertices):
        vlat, vlon = destination_point(lat, lon, 360.0 * i / n_vertices, radius_km)
        ring.append((vlon, vlat))
    ring.append(ring[0])
    return ring


def polygon_wkt(ring) -> str:
    return "POLYGON ((" + ", ".join(f"{x:.6f} {y:.6f}" for x, y in ring) + "))"


def service_area_wkt(location: FeederLocation, n_vertices: int = 64) -> str | None:
    if not location.is_located:
        return None
    ring = service_area_polygon(
        location.latitude, location.longitude, location.service_radius_km, n_vertices
    )
    return polygon_wkt(ring)
