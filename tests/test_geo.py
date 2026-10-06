"""
Tests the feeder geography reference data and the service-area geometry in etl/geo.py. No
Spark, no AWS, no geospatial libraries.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "etl"))

from geo import (  # noqa: E402
    CONFIDENCE_LEVELS,
    haversine_km,
    load_feeder_locations,
    service_area_polygon,
    service_area_wkt,
)
from transforms import CANONICAL_FEEDERS  # noqa: E402

LOCATIONS = load_feeder_locations()
LOCATED = [loc for loc in LOCATIONS if loc.is_located]

# Generous box around Ibadan and the Ibarapa towns (Eruwa, Lanlate). Catches swapped lat/lon
# and sign errors, which would land a feeder in the Gulf of Guinea or the Sahara.
OYO_BBOX = {"lat": (7.1, 7.8), "lon": (3.2, 4.2)}


class TestReferenceData:
    def test_every_canonical_feeder_appears_exactly_once(self):
        names = [loc.feeder_canonical for loc in LOCATIONS]
        assert sorted(names) == sorted(CANONICAL_FEEDERS)

    def test_confidence_is_a_known_level(self):
        for loc in LOCATIONS:
            assert loc.confidence in CONFIDENCE_LEVELS, loc.feeder_canonical

    def test_located_feeders_have_coordinates_and_radius(self):
        for loc in LOCATED:
            assert loc.latitude is not None and loc.longitude is not None, loc.feeder_canonical
            assert loc.service_radius_km and loc.service_radius_km > 0, loc.feeder_canonical

    def test_located_feeders_fall_inside_the_region(self):
        for loc in LOCATED:
            assert OYO_BBOX["lat"][0] < loc.latitude < OYO_BBOX["lat"][1], loc.feeder_canonical
            assert OYO_BBOX["lon"][0] < loc.longitude < OYO_BBOX["lon"][1], loc.feeder_canonical

    def test_unlocated_feeders_carry_no_coordinates(self):
        # A half-filled row (a coordinate but "unlocated", or vice versa) would mean either a
        # guess slipped in or a real location is being ignored downstream.
        for loc in LOCATIONS:
            if not loc.is_located:
                assert loc.latitude is None and loc.longitude is None, loc.feeder_canonical
                assert service_area_wkt(loc) is None

    def test_every_row_documents_its_anchor(self):
        for loc in LOCATIONS:
            assert loc.anchor.strip(), loc.feeder_canonical


class TestGeometry:
    def test_haversine_known_distance(self):
        # Eruwa to Lanlate, both OSM place nodes: ~8.2 km apart.
        assert haversine_km(7.5333, 3.4167, 7.5998, 3.4496) == pytest.approx(8.2, abs=0.1)

    def test_polygon_ring_is_closed(self):
        ring = service_area_polygon(7.4, 3.9, 2.5, n_vertices=32)
        assert len(ring) == 33
        assert ring[0] == ring[-1]

    def test_every_vertex_is_at_the_radius(self):
        lat, lon, r = 7.4306, 3.9081, 2.5
        for vlon, vlat in service_area_polygon(lat, lon, r):
            assert haversine_km(lat, lon, vlat, vlon) == pytest.approx(r, rel=1e-6)

    def test_ring_is_lon_lat_order(self):
        # First vertex is due north of the centre: same longitude, higher latitude.
        vlon, vlat = service_area_polygon(7.4, 3.9, 2.5)[0]
        assert vlon == pytest.approx(3.9)
        assert vlat > 7.4

    def test_wkt_shape(self):
        wkt = service_area_wkt(LOCATED[0], n_vertices=8)
        assert wkt.startswith("POLYGON ((") and wkt.endswith("))")
        assert wkt.count(",") == 8  # 9 vertices including the closing one
