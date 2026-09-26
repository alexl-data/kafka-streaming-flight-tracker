import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "producer"))

from flights_producer import region_for, LAT_MID, LON_MID  # noqa: E402


def test_region_for_northeast():
    assert region_for(LAT_MID + 1, LON_MID + 1) == "ne"


def test_region_for_northwest():
    assert region_for(LAT_MID + 1, LON_MID - 1) == "nw"


def test_region_for_southeast():
    assert region_for(LAT_MID - 1, LON_MID + 1) == "se"


def test_region_for_southwest():
    assert region_for(LAT_MID - 1, LON_MID - 1) == "sw"


def test_region_for_boundary_is_inclusive_north_east():
    # lat/lon exactly on the midpoint counts as north/east (>=, not >)
    assert region_for(LAT_MID, LON_MID) == "ne"
