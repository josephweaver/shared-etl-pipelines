#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    return str(value).strip()


def main() -> int:
    ap = argparse.ArgumentParser(description="Export TIGER county geometries and attributes to CSV.")
    ap.add_argument("--county-path", required=True)
    ap.add_argument("--state-path", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--county-id-field", default="GEOID")
    ap.add_argument("--county-name-field", default="NAME")
    ap.add_argument("--state-id-field", default="STATEFP")
    ap.add_argument("--state-name-field", default="NAME")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    try:
        import geopandas as gpd
        import pandas as pd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("export_counties_csv requires geopandas and pandas") from exc

    county_path = Path(str(args.county_path)).expanduser().resolve()
    state_path = Path(str(args.state_path)).expanduser().resolve()
    output_csv = Path(str(args.output_csv)).expanduser().resolve()

    if not county_path.exists():
        raise FileNotFoundError(f"county path not found: {county_path}")
    if not state_path.exists():
        raise FileNotFoundError(f"state path not found: {state_path}")

    county_gdf = gpd.read_file(county_path)
    state_gdf = gpd.read_file(state_path)
    if county_gdf.empty:
        raise RuntimeError("county dataset is empty")
    if state_gdf.empty:
        raise RuntimeError("state dataset is empty")
    if county_gdf.crs is None:
        raise RuntimeError("county dataset missing CRS")
    if state_gdf.crs is None:
        raise RuntimeError("state dataset missing CRS")

    if args.county_id_field not in county_gdf.columns:
        raise RuntimeError(f"missing county id field: {args.county_id_field}")
    if args.county_name_field not in county_gdf.columns:
        raise RuntimeError(f"missing county name field: {args.county_name_field}")
    if args.state_id_field not in county_gdf.columns:
        raise RuntimeError(f"missing county state id field: {args.state_id_field}")
    if args.state_id_field not in state_gdf.columns:
        raise RuntimeError(f"missing state id field: {args.state_id_field}")
    if args.state_name_field not in state_gdf.columns:
        raise RuntimeError(f"missing state name field: {args.state_name_field}")

    county_gdf = county_gdf[[args.county_id_field, args.county_name_field, args.state_id_field, "geometry"]].copy()
    state_gdf = state_gdf[[args.state_id_field, args.state_name_field]].copy()
    county_gdf = county_gdf[county_gdf.geometry.notna() & ~county_gdf.geometry.is_empty].copy()
    state_gdf = state_gdf.drop_duplicates(subset=[args.state_id_field]).copy()

    county_4326 = county_gdf.to_crs(4326)
    state_lookup = {
        _to_text(row[args.state_id_field]): _to_text(row[args.state_name_field])
        for _, row in state_gdf.iterrows()
    }

    rows: list[dict[str, Any]] = []
    for _, row in county_4326.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        minx, miny, maxx, maxy = geom.bounds
        centroid = geom.centroid
        state_fp = _to_text(row.get(args.state_id_field))
        county_fp = _to_text(row.get(args.county_id_field))[-3:]
        rows.append(
            {
                "FIPS": _to_text(row.get(args.county_id_field)),
                "State_code": state_fp,
                "County_code": county_fp,
                "State_name": state_lookup.get(state_fp, ""),
                "County_name": _to_text(row.get(args.county_name_field)),
                "bbox_min_lon": float(minx),
                "bbox_min_lat": float(miny),
                "bbox_max_lon": float(maxx),
                "bbox_max_lat": float(maxy),
                "centroid_lon": float(centroid.x),
                "centroid_lat": float(centroid.y),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("no county rows were produced")
    df.sort_values(by=["State_code", "County_code", "FIPS"], inplace=True)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "FIPS",
                "State_code",
                "County_code",
                "State_name",
                "County_name",
                "bbox_min_lon",
                "bbox_min_lat",
                "bbox_max_lon",
                "bbox_max_lat",
                "centroid_lon",
                "centroid_lat",
            ],
        )
        writer.writeheader()
        for _, row in df.iterrows():
            writer.writerow(row.to_dict())

    if args.verbose:
        print(f"Wrote {len(df)} county rows to {output_csv.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
