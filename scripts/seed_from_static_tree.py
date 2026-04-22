"""One-off seeder: loads the 10 systems that never got DB-ingested from the
static tree JSONs shipped in frontend/src/content/.

Gets 990 → 1,000 across the site counters (homepage, /codes, /crosswalks) by
filling in these systems' rows in classification_system + classification_node.

The tree JSONs already have the exact same fields the DB expects (code, title,
level, parent_code, sector_code, is_leaf) so this is a straight insert — no
transformation needed.

Runs as a Cloud Run Job against the wot-db Cloud SQL instance. Fetches all
inputs (systems.json + tree JSONs) from the public repo's raw URLs, so no
local filesystem dependency — can run inside the wot-api image unchanged.

    gcloud run jobs execute wot-seed --region=us-east1 --wait
"""
from __future__ import annotations

import asyncio
import json
import os
import ssl
import sys
import urllib.request

import asyncpg

RAW_BASE = (
    "https://raw.githubusercontent.com/colaberry/WorldOfTaxonomy/main"
    "/frontend/src/content"
)
SYSTEMS_URL = f"{RAW_BASE}/crosswalk/systems.json"
TREE_URL = f"{RAW_BASE}/tree/{{sid}}.json"

MISSING_SYSTEM_IDS = [
    "esco_occupations",
    "esco_skills",
    "icd10_pcs",
    "icd10cm",
    "icd_11",
    "loinc",
    "mesh",
    "nci_thesaurus",
    "ndc_fda",
    "unspsc_v24",
]

CHUNK = 500


def fetch_json(url: str):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "wot-seeder/0.1"})
    with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
        return json.loads(r.read())


async def seed_system(conn: asyncpg.Connection, meta: dict, tree: list[dict]) -> int:
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, version, region, authority, url,
             source_url, source_date, data_provenance, license, node_count)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 0)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            full_name = EXCLUDED.full_name,
            version = EXCLUDED.version,
            region = EXCLUDED.region,
            authority = EXCLUDED.authority,
            url = EXCLUDED.url,
            source_url = EXCLUDED.source_url,
            source_date = EXCLUDED.source_date,
            data_provenance = EXCLUDED.data_provenance,
            license = EXCLUDED.license,
            node_count = 0
        """,
        meta["id"],
        meta["name"],
        meta.get("full_name"),
        meta.get("version"),
        meta.get("region"),
        meta.get("authority"),
        meta.get("url"),
        meta.get("source_url"),
        meta.get("source_date"),
        meta.get("data_provenance"),
        meta.get("license"),
    )

    records = [
        (
            meta["id"],
            node["code"],
            node["title"],
            node.get("level", 1),
            node.get("parent_code"),
            node.get("sector_code"),
            bool(node.get("is_leaf", False)),
            i,
        )
        for i, node in enumerate(tree)
    ]

    count = 0
    for i in range(0, len(records), CHUNK):
        chunk = records[i : i + CHUNK]
        await conn.executemany(
            """
            INSERT INTO classification_node
                (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (system_id, code) DO UPDATE SET
                title = EXCLUDED.title,
                level = EXCLUDED.level,
                parent_code = EXCLUDED.parent_code,
                sector_code = EXCLUDED.sector_code,
                is_leaf = EXCLUDED.is_leaf
            """,
            chunk,
        )
        count += len(chunk)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count,
        meta["id"],
    )
    return count


async def main():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        sys.exit("DATABASE_URL env var required")

    print(f"Fetching systems metadata from {SYSTEMS_URL}")
    systems_meta = {s["id"]: s for s in fetch_json(SYSTEMS_URL)}
    print(f"Got {len(systems_meta)} systems in static catalog")

    conn = await asyncpg.connect(dsn)
    try:
        total_nodes = 0
        for sid in MISSING_SYSTEM_IDS:
            meta = systems_meta.get(sid)
            if not meta:
                print(f"  x {sid}: missing from systems.json, skipping")
                continue

            try:
                tree = fetch_json(TREE_URL.format(sid=sid))
            except Exception as exc:  # noqa: BLE001
                print(f"  x {sid}: failed to fetch tree ({exc})")
                continue

            nodes = await seed_system(conn, meta, tree)
            print(f"  ok {sid}: {nodes:,} nodes")
            total_nodes += nodes

        count = await conn.fetchval("SELECT COUNT(*) FROM classification_system")
        print(f"\nTotal systems in DB: {count}")
        print(f"Nodes inserted this run: {total_nodes:,}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
