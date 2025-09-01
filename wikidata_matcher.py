#!/usr/bin/env python3
"""Simplified helpers to match animals to Wikidata QIDs.

Each animal is looked up individually. First we try a direct SPARQL
`P225` match; if that fails we fall back to the Wikidata search API and
validate candidates by checking their taxon name. Only the QID is stored
in the database – additional details can be fetched later.
"""

from __future__ import annotations

import asyncio
import sqlite3
from typing import List, Optional

import httpx

from zootier_scraper_sqlite import DB_FILE, ensure_db_schema

SPARQL_URL = "https://query.wikidata.org/sparql"
API_URL = "https://www.wikidata.org/w/api.php"
USER_AGENT = "ZooTracker/1.0 (contact: contact@zootracker.app)"
HEADERS = {"User-Agent": USER_AGENT}


async def sparql_search_taxon(client: httpx.AsyncClient, name: str) -> Optional[str]:
    """Return the QID of a taxon whose `P225` matches ``name`` exactly."""
    if not name:
        return None
    query = f"""
SELECT ?item WHERE {{
  ?item wdt:P31 wd:Q16521;
        wdt:P225 \"{_escape_for_sparql_literal(name)}\".
}} LIMIT 1
"""
    r = await client.get(
        SPARQL_URL,
        params={"query": query, "format": "json"},
        headers={"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"},
    )
    r.raise_for_status()
    bindings = r.json().get("results", {}).get("bindings", [])
    if bindings:
        return bindings[0]["item"]["value"].rsplit("/", 1)[-1]
    return None


async def search_wikidata_api(client: httpx.AsyncClient, name: str, limit: int = 10) -> List[str]:
    """Search the Wikidata API for ``name`` and return candidate QIDs."""
    delay = 2.0
    params = {
        "action": "wbsearchentities",
        "search": name,
        "language": "en",
        "format": "json",
        "type": "item",
        "limit": str(limit),
    }
    for _ in range(5):
        r = await client.get(API_URL, params=params, headers=HEADERS, timeout=10.0)
        if r.status_code == 429:
            await asyncio.sleep(delay)
            delay *= 2
            continue
        r.raise_for_status()
        return [entry["id"] for entry in r.json().get("search", [])]
    return []


async def validate_qid(client: httpx.AsyncClient, name: str, qid: str) -> bool:
    """Check that ``qid`` represents a taxon with scientific name ``name``."""
    query = f"""
SELECT ?tn WHERE {{
  wd:{qid} wdt:P31 wd:Q16521;
             wdt:P225 ?tn.
}} LIMIT 1
"""
    r = await client.get(
        SPARQL_URL,
        params={"query": query, "format": "json"},
        headers={"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"},
    )
    r.raise_for_status()
    bindings = r.json().get("results", {}).get("bindings", [])
    if bindings:
        return bindings[0]["tn"]["value"].lower() == name.lower()
    return False


async def find_qid(client: httpx.AsyncClient, latin_name: str) -> Optional[str]:
    """Resolve ``latin_name`` to a Wikidata QID."""
    if not latin_name:
        return None
    qid = await sparql_search_taxon(client, latin_name)
    if qid:
        return qid
    for cand in await search_wikidata_api(client, latin_name):
        if await validate_qid(client, latin_name, cand):
            return cand
    return None


async def process_animals(db_path: str = DB_FILE, client: httpx.AsyncClient | None = None) -> None:
    """Look up QIDs for animals and store them in the database."""
    conn = sqlite3.connect(db_path)
    ensure_db_schema(conn)
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT art, normalized_latin_name
        FROM animal
        WHERE wikidata_qid IS NULL
          AND klasse < 6
          AND qualifier IS NULL
          AND qualifier_target IS NULL
          AND locality IS NULL
          AND trade_code IS NULL
        ORDER BY zoo_count DESC
        """
    ).fetchall()

    assigned: dict[str, str] = {}
    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=90.0, headers=HEADERS)
        close_client = True

    try:
        for art, latin in rows:
            try:
                qid = await find_qid(client, latin)
            except Exception:
                qid = None
            status = "auto" if qid else "none"
            if qid and qid in assigned:
                status = "collision"
                qid = None
            if qid:
                assigned[qid] = art
            cur.execute(
                "UPDATE animal SET wikidata_qid=?, wikidata_match_status=? WHERE art=?",
                (qid, status, art),
            )
            conn.commit()
    finally:
        if close_client:
            await client.aclose()
        conn.close()


def _escape_for_sparql_literal(s: str) -> str:
    """Escape backslashes and quotes inside a SPARQL literal."""
    return s.replace("\\", "\\\\").replace('"', '\\"')
