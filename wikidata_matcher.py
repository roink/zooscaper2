#!/usr/bin/env python3
"""Minimal per-item Wikidata matcher.

This version sends one SPARQL query per animal and only retrieves the QID of
the matching taxon. It falls back to searching by English or German name via
the Wikidata API when a direct scientific-name match is not found. Results are
written back to the SQLite database.
"""

from __future__ import annotations

import asyncio
import json
import random
import sqlite3
from typing import Dict, Optional, Tuple

import httpx

from zootier_scraper_sqlite import DB_FILE, ensure_db_schema


SPARQL_URL = "https://query.wikidata.org/sparql"
API_URL = "https://www.wikidata.org/w/api.php"
USER_AGENT = "ZooTracker/1.0 (contact: contact@zootracker.app)"

MAX_ATTEMPTS = 5
_SEM = asyncio.Semaphore(2)


def _escape_for_sparql_literal(s: str) -> str:
    """Escape backslashes and quotes for SPARQL string literals."""

    return s.replace("\\", "\\\\").replace('"', '\\"')


async def _sparql(client: httpx.AsyncClient, query: str) -> dict:
    """Run a SPARQL query with basic retry logic."""

    for attempt in range(MAX_ATTEMPTS):
        try:
            async with _SEM:
                r = await client.get(
                    SPARQL_URL,
                    params={"query": query, "format": "json"},
                    headers={
                        "User-Agent": USER_AGENT,
                        "Accept": "application/sparql-results+json",
                    },
                )
            if r.status_code in (429, 502, 503, 504):
                retry = r.headers.get("Retry-After")
                delay = float(retry) if retry else (2**attempt) + random.random()
                await asyncio.sleep(delay)
                continue
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            await asyncio.sleep((2**attempt) + random.random())
    return {}


async def _sparql_taxon_by_p225(client: httpx.AsyncClient, name: str) -> Optional[str]:
    q = f"""
SELECT ?item WHERE {{
  ?item wdt:P31 wd:Q16521;
        wdt:P225 "{_escape_for_sparql_literal(name)}".
}} LIMIT 1
"""
    data = await _sparql(client, q)
    bindings = data.get("results", {}).get("bindings", [])
    if bindings:
        return bindings[0]["item"]["value"].rsplit("/", 1)[-1]
    return None


async def _sparql_taxon_by_label(
    client: httpx.AsyncClient, name: str, lang: str
) -> Optional[str]:
    q = f"""
SELECT ?item WHERE {{
  ?item wdt:P31 wd:Q16521;
        rdfs:label "{_escape_for_sparql_literal(name)}"@{lang}.
}} LIMIT 1
"""
    data = await _sparql(client, q)
    bindings = data.get("results", {}).get("bindings", [])
    if bindings:
        return bindings[0]["item"]["value"].rsplit("/", 1)[-1]
    return None


async def search_wikidata_api(client: httpx.AsyncClient, name: str, lang: str) -> list[str]:
    """Search the Wikidata API for a term and return QID candidates."""

    delay = 2.0
    for _ in range(MAX_ATTEMPTS):
        try:
            r = await client.get(
                API_URL,
                params={
                    "action": "wbsearchentities",
                    "search": name,
                    "language": lang,
                    "format": "json",
                    "type": "item",
                    "limit": 5,
                },
                headers={"User-Agent": USER_AGENT},
            )
            if r.status_code == 429:
                await asyncio.sleep(delay)
                delay *= 2
                continue
            r.raise_for_status()
            return [e["id"] for e in r.json().get("search", [])]
        except Exception:
            break
    return []


async def validate_qid(
    client: httpx.AsyncClient, latin_name: str, qid: str
) -> bool:
    """Check that a QID represents the given scientific name."""

    q = f"""
SELECT ?tn WHERE {{
  wd:{qid} wdt:P31 wd:Q16521;
             wdt:P225 ?tn.
}} LIMIT 1
"""
    data = await _sparql(client, q)
    bindings = data.get("results", {}).get("bindings", [])
    if bindings:
        return bindings[0]["tn"]["value"].casefold() == latin_name.casefold()
    return False


async def find_qid(
    client: httpx.AsyncClient, animal: Dict[str, Optional[str]]
) -> Tuple[Optional[str], Optional[str]]:
    """Return `(qid, method)` for a single animal."""

    latin = animal.get("normalized_latin_name")
    if latin:
        qid = await _sparql_taxon_by_p225(client, latin)
        if qid:
            return qid, "p225"

    try:
        alts = [a for a in json.loads(animal.get("alternative_latin_names") or "[]") if a]
    except json.JSONDecodeError:
        alts = []
    for alt in alts:
        qid = await _sparql_taxon_by_p225(client, alt)
        if qid:
            return qid, "p225_alt"

    if animal.get("name_en"):
        qid = await _sparql_taxon_by_label(client, animal["name_en"], "en")
        if qid and latin and await validate_qid(client, latin, qid):
            return qid, "label_en"

    if animal.get("name_de"):
        qid = await _sparql_taxon_by_label(client, animal["name_de"], "de")
        if qid and latin and await validate_qid(client, latin, qid):
            return qid, "label_de"

    for name, lang in ((animal.get("name_en"), "en"), (animal.get("name_de"), "de")):
        if name:
            for qid in await search_wikidata_api(client, name, lang):
                if latin and await validate_qid(client, latin, qid):
                    return qid, f"api_{lang}"

    return None, None


async def process_animals(
    db_path: str = DB_FILE, client: Optional[httpx.AsyncClient] = None
) -> None:
    """Find and store Wikidata QIDs for eligible animals."""

    conn = sqlite3.connect(db_path)
    ensure_db_schema(conn)
    rows = conn.execute(
        """
        SELECT art, normalized_latin_name, alternative_latin_names, name_en, name_de
        FROM animal
        WHERE klasse < 6
          AND qualifier IS NULL
          AND qualifier_target IS NULL
          AND locality IS NULL
          AND trade_code IS NULL
          AND wikidata_qid IS NULL
        ORDER BY zoo_count DESC, art
        """
    ).fetchall()

    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=httpx.Timeout(90.0))
        own_client = True

    assigned: Dict[str, str] = {}
    try:
        for art, latin, alts, name_en, name_de in rows:
            animal = {
                "normalized_latin_name": latin,
                "alternative_latin_names": alts,
                "name_en": name_en,
                "name_de": name_de,
            }
            qid, method = await find_qid(client, animal)
            status = "auto" if qid else "none"
            if qid:
                if qid in assigned:
                    status = "review"
                    qid = None
                else:
                    assigned[qid] = art
            conn.execute(
                "UPDATE animal SET wikidata_qid=?, wikidata_match_status=?, wikidata_match_method=? WHERE art=?",
                (qid, status, method, art),
            )
            conn.commit()
    finally:
        conn.close()
        if own_client:
            await client.aclose()


__all__ = ["find_qid", "process_animals"]

