#!/usr/bin/env python3
"""Simple helpers to look up Wikidata QIDs for animals.

The previous matcher used large batched queries and fetched many fields at
once.  For now we keep things minimal and issue one SPARQL request per name
and only retrieve the QID.  Additional data can be fetched later if needed.

The module exposes a ``find_qid`` helper that tries, in order:

1. The ``normalized_latin_name``.
2. Any ``alternative_latin_names``.
3. The English name via the Wikidata search API.
4. The German name via the same API.

Each step is attempted separately; no batching is performed.  Only the QID is
returned.  When processing many animals `process_animals` updates the SQLite
database one row at a time and continues even if some lookups fail.
"""

from __future__ import annotations

import asyncio
import json
import random
import sqlite3
from typing import Iterable, Optional

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
    """Run a SPARQL query with basic retry/backoff logic."""

    for attempt in range(MAX_ATTEMPTS):
        try:
            async with _SEM:
                resp = await client.post(
                    SPARQL_URL,
                    data={"query": query, "format": "json"},
                    headers={
                        "User-Agent": USER_AGENT,
                        "Accept": "application/sparql-results+json",
                        "Content-Type": "application/x-www-form-urlencoded",
                    },
                    timeout=90.0,
                )
            if resp.status_code in (429, 502, 503, 504):
                retry = resp.headers.get("Retry-After")
                try:
                    delay = float(retry) if retry is not None else None
                except ValueError:
                    delay = None
                if delay is None:
                    delay = (2**attempt) + random.random()
                await asyncio.sleep(delay)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            await asyncio.sleep((2**attempt) + random.random())
    return {}


async def sparql_search_taxon(client: httpx.AsyncClient, name: str) -> Optional[str]:
    """Return the QID for a scientific name using P225 lookup."""

    if not name:
        return None
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


async def validate_qid(client: httpx.AsyncClient, qid: str) -> bool:
    """Return True if ``qid`` is an item with instance of taxon."""

    q = f"ASK {{ wd:{qid} wdt:P31 wd:Q16521 }}"
    data = await _sparql(client, q)
    return bool(data.get("boolean"))


async def search_wikidata_api(
    client: httpx.AsyncClient, name: str, *, lang: str = "en", limit: int = 10
) -> list[str]:
    """Use the Wikidata search API to look up candidate QIDs."""

    delay = 2.0
    params = {
        "action": "wbsearchentities",
        "search": name,
        "language": lang,
        "format": "json",
        "type": "item",
        "limit": str(limit),
    }
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = await client.get(API_URL, params=params, headers={"User-Agent": USER_AGENT})
            if resp.status_code == 429:
                await asyncio.sleep(delay)
                delay *= 2
                continue
            resp.raise_for_status()
            data = resp.json()
            return [entry["id"] for entry in data.get("search", [])]
        except Exception:
            if attempt == MAX_ATTEMPTS:
                break
            await asyncio.sleep(delay)
            delay *= 2
    return []


def _iter_alt_names(animal: dict) -> Iterable[str]:
    raw = animal.get("alternative_latin_names") or "[]"
    try:
        alts = json.loads(raw)
    except json.JSONDecodeError:
        alts = []
    for a in alts:
        if a:
            yield a


async def find_qid(client: httpx.AsyncClient, animal: dict) -> Optional[str]:
    """Find the best QID for a single animal.

    Only the QID is returned; no other fields are fetched.
    """

    names = [animal.get("normalized_latin_name")] + list(_iter_alt_names(animal))
    for name in names:
        qid = await sparql_search_taxon(client, name)
        if qid:
            return qid

    if animal.get("name_en"):
        for qid in await search_wikidata_api(client, animal["name_en"], lang="en"):
            if await validate_qid(client, qid):
                return qid

    if animal.get("name_de"):
        for qid in await search_wikidata_api(client, animal["name_de"], lang="de"):
            if await validate_qid(client, qid):
                return qid

    return None


async def process_animals(*, db_path: str = DB_FILE, client: httpx.AsyncClient | None = None) -> None:
    """Look up QIDs for animals in the database one by one."""

    own_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=90.0, headers={"User-Agent": USER_AGENT})

    conn = sqlite3.connect(db_path)
    ensure_db_schema(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT art, normalized_latin_name, alternative_latin_names, name_en, name_de
        FROM animal
        WHERE klasse < 6
          AND qualifier IS NULL
          AND qualifier_target IS NULL
          AND locality IS NULL
          AND trade_code IS NULL
          AND wikidata_qid IS NULL
        ORDER BY zoo_count DESC
        """
    )
    rows = cur.fetchall()

    for art, latin, alt_json, name_en, name_de in rows:
        animal = {
            "normalized_latin_name": latin,
            "alternative_latin_names": alt_json,
            "name_en": name_en,
            "name_de": name_de,
        }
        try:
            qid = await find_qid(client, animal)
        except Exception:
            qid = None
        status = "auto" if qid else "none"
        with conn:
            conn.execute(
                "UPDATE animal SET wikidata_qid=?, wikidata_match_status=? WHERE art=?",
                (qid, status, art),
            )

    conn.close()
    if own_client:
        await client.aclose()


if __name__ == "__main__":  # pragma: no cover - manual invocation helper
    asyncio.run(process_animals())

