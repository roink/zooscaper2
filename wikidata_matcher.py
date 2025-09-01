#!/usr/bin/env python3
"""Simplified Wikidata matcher.

For each animal in the database, query Wikidata individually to find a
matching QID. The lookup tries a direct SPARQL P225 match first and falls
back to the `wbsearchentities` API. Only the QID is fetched; additional
labels or metadata are ignored.
"""

from __future__ import annotations

import asyncio
import json
import random
import sqlite3
from typing import Optional, Sequence

import httpx

from zootier_scraper_sqlite import DB_FILE, ensure_db_schema

SPARQL_URL = "https://query.wikidata.org/sparql"
API_URL = "https://www.wikidata.org/w/api.php"
USER_AGENT = "ZooTracker/1.0 (contact: contact@zootracker.app)"
MAX_API_RETRIES = 5
MAX_ATTEMPTS = 5
_SEM = asyncio.Semaphore(2)


async def _sparql(client: httpx.AsyncClient, query: str) -> dict:
    for attempt in range(MAX_ATTEMPTS):
        try:
            await asyncio.sleep(random.uniform(0.05, 0.15))
            async with _SEM:
                r = await client.post(
                    SPARQL_URL,
                    data={"query": query, "format": "json"},
                    headers={
                        "User-Agent": USER_AGENT,
                        "Accept": "application/sparql-results+json",
                        "Content-Type": "application/x-www-form-urlencoded",
                    },
                    timeout=90.0,
                )
            if r.status_code in (429, 502, 503, 504):
                ra = r.headers.get("Retry-After")
                delay = float(ra) if ra and ra.isdigit() else (2**attempt) + random.random()
                await asyncio.sleep(delay)
                continue
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            await asyncio.sleep((2**attempt) + random.random())
    return {}


def _escape(name: str) -> str:
    """Escape for inclusion inside single-quoted SPARQL string literals."""
    return name.replace("\\", "\\\\").replace("'", "\\'")


async def _sparql_taxon(client: httpx.AsyncClient, name: str) -> Optional[str]:
    """Return the QID for a taxon with exact P225 ``name``."""
    query = f"""
SELECT ?item WHERE {{
  ?item wdt:P31 wd:Q16521;
        wdt:P225 '{_escape(name)}'.
}} LIMIT 1
"""
    data = await _sparql(client, query)
    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        return None
    return bindings[0]["item"]["value"].rsplit("/", 1)[-1]


async def _search_wikidata_api(
    client: httpx.AsyncClient, name: str, language: str = "en", limit: int = 10
) -> Sequence[str]:
    delay = 1.0
    params = {
        "action": "wbsearchentities",
        "search": name,
        "language": language,
        "format": "json",
        "type": "item",
        "limit": limit,
    }
    for _ in range(MAX_API_RETRIES):
        r = await client.get(API_URL, params=params, headers={"User-Agent": USER_AGENT})
        if r.status_code == 429:
            await asyncio.sleep(delay)
            delay *= 2
            continue
        r.raise_for_status()
        return [entry["id"] for entry in r.json().get("search", [])]
    return []


async def _validate_qid(
    client: httpx.AsyncClient, latin_names: Sequence[str], qid: str
) -> bool:
    """Check that ``qid`` is a taxon whose P225 matches one of ``latin_names``."""
    query = f"""
SELECT ?tn WHERE {{
  wd:{qid} wdt:P31 wd:Q16521;
         wdt:P225 ?tn.
}} LIMIT 1
"""
    data = await _sparql(client, query)
    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        return False
    tn = bindings[0]["tn"]["value"].lower()
    return any(tn == ln.lower() for ln in latin_names)


def _expected_rank_token_count(name: str) -> int:
    return len((name or "").split())


async def _rank_ok(client: httpx.AsyncClient, qid: str, expected_tokens: int) -> bool:
    query = f"SELECT ?rank WHERE {{ wd:{qid} wdt:P105 ?rank }}"
    data = await _sparql(client, query)
    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        return True
    rank = bindings[0]["rank"]["value"].rsplit("/", 1)[-1]
    if expected_tokens == 2:
        return rank in ("Q7432", "Q68947")
    if expected_tokens == 3:
        return rank == "Q68947"
    return True


def _save(conn, art, qid, status, method=None, score=None):
    try:
        conn.execute(
            "UPDATE animal SET wikidata_qid=?, wikidata_match_status=?, wikidata_match_method=?, wikidata_match_score=? WHERE art=?",
            (qid, status, method, score, art),
        )
    except sqlite3.IntegrityError:
        conn.rollback()
        conn.execute(
            "UPDATE animal SET wikidata_qid=?, wikidata_match_status=?, wikidata_match_method=?, wikidata_match_score=? WHERE art=?",
            (None, "collision", method, score, art),
        )


def _store_candidates(conn, art, candidates, method, debug=None):
    for q in candidates[:5]:
        conn.execute(
            "INSERT OR REPLACE INTO animal_wikidata_candidates (art, candidate_qid, score, method, debug) VALUES (?,?,?,?,?)",
            (art, q, None, method, json.dumps(debug or {})),
        )


async def find_qid(client: httpx.AsyncClient, animal: dict) -> tuple[Optional[str], Optional[str], Optional[int], list[str]]:
    """Return (qid, method, score, rejected_candidates)."""
    latin_names: list[str] = []
    if animal.get("normalized_latin_name"):
        latin_names.append(animal["normalized_latin_name"])
    try:
        latin_names.extend(
            [n for n in json.loads(animal.get("alternative_latin_names") or "[]") if n]
        )
    except json.JSONDecodeError:
        pass

    rejected: list[str] = []
    for idx, name in enumerate(latin_names):
        qid = await _sparql_taxon(client, name)
        if qid:
            method = "p225_exact_primary" if idx == 0 else "p225_exact_alt"
            score = 95 if idx == 0 else 90
            return qid, method, score, []
        api_candidates = await _search_wikidata_api(client, name)
        rejected.extend(api_candidates)
        for candidate in api_candidates:
            if await _validate_qid(client, latin_names, candidate):
                return candidate, "api_p225_validated_en", 85, []

    for field, lang in (("name_en", "en"), ("name_de", "de")):
        if not animal.get(field):
            continue
        api_candidates = await _search_wikidata_api(client, animal[field], language=lang)
        rejected.extend(api_candidates)
        for candidate in api_candidates:
            if await _validate_qid(client, latin_names, candidate):
                return candidate, f"api_p225_validated_{lang}", 85, []
    return None, None, None, rejected


async def process_animals(
    db_path: str = DB_FILE, client: httpx.AsyncClient | None = None
) -> None:
    """Process all eligible animals and update their Wikidata QIDs."""
    conn = sqlite3.connect(db_path)
    ensure_db_schema(conn)
    cur = conn.cursor()
    cur.execute(
        """
SELECT art, normalized_latin_name, alternative_latin_names, name_en, name_de, wikidata_qid
FROM animal
WHERE klasse < 6
  AND qualifier IS NULL
  AND qualifier_target IS NULL
  AND locality IS NULL
  AND trade_code IS NULL
  AND wikidata_match_status IS NULL
ORDER BY zoo_count DESC
"""
    )
    rows = cur.fetchall()
    assigned: set[str] = {
        qid
        for (qid,) in conn.execute(
            "SELECT wikidata_qid FROM animal WHERE wikidata_qid IS NOT NULL"
        )
    }
    http_client = client or httpx.AsyncClient(timeout=90)
    if client is None:
        await http_client.__aenter__()
    try:
        http_client.headers.update({"User-Agent": USER_AGENT})
        for art, latin, alts, name_en, name_de, existing_qid in rows:
            if existing_qid:
                expected = _expected_rank_token_count(latin)
                status = (
                    "auto"
                    if await _rank_ok(http_client, existing_qid, expected)
                    else "review"
                )
                _save(
                    conn,
                    art,
                    existing_qid,
                    status,
                    method="existing_qid",
                    score=100,
                )
                assigned.add(existing_qid)
                conn.commit()
                continue
            animal = {
                "normalized_latin_name": latin,
                "alternative_latin_names": alts,
                "name_en": name_en,
                "name_de": name_de,
            }
            try:
                qid, method, score, candidates = await find_qid(http_client, animal)
            except Exception:
                qid = method = score = None
                candidates = []
            if qid and qid not in assigned:
                expected = _expected_rank_token_count(latin)
                status = (
                    "auto" if await _rank_ok(http_client, qid, expected) else "review"
                )
                _save(conn, art, qid, status, method, score)
                assigned.add(qid)
            elif qid:
                print(f"collision for {art}: {qid}")
                _save(conn, art, None, "collision")
            else:
                _save(conn, art, None, "none")
                if candidates:
                    _store_candidates(conn, art, candidates, "api_search")
            conn.commit()
    finally:
        if client is None:
            await http_client.__aexit__(None, None, None)
        conn.close()


if __name__ == "__main__":  # pragma: no cover - manual invocation
    asyncio.run(process_animals())
