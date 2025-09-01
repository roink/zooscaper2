#!/usr/bin/env python3
"""Match animals in the SQLite database to Wikidata items.

This script batches SPARQL queries against Wikidata, scores candidates and
stores the best match for each animal. Ambiguous or missing matches are marked
for later review.
"""

from __future__ import annotations

import asyncio
import json
import random
import re
import sqlite3
import unicodedata
from typing import Dict, List, Optional

import httpx

from zootier_scraper_sqlite import DB_FILE, ensure_db_schema

SPARQL_URL = "https://query.wikidata.org/sparql"
USER_AGENT = "ZooTracker/1.0 (contact: your-email@example.org)"

SPECIES_QID = "Q7432"
SUBSPECIES_QID = "Q68947"
ACCEPTED_NAME_QID = "Q3958441"

_CACHE_P225: Dict[str, List[dict]] = {}
_CACHE_LABEL: Dict[tuple[str, str], List[dict]] = {}
_SEM = asyncio.Semaphore(4)


def _escape_for_sparql_literal(s: str) -> str:
    """Escape backslashes and quotes for SPARQL string literals."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _norm(name: str) -> str:
    return unicodedata.normalize("NFC", re.sub(r"\s+", " ", name.strip())).casefold()


async def _sparql(client: httpx.AsyncClient, query: str) -> dict:
    for attempt in range(3):
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
                    timeout=30.0,
                )
            if r.status_code in (429, 503):
                raise httpx.HTTPStatusError("retry", request=r.request, response=r)
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == 2:
                raise
            await asyncio.sleep((2 ** attempt) + random.random())
    return {}


def _infer_expected_rank(latin: str) -> str | None:
    toks = latin.split()
    return SPECIES_QID if len(toks) == 2 else (SUBSPECIES_QID if len(toks) == 3 else None)


def _score_candidate(animal, cand) -> int:
    expected = _infer_expected_rank(animal["normalized_latin_name"] or "")
    score = 0
    exact = False
    if cand["taxon_name"] and cand["taxon_name"].lower() == (animal["normalized_latin_name"] or "").lower():
        score += 80
        exact = True
    if expected and cand.get("rank") == expected:
        score += 20
    elif cand.get("rank") and cand["rank"] not in {SPECIES_QID, SUBSPECIES_QID}:
        score -= 80
    elif expected and cand.get("rank") and cand["rank"] != expected:
        score -= 60

    if animal.get("name_en") and cand.get("label_en") and cand["label_en"].lower() == animal["name_en"].lower():
        score += 15
    if animal.get("name_de") and cand.get("label_de") and cand["label_de"].lower() == animal["name_de"].lower():
        score += 15

    if animal.get("name_en"):
        if any(v.lower() == animal["name_en"].lower() for v in cand.get("vern_en", [])):
            score += 10
    if animal.get("name_de"):
        if any(v.lower() == animal["name_de"].lower() for v in cand.get("vern_de", [])):
            score += 10

    try:
        alts = json.loads(animal.get("alternative_latin_names") or "[]")
    except json.JSONDecodeError:
        alts = []
    if any(cand.get("taxon_name", "").lower() == a.lower() for a in alts):
        score += 40
    if cand.get("status") == ACCEPTED_NAME_QID:
        score += 15

    if not exact and score > 59:
        score = 59
    return score


def _pick_best(cands: List[dict]) -> dict | None:
    cands.sort(key=lambda c: c["score"], reverse=True)
    if not cands:
        return None
    if len(cands) > 1 and cands[1]["score"] >= cands[0]["score"] - 5 and cands[1]["qid"] != cands[0]["qid"]:
        return None
    return cands[0]


async def _sparql_batch_p225(client: httpx.AsyncClient, names: List[str], method: str) -> dict[str, List[dict]]:
    result: dict[str, List[dict]] = {}
    names = [n for n in names if n]
    if not names:
        return {}
    to_query: List[str] = []
    seen = set()
    for n in names:
        key = _norm(n)
        if key not in _CACHE_P225 and key not in seen:
            to_query.append(n)
            seen.add(key)
    for i in range(0, len(to_query), 40):
        chunk = to_query[i : i + 40]
        values = " ".join(
            f'("{_escape_for_sparql_literal(c)}")' for c in chunk
        )
        q = f"""
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?input ?item ?taxonName ?rank ?status ?enLabel ?deLabel ?vernEn ?vernDe WHERE {{
  VALUES (?input) {{ {values} }}
  ?item wdt:P31 wd:Q16521; wdt:P225 ?taxonName.
  FILTER(lcase(?taxonName)=lcase(?input))
  OPTIONAL{{?item wdt:P105 ?rank}}
  OPTIONAL{{?item wdt:P2316 ?status}}
  OPTIONAL{{?item rdfs:label ?enLabel FILTER(lang(?enLabel)="en")}}
  OPTIONAL{{?item rdfs:label ?deLabel FILTER(lang(?deLabel)="de")}}
  OPTIONAL{{?item wdt:P1843 ?vernEn FILTER(lang(?vernEn)="en")}}
  OPTIONAL{{?item wdt:P1843 ?vernDe FILTER(lang(?vernDe)="de")}}
}}"""
        data = await _sparql(client, q)
        for b in data.get("results", {}).get("bindings", []):
            inp = b["input"]["value"]
            key = _norm(inp)
            cand = {
                "qid": b["item"]["value"].rsplit("/", 1)[-1],
                "taxon_name": b["taxonName"]["value"],
                "rank": b.get("rank", {}).get("value", "").rsplit("/", 1)[-1] or None,
                "status": b.get("status", {}).get("value", "").rsplit("/", 1)[-1] or None,
                "label_en": b.get("enLabel", {}).get("value"),
                "label_de": b.get("deLabel", {}).get("value"),
                "vern_en": [b["vernEn"]["value"]] if "vernEn" in b else [],
                "vern_de": [b["vernDe"]["value"]] if "vernDe" in b else [],
                "method": method,
            }
            _CACHE_P225.setdefault(key, []).append(cand)
    for n in names:
        key = _norm(n)
        result[n] = list(_CACHE_P225.get(key, []))
    return result


async def _sparql_batch_label_or_vern(client: httpx.AsyncClient, names: List[str], lang: str, method: str) -> dict[str, List[dict]]:
    result: dict[str, List[dict]] = {}
    names = [n for n in names if n]
    if not names:
        return {}
    to_query: List[str] = []
    seen = set()
    for n in names:
        key = (lang, _norm(n))
        if key not in _CACHE_LABEL and key not in seen:
            to_query.append(n)
            seen.add(key)
    for i in range(0, len(to_query), 40):
        chunk = to_query[i : i + 40]
        values = " ".join(
            f'("{_escape_for_sparql_literal(c)}")' for c in chunk
        )
        q = f"""
PREFIX wdt:<http://www.wikidata.org/prop/direct/>
PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
SELECT ?input ?item ?taxonName ?rank ?status ?enLabel ?deLabel ?vernEn ?vernDe WHERE {{
  VALUES (?input) {{ {values} }}
  ?item wdt:P31 wd:Q16521.
  OPTIONAL{{?item wdt:P225 ?taxonName}}
  OPTIONAL{{?item wdt:P105 ?rank}}
  OPTIONAL{{?item wdt:P2316 ?status}}
  {{
    ?item rdfs:label ?lab . FILTER(lang(?lab)="{lang}") FILTER(lcase(?lab)=lcase(?input))
  }} UNION {{
    ?item wdt:P1843 ?vern . FILTER(lang(?vern)="{lang}") FILTER(lcase(?vern)=lcase(?input))
  }}
  OPTIONAL{{?item rdfs:label ?enLabel FILTER(lang(?enLabel)="en")}}
  OPTIONAL{{?item rdfs:label ?deLabel FILTER(lang(?deLabel)="de")}}
  OPTIONAL{{?item wdt:P1843 ?vernEn FILTER(lang(?vernEn)="en")}}
  OPTIONAL{{?item wdt:P1843 ?vernDe FILTER(lang(?vernDe)="de")}}
}}"""
        data = await _sparql(client, q)
        for b in data.get("results", {}).get("bindings", []):
            inp = b["input"]["value"]
            key_cache = (lang, _norm(inp))
            cand = {
                "qid": b["item"]["value"].rsplit("/", 1)[-1],
                "taxon_name": b.get("taxonName", {}).get("value"),
                "rank": b.get("rank", {}).get("value", "").rsplit("/", 1)[-1] or None,
                "status": b.get("status", {}).get("value", "").rsplit("/", 1)[-1] or None,
                "label_en": b.get("enLabel", {}).get("value"),
                "label_de": b.get("deLabel", {}).get("value"),
                "vern_en": [b["vernEn"]["value"]] if "vernEn" in b else [],
                "vern_de": [b["vernDe"]["value"]] if "vernDe" in b else [],
                "method": method,
            }
            _CACHE_LABEL.setdefault(key_cache, []).append(cand)
    for n in names:
        key_cache = (lang, _norm(n))
        result[n] = list(_CACHE_LABEL.get(key_cache, []))
    return result


async def find_qid(client: httpx.AsyncClient, animal: dict[str, Optional[str]]) -> tuple[Optional[str], Optional[int]]:
    """Return the best QID and score for a single animal without DB writes."""
    names = []
    if animal.get("normalized_latin_name"):
        names.append(animal["normalized_latin_name"])
    alts: List[str] = []
    try:
        alts = [a for a in json.loads(animal.get("alternative_latin_names") or "[]") if a]
    except json.JSONDecodeError:
        pass
    cands: List[dict] = []
    if names:
        res = await _sparql_batch_p225(client, names, "p225_exact")
        cands.extend(res.get(names[0], []))
    if alts:
        res = await _sparql_batch_p225(client, alts, "p225_alt_exact")
        for a in alts:
            cands.extend(res.get(a, []))
    if animal.get("name_en"):
        res = await _sparql_batch_label_or_vern(client, [animal["name_en"]], "en", "label_en")
        cands.extend(res.get(animal["name_en"], []))
    if animal.get("name_de"):
        res = await _sparql_batch_label_or_vern(client, [animal["name_de"]], "de", "label_de")
        cands.extend(res.get(animal["name_de"], []))
    by_qid: Dict[str, dict] = {}
    for c in cands:
        q = c["qid"]
        if q in by_qid:
            by_qid[q]["vern_en"] = list({*by_qid[q]["vern_en"], *c.get("vern_en", [])})
            by_qid[q]["vern_de"] = list({*by_qid[q]["vern_de"], *c.get("vern_de", [])})
        else:
            by_qid[q] = {**c}
    cands = list(by_qid.values())
    for c in cands:
        c["score"] = _score_candidate(animal, c)
    best = _pick_best(cands)
    return (best["qid"], best["score"]) if best else (None, None)


async def process_animals(db_path: str = DB_FILE, client: Optional[httpx.AsyncClient] = None) -> None:
    conn = sqlite3.connect(db_path)
    ensure_db_schema(conn)
    read_cur = conn.cursor()
    rows = read_cur.execute(
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
    animals = [
        {
            "art": art,
            "normalized_latin_name": nln,
            "alternative_latin_names": alts,
            "name_en": name_en,
            "name_de": name_de,
        }
        for art, nln, alts, name_en, name_de in rows
    ]
    assigned: Dict[str, tuple[str, int]] = {}
    own_client = False
    if client is None:
        client = httpx.AsyncClient(
            timeout=30,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )
        own_client = True
    try:
        for i in range(0, len(animals), 300):
            batch = animals[i : i + 300]
            names_latin = [a["normalized_latin_name"] for a in batch if a["normalized_latin_name"]]
            p225_res = await _sparql_batch_p225(client, names_latin, "p225_exact")
            alt_names: List[str] = []
            for a in batch:
                try:
                    alts = json.loads(a.get("alternative_latin_names") or "[]")
                except json.JSONDecodeError:
                    alts = []
                alt_names.extend(alts)
            alt_names = list(dict.fromkeys([a for a in alt_names if a]))
            p225_alt_res = await _sparql_batch_p225(client, alt_names, "p225_alt_exact")
            en_names = [a["name_en"] for a in batch if a["name_en"]]
            de_names = [a["name_de"] for a in batch if a["name_de"]]
            label_en_res = await _sparql_batch_label_or_vern(client, en_names, "en", "label_en")
            label_de_res = await _sparql_batch_label_or_vern(client, de_names, "de", "label_de")

            with conn:
                write_cur = conn.cursor()
                for animal in batch:
                    art = animal["art"]
                    cands: List[dict] = []
                    name = animal.get("normalized_latin_name")
                    if name:
                        cands.extend(p225_res.get(name, []))
                    try:
                        alts = json.loads(animal.get("alternative_latin_names") or "[]")
                    except json.JSONDecodeError:
                        alts = []
                    for alt in alts:
                        cands.extend(p225_alt_res.get(alt, []))
                    if animal.get("name_en"):
                        cands.extend(label_en_res.get(animal["name_en"], []))
                    if animal.get("name_de"):
                        cands.extend(label_de_res.get(animal["name_de"], []))

                    by_qid: Dict[str, dict] = {}
                    for cand in cands:
                        qid = cand["qid"]
                        if qid in by_qid:
                            by_qid[qid]["vern_en"] = list(
                                set(by_qid[qid]["vern_en"] + cand["vern_en"])
                            )
                            by_qid[qid]["vern_de"] = list(
                                set(by_qid[qid]["vern_de"] + cand["vern_de"])
                            )
                        else:
                            by_qid[qid] = cand
                    cands = list(by_qid.values())
                    for cand in cands:
                        cand["score"] = _score_candidate(animal, cand)
                    cands.sort(key=lambda c: c["score"], reverse=True)
                    best = _pick_best(list(cands))
                    score = best["score"] if best else None
                    status = "none"
                    if best:
                        if score is not None and score >= 90:
                            status = "auto"
                        elif score is not None and score >= 60:
                            status = "review"
                    elif cands:
                        status = "review"
                    qid: Optional[str] = best["qid"] if best and status == "auto" else None
                    top5 = cands[:5]
                    for cand in top5:
                        write_cur.execute(
                            "INSERT OR REPLACE INTO animal_wikidata_candidates (art, candidate_qid, score, method, debug) VALUES (?,?,?,?,?)",
                            (
                                art,
                                cand["qid"],
                                cand["score"],
                                cand.get("method"),
                                json.dumps(cand, ensure_ascii=False),
                            ),
                        )
                    write_cur.execute(
                        "UPDATE animal SET wikidata_review_json=? WHERE art=?",
                        (json.dumps(top5, ensure_ascii=False), art),
                    )
                    if status == "auto" and qid:
                        if qid in assigned:
                            prev_art, prev_score = assigned[qid]
                            if score is not None and score > prev_score:
                                write_cur.execute(
                                    "UPDATE animal SET wikidata_qid=NULL, wikidata_match_status='review' WHERE art=?",
                                    (prev_art,),
                                )
                                assigned[qid] = (art, score)
                            else:
                                status = "review"
                                qid = None
                        else:
                            assigned[qid] = (art, score or 0)
                    chosen_method = best.get("method") if best else None
                    write_cur.execute(
                        "UPDATE animal SET wikidata_qid=?, wikidata_match_status=?, wikidata_match_method=?, wikidata_match_score=? WHERE art=?",
                        (qid, status, chosen_method, score, art),
                    )
    finally:
        if own_client:
            await client.aclose()
        conn.close()


if __name__ == "__main__":  # pragma: no cover - manual invocation
    asyncio.run(process_animals())
