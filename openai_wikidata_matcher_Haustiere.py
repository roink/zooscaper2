#!/usr/bin/env python3
"""Use the OpenAI API to look up Wikidata QIDs.

This script queries the OpenAI Responses API for every animal in the
local database that is missing a ``wikidata_qid``.  The model is asked to
provide the correct Wikidata item identifier (``QID``) for the species
given its original Latin name as well as its German and English common
names.  The model must return ``null``/``unknown`` if no Wikidata entry is
found.  Returned identifiers are written back to the database only if no
other animal already uses the same QID.

The heavy lifting is delegated to :func:`process_animals` which accepts an
optional OpenAI client instance for easier testing.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Callable, Optional, Tuple

import httpx
from zootierliste_enrich_async import fetch_details, fetch_wikipedia, HEADERS

try:  # pragma: no cover - fallback for environments without python-dotenv
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - library missing
    import os

    def load_dotenv(*args, **kwargs):  # type: ignore
        dotenv_path = kwargs.get("dotenv_path") if kwargs else None
        override = kwargs.get("override", False)
        if not dotenv_path:
            return False
        try:
            with open(dotenv_path, "r") as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    key, _, value = line.partition("=")
                    if key and (override or key not in os.environ):
                        os.environ[key] = value
            return True
        except OSError:
            return False
try:  # pragma: no cover - allow running without pydantic
    from pydantic import BaseModel
except Exception:  # pragma: no cover - library missing
    class BaseModel:  # type: ignore
        pass
from zootier_scraper_sqlite import DB_FILE, ensure_db_schema

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env", override=False)


class WikidataLookup(BaseModel):
    """Structured output parsed from the model response."""

    wikidata_qid: str | None = None


class BreedLookup(BaseModel):
    """Structured output for domesticated form / breed lookup."""
    wikidata_qid: str | None = None
    wikipedia_en: str | None = None  # page title OR full URL accepted
    wikipedia_de: str | None = None  # page title OR full URL accepted

class CollisionLookup(BaseModel):
    """Structured output for resolving a QID collision."""

    existing_qid: str | None = None
    new_qid: str | None = None


_QID_RE = re.compile(r"^Q\d+$")
_RESET_COLS = [
    "wikidata_match_score",
    "wikidata_review_json",
    "wikidata_id",
    "taxon_rank",
    "parent_taxon",
    "wikipedia_en",
    "wikipedia_de",
    "iucn_conservation_status",
]


def _ensure_enrichment_columns(conn: sqlite3.Connection) -> None:
    """Add enrichment columns if they do not yet exist."""

    cur = conn.cursor()
    for col in (
        "taxon_rank TEXT",
        "parent_taxon TEXT",
        "wikipedia_en TEXT",
        "wikipedia_de TEXT",
        "iucn_conservation_status TEXT",
    ):
        try:
            cur.execute(f"ALTER TABLE animal ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass
    conn.commit()


async def _fetch_all_async(qid: str) -> dict[str, str]:
    async with httpx.AsyncClient(http2=True, headers=HEADERS, timeout=30) as client:
        results = await asyncio.gather(
            fetch_wikipedia(client, qid, "en"),
            fetch_wikipedia(client, qid, "de"),
            fetch_details(client, qid),
        )
    merged: dict[str, str] = {}
    for res in results:
        merged.update(res)
    return merged


def fetch_wikidata_enrichment(qid: str) -> dict[str, str]:
    """Fetch Wikipedia links and basic taxonomic data for *qid*."""

    try:
        return asyncio.run(_fetch_all_async(qid))
    except RuntimeError:  # event loop already running
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(_fetch_all_async(qid))


def update_enrichment(
    cur: sqlite3.Cursor, art: str, qid: str, fetch: Callable[[str], dict[str, str]] = fetch_wikidata_enrichment
) -> None:
    """Fetch metadata for *qid* and store it for *art*."""

    data = fetch(qid)
    # Enforce uniqueness for wikipedia_* before writing
    def _is_unique(col: str, title: str) -> bool:
        if not title:
            return False
        cur.execute(f"SELECT art, klasse FROM animal WHERE {col}=? AND art<>?", (title, art))
        row = cur.fetchone()
        if not row:
            return True
        # If someone else already has this link, we keep theirs—especially if klasse<6
        return False

    update = {}
    for k, v in data.items():
        if not v:
            continue
        if k == "parent_taxon" and v == "":
            continue
        if k in ("wikipedia_en", "wikipedia_de"):
            if _is_unique(k, v):
                update[k] = v
        else:
            update[k] = v
    if not update:
        return
    set_bits = ", ".join(f"{k}=?" for k in update)
    cur.execute(f"UPDATE animal SET {set_bits} WHERE art=?", (*update.values(), art))


def _apply_qid_update(
    cur: sqlite3.Cursor,
    art: str,
    qid: Optional[str],
    *,
    status: str,
    reset_fields: bool,
    clear_cols: tuple[str, ...],
    ) -> None:
    """Update a row with a new QID and optionally reset metadata."""

    set_bits = ["wikidata_qid=?", "wikidata_match_status=?", "wikidata_match_method=?"]
    params: list[object] = [qid, status, "gpt-5-mini"]
    if reset_fields:
        set_bits += [f"{c}=NULL" for c in clear_cols]
    cur.execute(
        f"UPDATE animal SET {', '.join(set_bits)} WHERE art=?",
        (*params, art),
    )


def _normalize_wiki_title(value: Optional[str]) -> Optional[str]:
    """Accept a Wikipedia page title OR full URL and return a canonical page title (spaces, decoded)."""
    if not value:
        return None
    val = value.strip()
    if not val:
        return None
    # URL form: https://XX.wikipedia.org/wiki/Page_Title or /w/index.php?title=...
    import urllib.parse as _u
    if "wikipedia.org" in val:
        try:
            # Extract after '/wiki/' or from query param 'title'
            if "/wiki/" in val:
                page = val.split("/wiki/", 1)[1]
            else:
                qs = _u.urlparse(val).query
                qp = dict(_u.parse_qsl(qs))
                page = qp.get("title")
            if not page:
                return None
            page = _u.unquote(page)
            page = page.replace("_", " ").strip()
            return page or None
        except Exception:
            return None
    # Otherwise assume it's already a title
    return val.replace("_", " ").strip() or None


def lookup_breed(
    client: Any,
    latin: str,
    name_de: Optional[str],
    name_en: Optional[str],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Query the OpenAI API for a DOMESTICATED FORM / BREED.
    Returns (qid, wikipedia_en_title, wikipedia_de_title).

    Parameters
    ----------
    client:
        An object exposing ``responses.parse`` compatible with the OpenAI
        Python SDK.
    latin, name_de, name_en:
        Names used to query the model. The latin name may match the wild form;
        prefer domesticated/breed items.
    """

    prompt = (
        "You are matching DOMESTICATED FORMS / BREEDS of animals (not the wild species). "
        "Given the names below, return:\n"
        "• wikidata_qid: The QID for the domesticated form OR specific breed (NOT the wild species item). If none exists, return null.\n"
        "• wikipedia_en: The exact EN Wikipedia page title or URL for the domesticated form/breed, if such a page exists; otherwise null.\n"
        "• wikipedia_de: The exact DE Wikipedia page title or URL for the domesticated form/breed, if such a page exists; otherwise null.\n"
        "Rules:\n"
        "– Prefer items explicitly describing domestication/breed (e.g., 'Domestic cat', 'Domestic yak', 'Siamese cat').\n"
        "– Do NOT return the wild species page/item.\n"
        "– If multiple breeds exist, pick the one that best matches the provided common names; otherwise pick the general domesticated form.\n"
        "– If unsure, return null.\n\n"
        f"Latin (may match wild taxon): {latin}\n"
        f"German common/breed name: {name_de or 'unknown'}\n"
        f"English common/breed name: {name_en or 'unknown'}\n"
    )
    client_opt = (
        client.with_options(timeout=900.0)
        if hasattr(client, "with_options")
        else client
    )
    for attempt in range(3):
        try:
            resp = client_opt.responses.parse(
                model="gpt-5-mini",
                input=[
                    {
                        "role": "system",
                        "content": (
                            "You find Wikidata QIDs for DOMESTICATED FORMS / BREEDS of animals, "
                            "and their EN/DE Wikipedia page titles when available. "
                            "Return null for fields that do not exist."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                tools=[{"type": "web_search"}],
                reasoning={"effort": "minimal"},
                service_tier="flex",
                text_format=BreedLookup,
            )
            out = resp.output_parsed
            raw_qid = (out.wikidata_qid or "").strip()
            qid = None
            if raw_qid and raw_qid.lower() not in {"none", "null", "unknown"} and _QID_RE.match(raw_qid):
                qid = raw_qid
            wiki_en = _normalize_wiki_title(out.wikipedia_en)
            wiki_de = _normalize_wiki_title(out.wikipedia_de)
            return qid, wiki_en, wiki_de
        except Exception as exc:  # pragma: no cover - network faults
            status = getattr(exc, "status_code", None)
            if status in {408, 429} and attempt < 2:
                time.sleep(2**attempt)
                continue
            raise
    return None, None, None


def resolve_collision(
    client: Any,
    existing: Tuple[str, str, Optional[str], Optional[str]],
    new: Tuple[str, str, Optional[str], Optional[str]],
    collided_qid: str,
) -> Tuple[Optional[str], Optional[str]]:
    """Ask the model for QIDs for two colliding entries."""

    _e_art, e_latin, e_de, e_en = existing
    _n_art, n_latin, n_de, n_en = new
    prompt = (
        "Two different animal entries yielded the same Wikidata QID. "
        "Return exactly one Wikidata QID for EACH entry so they do NOT share the same QID. "
        "Rules:\n"
        "• If an entry is a subspecies (Latin has three terms like Genus species subspecies), "
        "  prefer a subspecies QID; if none exists, return null for that entry.\n"
        "• If an entry says 'sensu lato' (species complex), map it to the species-level item if appropriate, "
        "  but then the other entry MUST be a different QID or null.\n"
        "• Never return the same QID for both. If only one valid item exists, one entry must be null.\n"
        f"Previously returned (collided) QID: {collided_qid}\n"
        f"Entry A – Latin: {e_latin}\nGerman: {e_de or 'unknown'}\nEnglish: {e_en or 'unknown'}\n"
        f"Entry B – Latin: {n_latin}\nGerman: {n_de or 'unknown'}\nEnglish: {n_en or 'unknown'}"
    )
    client_opt = (
        client.with_options(timeout=900.0)
        if hasattr(client, "with_options")
        else client
    )
    for attempt in range(3):
        try:
            resp = client_opt.responses.parse(
                model="gpt-5-mini",
                input=[
                    {
                        "role": "system",
                        "content": (
                            "You disambiguate animal taxa and provide distinct Wikidata QIDs. "
                            "Return null if unsure."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                tools=[{"type": "web_search"}],

                service_tier="flex",
                text_format=CollisionLookup,
            )
            existing_qid = (resp.output_parsed.existing_qid or "").strip()
            new_qid = (resp.output_parsed.new_qid or "").strip()
            if existing_qid.lower() in {"", "none", "null", "unknown"}:
                existing_qid = None
            if new_qid.lower() in {"", "none", "null", "unknown"}:
                new_qid = None
            if existing_qid and not _QID_RE.match(existing_qid):
                existing_qid = None
            if new_qid and not _QID_RE.match(new_qid):
                new_qid = None
            return existing_qid, new_qid
        except Exception as exc:  # pragma: no cover - network faults
            status = getattr(exc, "status_code", None)
            if status in {408, 429} and attempt < 2:
                time.sleep(2**attempt)
                continue
            raise
    return None, None

def resolve_breed_collision_restricted(
    client: Any,
    keeper: Tuple[str, int, str, Optional[str], Optional[str]],
    new: Tuple[str, str, Optional[str], Optional[str]],
    collided_qid: str,
) -> Optional[str]:
    """Existing row keeps its QID (often klasse<6, finalized). Ask for a DIFFERENT domesticated/breed QID for new row."""
    _k_art, k_klasse, k_latin, k_de, k_en = keeper
    _n_art, n_latin, n_de, n_en = new
    assert k_klasse < 6, "restricted resolver is for finalized keeper rows"
    prompt = (
        "An existing (finalized) animal entry already uses this QID and must keep it. "
        "We are adding a DOMESTICATED FORM / BREED and need a DIFFERENT QID for the new row. "
        "Return exactly ONE Wikidata QID for the domesticated form/breed (NOT the wild species). "
        "If none exists, return null.\n"
        f"Collided QID (belongs to keeper): {collided_qid}\n"
        f"Keeper (finalized) – Latin: {k_latin}; DE: {k_de or 'unknown'}; EN: {k_en or 'unknown'}\n"
        f"New (domestic/breed) – Latin: {n_latin}; DE: {n_de or 'unknown'}; EN: {n_en or 'unknown'}\n"
    )
    client_opt = client.with_options(timeout=900.0) if hasattr(client, "with_options") else client
    for attempt in range(3):
        try:
            resp = client_opt.responses.parse(
                model="gpt-5-mini",
                input=[
                    {"role": "system", "content": "Return one QID for the DOMESTICATED/BREED item or null. Do NOT return the keeper's QID."},
                    {"role": "user", "content": prompt},
                ],
                tools=[{"type": "web_search"}],
                service_tier="flex",
                text_format=WikidataLookup,
            )
            qid = (resp.output_parsed.wikidata_qid or "").strip()
            if not qid or qid.lower() in {"none", "null", "unknown"}:
                return None
            return qid if _QID_RE.match(qid) else None
        except Exception as exc:
            status = getattr(exc, "status_code", None)
            if status in {408, 429} and attempt < 2:
                time.sleep(2**attempt)
                continue
            raise
    return None

def _update_wikipedia_links_if_unique(cur: sqlite3.Cursor, art: str, wiki_en: Optional[str], wiki_de: Optional[str]) -> None:
    """Write wikipedia_en/de if present AND unique across the table."""
    updates = {}
    if wiki_en:
        cur.execute("SELECT art, klasse FROM animal WHERE wikipedia_en=? AND art<>?", (wiki_en, art))
        row = cur.fetchone()
        if not row:
            updates["wikipedia_en"] = wiki_en
    if wiki_de:
        cur.execute("SELECT art, klasse FROM animal WHERE wikipedia_de=? AND art<>?", (wiki_de, art))
        row = cur.fetchone()
        if not row:
            updates["wikipedia_de"] = wiki_de
    if updates:
        set_bits = ", ".join(f"{k}=?" for k in updates)
        cur.execute(f"UPDATE animal SET {set_bits} WHERE art=?", (*updates.values(), art))




def process_animals(
    db_path: str = DB_FILE,
    client: Any | None = None,
    lookup: Callable[[Any, str, Optional[str], Optional[str]], tuple[Optional[str], Optional[str], Optional[str]]] | None = None,
    resolve: Callable[
        [
            Any,
            Tuple[str, str, Optional[str], Optional[str]],
            Tuple[str, str, Optional[str], Optional[str]],
            str,
        ],
        Tuple[Optional[str], Optional[str]],
    ] | None = None,
) -> None:
    """Process all animals and update their ``wikidata_qid`` values."""

    if client is None:  # pragma: no cover - exercised only in manual runs
        from openai import OpenAI  # type: ignore

        client = OpenAI(timeout=900.0)

    if lookup is None:
        lookup = lookup_breed
    if resolve is None:
        resolve = resolve_collision

    conn = sqlite3.connect(db_path)
    ensure_db_schema(conn)
    _ensure_enrichment_columns(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT art, latin_name, name_de, name_en
        FROM animal
        WHERE klasse = 6
          AND wikidata_qid IS NULL
  AND zoo_count > 0
        ORDER BY zoo_count DESC
        """
    )
    rows = cur.fetchall()

    existing_qids = {
        qid
        for (qid,) in cur.execute(
            "SELECT wikidata_qid FROM animal WHERE wikidata_qid IS NOT NULL"
        )
        if qid
    }
    # Also track existing Wikipedia titles to aid quick checks (DB checks still authoritative)
    existing_wiki_en = {
        t for (t,) in cur.execute("SELECT wikipedia_en FROM animal WHERE wikipedia_en IS NOT NULL")
        if t
    }
    existing_wiki_de = {
        t for (t,) in cur.execute("SELECT wikipedia_de FROM animal WHERE wikipedia_de IS NOT NULL")
        if t
    }
    cols = {row[1] for row in cur.execute("PRAGMA table_info(animal)")}
    clear_cols = tuple(c for c in _RESET_COLS if c in cols)

    print(f"{len(rows)} animals to process")
    for art, latin, name_de, name_en in rows:
        print(f"Processing {art} ({latin})")
        qid, wiki_en, wiki_de = lookup(client, latin, name_de, name_en)
        # Store LLM-provided wikipedia_* early if unique (even if qid ends up null)
        if wiki_en and wiki_en not in existing_wiki_en:
            _update_wikipedia_links_if_unique(cur, art, wiki_en, None)
            existing_wiki_en.add(wiki_en)
        if wiki_de and wiki_de not in existing_wiki_de:
            _update_wikipedia_links_if_unique(cur, art, None, wiki_de)
            existing_wiki_de.add(wiki_de)

        if qid and qid not in existing_qids:
            _apply_qid_update(
                cur,
                art,
                qid,
                status="llm",
                reset_fields=False,
                clear_cols=clear_cols,
            )
            update_enrichment(cur, art, qid)
            existing_qids.add(qid)
            conn.commit()
            print(f" -> assigned QID {qid}")
        elif qid:
            print(f" -> collision on QID {qid}")
            cur.execute(
                "SELECT art, klasse, latin_name, name_de, name_en FROM animal WHERE wikidata_qid=?",
                (qid,),
            )
            existing = cur.fetchone()
            if existing:
                ex_art, ex_klasse, ex_latin, ex_de, ex_en = existing
                new_info = (art, latin, name_de, name_en)
                if ex_klasse < 6:
                    # Existing row is FINALIZED: do not change it; try to find a different QID ONLY for the new (klasse=6) row.
                    new_qid = resolve_breed_collision_restricted(
                        client,
                        (ex_art, ex_klasse, ex_latin, ex_de, ex_en),
                        new_info,
                        qid,
                    )
                    print(f"    restricted resolver returned new={new_qid}")
                    if new_qid and new_qid not in existing_qids:
                        _apply_qid_update(
                            cur,
                            art,
                            new_qid,
                            status="llm",
                            reset_fields=False,
                            clear_cols=clear_cols,
                        )
                        update_enrichment(cur, art, new_qid)
                        existing_qids.add(new_qid)
                        conn.commit()
                        print(f"    assigned {new_qid} to {art}")
                    else:
                        print("    no alternative QID found or duplicate; leaving as NULL")
                else:
                    # Both sides are klasse >= 6 -> we may resolve both if needed.
                    pre_existing_qids = set(existing_qids)
                    existing_qid, new_qid = resolve(client, (ex_art, ex_latin, ex_de, ex_en), new_info, qid)
                    print(f"    resolver returned: existing={existing_qid}, new={new_qid}")
                    if existing_qid and existing_qid != qid:
                        # We ARE allowed to move another klasse=6 row off this QID.
                        _apply_qid_update(
                            cur,
                            ex_art,
                            existing_qid,
                            status="llm",
                            reset_fields=True,
                            clear_cols=clear_cols,
                        )
                        update_enrichment(cur, ex_art, existing_qid)
                        existing_qids.discard(qid)
                        existing_qids.add(existing_qid)
                        print(f"    updated {ex_art} to {existing_qid} (was {qid})")
                    if new_qid and new_qid not in existing_qids:
                        _apply_qid_update(
                            cur,
                            art,
                            new_qid,
                            status="llm",
                            reset_fields=False,
                            clear_cols=clear_cols,
                        )
                        update_enrichment(cur, art, new_qid)
                        existing_qids.add(new_qid)
                        print(f"    assigned {new_qid} to {art}")
                    conn.commit()
                    if (not existing_qid or existing_qid == qid) and (not new_qid or new_qid in pre_existing_qids):
                        print("    resolver made no changes (kept existing; new was null/duplicate)")
            else:
                print(f"    no existing row found for collision {qid}")
        else:
            print(" -> no QID found")

    conn.close()
