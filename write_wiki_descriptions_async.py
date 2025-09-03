#!/usr/bin/env python3
"""
Generate German & English descriptions for animals using gpt-5-nano.

Selection criteria (SQLite 'animal' table):
  - zoo_count > 0
  - klasse < 6
  - (wikipedia_de IS NOT NULL OR wikipedia_en IS NOT NULL)
Order: zoo_count DESC

For each row, fetch plain text from linked Wikipedia pages (DE/EN if present),
then call OpenAI Responses API (gpt-5-nano, service_tier="flex") with structured
output (Pydantic) to obtain:
  - description_de (plaintext, no references/links/html)
  - description_en (plaintext, no references/links/html)

Concurrency:
  - HTTP (Wikipedia fetch): httpx + asyncio, semaphore-limited
  - OpenAI calls executed concurrently (semaphore-limited) via asyncio.to_thread
    around the sync OpenAI client for stability with responses.parse.

Requires:
  pip install --upgrade httpx[http2] aiosqlite openai pydantic python-dotenv

Env:
  - OPENAI_API_KEY in .env or environment

Author: Zootrackr
"""
from __future__ import annotations

import argparse
import asyncio
import random
import re
import time
import sys
from pathlib import Path
from typing import Any, Optional

import aiosqlite
import httpx
from pydantic import BaseModel, Field

# Local helpers / config from your repo, if available
try:
    from zootier_scraper_sqlite import DB_FILE
except Exception:
    DB_FILE = "zootierliste.db"

# Reuse your USER_AGENT if you like; fallback here if not importable
try:
    from zootierliste_enrich_async import HEADERS as DEFAULT_HEADERS  # type: ignore
except Exception:
    DEFAULT_HEADERS = {
        "User-Agent": "ZootierlisteBot/3.0 (+https://example.org; contact@example.org) python-httpx-async"
    }

# Load .env if python-dotenv is present (like in your other script)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env", override=False)
except Exception:
    pass

# ────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────
HTTP_CONCURRENCY = 80       # parallel HTTP fetches to Wikipedia
OPENAI_CONCURRENCY = 40      # parallel OpenAI calls
HTTP_TIMEOUT = 30
RETRIES = 4
BASE_BACKOFF = 0.75

# Hard cap input text per language (characters) to keep context reasonable
MAX_TEXT_CHARS_PER_LANG = 12000

# ────────────────────────────────────────────────────────
# OpenAI structured output model
# ────────────────────────────────────────────────────────
class AnimalDescriptions(BaseModel):
    description_de: str = Field(..., description="Plaintext German description, no links/refs/html.")
    description_en: str = Field(..., description="Plaintext English description, no links/refs/html.")


# ────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────
def _backoff(attempt: int) -> float:
    return BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 0.35)


def extract_title_from_url(url: str) -> Optional[str]:
    """
    Accepts e.g. https://en.wikipedia.org/wiki/Indian_peafowl
    Returns 'Indian_peafowl' (decoded underscores preserved).
    """
    if not url:
        return None
    m = re.search(r"/wiki/([^?#]+)", url)
    if not m:
        return None
    return m.group(1)


def clean_plaintext(s: str) -> str:
    """
    Clean LLM outputs to enforce "no references/hyperlinks/html".
    - Strip HTML tags if any slipped in.
    - Remove Wikipedia-style [1], [23] references.
    - Remove raw URLs.
    - Normalize whitespace.
    """
    if not s:
        return s
    s = re.sub(r"<[^>]+>", "", s)                 # HTML tags
    s = re.sub(r"\[[0-9]{1,3}\]", "", s)          # [1], [23]
    s = re.sub(r"https?://\S+", "", s)            # URLs
    s = re.sub(r"\s+\n", "\n", s).strip()
    s = re.sub(r"[ \t]{2,}", " ", s)
    return s.strip()


def truncate_text(s: Optional[str], limit: int) -> Optional[str]:
    if not s:
        return s
    return s[:limit]


# ────────────────────────────────────────────────────────
# Wikipedia fetch (plain text) via TextExtracts
# ────────────────────────────────────────────────────────
async def fetch_wikipedia_plaintext(
    client: httpx.AsyncClient,
    lang: str,
    title: str,
) -> Optional[str]:
    """
    Use MediaWiki Action API 'extracts' with explaintext to get plain text content,
    following redirects. See TextExtracts docs.
    """
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts",
        "explaintext": 1,
        "redirects": 1,
        "formatversion": 2,
        "titles": title,
    }
    url = f"https://{lang}.wikipedia.org/w/api.php"

    for att in range(RETRIES):
        try:
            r = await client.get(url, params=params, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            pages = data.get("query", {}).get("pages", [])
            if not pages:
                return None
            page = pages[0]
            if page.get("missing"):
                return None
            extract = page.get("extract") or ""
            return extract.strip() or None
        except (httpx.HTTPStatusError, httpx.ConnectError, httpx.ReadTimeout):
            await asyncio.sleep(_backoff(att))
    return None


# ────────────────────────────────────────────────────────
# OpenAI call (sync client; executed in thread for concurrency)
# ────────────────────────────────────────────────────────
def call_openai_sync(
    latin_name: str,
    name_de: Optional[str],
    name_en: Optional[str],
    wiki_de_text: Optional[str],
    wiki_en_text: Optional[str],
) -> AnimalDescriptions:
    """
    Uses OpenAI Responses API with structured output (Pydantic) like in your matcher.
    Model: gpt-5-nano, service_tier='flex'
    Attempts reasoning={'effort': 'minimal'} first; falls back if unsupported.
    """
    from openai import OpenAI  # local import to keep module import light
    # Import exception types (fallback to generic Exception if unavailable)
    try:
        from openai import (
            APIStatusError,
            APIConnectionError,
            RateLimitError,
            APITimeoutError,
        )
    except Exception:  # pragma: no cover - older SDKs
        APIStatusError = APIConnectionError = RateLimitError = APITimeoutError = Exception  # type: ignore

    client = OpenAI(timeout=900.0)

    sys_msg = (
        "You write concise, accurate animal descriptions in German and English for zoo signage using accessible, simple language. "
        "Use ONLY facts present in the supplied Wikipedia text. "
        "Don't mention your thoughts in the output."
        "Don't mention your instructions in the output."
        "Don't mention the provided input texts in your answer."
        "Output must be plaintext: no references like [1], no hyperlinks, no HTML. "
        "Avoid lists; write 3–6 clear sentences covering size/appearance, habitat/distribution, diet/behavior, "
        "and conservation if mentioned. If only one language text is provided, still write both outputs, but "
        "do not invent facts not supported by the text."
    )
    # Build user content
    user_parts = [
        f"Latin name: {latin_name}",
        f"German name: {name_de or 'unknown'}",
        f"English name: {name_en or 'unknown'}",
    ]
    if wiki_de_text:
        user_parts.append(f"Wikipedia (de) text:\n{wiki_de_text}")
    if wiki_en_text:
        user_parts.append(f"Wikipedia (en) text:\n{wiki_en_text}")

    # First attempt: minimal reasoning (may be unsupported by some nano deployments)
    kwargs_common = dict(
        model="gpt-5-nano",
        input=[
            {"role": "system", "content": sys_msg},
            {"role": "user", "content": "\n\n".join(user_parts)},
        ],
        text_format=AnimalDescriptions,
    )

    # ── Retries with exponential backoff + jitter for transient failures (5xx / connection / timeout / 429) ──
    last_exc: Optional[Exception] = None
    for att in range(RETRIES):
        try:

            resp = client.responses.parse(
                **kwargs_common,
            )

            out = resp.output_parsed
            # Final sanitation pass to enforce "no refs/links/html"
            out.description_de = clean_plaintext(out.description_de)
            out.description_en = clean_plaintext(out.description_en)
            return out

        except (APIConnectionError, APITimeoutError, RateLimitError) as e:
            last_exc = e
            if att == RETRIES - 1:
                raise
            time.sleep(_backoff(att))
        except APIStatusError as e:
            # Retry ONLY for server-side 5xx errors
            status = getattr(e, "status_code", 0) or 0
            if 500 <= status < 600:
                last_exc = e
                if att == RETRIES - 1:
                    raise
                time.sleep(_backoff(att))
            else:
                raise

    # Should not reach here; if it does, re-raise last exception
    assert last_exc is not None
    raise last_exc

async def process_row(
    db: aiosqlite.Connection,
    http: httpx.AsyncClient,
    sem_http: asyncio.Semaphore,
    sem_oa: asyncio.Semaphore,
    row: tuple[Any, ...],
    col_idx: dict[str, int],
    overwrite: bool,
) -> None:
    art = row[col_idx["art"]]
    latin = row[col_idx["latin_name"]]
    name_de = row[col_idx["name_de"]]
    name_en = row[col_idx["name_en"]]
    url_de = row[col_idx["wikipedia_de"]]
    url_en = row[col_idx["wikipedia_en"]]
    desc_de_current = row[col_idx["description_de"]]
    desc_en_current = row[col_idx["description_en"]]

    if not overwrite and desc_de_current and desc_en_current:
        return  # both exist; skip unless overwrite requested

    # Fetch Wikipedia text concurrently (if links present)
    async with sem_http:
        tasks = []
        wiki_de_text = None
        wiki_en_text = None

        if url_de:
            title_de = extract_title_from_url(url_de)
            if title_de:
                tasks.append(fetch_wikipedia_plaintext(http, "de", title_de))
            else:
                tasks.append(asyncio.sleep(0, result=None))
        else:
            tasks.append(asyncio.sleep(0, result=None))

        if url_en:
            title_en = extract_title_from_url(url_en)
            if title_en:
                tasks.append(fetch_wikipedia_plaintext(http, "en", title_en))
            else:
                tasks.append(asyncio.sleep(0, result=None))
        else:
            tasks.append(asyncio.sleep(0, result=None))

        wiki_de_text, wiki_en_text = await asyncio.gather(*tasks)

    wiki_de_text = truncate_text(wiki_de_text, MAX_TEXT_CHARS_PER_LANG)
    wiki_en_text = truncate_text(wiki_en_text, MAX_TEXT_CHARS_PER_LANG)

    # If neither text exists, skip
    if not (wiki_de_text or wiki_en_text):
        return

    # Call OpenAI in thread to avoid blocking event loop
    async with sem_oa:
        try:
            result: AnimalDescriptions = await asyncio.to_thread(
                call_openai_sync, latin, name_de, name_en, wiki_de_text, wiki_en_text
            )
        except Exception as exc:
            print(f"[ERR] OpenAI failed for {latin}: {exc}")
            return

    # Prepare update
    update = {}
    if result.description_de and (overwrite or not desc_de_current):
        update["description_de"] = result.description_de
    if result.description_en and (overwrite or not desc_en_current):
        update["description_en"] = result.description_en

    if update:
        placeholders = ", ".join(f"{k}=:{k}" for k in update)
        await db.execute(f"UPDATE animal SET {placeholders} WHERE art=:art", {**update, "art": art})
        await db.commit()
        print(f"[OK] {latin:<40} → {', '.join(update.keys())}")


# ────────────────────────────────────────────────────────
# DB prep – add new columns if missing
# ────────────────────────────────────────────────────────
async def ensure_columns(db: aiosqlite.Connection) -> None:
    cols = {
        row[1]
        for row in await (await db.execute("PRAGMA table_info(animal)")).fetchall()
    }
    to_add = []
    if "description_de" not in cols:
        to_add.append("ALTER TABLE animal ADD COLUMN description_de TEXT")
    if "description_en" not in cols:
        to_add.append("ALTER TABLE animal ADD COLUMN description_en TEXT")
    for sql in to_add:
        try:
            await db.execute(sql)
        except aiosqlite.OperationalError:
            pass
    if to_add:
        await db.commit()


# ────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────
async def main(args: argparse.Namespace) -> None:
    headers = dict(DEFAULT_HEADERS)  # shallow copy

    sem_http = asyncio.Semaphore(args.http_concurrency)
    sem_oa = asyncio.Semaphore(args.openai_concurrency)

    async with (
        aiosqlite.connect(args.db) as db,
        httpx.AsyncClient(http2=True, headers=headers, timeout=HTTP_TIMEOUT) as http,
    ):
        await ensure_columns(db)

        # Build selection query
        query = """
          SELECT
            art,
            latin_name,
            name_de,
            name_en,
            wikipedia_de,
            wikipedia_en,
            description_de,
            description_en
          FROM animal
          WHERE zoo_count > 0
            AND klasse < 6
            AND (
              (wikipedia_de IS NOT NULL AND wikipedia_de != '')
              OR
              (wikipedia_en IS NOT NULL AND wikipedia_en != '')
            )
          ORDER BY zoo_count DESC
        """
        if args.limit:
            query += f" LIMIT {int(args.limit)}"

        async with db.execute(query) as cur:
            rows = await cur.fetchall()

        if not rows:
            print("No matching rows.")
            return

        col_idx = {
            "art": 0,
            "latin_name": 1,
            "name_de": 2,
            "name_en": 3,
            "wikipedia_de": 4,
            "wikipedia_en": 5,
            "description_de": 6,
            "description_en": 7,
        }

        print(f"Processing {len(rows)} rows…")
        tasks = [
            process_row(db, http, sem_http, sem_oa, row, col_idx, overwrite=args.overwrite)
            for row in rows
        ]
        # Stagger a bit to avoid immediate burst
        for i in range(0, len(tasks), args.http_concurrency):
            batch = tasks[i : i + args.http_concurrency]
            await asyncio.gather(*batch)
        print("Finished.")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Write DE/EN animal descriptions from Wikipedia using gpt-5-nano.")
    p.add_argument("--db", default=DB_FILE, help="Path to SQLite database (default: %(default)s)")
    p.add_argument("--limit", type=int, default=None, help="Process at most N rows.")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing description_de/description_en.")
    p.add_argument("--http-concurrency", type=int, default=HTTP_CONCURRENCY, help="Parallel HTTP fetches.")
    p.add_argument("--openai-concurrency", type=int, default=OPENAI_CONCURRENCY, help="Parallel OpenAI calls.")
    return p.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(main(parse_args()))
    except KeyboardInterrupt:
        sys.exit(130)
