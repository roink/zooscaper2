#!/usr/bin/env python3
"""
Build an image dataset for animals from Wikidata (P18) and Wikipedia lead images
(de/en), **only** when the file is hosted on Wikimedia Commons.

• DB backend: SQLite (your existing `animal` table is used as input)
• New tables:
    - image         (one row per Commons file; PK = Commons MediaInfo ID "M{pageid}")
    - image_variant (direct Wikimedia thumbnail URLs for a standard set of widths)

Selection:
    Process all animals with klasse < 6 AND zoo_count > 0, ordered by zoo_count DESC.

Sources (no downloads performed):
    1) Wikidata P18 for each row with a wikidata_qid
    2) Wikipedia lead image for de/en if a wikipedia_de / wikipedia_en link exists
       (fallback: first image in the article), but ONLY if the file is on Commons.

Notes:
    - Each image row links to exactly one animal via `animal_art` (FK → animal.art).
    - Primary key is the Commons MediaInfo ID (M-ID = "M" + pageid of the file on Commons).
    - We store direct URLs to thumbnail sizes returned by Commons (`imageinfo&iiurlwidth=`).
    - No Commons Structured Data (SDC) search is performed.
    - No files are downloaded.

Requires:
    pip install --upgrade aiosqlite httpx[http2]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import re
import sys
from html import unescape
from typing import Any, Mapping
from urllib.parse import quote, urlparse, unquote

import aiosqlite
import httpx

# ────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────

DB_PATH_DEFAULT = "zootierliste-neu.db"

USER_AGENT = (
    "ZooImageHarvester/1.0 "
    "(https://example.org; contact@example.org) "
    "python-httpx-async"
)
HEADERS = {"User-Agent": USER_AGENT}

CONCURRENT_REQ = 15
RETRIES = 4
BASE_BACKOFF = 0.5  # seconds

# Standard thumbnail widths to persist (you can adjust)
THUMB_WIDTHS = (320, 640, 1024, 1280, 2560)

# API endpoints
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
WIKI_API = {
    "en": "https://en.wikipedia.org/w/api.php",
    "de": "https://de.wikipedia.org/w/api.php",
}


# ────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────

def _backoff(attempt: int) -> float:
    """Exponential backoff with jitter."""
    return BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 0.25)


async def fetch_json(
    client: httpx.AsyncClient,
    url: str,
    params: Mapping[str, Any],
    retries: int = RETRIES,
) -> Mapping[str, Any] | None:
    """GET → JSON with retry on 429/5xx/connect/read timeouts."""
    for att in range(retries):
        try:
            r = await client.get(url, params=params, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPStatusError, httpx.ConnectError, httpx.ReadTimeout) as exc:
            status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
            if status not in {429, 502, 503, 504}:
                raise
            await asyncio.sleep(_backoff(att))
    return None


def ensure_file_prefix(name: str) -> str:
    """Ensure a filename has the 'File:' prefix."""
    n = name.strip().replace("_", " ")
    return n if n.lower().startswith("file:") else f"File:{n}"


def wiki_title_from_url_or_title(url_or_title: str) -> str | None:
    """Accept a full wiki URL or a page title and return the normalized title."""
    if not url_or_title:
        return None
    if "://" in url_or_title:
        # URL: take last path component
        path = urlparse(url_or_title).path  # e.g. /wiki/Panthera_leo
        if not path:
            return None
        title = path.split("/", 2)[-1]
        return unquote(title)
    # Already a title
    return url_or_title.replace(" ", "_")


def strip_html(text: str) -> str:
    """Very small HTML → plaintext (strip tags, unescape entities)."""
    if not text:
        return ""
    no_tags = re.sub(r"<[^>]+>", "", text)
    return unescape(no_tags).strip()


def commons_page_url_from_title(title: str) -> str:
    """Build the Commons description page URL from 'File:…'."""
    title_norm = title.replace(" ", "_")
    return f"https://commons.wikimedia.org/wiki/{quote(title_norm)}"


def mid_from_pageid(pageid: int | str) -> str:
    """MediaInfo ID from Commons pageid."""
    return f"M{pageid}"


# ────────────────────────────────────────────────────────
# Commons: file metadata & thumbnails
# ────────────────────────────────────────────────────────

async def commons_file_core(
    client: httpx.AsyncClient,
    title: str,
) -> dict[str, Any] | None:
    """
    Query Commons for a single file title and return core metadata:
    pageid, canonicaltitle, original url/size, sha1, mime, timestamp, user, extmetadata.
    """
    params = {
        "action": "query",
        "format": "json",
        "formatversion": "2",
        "prop": "imageinfo",
        "iiprop": "url|size|sha1|mime|timestamp|user|extmetadata",
        "titles": ensure_file_prefix(title),
    }
    data = await fetch_json(client, COMMONS_API, params)
    if not data or "query" not in data:
        return None

    pages = data["query"].get("pages", [])
    if not pages:
        return None

    page = pages[0]
    if page.get("missing"):
        # Not on Commons (likely a local wiki file); skip
        return None

    pageid = page.get("pageid")
    iis = page.get("imageinfo", [])
    if not iis:
        return None
    ii = iis[0]

    return {
        "pageid": pageid,
        "canonicaltitle": page.get("title") or page.get("canonicaltitle") or ensure_file_prefix(title),
        "url": ii.get("url"),
        "width": ii.get("width"),
        "height": ii.get("height"),
        "size": ii.get("size"),
        "sha1": ii.get("sha1"),
        "mime": ii.get("mime"),
        "timestamp": ii.get("timestamp"),
        "user": ii.get("user"),
        "extmetadata": ii.get("extmetadata") or {},
    }


async def commons_thumb_for_width(
    client: httpx.AsyncClient,
    title: str,
    width: int,
) -> dict[str, Any] | None:
    """Request a direct thumbnail URL for a given width; return url + actual width/height."""
    params = {
        "action": "query",
        "format": "json",
        "formatversion": "2",
        "prop": "imageinfo",
        "iiprop": "url|size",
        "iiurlwidth": str(width),
        "titles": ensure_file_prefix(title),
    }
    data = await fetch_json(client, COMMONS_API, params)
    if not data or "query" not in data:
        return None
    pages = data["query"].get("pages", [])
    if not pages:
        return None
    page = pages[0]
    if page.get("missing"):
        return None
    iis = page.get("imageinfo", [])
    if not iis:
        return None
    ii = iis[0]
    # thumburl/thumbwidth/thumbheight present when iiurlwidth used
    return {
        "thumb_url": ii.get("thumburl"),
        "thumb_width": ii.get("thumbwidth"),
        "thumb_height": ii.get("thumbheight"),
    }


# ────────────────────────────────────────────────────────
# Wikidata: P18
# ────────────────────────────────────────────────────────

async def wikidata_p18_titles(
    client: httpx.AsyncClient,
    qid: str,
) -> list[str]:
    """
    Return list of Commons file titles from Wikidata P18 (image) for the given Q-ID.
    """
    params = {
        "action": "wbgetentities",
        "format": "json",
        "ids": qid,
        "props": "claims",
    }
    data = await fetch_json(client, WIKIDATA_API, params)
    if not data or "entities" not in data:
        return []
    ent = data["entities"].get(qid)
    if not ent:
        return []
    claims = ent.get("claims", {})
    p18 = claims.get("P18", [])
    titles: list[str] = []
    for c in p18:
        try:
            value = c["mainsnak"]["datavalue"]["value"]
            if isinstance(value, str) and value.strip():
                titles.append(ensure_file_prefix(value))
        except Exception:
            continue
    # Deduplicate while preserving order
    seen = set()
    uniq = []
    for t in titles:
        if t.lower() not in seen:
            seen.add(t.lower())
            uniq.append(t)
    return uniq


# ────────────────────────────────────────────────────────
# Wikipedia: lead image (PageImages) with fallback
# ────────────────────────────────────────────────────────

async def wikipedia_lead_file_title(
    client: httpx.AsyncClient,
    lang: str,
    page_title: str,
) -> str | None:
    """
    Try to get the lead/infobox image using PageImages.
    If none, fallback to first file in prop=images list.
    Return a Commons-style "File:..." title (or None).
    """
    api = WIKI_API[lang]
    title_norm = page_title.replace(" ", "_")

    # 1) PageImages
    params = {
        "action": "query",
        "format": "json",
        "formatversion": "2",
        "prop": "pageimages",
        "piprop": "name|thumbnail|original",
        "pilicense": "free",
        "titles": title_norm,
    }
    data = await fetch_json(client, api, params)
    if data and "query" in data and data["query"].get("pages"):
        pg = data["query"]["pages"][0]
        if "pageimage" in pg:
            # name often available via piprop=name; otherwise derive from thumbnail/original
            # pageimage is typically the filename without "File:"; we add prefix.
            name = pg.get("pageimage")
            if name:
                return ensure_file_prefix(name)
        # If original/thumbnail exists, try to derive a filename from URL path
        thumb = (pg.get("thumbnail") or {}).get("source")
        orig = (pg.get("original") or {}).get("source")
        src = orig or thumb
        if src and "/wikipedia/commons/" in src:
            # Derive filename from the URL end component
            try:
                path = urlparse(src).path
                # Commons thumb original names are at end; decode
                fname = unquote(path.split("/")[-1])
                return ensure_file_prefix(fname)
            except Exception:
                pass

    # 2) Fallback: first file in the article (prop=images)
    params = {
        "action": "query",
        "format": "json",
        "formatversion": "2",
        "prop": "images",
        "imlimit": "max",
        "titles": title_norm,
    }
    data = await fetch_json(client, api, params)
    if data and "query" in data and data["query"].get("pages"):
        pg = data["query"]["pages"][0]
        for img in pg.get("images", []) or []:
            t = img.get("title")
            if not t:
                continue
            # We'll later confirm if it's on Commons by asking Commons API.
            return ensure_file_prefix(t)

    return None


# ────────────────────────────────────────────────────────
# DB schema (image, image_variant)
# ────────────────────────────────────────────────────────

CREATE_IMAGE = """
CREATE TABLE IF NOT EXISTS image (
    mid                     TEXT PRIMARY KEY,             -- "M" + Commons pageid
    animal_art              TEXT NOT NULL REFERENCES animal(art) ON DELETE CASCADE,
    commons_title           TEXT NOT NULL,
    commons_page_url        TEXT NOT NULL,
    original_url            TEXT NOT NULL,
    width                   INTEGER NOT NULL,
    height                  INTEGER NOT NULL,
    size_bytes              INTEGER NOT NULL,
    sha1                    TEXT NOT NULL,
    mime                    TEXT NOT NULL,
    uploaded_at             TEXT,
    uploader                TEXT,
    title                   TEXT,
    artist_raw              TEXT,
    artist_plain            TEXT,
    license                 TEXT,
    license_short           TEXT,
    license_url             TEXT,
    attribution_required    INTEGER,
    usage_terms             TEXT,
    credit_line             TEXT,
    extmetadata_json        TEXT NOT NULL,
    source                  TEXT NOT NULL CHECK (source IN ('WIKIDATA_P18','WIKI_LEAD_DE','WIKI_LEAD_EN')),
    retrieved_at            TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
    UNIQUE(commons_title)
);
"""

CREATE_IMAGE_VARIANT = """
CREATE TABLE IF NOT EXISTS image_variant (
    mid         TEXT NOT NULL REFERENCES image(mid) ON DELETE CASCADE,
    width       INTEGER NOT NULL,
    height      INTEGER NOT NULL,
    thumb_url   TEXT NOT NULL,
    PRIMARY KEY (mid, width)
);
"""

# ────────────────────────────────────────────────────────
# Row upserts
# ────────────────────────────────────────────────────────

async def upsert_image(
    db: aiosqlite.Connection,
    animal_art: str,
    source: str,
    core: dict[str, Any],
) -> str | None:
    """
    Insert/update an image row from Commons core metadata.
    Returns mid on success, None on failure or conflict.
    """
    pageid = core.get("pageid")
    if pageid is None:
        return None
    mid = mid_from_pageid(pageid)

    commons_title = core.get("canonicaltitle") or ""
    commons_title = ensure_file_prefix(commons_title)

    # Normalize extmetadata fields
    extm = core.get("extmetadata") or {}
    def em(key: str) -> str:
        v = extm.get(key, {})
        if isinstance(v, dict):
            v = v.get("value", "")
        return v or ""

    title_txt = strip_html(em("ImageDescription")) or strip_html(em("ObjectName"))
    artist_raw = em("Artist")
    artist_plain = strip_html(artist_raw)
    credit_line = strip_html(em("Credit"))
    usage_terms = strip_html(em("UsageTerms"))
    license_full = em("License")
    license_short = em("LicenseShortName")
    license_url = em("LicenseUrl")
    attr_required = em("AttributionRequired")
    attr_required_int = None
    if attr_required:
        attr_required_int = 1 if attr_required.lower() in {"yes", "true", "1"} else 0

    commons_page_url = commons_page_url_from_title(commons_title)

    # Attempt insert; on conflict update selected fields but do not change animal_art unless it's the same
    await db.execute(
        """
        INSERT INTO image (
            mid, animal_art, commons_title, commons_page_url,
            original_url, width, height, size_bytes, sha1, mime,
            uploaded_at, uploader, title,
            artist_raw, artist_plain,
            license, license_short, license_url,
            attribution_required, usage_terms, credit_line,
            extmetadata_json, source
        )
        VALUES (:mid, :animal_art, :commons_title, :commons_page_url,
                :original_url, :width, :height, :size_bytes, :sha1, :mime,
                :uploaded_at, :uploader, :title,
                :artist_raw, :artist_plain,
                :license, :license_short, :license_url,
                :attribution_required, :usage_terms, :credit_line,
                :extmetadata_json, :source)
        ON CONFLICT(mid) DO UPDATE SET
            commons_title = excluded.commons_title,
            commons_page_url = excluded.commons_page_url,
            original_url = excluded.original_url,
            width = excluded.width,
            height = excluded.height,
            size_bytes = excluded.size_bytes,
            sha1 = excluded.sha1,
            mime = excluded.mime,
            uploaded_at = excluded.uploaded_at,
            uploader = excluded.uploader,
            title = excluded.title,
            artist_raw = excluded.artist_raw,
            artist_plain = excluded.artist_plain,
            license = excluded.license,
            license_short = excluded.license_short,
            license_url = excluded.license_url,
            attribution_required = excluded.attribution_required,
            usage_terms = excluded.usage_terms,
            credit_line = excluded.credit_line,
            extmetadata_json = excluded.extmetadata_json,
            source = CASE
                WHEN image.source = 'WIKIDATA_P18' THEN image.source
                WHEN excluded.source = 'WIKIDATA_P18' THEN excluded.source
                ELSE excluded.source
            END
        """,
        {
            "mid": mid,
            "animal_art": animal_art,
            "commons_title": commons_title,
            "commons_page_url": commons_page_url,
            "original_url": core.get("url") or "",
            "width": int(core.get("width") or 0),
            "height": int(core.get("height") or 0),
            "size_bytes": int(core.get("size") or 0),
            "sha1": core.get("sha1") or "",
            "mime": core.get("mime") or "",
            "uploaded_at": core.get("timestamp"),
            "uploader": core.get("user"),
            "title": title_txt or None,
            "artist_raw": artist_raw or None,
            "artist_plain": artist_plain or None,
            "license": license_full or None,
            "license_short": license_short or None,
            "license_url": license_url or None,
            "attribution_required": attr_required_int,
            "usage_terms": usage_terms or None,
            "credit_line": credit_line or None,
            "extmetadata_json": json.dumps(extm, ensure_ascii=False),
            "source": source,
        },
    )
    await db.commit()

    # Enforce the "one animal per image" assumption:
    # If the row existed with a *different* animal_art, keep the existing association and warn.
    async with db.execute("SELECT animal_art FROM image WHERE mid = ?", (mid,)) as cur:
        prev = await cur.fetchone()
    if prev and prev[0] != animal_art:
        print(f"[WARN] {mid} already linked to animal {prev[0]} (skipping relink to {animal_art})")
        # Optionally you could return None to skip variants; we still return mid so variants update.

    return mid


async def upsert_variant(
    db: aiosqlite.Connection,
    mid: str,
    width: int,
    height: int,
    thumb_url: str,
) -> None:
    await db.execute(
        """
        INSERT INTO image_variant (mid, width, height, thumb_url)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(mid, width) DO UPDATE SET
            height = excluded.height,
            thumb_url = excluded.thumb_url
        """,
        (mid, int(width), int(height), thumb_url),
    )


# ────────────────────────────────────────────────────────
# Per-animal processing
# ────────────────────────────────────────────────────────

async def collect_candidates_for_animal(
    client: httpx.AsyncClient,
    qid: str | None,
    wiki_en: str | None,
    wiki_de: str | None,
) -> list[tuple[str, str]]:
    """
    Return a list of (source, file_title) candidates without duplicates.
    source ∈ {'WIKIDATA_P18','WIKI_LEAD_EN','WIKI_LEAD_DE'}
    """
    seen: set[str] = set()
    out: list[tuple[str, str]] = []

    # Wikidata P18
    if qid:
        try:
            titles = await wikidata_p18_titles(client, qid)
            for t in titles:
                key = t.lower()
                if key not in seen:
                    seen.add(key)
                    out.append(("WIKIDATA_P18", t))
        except Exception as e:
            print(f"[WARN] P18 fetch failed for {qid}: {e}")

    # Wikipedia EN
    if wiki_en:
        t = wiki_title_from_url_or_title(wiki_en)
        if t:
            try:
                fname = await wikipedia_lead_file_title(client, "en", t)
                if fname:
                    key = fname.lower()
                    if key not in seen:
                        seen.add(key)
                        out.append(("WIKI_LEAD_EN", fname))
            except Exception as e:
                print(f"[WARN] EN lead fetch failed for {t}: {e}")

    # Wikipedia DE
    if wiki_de:
        t = wiki_title_from_url_or_title(wiki_de)
        if t:
            try:
                fname = await wikipedia_lead_file_title(client, "de", t)
                if fname:
                    key = fname.lower()
                    if key not in seen:
                        seen.add(key)
                        out.append(("WIKI_LEAD_DE", fname))
            except Exception as e:
                print(f"[WARN] DE lead fetch failed for {t}: {e}")

    return out


async def process_animal(
    db: aiosqlite.Connection,
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    row: tuple[Any, ...],
) -> None:
    """
    Process a single animal row:
      - collect candidate Commons titles from P18 + Wikipedia leads
      - for each, fetch Commons core metadata
      - upsert image
      - upsert thumbnail variants
    """
    async with sem:
        (art, klasse, zoo_count, qid, wiki_en, wiki_de, latin_name) = row

        candidates = await collect_candidates_for_animal(client, qid, wiki_en, wiki_de)
        if not candidates:
            return

        for source, file_title in candidates:
            # Confirm on Commons and fetch metadata
            core = await commons_file_core(client, file_title)
            if not core:
                # not on Commons (or missing)
                continue

            mid = await upsert_image(db, art, source, core)
            if not mid:
                continue

            # Thumbnail variants
            for w in THUMB_WIDTHS:
                try:
                    th = await commons_thumb_for_width(client, file_title, w)
                    if not th or not th.get("thumb_url"):
                        continue
                    await upsert_variant(
                        db,
                        mid,
                        th.get("thumb_width") or w,
                        th.get("thumb_height") or 0,
                        th["thumb_url"],
                    )
                except Exception as e:
                    print(f"[WARN] thumb {w}px for {file_title} failed: {e}")

            await db.commit()
            print(f"[OK]   {latin_name or art}: {file_title} ({mid}) from {source}")


# ────────────────────────────────────────────────────────
# DB prep and main
# ────────────────────────────────────────────────────────

async def ensure_tables(db: aiosqlite.Connection) -> None:
    await db.execute("PRAGMA foreign_keys = ON;")
    await db.execute(CREATE_IMAGE)
    await db.execute(CREATE_IMAGE_VARIANT)
    await db.commit()


async def fetch_animals(db: aiosqlite.Connection) -> list[tuple[Any, ...]]:
    query = """
    SELECT
        art,
        klasse,
        zoo_count,
        NULLIF(wikidata_qid, '') as wikidata_qid,
        NULLIF(wikipedia_en, '') as wikipedia_en,
        NULLIF(wikipedia_de, '') as wikipedia_de,
        latin_name
    FROM animal
    WHERE (klasse IS NULL OR klasse < 6)
      AND zoo_count > 0
    ORDER BY zoo_count DESC
    """
    async with db.execute(query) as cur:
        rows = await cur.fetchall()
    return rows


async def main(args: argparse.Namespace) -> None:
    sem = asyncio.Semaphore(CONCURRENT_REQ)
    async with (
        aiosqlite.connect(args.db) as db,
        httpx.AsyncClient(http2=True, headers=HEADERS, timeout=30) as client,
    ):
        await ensure_tables(db)
        rows = await fetch_animals(db)
        print(f"Processing {len(rows)} animals…")

        # Process in reasonably sized batches to keep memory in check
        BATCH = 100
        for i in range(0, len(rows), BATCH):
            batch = rows[i : i + BATCH]
            await asyncio.gather(*(process_animal(db, client, sem, r) for r in batch))

        print("Finished.")


def parse_args() -> argparse.Namespace:
    # Mutate globals for simplicity (optional)
    global CONCURRENT_REQ, THUMB_WIDTHS
    p = argparse.ArgumentParser(description="Harvest Commons images for animals (SQLite)")
    p.add_argument("--db", default=DB_PATH_DEFAULT, help="Path to SQLite DB (default: %(default)s)")
    p.add_argument(
        "--concurrency", type=int, default=CONCURRENT_REQ, help="Max concurrent HTTP requests"
    )
    p.add_argument(
        "--widths",
        type=str,
        default=",".join(map(str, THUMB_WIDTHS)),
        help="Comma-separated thumbnail widths to store (default: %(default)s)",
    )
    args = p.parse_args()


    CONCURRENT_REQ = int(args.concurrency)
    THUMB_WIDTHS = tuple(int(x) for x in args.widths.split(",") if x.strip())

    return args


if __name__ == "__main__":
    try:
        asyncio.run(main(parse_args()))
    except KeyboardInterrupt:
        sys.exit(130)

