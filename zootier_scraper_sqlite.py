#!/usr/bin/env python3
import html
import time
import re
from urllib.parse import urlparse, urlunparse, parse_qs
import sqlite3
import json
import csv
import argparse
from dataclasses import dataclass
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

BASE_URL      = "https://www.zootierliste.de/index.php"
DB_FILE       = "zootierliste.db"
SLEEP_SECONDS = 1
MAP_ZOOS_URL  = "https://www.zootierliste.de/map_zoos.php"

HEADERS = {"User-Agent": "Mozilla/5.0"}
AJAX_HEADERS = {"X-Requested-With": "XMLHttpRequest"}


def _session_with_retries() -> requests.Session:
    sess = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update(HEADERS)
    return sess


SESSION = _session_with_retries()


def get_links(params, pattern, description):
    print(f"[+] Fetching {description}: {params}")
    r = SESSION.get(BASE_URL, params=params)
    r.raise_for_status()
    time.sleep(SLEEP_SECONDS)
    soup = BeautifulSoup(r.text, "html.parser")
    found = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(pattern, href):
            found.add(requests.compat.urljoin(BASE_URL, href))
    found = sorted(found)
    print(f"    → {len(found)} found.")
    return found


def build_locale_url(url: str, locale: str) -> str:
    p = urlparse(url)
    path = p.path
    if not path.startswith(f"/{locale}/"):
        path = f"/{locale}{path}"
    return p._replace(path=path).geturl()


def fetch_localized_name(url: str, locale: str) -> Optional[str]:
    try:
        r = SESSION.get(build_locale_url(url, locale))
        r.raise_for_status()
        time.sleep(SLEEP_SECONDS)
        soup = BeautifulSoup(r.text, "html.parser")
        td = soup.find("td", class_="pageName")
        raw = td.get_text(" ", strip=True) if td else ""
        name = re.sub(r"\s*\(.*?\)", "", raw).strip()
        return name or None
    except Exception:
        return None


_ZTL_RE = re.compile(r"^https?://(?:www\.)?zootier-lexikon\.org/", re.I)


def _normalize_ztl(url: str) -> Optional[str]:
    url = url.strip()
    if not url:
        return None
    p = urlparse(url)
    if not p.scheme or not p.netloc:
        return None
    return urlunparse(
        (p.scheme, p.netloc.lower(), p.path.rstrip('/'), p.params, p.query, p.fragment)
    )


def extract_ztl_link(soup: BeautifulSoup) -> Optional[str]:
    """Extract a zootier-lexikon link from the provided soup if present."""
    a = soup.find("a", href=_ZTL_RE)
    if a and a.has_attr("href"):
        return _normalize_ztl(a["href"])

    label = soup.find(
        string=re.compile(
            r"(Zus(?:\u00e4|ä)tzliche\s+Artinfos|Additional\s+species\s+info)",
            re.I,
        )
    )
    if label:
        holder = label.find_parent(["td", "div"])
        if holder:
            a = holder.find("a", href=True)
            if a and _ZTL_RE.search(a["href"]):
                return _normalize_ztl(a["href"])

    return None


def parse_species(species_url: str, animal_id: int):
    print(f"[>] Fetching species page: {species_url}")
    r = SESSION.get(species_url)
    r.raise_for_status()
    time.sleep(SLEEP_SECONDS)
    soup = BeautifulSoup(r.text, "html.parser")

    ztl_link = extract_ztl_link(soup)

    # German page name (like async version), stripped of any parenthesized suffix
    page_td = soup.find('td', class_='pageName')
    raw_name = page_td.get_text(" ", strip=True) if page_td else ""
    name_de = re.sub(r"\s*\(.*?\)", "", raw_name).strip() or None

    # English page name – fetch the English version of the page
    name_en = fetch_localized_name(species_url, "en")

    # Description table (like async version)
    desc_table = soup.find('table', attrs={'style': re.compile(r"max-width:500px")})
    desc = {}
    if desc_table:
        for row in desc_table.find_all('tr'):
            cols = row.find_all('td')
            if len(cols) >= 2:
                key = cols[0].get_text(strip=True).rstrip(':')
                if key:
                    desc[key] = cols[1].get_text(" ", strip=True)

    # Latin name
    latin = None
    for td in soup.find_all("td", id="tagline"):
        if (i := td.find("i")):
            latin = i.text.strip()
            break

    # Fetch zoo locations via map endpoint
    map_soup = fetch_zoo_map_soup(animal_id)
    zoos = parse_zoo_map(map_soup)

    print(
        f"    → Latin: {latin}, Zoos found: {len(zoos)}, Page DE: {name_de}, Page EN: {name_en}, Desc: {len(desc)} fields, ZTL: {ztl_link}",
    )
    return latin, zoos, name_de, name_en, desc, ztl_link

@dataclass(frozen=True)
class ZooLocation:
    zoo_id: int
    latitude: float
    longitude: float

@dataclass(frozen=True)
class ZooInfo:
    country: Optional[str]
    website: Optional[str]
    city: Optional[str]
    name: Optional[str]


def fetch_zoo_popup_soup(zoo_id: int, session: requests.Session | None = None) -> BeautifulSoup:
    sess = session or SESSION
    r = sess.get(
        MAP_ZOOS_URL,
        params={"showzoo": str(zoo_id), "popup": "true"},
        headers=AJAX_HEADERS,
        timeout=(5, 20),
    )
    r.raise_for_status()
    time.sleep(SLEEP_SECONDS)
    return BeautifulSoup(r.text, "html.parser")


def parse_zoo_popup(soup: BeautifulSoup) -> ZooInfo:
    city = name = None
    title = soup.select_one("div.datum")
    if title:
        raw = title.get_text(strip=True)
        m = re.match(r"^\s*(.*?)\s*\((.*?)\)\s*$", raw)
        if m:
            city, name = m.group(1).strip(), m.group(2).strip()
        else:
            city = raw.strip()

    country = website = None
    info = soup.select_one("div.inhalt")
    if info:
        html_block = info.decode_contents().replace("&nbsp;", " ")
        m_country = re.search(r"Land:\s*([^<]+)", html_block, flags=re.IGNORECASE)
        if m_country:
            country = html.unescape(m_country.group(1)).strip()
        link = info.find("a", href=True)
        if link:
            website = link["href"].strip()

    return ZooInfo(country=country, website=website, city=city, name=name)


def fetch_zoo_details(zoo_id: int) -> dict:
    """Backwards-compatible wrapper returning a dict of zoo details."""
    soup = fetch_zoo_popup_soup(zoo_id)
    info = parse_zoo_popup(soup)
    return {
        "country": info.country or "",
        "website": info.website or "",
        "city": info.city or "",
        "name": info.name or "",
    }


def fetch_zoo_map_soup(art: int) -> BeautifulSoup:
    """Fetch zoo map data for a given animal art code and return the BeautifulSoup."""
    params = {"art": str(art), "tab": "tab_zootier"}
    r = SESSION.get(MAP_ZOOS_URL, params=params, timeout=(5, 20))
    r.raise_for_status()
    time.sleep(SLEEP_SECONDS)
    return BeautifulSoup(r.text, "html.parser")


def parse_zoo_map(soup: BeautifulSoup) -> list[ZooLocation]:
    """Return list of ZooLocation instances from map soup."""
    results: list[ZooLocation] = []
    text = soup.get_text().lstrip("\ufeff")
    reader = csv.reader(text.splitlines(), delimiter="\t")
    next(reader, None)  # Skip header
    for row in reader:
        if not row or len(row) < 2:
            continue
        latlon, zoo_id = row[0], row[1]
        try:
            lat_str, lon_str = latlon.split(",", 1)
            lat = float(lat_str)
            lon = float(lon_str)
            results.append(ZooLocation(int(zoo_id), lat, lon))
        except ValueError:
            continue
    return results


def with_retry(fn, *args, retries: int = 5, base: float = 0.05, **kwargs):
    """Retry SQLite operations briefly if the database is locked."""
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except sqlite3.OperationalError as e:
            if "database is locked" not in str(e).lower() or i == retries - 1:
                raise
            time.sleep(base * (2 ** i))


def ensure_db_schema(conn):
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys = ON;")
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS animal (
            art         TEXT PRIMARY KEY,
            klasse      INTEGER,
            ordnung     INTEGER,
            familie     INTEGER,
            latin_name  TEXT,
            zootierliste_description TEXT,
            name_de     TEXT,
            name_en     TEXT,
            zootierlexikon_link TEXT,
            normalized_latin_name TEXT,
            alternative_latin_names TEXT,
            qualifier TEXT,
            qualifier_target TEXT,
            locality TEXT,
            trade_code TEXT,
            zoo_count  INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS zoo (
            zoo_id      INTEGER PRIMARY KEY,
            continent   TEXT,
            country     TEXT,
            city        TEXT,
            name        TEXT,
            latitude    REAL,
            longitude   REAL,
            website     TEXT,
            species_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS zoo_animal (
            zoo_id      INTEGER,
            art         TEXT,
            PRIMARY KEY(zoo_id, art),
            FOREIGN KEY(zoo_id) REFERENCES zoo(zoo_id),
            FOREIGN KEY(art) REFERENCES animal(art)
        )
        """
    )
    c.execute(
        """
        CREATE INDEX IF NOT EXISTS zoo_animal_art_idx ON zoo_animal(art);
        """
    )
    c.execute(
        """
        CREATE INDEX IF NOT EXISTS zoo_name_idx ON zoo(country, city, name);
        """
    )
    # Perform simple migrations for renamed/added columns
    cols = [row[1] for row in c.execute("PRAGMA table_info(animal)")]
    if "zootierliste_name" in cols and "name_de" not in cols:
        c.execute("ALTER TABLE animal RENAME COLUMN zootierliste_name TO name_de")
        cols = [row[1] for row in c.execute("PRAGMA table_info(animal)")]
    if "name_de" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN name_de TEXT")
    if "name_en" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN name_en TEXT")
    if "zootierlexikon_link" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN zootierlexikon_link TEXT")
    if "normalized_latin_name" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN normalized_latin_name TEXT")
    if "alternative_latin_names" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN alternative_latin_names TEXT")
    if "qualifier" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN qualifier TEXT")
    if "qualifier_target" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN qualifier_target TEXT")
    if "locality" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN locality TEXT")
    if "trade_code" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN trade_code TEXT")
    if "zoo_count" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN zoo_count INTEGER DEFAULT 0")
    if "wikidata_qid" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN wikidata_qid TEXT")
    if "wikidata_match_status" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN wikidata_match_status TEXT")
    if "wikidata_match_method" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN wikidata_match_method TEXT")
    if "wikidata_match_score" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN wikidata_match_score REAL")
    if "wikidata_review_json" not in cols:
        c.execute("ALTER TABLE animal ADD COLUMN wikidata_review_json TEXT")

    cols_zoo = [row[1] for row in c.execute("PRAGMA table_info(zoo)")]
    if "species_count" not in cols_zoo:
        c.execute("ALTER TABLE zoo ADD COLUMN species_count INTEGER DEFAULT 0")

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS animal_wikidata_candidates (
            art TEXT,
            candidate_qid TEXT,
            score REAL,
            method TEXT,
            debug TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(art, candidate_qid),
            FOREIGN KEY(art) REFERENCES animal(art)
        )
        """
    )
    c.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_wikidata_qid_processed
        ON animal(wikidata_qid)
        WHERE klasse < 6
          AND qualifier IS NULL
          AND qualifier_target IS NULL
          AND locality IS NULL
          AND trade_code IS NULL
          AND wikidata_qid IS NOT NULL
        """
    )

    conn.commit()

def update_animal_enrichment(conn, art, name_de, name_en, desc_dict, ztl_link):
    """Store page names, description JSON, and ZTL link on the animal row."""
    desc_json = json.dumps(desc_dict or {}, ensure_ascii=False)
    c = conn.cursor()
    with_retry(
        c.execute,
        "UPDATE animal SET zootierliste_description = ?, name_de = ?, name_en = ?, zootierlexikon_link = COALESCE(?, zootierlexikon_link) WHERE art = ?",
        (desc_json, name_de, name_en, ztl_link, art),
    )

def get_or_create_animal(conn, klasse, ordnung, familie, art, latin_name):
    c = conn.cursor()
    with_retry(
        c.execute,
        """
        INSERT INTO animal (art, klasse, ordnung, familie, latin_name)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(art) DO UPDATE SET
            klasse = excluded.klasse,
            ordnung = excluded.ordnung,
            familie = excluded.familie,
            latin_name = COALESCE(excluded.latin_name, animal.latin_name)
        """,
        (art, klasse, ordnung, familie, latin_name),
    )
    return art


def get_or_create_zoo(conn, location: ZooLocation) -> int:
    """Insert a new zoo only if it does not already exist."""
    assert isinstance(location.zoo_id, int)
    assert isinstance(location.latitude, float)
    assert isinstance(location.longitude, float)

    cur = conn.cursor()
    with_retry(cur.execute, "SELECT 1 FROM zoo WHERE zoo_id = ?", (location.zoo_id,))
    if cur.fetchone():
        return location.zoo_id

    try:
        info = parse_zoo_popup(fetch_zoo_popup_soup(location.zoo_id))
    except Exception:
        info = ZooInfo(country=None, website=None, city=None, name=None)

    with_retry(
        cur.execute,
        """
        INSERT OR IGNORE INTO zoo (zoo_id, continent, country, city, name, latitude, longitude, website)
        VALUES (?, NULL, ?, ?, ?, ?, ?, ?)
        """,
        (
            location.zoo_id,
            info.country,
            info.city,
            info.name,
            location.latitude,
            location.longitude,
            info.website,
        ),
    )

    return location.zoo_id


def create_zoo_animal(conn, zoo_id, art):
    c = conn.cursor()
    with_retry(
        c.execute,
        """
        INSERT INTO zoo_animal (zoo_id, art) VALUES (?, ?)
        ON CONFLICT(zoo_id, art) DO NOTHING
        """,
        (zoo_id, art),
    )



def update_counts(conn):
    """Populate zoo_count on animal and species_count on zoo."""
    cur = conn.cursor()
    with_retry(
        cur.execute,
        """
        UPDATE animal
        SET zoo_count = (
            SELECT COUNT(DISTINCT zoo_id)
            FROM zoo_animal za
            WHERE za.art = animal.art
        )
        """,
    )
    with_retry(
        cur.execute,
        """
        UPDATE zoo
        SET species_count = (
            SELECT COUNT(DISTINCT art)
            FROM zoo_animal za
            WHERE za.zoo_id = zoo.zoo_id
        )
        """,
    )



def main(klasses: list[int]):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    # conn.execute("PRAGMA wal_autocheckpoint = 1000;")  # optional tuning
    ensure_db_schema(conn)
    cursor = conn.cursor()

    for klasse in klasses:
        print(f"\n=== CLASS {klasse} ===")
        orders = get_links(
            {"klasse": klasse},
            fr"\?klasse={klasse}&ordnung=\d+$",
            f"orders in class {klasse}"
        )

        for ord_url in orders:
            ordnung = parse_qs(urlparse(ord_url).query)["ordnung"][0]
            families = get_links(
                {"klasse": klasse, "ordnung": ordnung},
                fr"\?klasse={klasse}&ordnung={ordnung}&familie=\d+$",
                f"families in {klasse}/{ordnung}"
            )

            for fam_url in families:
                familie = parse_qs(urlparse(fam_url).query)["familie"][0]
                species_list = get_links(
                    {"klasse": klasse, "ordnung": ordnung, "familie": familie},
                    fr"\?klasse={klasse}&ordnung={ordnung}&familie={familie}&art=\d+",
                    f"species in {klasse}/{ordnung}/{familie}"
                )

                for sp_url in species_list:
                    art = parse_qs(urlparse(sp_url).query)["art"][0]

                    # Skip if this species is already in the database
                    cursor.execute(
                        "SELECT art FROM animal WHERE klasse=? AND ordnung=? AND familie=? AND art=?",
                        (klasse, ordnung, familie, art)
                    )
                    if cursor.fetchone():
                        print(f"    → Skipping species art={art}, already in DB")
                        continue

                    try:
                        latin, zoos, name_de, name_en, desc, ztl_link = parse_species(sp_url, int(art))
                        if latin is None:
                            print(f"    ! Skipping species art={art} ({sp_url}) – missing Latin name.")
                            continue

                        with conn:
                            art_key = get_or_create_animal(
                                conn, klasse, ordnung, familie, art, latin
                            )
                            update_animal_enrichment(conn, art_key, name_de, name_en, desc, ztl_link)

                        for z in zoos:
                            with conn:
                                get_or_create_zoo(conn, z)
                                create_zoo_animal(conn, z.zoo_id, art_key)
                    except Exception as e:
                        print(f"[!] Error processing species art={art}: {e}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape zoo data")
    parser.add_argument(
        "--klasse",
        "-k",
        type=int,
        nargs="+",
        default=[1],
        help="Klasse numbers to process",
    )
    main(parser.parse_args().klasse)

