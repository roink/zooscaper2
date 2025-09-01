import sqlite3

import httpx
import pytest

from zootier_scraper_sqlite import ensure_db_schema
from wikidata_matcher import (
    ACCEPTED_NAME_QID,
    SPECIES_QID,
    _pick_best,
    _score_candidate,
    _sparql_batch_p225,
    find_qid,
    process_animals,
)


def test_rank_penalty():
    animal = {
        "normalized_latin_name": "Canis lupus familiaris",
        "name_en": None,
        "name_de": None,
        "alternative_latin_names": "[]",
    }
    cand = {
        "taxon_name": "Canis lupus familiaris",
        "rank": SPECIES_QID,
        "status": None,
        "label_en": None,
        "label_de": None,
        "vern_en": [],
        "vern_de": [],
    }
    assert _score_candidate(animal, cand) == 20


def test_accepted_name_bonus():
    animal = {
        "normalized_latin_name": "Pavo cristatus",
        "name_en": None,
        "name_de": None,
        "alternative_latin_names": "[]",
    }
    cand = {
        "taxon_name": "Pavo cristatus",
        "rank": SPECIES_QID,
        "status": ACCEPTED_NAME_QID,
        "label_en": None,
        "label_de": None,
        "vern_en": [],
        "vern_de": [],
    }
    assert _score_candidate(animal, cand) == 115


def test_tie_within_five_points():
    c1 = {"qid": "Q1", "score": 95}
    c2 = {"qid": "Q2", "score": 92}
    assert _pick_best([c1, c2]) is None


@pytest.mark.asyncio
async def test_collision_keeps_better(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    ensure_db_schema(conn)
    conn.execute(
        "INSERT INTO animal (art, klasse, normalized_latin_name, zoo_count) VALUES (?,?,?,?)",
        ("1", 1, "A", 2),
    )
    conn.execute(
        "INSERT INTO animal (art, klasse, normalized_latin_name, zoo_count) VALUES (?,?,?,?)",
        ("2", 1, "B", 1),
    )
    conn.commit()
    conn.close()

    def handler(request: httpx.Request) -> httpx.Response:
        params = (
            httpx.QueryParams(request.url.query)
            if request.method == "GET"
            else httpx.QueryParams(request.content.decode())
        )
        q = params.get("query", "")
        bindings = []
        if "wdt:P225" in q:
            if "A" in q:
                bindings.append(
                    {
                        "input": {"type": "literal", "value": "A"},
                        "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q1"},
                        "taxonName": {"type": "literal", "value": "A"},
                        "rank": {"type": "uri", "value": f"http://www.wikidata.org/entity/{SPECIES_QID}"},
                        "status": {"type": "uri", "value": f"http://www.wikidata.org/entity/{ACCEPTED_NAME_QID}"},
                    }
                )
            if "B" in q:
                bindings.append(
                    {
                        "input": {"type": "literal", "value": "B"},
                        "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q1"},
                        "taxonName": {"type": "literal", "value": "B"},
                        "rank": {"type": "uri", "value": f"http://www.wikidata.org/entity/{SPECIES_QID}"},
                    }
                )
        return httpx.Response(200, json={"results": {"bindings": bindings}})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        await process_animals(db_path=str(db_path), client=client)

    conn = sqlite3.connect(db_path)
    rows = {
        art: (qid, status)
        for art, qid, status in conn.execute(
            "SELECT art, wikidata_qid, wikidata_match_status FROM animal"
        ).fetchall()
    }
    conn.close()
    assert rows["1"] == ("Q1", "auto")
    assert rows["2"] == (None, "review")


@pytest.mark.asyncio
async def test_batch_query_uses_values():
    queries = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = (
            httpx.QueryParams(request.url.query)
            if request.method == "GET"
            else httpx.QueryParams(request.content.decode())
        )
        queries.append(params["query"])
        return httpx.Response(200, json={"results": {"bindings": []}})

    transport = httpx.MockTransport(handler)
    from wikidata_matcher import _CACHE_P225

    _CACHE_P225.clear()
    async with httpx.AsyncClient(transport=transport) as client:
        await _sparql_batch_p225(client, ["A", "B"], "p225_exact")

    assert len(queries) == 1
    q = queries[0]
    assert "VALUES" in q
    assert "A" in q and "B" in q


@pytest.mark.asyncio
async def test_single_name_uses_get():
    methods = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        return httpx.Response(200, json={"results": {"bindings": []}})

    transport = httpx.MockTransport(handler)
    from wikidata_matcher import _CACHE_P225

    _CACHE_P225.clear()
    async with httpx.AsyncClient(transport=transport) as client:
        await _sparql_batch_p225(client, ["A"], "p225_exact")

    assert methods == ["GET"]


@pytest.mark.asyncio
async def test_find_qid_helper():
    def handler(request: httpx.Request) -> httpx.Response:
        params = (
            httpx.QueryParams(request.url.query)
            if request.method == "GET"
            else httpx.QueryParams(request.content.decode())
        )
        q = params.get("query", "")
        bindings = []
        if "wdt:P225" in q:
            bindings.append(
                {
                    "input": {"type": "literal", "value": "Pavo cristatus"},
                    "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q1"},
                    "taxonName": {"type": "literal", "value": "Pavo cristatus"},
                    "rank": {"type": "uri", "value": f"http://www.wikidata.org/entity/{SPECIES_QID}"},
                    "status": {"type": "uri", "value": f"http://www.wikidata.org/entity/{ACCEPTED_NAME_QID}"},
                }
            )
        return httpx.Response(200, json={"results": {"bindings": bindings}})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        qid, score = await find_qid(
            client,
            {
                "normalized_latin_name": "Pavo cristatus",
                "name_en": None,
                "name_de": None,
                "alternative_latin_names": "[]",
            },
        )
    assert (qid, score) == ("Q1", 115)
