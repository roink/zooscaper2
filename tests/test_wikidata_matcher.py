import sqlite3

import httpx
import pytest

from zootier_scraper_sqlite import ensure_db_schema
from wikidata_matcher import find_qid, process_animals, sparql_search_taxon


@pytest.mark.asyncio
async def test_sparql_search_taxon():
    def handler(request: httpx.Request) -> httpx.Response:
        params = httpx.QueryParams(request.url.query)
        assert "P225" in params.get("query", "")
        return httpx.Response(
            200,
            json={"results": {"bindings": [{"item": {"value": "http://www.wikidata.org/entity/Q1"}}]}},
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        qid = await sparql_search_taxon(client, "Pavo cristatus")
    assert qid == "Q1"


@pytest.mark.asyncio
async def test_find_qid_fallback_api():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "query.wikidata.org":
            params = httpx.QueryParams(request.url.query)
            q = params.get("query", "")
            if "SELECT ?item" in q:
                return httpx.Response(200, json={"results": {"bindings": []}})
            return httpx.Response(
                200,
                json={"results": {"bindings": [{"tn": {"value": "Pavo cristatus"}}]}},
            )
        return httpx.Response(200, json={"search": [{"id": "Q1"}]})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        qid = await find_qid(client, "Pavo cristatus")
    assert qid == "Q1"


@pytest.mark.asyncio
async def test_process_animals_collision(tmp_path):
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
        return httpx.Response(
            200,
            json={"results": {"bindings": [{"item": {"value": "http://www.wikidata.org/entity/Q1"}}]}},
        )

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
    assert rows["2"] == (None, "collision")
