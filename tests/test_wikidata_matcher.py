import sqlite3

import httpx
import pytest

from zootier_scraper_sqlite import ensure_db_schema
from wikidata_matcher import find_qid, process_animals


@pytest.mark.asyncio
async def test_find_qid_p225():
    def handler(request: httpx.Request) -> httpx.Response:
        params = httpx.QueryParams(request.url.query)
        q = params.get("query", "")
        bindings = []
        if "Pavo cristatus" in q:
            bindings.append(
                {"item": {"value": "http://www.wikidata.org/entity/Q1"}}
            )
        return httpx.Response(200, json={"results": {"bindings": bindings}})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        qid, method = await find_qid(
            client,
            {
                "normalized_latin_name": "Pavo cristatus",
                "alternative_latin_names": "[]",
                "name_en": None,
                "name_de": None,
            },
        )
    assert (qid, method) == ("Q1", "p225")


@pytest.mark.asyncio
async def test_process_animals_no_batch(tmp_path):
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

    queries: list[str] = []
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = httpx.QueryParams(request.url.query)
        queries.append(params.get("query", ""))
        methods.append(request.method)
        if "A" in params.get("query", ""):
            bindings = [
                {"item": {"value": "http://www.wikidata.org/entity/Q1"}}
            ]
        else:
            bindings = []
        return httpx.Response(200, json={"results": {"bindings": bindings}})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        await process_animals(db_path=str(db_path), client=client)

    assert len(queries) == 2
    assert methods == ["GET", "GET"]

    conn = sqlite3.connect(db_path)
    rows = {
        art: (qid, status)
        for art, qid, status in conn.execute(
            "SELECT art, wikidata_qid, wikidata_match_status FROM animal"
        ).fetchall()
    }
    conn.close()

    assert rows["1"] == ("Q1", "auto")
    assert rows["2"] == (None, "none")

