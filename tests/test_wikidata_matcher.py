import sqlite3

import httpx
import pytest

from zootier_scraper_sqlite import ensure_db_schema
from wikidata_matcher import find_qid, process_animals


@pytest.mark.asyncio
async def test_find_qid_uses_sparql_first():
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url)
        if request.url.host == "query.wikidata.org":
            return httpx.Response(
                200,
                json={
                    "results": {
                        "bindings": [
                            {
                                "item": {
                                    "type": "uri",
                                    "value": "http://www.wikidata.org/entity/Q1",
                                }
                            }
                        ]
                    }
                },
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    animal = {
        "normalized_latin_name": "Pavo cristatus",
        "alternative_latin_names": "[]",
        "name_en": None,
        "name_de": None,
    }
    async with httpx.AsyncClient(transport=transport) as client:
        qid, method, score, candidates = await find_qid(client, animal)
    assert qid == "Q1"
    assert method == "p225_exact_primary"
    assert score == 95
    assert candidates == []
    assert all(url.host == "query.wikidata.org" for url in requests)


@pytest.mark.asyncio
async def test_find_qid_falls_back_to_api():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url)
        if request.url.host == "query.wikidata.org":
            params = httpx.QueryParams(request.content.decode())
            q = params.get("query", "")
            if q.strip().startswith("SELECT ?tn"):
                return httpx.Response(
                    200,
                    json={
                        "results": {
                            "bindings": [
                                {"tn": {"type": "literal", "value": "Pavo cristatus"}}
                            ]
                        }
                    },
                )
            return httpx.Response(200, json={"results": {"bindings": []}})
        if request.url.path == "/w/api.php":
            return httpx.Response(200, json={"search": [{"id": "Q1"}]})
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    animal = {
        "normalized_latin_name": "Pavo cristatus",
        "alternative_latin_names": "[]",
        "name_en": None,
        "name_de": None,
    }
    async with httpx.AsyncClient(transport=transport) as client:
        qid, method, score, candidates = await find_qid(client, animal)
    assert qid == "Q1"
    assert method == "api_p225_validated_en"
    assert score == 85
    assert candidates == []
    assert any(url.path == "/w/api.php" for url in calls)


@pytest.mark.asyncio
async def test_process_animals_updates_db(tmp_path):
    db_path = tmp_path / "t.db"
    conn = sqlite3.connect(db_path)
    ensure_db_schema(conn)
    conn.execute(
        "INSERT INTO animal (art, klasse, normalized_latin_name, zoo_count) VALUES (?,?,?,?)",
        ("1", 1, "Pavo cristatus", 5),
    )
    conn.execute(
        "INSERT INTO animal (art, klasse, normalized_latin_name, zoo_count) VALUES (?,?,?,?)",
        ("2", 1, "Unknown", 4),
    )
    conn.commit()
    conn.close()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "query.wikidata.org":
            params = httpx.QueryParams(request.content.decode())
            q = params.get("query", "")
            if "Pavo cristatus" in q and "P105" not in q:
                return httpx.Response(
                    200,
                    json={
                        "results": {
                            "bindings": [
                                {
                                    "item": {
                                        "type": "uri",
                                        "value": "http://www.wikidata.org/entity/Q1",
                                    }
                                }
                            ]
                        }
                    },
                )
            if "wd:Q1 wdt:P105" in q:
                return httpx.Response(
                    200,
                    json={
                        "results": {
                            "bindings": [
                                {
                                    "rank": {
                                        "type": "uri",
                                        "value": "http://www.wikidata.org/entity/Q7432",
                                    }
                                }
                            ]
                        }
                    },
                )
            if "wd:Q2" in q and "P225" in q:
                return httpx.Response(
                    200,
                    json={
                        "results": {
                            "bindings": [
                                {"tn": {"type": "literal", "value": "Other"}}
                            ]
                        }
                    },
                )
            return httpx.Response(200, json={"results": {"bindings": []}})
        if request.url.path == "/w/api.php":
            params = httpx.QueryParams(request.url.query)
            if params.get("search") == "Unknown":
                return httpx.Response(200, json={"search": [{"id": "Q2"}]})
            return httpx.Response(200, json={"search": []})
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        await process_animals(db_path=str(db_path), client=client)

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT art, wikidata_qid, wikidata_match_status, wikidata_match_method, wikidata_match_score FROM animal"
    ).fetchall()
    cand_rows = conn.execute(
        "SELECT art, candidate_qid, method FROM animal_wikidata_candidates"
    ).fetchall()
    conn.close()
    data = {art: (qid, status, method, score) for art, qid, status, method, score in rows}
    assert data["1"] == ("Q1", "auto", "p225_exact_primary", 95.0)
    assert data["2"] == (None, "none", None, None)
    assert cand_rows == [("2", "Q2", "api_search")]


@pytest.mark.asyncio
async def test_process_animals_handles_existing_collision(tmp_path):
    db_path = tmp_path / "t.db"
    conn = sqlite3.connect(db_path)
    ensure_db_schema(conn)
    conn.execute(
        "INSERT INTO animal (art, klasse, normalized_latin_name, zoo_count, wikidata_qid, wikidata_match_status) VALUES (?,?,?,?,?,?)",
        ("1", 1, "Pavo cristatus", 5, "Q1", "auto"),
    )
    conn.execute(
        "INSERT INTO animal (art, klasse, normalized_latin_name, zoo_count) VALUES (?,?,?,?)",
        ("2", 1, "Pavo cristatus", 4),
    )
    conn.commit()
    conn.close()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "query.wikidata.org":
            params = httpx.QueryParams(request.content.decode())
            q = params.get("query", "")
            if "Pavo cristatus" in q and "P105" not in q:
                return httpx.Response(
                    200,
                    json={
                        "results": {
                            "bindings": [
                                {
                                    "item": {
                                        "type": "uri",
                                        "value": "http://www.wikidata.org/entity/Q1",
                                    }
                                }
                            ]
                        }
                    },
                )
            return httpx.Response(200, json={"results": {"bindings": []}})
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        await process_animals(db_path=str(db_path), client=client)

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT wikidata_qid, wikidata_match_status FROM animal WHERE art='2'"
    ).fetchone()
    conn.close()
    assert row == (None, "collision")


@pytest.mark.asyncio
async def test_process_animals_is_resumable_and_idempotent(tmp_path):
    db_path = tmp_path / "t.db"
    conn = sqlite3.connect(db_path)
    ensure_db_schema(conn)
    conn.execute(
        "INSERT INTO animal (art, klasse, normalized_latin_name, zoo_count) VALUES (?,?,?,?)",
        ("1", 1, "Pavo cristatus", 5),
    )
    conn.commit()
    conn.close()

    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if request.url.host == "query.wikidata.org":
            params = httpx.QueryParams(request.content.decode())
            q = params.get("query", "")
            if "Pavo cristatus" in q and "P105" not in q:
                return httpx.Response(
                    200,
                    json={
                        "results": {
                            "bindings": [
                                {
                                    "item": {
                                        "type": "uri",
                                        "value": "http://www.wikidata.org/entity/Q1",
                                    }
                                }
                            ]
                        }
                    },
                )
            if "wd:Q1 wdt:P105" in q:
                return httpx.Response(
                    200,
                    json={
                        "results": {
                            "bindings": [
                                {
                                    "rank": {
                                        "type": "uri",
                                        "value": "http://www.wikidata.org/entity/Q7432",
                                    }
                                }
                            ]
                        }
                    },
                )
            return httpx.Response(200, json={"results": {"bindings": []}})
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        await process_animals(db_path=str(db_path), client=client)
        calls_after_first = calls["count"]
        await process_animals(db_path=str(db_path), client=client)
    assert calls["count"] == calls_after_first


@pytest.mark.asyncio
async def test_process_animals_uses_existing_qid_without_status(tmp_path):
    db_path = tmp_path / "t.db"
    conn = sqlite3.connect(db_path)
    ensure_db_schema(conn)
    conn.execute(
        "INSERT INTO animal (art, klasse, normalized_latin_name, zoo_count, wikidata_qid) VALUES (?,?,?,?,?)",
        ("1", 1, "Pavo cristatus", 5, "Q1"),
    )
    conn.commit()
    conn.close()

    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url)
        assert request.url.path != "/w/api.php"
        params = httpx.QueryParams(request.content.decode())
        q = params.get("query", "")
        assert "P225" not in q
        if "wd:Q1 wdt:P105" in q:
            return httpx.Response(
                200,
                json={
                    "results": {
                        "bindings": [
                            {
                                "rank": {
                                    "type": "uri",
                                    "value": "http://www.wikidata.org/entity/Q7432",
                                }
                            }
                        ]
                    }
                },
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        await process_animals(db_path=str(db_path), client=client)

    assert len(calls) == 1
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT wikidata_qid, wikidata_match_status, wikidata_match_method, wikidata_match_score FROM animal"
    ).fetchone()
    conn.close()
    assert row == ("Q1", "auto", "existing_qid", 100.0)
