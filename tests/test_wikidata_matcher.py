import httpx
import pytest

from wikidata_matcher import find_qid, sparql_search_taxon


@pytest.mark.asyncio
async def test_sparql_search_taxon_single_name():
    def handler(request: httpx.Request) -> httpx.Response:
        params = httpx.QueryParams(request.content.decode())
        assert "VALUES" not in params["query"]
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

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        qid = await sparql_search_taxon(client, "Pavo cristatus")
    assert qid == "Q1"


@pytest.mark.asyncio
async def test_find_qid_falls_back_to_api():
    calls = {"sparql": 0, "api": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "query.wikidata.org":
            calls["sparql"] += 1
            params = httpx.QueryParams(request.content.decode())
            q = params.get("query", "")
            if "wdt:P225" in q:
                # No direct hit
                return httpx.Response(200, json={"results": {"bindings": []}})
            # validation step
            return httpx.Response(200, json={"boolean": True})
        calls["api"] += 1
        return httpx.Response(200, json={"search": [{"id": "Q2"}]})

    transport = httpx.MockTransport(handler)
    animal = {
        "normalized_latin_name": "Missing",
        "alternative_latin_names": "[]",
        "name_en": "Some name",
        "name_de": None,
    }
    async with httpx.AsyncClient(transport=transport) as client:
        qid = await find_qid(client, animal)
    assert qid == "Q2"
    assert calls["sparql"] == 2  # search + validation
    assert calls["api"] == 1

