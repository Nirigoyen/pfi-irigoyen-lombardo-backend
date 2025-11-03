#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import importlib.util
import os
import sys
import types
import unittest
from types import ModuleType
from unittest.mock import patch


def _install_stub_modules() -> None:
    db_stub = ModuleType("db")
    def _noop(*args, **kwargs):
        return None
    db_stub.upsert_book = _noop
    db_stub.attach_genres = lambda isbn, genres, top_n=3: len(list(genres))
    db_stub.attach_characters = lambda isbn, chars: len(list(chars))
    db_stub.attach_places = lambda isbn, places, top_n=5: len(list(places)[:top_n])
    db_stub.update_author_description = _noop
    db_stub.get_book_info = lambda isbn: None
    db_stub.db_search_books_by_title = lambda title, limit=5: []
    sys.modules.setdefault("db", db_stub)

    lt_stub = ModuleType("lt_client")
    lt_stub.try_get_characters_and_places = lambda isbn, key: ([], [], "ok")
    sys.modules.setdefault("lt_client", lt_stub)

    obs_stub = ModuleType("obs_client")
    obs_stub.upload_bytes = lambda key, data, content_type=None: None
    sys.modules.setdefault("obs_client", obs_stub)

    # Minimal stub for fastapi to import module without real dependency
    if "fastapi" not in sys.modules:
        fastapi_stub = ModuleType("fastapi")

        class HTTPException(Exception):
            def __init__(self, status_code: int, detail: str = ""):
                super().__init__(detail)
                self.status_code = status_code
                self.detail = detail

        def _identity(*args, **kwargs):
            return object()

        class FastAPI:
            def __init__(self, *args, **kwargs):
                self.routes = []
            def add_middleware(self, *args, **kwargs):
                return None
            def get(self, *args, **kwargs):
                def deco(fn):
                    self.routes.append(("GET", args, kwargs, fn))
                    return fn
                return deco
            def post(self, *args, **kwargs):
                def deco(fn):
                    self.routes.append(("POST", args, kwargs, fn))
                    return fn
                return deco

        fastapi_stub.Body = _identity
        fastapi_stub.FastAPI = FastAPI
        fastapi_stub.HTTPException = HTTPException
        fastapi_stub.Query = _identity
        fastapi_stub.UploadFile = object
        fastapi_stub.File = _identity
        fastapi_stub.Form = _identity

        # fastapi.middleware.cors.CORSMiddleware
        middleware_pkg = ModuleType("fastapi.middleware")
        cors_mod = ModuleType("fastapi.middleware.cors")
        class CORSMiddleware:  # noqa: N801
            pass
        cors_mod.CORSMiddleware = CORSMiddleware
        middleware_pkg.cors = cors_mod

        sys.modules["fastapi"] = fastapi_stub
        sys.modules["fastapi.middleware"] = middleware_pkg
        sys.modules["fastapi.middleware.cors"] = cors_mod


def load_module() -> types.ModuleType:
    _install_stub_modules()
    spec = importlib.util.spec_from_file_location("main_estable", os.path.join("ingest", "main-estable.py"))
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


class TestHelpers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def test_strip_html(self):
        self.assertEqual(self.m.strip_html("<p>Hello<br/>world &amp; more</p>"), "Hello world & more")

    def test_is_isbn13(self):
        self.assertTrue(self.m.is_isbn13("9780306406157"))
        self.assertFalse(self.m.is_isbn13("0306406152"))
        self.assertTrue(self.m.is_isbn13("978-0-306-40615-7"))

    def test_dedup(self):
        self.assertEqual(self.m.dedup([1, 1, 2, 3, 2, 4]), [1, 2, 3, 4])

    def test_extract_isbn13s(self):
        item = {
            "volumeInfo": {
                "industryIdentifiers": [
                    {"type": "ISBN_13", "identifier": "978-0-306-40615-7"},
                    {"type": "OTHER", "identifier": "X"},
                    {"type": "ISBN_13", "identifier": "9780306406157"},
                ]
            }
        }
        self.assertEqual(self.m.extract_isbn13s(item), ["9780306406157"])  # dedup + cleaned

    def test_choose_best_item_prefers_es_rich(self):
        es_rich = {
            "id": "es1",
            "volumeInfo": {
                "title": "Titulo",
                "language": "es",
                "description": "desc",
                "categories": ["A"],
                "industryIdentifiers": [{"type": "ISBN_13", "identifier": "9780306406157"}],
                "publishedDate": "2000-01-01",
            },
        }
        en_some = {
            "id": "en1",
            "volumeInfo": {"title": "Title", "language": "en", "description": "d"},
        }
        items = [en_some, es_rich]
        primary, fallback = self.m.choose_best_item(items, prefer_lang="es", title_query="titulo")
        self.assertEqual(primary["id"], "es1")
        self.assertIn(fallback["id"], {"es1", "en1"})

    def test_map_categories_to_es(self):
        cats = ["Science Fiction", "ROMANCE", "Horror", "Young Adult", "History"]
        mapped, raw = self.m.map_categories_to_es(cats)
        self.assertTrue(len(raw) == 5)
        # Compare normalized (no acentos) and lower
        try:
            import unicodedata
            def norm(s: str) -> str:
                s2 = unicodedata.normalize("NFD", s.lower())
                return "".join(ch for ch in s2 if not unicodedata.combining(ch))
        except Exception:
            def norm(s: str) -> str:
                return s.lower()
        mapped_norm = [norm(x) for x in mapped]
        self.assertIn("ciencia ficcion", mapped_norm)
        self.assertIn("romance", mapped_norm)
        self.assertIn("terror", mapped_norm)
        self.assertIn("juvenil", mapped_norm)
        self.assertIn("no ficcion", mapped_norm)

    def test_isbn10_to_isbn13(self):
        self.assertEqual(self.m.isbn10_to_isbn13("0306406152"), "9780306406157")


class TestBuildAndLT(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def test_build_output_json_no_translate(self):
        primary = {
            "id": "GB1",
            "volumeInfo": {
                "title": "The Book",
                "authors": ["John Doe"],
                "description": "A <b>great</b> book.",
                "language": "en",
                "industryIdentifiers": [
                    {"type": "ISBN_13", "identifier": "9780306406157"}
                ],
                "canonicalVolumeLink": "http://example",
                "publishedDate": "2001",
            },
        }
        fallback = {
            "id": "GB2",
            "volumeInfo": {
                "categories": ["Science Fiction"],
                "industryIdentifiers": [
                    {"type": "ISBN_13", "identifier": "9780306406157"},
                    {"type": "ISBN_13", "identifier": "9780000000002"},
                ],
            },
        }
        bios = {"John Doe": {"bio": "Bio text", "lang": "en", "url": "http://w/John_Doe"}}

        out = self.m.build_output_json(
            primary_item=primary,
            fallback_item=fallback,
            work_key="/works/OL1234W",
            author_bios=bios,
            preferred_isbn13=None,
            translate=False,
            target_lang="es",
        )

        self.assertEqual(out["id"], "OL1234W")
        self.assertEqual(out["title"], "The Book")
        self.assertEqual(out["authors"][0]["name"], "John Doe")
        self.assertIn("synopsis", out)
        self.assertEqual(out["google_volume_id"], "GB1")
        self.assertEqual(out["preferred_isbn13"], "9780306406157")

    def test_fetch_lt_best_effort_short_paths(self):
        with patch.dict(os.environ, {}, clear=True):
            chars, places, status = self.m.fetch_lt_best_effort(None, None)
            self.assertEqual((chars, places, status), ([], [], "lt_skipped_no_isbn"))
            chars, places, status = self.m.fetch_lt_best_effort("9780306406157", None)
            self.assertEqual(status, "lt_skipped_no_key")


if __name__ == "__main__":
    import unittest as _unittest
    from unittest import TextTestRunner, TextTestResult

    TICK = "\u2713"  # ✓
    CROSS = "\u2717"  # ✗
    PLAY = "\u25B6"   # ▶

    class PrettyResult(TextTestResult):
        def startTest(self, test):
            test_name = f"{test.__class__.__name__}.{getattr(test, '_testMethodName', str(test))}"
            self.stream.writeln(f"{PLAY} Running: {test_name}")
            super().startTest(test)

        def addSuccess(self, test):
            super().addSuccess(test)
            test_name = f"{test.__class__.__name__}.{getattr(test, '_testMethodName', str(test))}"
            self.stream.writeln(f"{TICK} Passed: {test_name}")

        def addFailure(self, test, err):
            super().addFailure(test, err)
            test_name = f"{test.__class__.__name__}.{getattr(test, '_testMethodName', str(test))}"
            self.stream.writeln(f"{CROSS} Failed: {test_name}")

        def addError(self, test, err):
            super().addError(test, err)
            test_name = f"{test.__class__.__name__}.{getattr(test, '_testMethodName', str(test))}"
            self.stream.writeln(f"{CROSS} Error:  {test_name}")

    class PrettyRunner(TextTestRunner):
        resultclass = PrettyResult

    suite = _unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    result = PrettyRunner(verbosity=0).run(suite)
    # Exit code similar to unittest
    raise SystemExit(0 if result.wasSuccessful() else 1)
