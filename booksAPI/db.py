# app/db.py
import os
import psycopg
from typing import Iterable, Optional

DB_URL = os.getenv("DATABASE_URL")  # p.ej. postgresql://user:pass@host:5432/db

def conn():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL no configurado")
    return psycopg.connect(DB_URL)

def upsert_book(isbn: str, title: Optional[str], author_name: Optional[str],
                synopsis: Optional[str], cover_obs_key: Optional[str]) -> None:
    with conn() as c, c.cursor() as cur:
        author_id = None
        if author_name:
            cur.execute("INSERT INTO authors (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (author_name,))
            cur.execute("SELECT id FROM authors WHERE name=%s", (author_name,))
            row = cur.fetchone()
            author_id = row[0] if row else None

        cur.execute("""
            INSERT INTO books (isbn, title, author_id, synopsis, cover_obs_key)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (isbn) DO UPDATE SET
              title = COALESCE(EXCLUDED.title, books.title),
              author_id = COALESCE(EXCLUDED.author_id, books.author_id),
              synopsis = COALESCE(EXCLUDED.synopsis, books.synopsis),
              cover_obs_key = COALESCE(EXCLUDED.cover_obs_key, books.cover_obs_key),
              updated_at = now()
        """, (isbn, title, author_id, synopsis, cover_obs_key))

def attach_characters(isbn: str, char_names: Iterable[str]) -> int:
    inserted = 0
    with conn() as c, c.cursor() as cur:
        for name in char_names:
            name = name.strip()
            if not name:
                continue
            cur.execute("INSERT INTO characters (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (name,))
            cur.execute("SELECT id FROM characters WHERE name=%s", (name,))
            cid = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO book_characters (isbn, character_id)
                VALUES (%s,%s) ON CONFLICT DO NOTHING
            """, (isbn, cid))
            inserted += 1
    return inserted

def attach_places(isbn: str, place_names: Iterable[str], top_n: int = 5) -> int:
    inserted = 0
    with conn() as c, c.cursor() as cur:
        for ord_idx, name in enumerate(place_names[:top_n], start=1):
            name = name.strip()
            if not name:
                continue
            cur.execute("INSERT INTO places (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (name,))
            cur.execute("SELECT id FROM places WHERE name=%s", (name,))
            pid = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO book_places (isbn, place_id, ord)
                VALUES (%s,%s,%s)
                ON CONFLICT (isbn, place_id) DO NOTHING
            """, (isbn, pid, ord_idx))
            inserted += 1
    return inserted

def attach_genres(isbn: str, genres: Iterable[str], top_n: int = 3) -> int:
    inserted = 0
    with conn() as c, c.cursor() as cur:
        for ord_idx, g in enumerate(list(genres)[:top_n], start=1):
            g = g.strip()
            if not g:
                continue
            cur.execute("""
                INSERT INTO book_genres (isbn, genre, ord)
                VALUES (%s,%s,%s)
                ON CONFLICT (isbn, genre) DO NOTHING
            """, (isbn, g, ord_idx))
            inserted += 1
    return inserted

# ----------------- NUEVOS READ HELPERS -----------------

def get_author_by_isbn(isbn: str) -> Optional[dict]:
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            SELECT a.id, a.name, a.description
            FROM books b
            LEFT JOIN authors a ON a.id = b.author_id
            WHERE b.isbn=%s
        """, (isbn,))
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return {"id": row[0], "name": row[1], "description": row[2]}

def get_cover_key_by_isbn(isbn: str) -> Optional[str]:
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT cover_obs_key FROM books WHERE isbn=%s", (isbn,))
        row = cur.fetchone()
        return row[0] if row and row[0] else None

def get_book_info(isbn: str) -> Optional[dict]:
    with conn() as c, c.cursor() as cur:
        # book + author
        cur.execute("""
            SELECT b.isbn, b.title, b.synopsis, b.cover_obs_key,
                   a.id, a.name, a.description
            FROM books b
            LEFT JOIN authors a ON a.id = b.author_id
            WHERE b.isbn=%s
        """, (isbn,))
        row = cur.fetchone()
        if not row:
            return None
        isbn_, title, synopsis, cover_key, author_id, author_name, author_desc = row

        # genres
        cur.execute("""
            SELECT genre FROM book_genres
            WHERE isbn=%s ORDER BY ord NULLS LAST, genre
        """, (isbn_,))
        genres = [r[0] for r in cur.fetchall()]

        # places (ordenados)
        cur.execute("""
            SELECT p.name FROM book_places bp
            JOIN places p ON p.id = bp.place_id
            WHERE bp.isbn=%s
            ORDER BY bp.ord NULLS LAST, p.name
        """, (isbn_,))
        places = [r[0] for r in cur.fetchall()]

        # characters (sin orden específico)
        cur.execute("""
            SELECT c.name FROM book_characters bc
            JOIN characters c ON c.id = bc.character_id
            WHERE bc.isbn=%s
            ORDER BY c.name
        """, (isbn_,))
        characters = [r[0] for r in cur.fetchall()]

        return {
            "isbn": isbn_,
            "title": title,
            "synopsis": synopsis,
            "cover_obs_key": cover_key,
            "author": {
                "id": author_id,
                "name": author_name,
                "description": author_desc
            } if author_id else None,
            "genres": genres,
            "places": places,
            "characters": characters
        }
