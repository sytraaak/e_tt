from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path("filesystem_state.db")


def get_connection(db_path: Path = DB_PATH):
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def init_db(db_path: Path = DB_PATH):
    conn = get_connection(db_path)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS items (
            path TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime REAL NOT NULL,
            is_dir INTEGER NOT NULL,
            last_seen_scan_id INTEGER NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def load_items(conn):
    data = conn.execute("SELECT * FROM items").fetchall()
    items = {}

    for row in data:
        items[row["path"]] = row

    return items


def get_next_scan_id(conn):
    result = conn.execute(
        "SELECT COALESCE(MAX(last_seen_scan_id), 0) AS max_scan_id FROM items"
    ).fetchone()

    return int(result["max_scan_id"]) + 1


def insert_items(conn, items, scan_id):
    values = []

    for path, name, size, mtime, is_dir in items:
        values.append((path, name, size, mtime, is_dir, scan_id))

    conn.executemany(
        """
        INSERT INTO items (path, name, size, mtime, is_dir, last_seen_scan_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        values,
    )
    conn.commit()


def update_seen_items(conn, paths, scan_id):
    values = []

    for path in paths:
        values.append((scan_id, path))

    conn.executemany(
        "UPDATE items SET last_seen_scan_id = ? WHERE path = ?",
        values,
    )
    conn.commit()


def update_modified_items(conn, items, scan_id):
    values = []

    for path, name, size, mtime, is_dir in items:
        values.append((name, size, mtime, is_dir, scan_id, path))

    conn.executemany(
        """
        UPDATE items
        SET name = ?, size = ?, mtime = ?, is_dir = ?, last_seen_scan_id = ?
        WHERE path = ?
        """,
        values,
    )
    conn.commit()


def rename_item(conn, old_path, new_path, new_name, new_size, new_mtime, new_is_dir, scan_id):
    conn.execute(
        """
        UPDATE items
        SET path = ?, name = ?, size = ?, mtime = ?, is_dir = ?, last_seen_scan_id = ?
        WHERE path = ?
        """,
        (
            new_path,
            new_name,
            new_size,
            new_mtime,
            new_is_dir,
            scan_id,
            old_path,
        ),
    )
    conn.commit()


def delete_missing_items(conn, scan_id):
    conn.execute(
        "DELETE FROM items WHERE last_seen_scan_id < ?",
        (scan_id,),
    )
    conn.commit()
