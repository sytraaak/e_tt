from __future__ import annotations

import logging
import os
from os import DirEntry
from dataclasses import dataclass
from pathlib import Path

from database import (
    delete_missing_items,
    get_connection,
    get_next_scan_id,
    init_db,
    insert_items,
    load_items,
    rename_item,
    update_modified_items,
    update_seen_items,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class ScannedItem:
    name: str
    rel_path: Path
    mtime: float
    size: int
    is_dir: bool


def scan_folder(folder_path: Path):
    items = []

    for item in os.scandir(folder_path):
        items.append(item)

    return items


def create_scanned_item(entry: DirEntry, root: Path):
    stats = entry.stat()
    is_dir = entry.is_dir()

    return ScannedItem(
        name=entry.name,
        rel_path=Path(os.path.relpath(entry.path, root)),
        mtime=stats.st_mtime,
        size=0 if is_dir else stats.st_size,
        is_dir=is_dir,
    )


def scan_tree(root: Path):
    stack = [root]
    scanned_items = []

    while stack:
        current_dir = stack.pop()
        items = scan_folder(current_dir)

        for item in items:
            scanned_item = create_scanned_item(item, root)
            scanned_items.append(scanned_item)

            if item.is_dir():
                stack.append(Path(item.path))

    return scanned_items


def main():
    init_db()

    root = Path("folder")
    scanned_items = scan_tree(root)
    items_by_path = {str(item.rel_path): item for item in scanned_items}

    conn = get_connection()

    try:
        db_items_by_path = load_items(conn)
        scan_id = get_next_scan_id(conn)

        new_items = []
        modified_items = []
        deleted_candidates = []

        for path, item in items_by_path.items():
            if path not in db_items_by_path:
                new_items.append(item)
                continue

            db_row = db_items_by_path[path]

            if (
                item.size != db_row["size"]
                or item.mtime != db_row["mtime"]
                or int(item.is_dir) != db_row["is_dir"]
                or item.name != db_row["name"]
            ):
                modified_items.append(item)

        for path, row in db_items_by_path.items():
            if path not in items_by_path:
                deleted_candidates.append(row)

        deleted_index = {}

        for row in deleted_candidates:
            key = (row["size"], row["mtime"], row["is_dir"])
            if key not in deleted_index:
                deleted_index[key] = []
            deleted_index[key].append(row)

        rename_pairs = []
        true_creates = []

        for item in new_items:
            key = (item.size, item.mtime, int(item.is_dir))
            candidates = deleted_index.get(key, [])

            if candidates:
                old_row = candidates.pop(0)
                rename_pairs.append((old_row, item))
            else:
                true_creates.append(item)

        existing_paths = []
        for path in items_by_path:
            if path in db_items_by_path:
                existing_paths.append(path)

        update_seen_items(conn, existing_paths, scan_id)

        if modified_items:
            update_modified_items(
                conn,
                [
                    (
                        str(item.rel_path),
                        item.name,
                        item.size,
                        item.mtime,
                        int(item.is_dir),
                    )
                    for item in modified_items
                ],
                scan_id,
            )

        for old_row, new_item in rename_pairs:
            rename_item(
                conn,
                old_row["path"],
                str(new_item.rel_path),
                new_item.name,
                new_item.size,
                new_item.mtime,
                int(new_item.is_dir),
                scan_id,
            )

        if true_creates:
            insert_items(
                conn,
                [
                    (
                        str(item.rel_path),
                        item.name,
                        item.size,
                        item.mtime,
                        int(item.is_dir),
                    )
                    for item in true_creates
                ],
                scan_id,
            )

        delete_missing_items(conn, scan_id)

        logger.info(f"Scan finished, scan_id={scan_id}")
        logger.info(f"Scanned items: {len(scanned_items)}")
        logger.info(f"Created: {len(true_creates)}")
        logger.info(f"Modified: {len(modified_items)}")
        logger.info(f"Renamed: {len(rename_pairs)}")
        logger.info(f"Deleted: {len(deleted_candidates) - len(rename_pairs)}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
