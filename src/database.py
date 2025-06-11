import os
import sqlite3
from typing import List, Tuple

from loguru import logger


class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        try:
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
                logger.info(f"Removed existing database at {self.db_path}")
            self.connection = sqlite3.connect(self.db_path)
            self.cursor = self.connection.cursor()
            logger.info(f"Connected to database at {self.db_path}")

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS plex_library_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    library_key INTEGER NOT NULL,
                    file_path TEXT NOT NULL UNIQUE
                )
                """
            )
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            self.connection = None
        except OSError as e:
            logger.error(f"Failed to remove existing database: {e}")
            self.connection = None

    def insert_plex_library_files(self, data: List[Tuple[int, str]]) -> None:
        if not self.connection:
            logger.error("Database connection is not established.")
            return

        try:
            self.cursor.executemany(
                """
                INSERT OR IGNORE INTO plex_library_files (library_key, file_path)
                VALUES (?, ?)
                """,
                [(key, path) for key, path in data],
            )
            self.connection.commit()
            logger.info(f"Inserted {len(data)} records into plex_library_files.")
        except sqlite3.Error as e:
            logger.error(f"Failed to insert records into database: {e}")
        except Exception as e:
            logger.error(f"Unexpected error inserting records into database: {e}")

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed.")
