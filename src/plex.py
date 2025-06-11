import os
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional

from loguru import logger
from plexapi.exceptions import PlexApiException
from plexapi.server import PlexServer


class LibraryInfo(NamedTuple):
    title: str
    key: str
    locations: List[str]
    library_type: str
    agent: str
    scanner: str


class Plex:
    def __init__(self, settings, db):
        self.db = db
        self.url = settings.plex.url
        self.token = settings.plex.token
        self.media_extensions = settings.media_extensions
        self.local_library_paths = settings.library_paths
        self.server = None
        self.connect()
        self.library_sections = self.get_library_sections()
        self.library_ids = self.get_library_ids()
        self.library_locations = self.get_library_locations()

    def connect(self):
        try:
            self.server = PlexServer(self.url, self.token)
            logger.info("Successfully connected to Plex server")
        except PlexApiException as e:
            logger.error(f"Failed to connect to Plex server: {e}")
            self.server = None
        except Exception as e:
            logger.error(f"Unexpected error connecting to Plex server: {e}")
            self.server = None

    def get_library_sections(self) -> List[LibraryInfo]:
        if not self.server:
            logger.error("Plex server is not connected.")
            return []

        try:
            sections = []

            for section in self.server.library.sections():
                library_info = LibraryInfo(
                    title=section.title,
                    key=section.key,
                    locations=section.locations,
                    library_type=section.type,
                    agent=getattr(section, "agent", "Unknown"),
                    scanner=getattr(section, "scanner", "Unknown"),
                )
                sections.append(library_info)
                logger.debug(
                    f"Found library: {section.title} ({section.type}) - {section.locations}"
                )

            logger.info(f"Successfully retrieved {len(sections)} library sections")
            return sections

        except PlexApiException as e:
            logger.error(f"Error fetching library sections: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching library sections: {e}")
            return []

    def get_library_ids(self) -> Dict[str, str]:
        sections = self.library_sections
        return {section.title: section.key for section in sections}

    def get_library_locations(self) -> Dict[str, List[str]]:
        sections = self.library_sections
        return {section.title: section.locations for section in sections}

    def get_libraries_by_type(self, library_type: str) -> List[LibraryInfo]:
        sections = self.library_sections
        return [section for section in sections if section.library_type == library_type]

    def find_library_by_path(self, file_path: str) -> Optional[LibraryInfo]:
        sections = self.library_sections

        for section in sections:
            for location in section.locations:
                if file_path.startswith(location):
                    return section

        return None

    def send_scan_request(self, library_key: int, file_path: str) -> None:
        if not self.server:
            logger.error("Plex server is not connected. Cannot send scan request.")
            return

        try:
            logger.debug(
                f"Sending scan request for library {library_key} at {file_path}"
            )
            self.server.library.sectionByID(library_key).update(path=str(file_path))
            logger.info(f"Scan request sent for library {library_key} at {file_path}")
        except PlexApiException as e:
            logger.error(f"Failed to send scan request: {e}")

    def cache_library_files(self, library_key: int) -> None:
        if not self.server:
            logger.error("Plex server is not connected. Cannot cache library files.")
            return

        try:
            section = self.server.library.sectionByID(library_key)
            logger.debug(f"Caching files for library {section.title} ({library_key})")

            library_files = []

            if section.type == "show":
                for show in section.all():
                    for episode in show.episodes():
                        for media in episode.media:
                            for part in media.parts:
                                if part.file:
                                    library_files.append((library_key, part.file))
            else:
                for movie in section.all():
                    for media in movie.media:
                        for part in media.parts:
                            if part.file:
                                library_files.append((library_key, part.file))
            self.db.insert_plex_library_files(library_files)
            logger.info(
                f"Cached {len(library_files)} files for library {section.title} ({library_key})"
            )

        except PlexApiException as e:
            logger.error(f"Failed to cache library files: {e}")

    def full_scan(self):
        if not self.server:
            logger.error("Plex server is not connected. Cannot perform initial scan.")
            return

        try:
            logger.debug("Performing initial scan of all libraries")
            for library_path in self.local_library_paths:
                path_obj = Path(library_path)
                if not path_obj.exists():
                    logger.warning(
                        f"Library path {library_path} does not exist. Skipping."
                    )
                    continue

                logger.info(f"Scanning library path: {library_path}")

                for root, dirs, files in os.walk(library_path):
                    for file in files:
                        file_path = Path(root) / file

                        if (
                            file_path.suffix.lower() in self.media_extensions
                        ) and not file_path.name.startswith("."):
                            library = self.find_library_by_path(str(file_path))
                            if library:
                                is_cached = self.db.cursor.execute(
                                    "SELECT 1 FROM plex_library_files WHERE library_key = ? AND file_path = ?",
                                    (library.key, str(file_path)),
                                ).fetchone()

                                if not is_cached:
                                    logger.debug(
                                        f"File {file_path} not cached, sending scan request"
                                    )
                                    self.send_scan_request(library.key, str(file_path))
                            else:
                                logger.warning(
                                    f"No library found for file {file_path}. Skipping scan request."
                                )
            logger.info("Initial scan completed successfully.")

        except PlexApiException as e:
            logger.error(f"Failed to perform initial scan: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during initial scan: {e}")
