import threading
import time
from pathlib import Path
from typing import Dict, NamedTuple

from loguru import logger
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from src.database import DatabaseManager
from src.plex import Plex
from src.settings import load_or_create_settings


class PendingScan(NamedTuple):
    library_key: str
    parent_dir: str
    timestamp: float


class BatchScanManager:
    def __init__(self, plex: Plex, delay_seconds: int = 30):
        self.plex = plex
        self.delay_seconds = delay_seconds
        self.pending_scans: Dict[
            str, PendingScan
        ] = {}  # Key: f"{library_key}:{parent_dir}"
        self.lock = threading.Lock()
        self.timer_thread = None
        self.stop_event = threading.Event()

    def add_scan_request(self, library_key: str, parent_dir: str):
        """Add a scan request to the batch queue"""
        scan_key = f"{library_key}:{parent_dir}"
        current_time = time.time()

        with self.lock:
            self.pending_scans[scan_key] = PendingScan(
                library_key=library_key, parent_dir=parent_dir, timestamp=current_time
            )
            logger.debug(f"Added scan request to batch: {scan_key}")

        if self.timer_thread is None or not self.timer_thread.is_alive():
            self.timer_thread = threading.Thread(
                target=self._batch_processor, daemon=True
            )
            self.timer_thread.start()

    def _batch_processor(self):
        while not self.stop_event.is_set():
            current_time = time.time()
            ready_scans = []

            with self.lock:
                for scan_key, pending_scan in list(self.pending_scans.items()):
                    if current_time - pending_scan.timestamp >= self.delay_seconds:
                        ready_scans.append(pending_scan)
                        del self.pending_scans[scan_key]

            for scan in ready_scans:
                try:
                    logger.info(
                        f"Executing batched scan for library {scan.library_key} at {scan.parent_dir}"
                    )
                    self.plex.send_scan_request(int(scan.library_key), scan.parent_dir)
                except Exception as e:
                    logger.error(f"Error executing batched scan: {e}")

            with self.lock:
                if not self.pending_scans:
                    break

            time.sleep(1)

    def shutdown(self):
        self.stop_event.set()

        with self.lock:
            remaining_scans = list(self.pending_scans.values())
            self.pending_scans.clear()

        for scan in remaining_scans:
            try:
                logger.info(
                    f"Processing remaining scan for library {scan.library_key} at {scan.parent_dir}"
                )
                self.plex.send_scan_request(int(scan.library_key), scan.parent_dir)
            except Exception as e:
                logger.error(f"Error processing remaining scan: {e}")

        if self.timer_thread and self.timer_thread.is_alive():
            self.timer_thread.join(timeout=5)


class RescanEventHandler(FileSystemEventHandler):
    def __init__(self, settings, plex: Plex):
        super().__init__()
        self.settings = settings
        self.plex = plex
        self.batch_manager = BatchScanManager(plex, delay_seconds=30)

    def _is_media_file(self, file_path: str) -> bool:
        return Path(file_path).suffix.lower() in self.settings.media_extensions

    def _should_process_event(self, event: FileSystemEvent) -> bool:
        if not self._is_media_file(event.src_path):
            return False
        if event.is_directory:
            return False
        if event.event_type not in ["created", "modified", "moved"]:
            return False
        return True

    def on_any_event(self, event: FileSystemEvent) -> None:
        if not self._should_process_event(event):
            return

        logger.info(f"Detected event: {event.event_type} on {event.src_path}")

        library = self.plex.find_library_by_path(event.src_path)
        if library:
            parent_dir = str(Path(event.src_path).parent)
            logger.info(f"Found library for file: {library.title} ({library.key})")

            self.batch_manager.add_scan_request(library.key, parent_dir)

    def shutdown(self):
        """Shutdown the event handler and process remaining scans"""
        self.batch_manager.shutdown()


def main():
    SETTINGS_FILE = Path("config/settings.json")
    settings = load_or_create_settings(SETTINGS_FILE)
    logger.info("Settings loaded successfully.")

    db = DatabaseManager("config/rescan.db")
    if not db.connection:
        logger.error("Failed to connect to the database. Exiting.")
        return
    logger.info("Database connection established successfully.")

    plex = Plex(settings, db)
    if not plex.server:
        logger.error("Failed to connect to Plex server. Exiting.")
        return
    logger.info("Connected to Plex server successfully.")

    for library, key in plex.library_ids.items():
        logger.info(f"Library: {library} (Key: {key})")
        plex.cache_library_files(key)

    plex.full_scan()

    event_handler = RescanEventHandler(settings, plex)
    observer = Observer()

    for path in settings.library_paths:
        if not Path(path).exists():
            logger.warning(f"Library path {path} does not exist. Skipping.")
            continue
        observer.schedule(event_handler, path=path, recursive=True)

    logger.info(f"Monitoring library path: {settings.library_paths}")
    observer.start()

    try:
        logger.info("Starting file system observer...")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        observer.stop()
        event_handler.shutdown()
        db.close()

    observer.join()
    logger.info("Observer stopped.")


if __name__ == "__main__":
    main()
