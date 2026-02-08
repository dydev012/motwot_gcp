import logging
from pathlib import Path

import requests
from tqdm import tqdm
from data_runner.auth import MOTOAuth2Client
from data_runner._base import DATA_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BULK_DOWNLOAD_URL = "https://history.mot.api.gov.uk/v1/trade/vehicles/bulk-download"


class DataPuller:
    """Pulls MOT vehicle data from the GOV.UK bulk-download endpoint."""

    def __init__(self):
        self.auth_client = MOTOAuth2Client()
        self.data_dir = Path(__file__).parent / DATA_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def pull(self) -> dict:
        """
        Authenticate and pull the bulk-download manifest.

        Returns:
            Parsed JSON response from the API.

        Raises:
            RuntimeError: If authentication fails or the request errors.
        """
        headers = self.auth_client.get_auth_headers()
        if headers is None:
            raise RuntimeError("Failed to obtain auth headers – check credentials")

        logger.info("Requesting bulk download from %s", BULK_DOWNLOAD_URL)
        response = requests.get(BULK_DOWNLOAD_URL, headers=headers)
        response.raise_for_status()

        data = response.json()
        logger.info("Bulk download response received (%d top-level keys)", len(data))
        return data

    def _download_file(self, url: str, dest: Path, total_size: int = 0) -> None:
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", total_size))
        with (
            open(dest, "wb") as f,
            tqdm(total=total, unit="B", unit_scale=True, desc=f"  {dest.name}") as pbar,
        ):
            for chunk in resp.iter_content(chunk_size=1024 * 512):
                f.write(chunk)  #
                pbar.update(len(chunk))

    def _download_entries(self, entries: list[dict], label: str) -> list[Path]:
        downloaded = []
        for entry in entries:
            filename = Path(entry["filename"]).name
            dest = self.data_dir / filename
            if dest.exists():
                logger.info("Skipping %s (already exists)", filename)
                downloaded.append(dest)
                continue
            self._download_file(entry["downloadUrl"], dest, entry.get("fileSize", 0))
            logger.info("Saved %s", dest)
            downloaded.append(dest)
        logger.info("Downloaded %d / %d %s files", len(downloaded), len(entries), label)
        return downloaded

    def download_latest_delta(self) -> Path | None:
        """
        Pull the manifest and download only the latest delta file.

        Returns:
            Path to the downloaded file, or None if no deltas are available.
        """
        manifest = self.pull()
        deltas = manifest.get("delta", [])
        if not deltas:
            logger.info("No delta files available")
            return None

        latest = deltas[-1]
        filename = Path(latest["filename"]).name
        dest = self.data_dir / filename

        if dest.exists():
            logger.info("Skipping %s (already exists)", filename)
            return dest

        self._download_file(latest["downloadUrl"], dest, latest.get("fileSize", 0))
        logger.info("Saved %s", dest)
        return dest

    def download_bulk(self) -> list[Path]:
        """
        Pull the manifest then download every bulk file into the data folder.

        Returns:
            List of paths to the downloaded files.
        """
        manifest = self.pull()
        bulk_files = manifest.get("bulk", [])
        if not bulk_files:
            logger.info("No bulk files available")
            return []
        return self._download_entries(bulk_files, "bulk")

    def download_deltas(self) -> list[Path]:
        """
        Pull the manifest then download every delta file into the data folder.

        Returns:
            List of paths to the downloaded files.
        """
        manifest = self.pull()
        deltas = manifest.get("delta", [])
        if not deltas:
            logger.info("No delta files available")
            return []
        return self._download_entries(deltas, "delta")


if __name__ == "__main__":
    puller = DataPuller()
    puller.download_latest_delta()
