#!/usr/bin/env python3
"""
Extract all .json.gz files from each date folder in /data and merge them into a single JSON file per folder.
Then combine all data into a single CSV file with a date field.
"""

import os
import csv
import gzip
import json
import re
import shutil
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm
from data_runner._base import DATA_DIR

CSV_FIELDNAMES = [
    "date",
    "registration",
    "make",
    "model",
    "primaryColour",
    "secondaryColour",
    "fuelType",
    "engineSize",
    "manufactureDate",
    "registrationDate",
    "firstUsedDate",
    "lastMotTestDate",
    "modification",
    "motTestCount",
    "motTests",
]


class DataProcessor:
    def __init__(self, data_dir: Path | None = None):
        self.data_dir = data_dir or Path(__file__).parent / DATA_DIR
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)

    @staticmethod
    def extract_date_from_folder(folder_name: str) -> str:
        """Extract date from folder name and subtract one day (download date refers to previous day's data)."""
        match = re.search(r"(\d{2}-\d{2}-\d{4})$", folder_name)
        if not match:
            return ""
        date_str = match.group(1)
        date_obj = datetime.strptime(date_str, "%d-%m-%Y")
        actual_date = date_obj - timedelta(days=1)
        return actual_date.strftime("%d-%m-%Y")

    def process_folder(
        self, folder_path: Path, delete_originals: bool = False
    ) -> Path | None:
        """Process a single date folder - merge all .json.gz files into one NDJSON file in data_dir."""
        gz_files = sorted(folder_path.glob("*.json.gz"))
        date_str = self.extract_date_from_folder(folder_path.name)
        output_file = self.data_dir / f"{folder_path.name}.json"

        if not gz_files:
            # Check if already processed
            if output_file.exists():
                print(f"  Already processed: {output_file.name}")
                return output_file
            print(f"  No .json.gz files found in {folder_path.name}")
            return None

        total_size = sum(gz.stat().st_size for gz in gz_files)
        record_count = 0
        with (
            open(output_file, "w", encoding="utf-8") as out,
            tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc=f"  {folder_path.name}",
            ) as pbar,
        ):
            for gz_file in gz_files:
                file_size = gz_file.stat().st_size
                try:
                    with gzip.open(gz_file, "rt", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                record = json.loads(line)
                                record["date"] = date_str
                                out.write(json.dumps(record) + "\n")
                                record_count += 1
                except Exception as e:
                    print(f"  Error reading {gz_file.name}: {e}")
                pbar.update(file_size)

        print(f"  {record_count} records -> {output_file.name}")

        # Delete original .gz files if requested
        if delete_originals:
            for gz_file in gz_files:
                gz_file.unlink()
            print(f"  Deleted {len(gz_files)} original .gz files")

        return output_file

    @staticmethod
    def flatten_record_for_csv(record: dict) -> dict:
        """Flatten a vehicle record for CSV output."""
        return {
            "date": record.get("date", ""),
            "registration": record.get("registration", ""),
            "make": record.get("make", ""),
            "model": record.get("model", ""),
            "primaryColour": record.get("primaryColour", ""),
            "secondaryColour": record.get("secondaryColour", ""),
            "fuelType": record.get("fuelType", ""),
            "engineSize": record.get("engineSize", ""),
            "manufactureDate": record.get("manufactureDate", ""),
            "registrationDate": record.get("registrationDate", ""),
            "firstUsedDate": record.get("firstUsedDate", ""),
            "lastMotTestDate": record.get("lastMotTestDate", ""),
            "modification": record.get("modification", ""),
            "motTestCount": len(record.get("motTests", [])),
            "motTests": json.dumps(record.get("motTests", [])),
        }

    def write_csv(self, records: list[dict], output_path: Path) -> None:
        """Write all records to a single CSV file."""
        if not records:
            print("No records to write to CSV")
            return

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            for record in records:
                writer.writerow(self.flatten_record_for_csv(record))

        print(f"CSV written to: {output_path}")

    def unzip_all(self) -> list[Path]:
        """Extract all .zip files in data_dir into folders, then remove the zips."""
        zip_files = sorted(self.data_dir.glob("*.zip"))
        folders = []
        for zf in zip_files:
            dest = self.data_dir / zf.stem  # e.g. delta-light-vehicle_02-02-2026
            if dest.exists():
                print(f"  Skipping {zf.name} (already extracted)")
                folders.append(dest)
                continue
            print(f"  Extracting {zf.name} …")
            dest.mkdir()
            with zipfile.ZipFile(zf, "r") as z:
                z.extractall(dest)
            zf.unlink()
            print(f"  Extracted to {dest.name}, zip removed")
            folders.append(dest)
        return folders

    def run(self) -> list[Path]:
        """Unzip downloads, process all date folders and return paths to output NDJSON files."""
        if not self.data_dir.exists():
            print(f"Error: {self.data_dir} does not exist")
            return []

        # Extract any zips into folders first
        self.unzip_all()

        # Find all date folders
        folders = [
            f
            for f in self.data_dir.iterdir()
            if f.is_dir()
            and (
                f.name.startswith("delta-light-vehicle")
                or f.name.startswith("bulk-light-vehicle")
            )
        ]
        folders.sort()

        print(f"Found {len(folders)} folders to process\n")

        output_files = []
        for folder in folders:
            print(f"Processing: {folder.name}")
            result = self.process_folder(folder, delete_originals=True)
            if result:
                output_files.append(result)
            shutil.rmtree(folder)
            print(f"  Removed folder: {folder.name}")
            print()

        print(f"Produced {len(output_files)} NDJSON files in {self.data_dir}")
        return output_files


if __name__ == "__main__":
    puller = DataProcessor(Path(DATA_DIR))
    puller.run()
