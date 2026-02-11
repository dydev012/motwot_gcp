import io
import os
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from tqdm import tqdm
from data_runner._base import ENV

class GCPUploader(ENV):
    def __init__(self, dataset_id, main_table_id):
        ENV.__init__(self)

        self.project_id = os.environ.get("GCP_PROJECT")
        key_path = os.environ.get("GCP_SERVICE_CREDS_PATH")

        self.credentials = service_account.Credentials.from_service_account_file(
            os.path.join(Path(__file__).parent, key_path)
        )
        self.dataset_id = dataset_id
        self.table_id = main_table_id

        self.main_table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        self.staging_table_ref = f"{self.main_table_ref}_staging"  # for deltas
        self.client = bigquery.Client(
            credentials=self.credentials, project=self.project_id
        )

    def _load_to_table(
        self,
        file_path,
        table_ref,
        write_disposition,
        limit=None,
        time_partitioning=None,
    ):
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        )
        if time_partitioning:
            job_config.time_partitioning = time_partitioning

        print(f"Loading {file_path} to {table_ref}...")

        if limit is not None:
            buf = io.BytesIO()
            with open(file_path, "rb") as raw:
                for _ in range(limit):
                    line = raw.readline()
                    if not line:
                        break
                    buf.write(line)
            buf.seek(0)
            total = buf.getbuffer().nbytes
            with tqdm.wrapattr(
                buf, "read", total=total, unit="B", unit_scale=True
            ) as f:
                job = self.client.load_table_from_file(
                    f, table_ref, job_config=job_config
                )
        else:
            with tqdm.wrapattr(
                open(file_path, "rb"),
                "read",
                total=os.path.getsize(file_path),
                unit="B",
                unit_scale=True,
            ) as f:
                job = self.client.load_table_from_file(
                    f, table_ref, job_config=job_config
                )

        job.result()
        return job

    def fetch_and_load(self, file_path, limit=None):
        job = self._load_to_table(
            file_path,
            self.main_table_ref,
            bigquery.WriteDisposition.WRITE_APPEND,
            limit,
        )
        print(f"Loaded {job.output_rows} rows successfully.")

    def merge_delta(self, file_path, limit=None):
        # 1 — Load delta into staging table (truncate each run)
        job = self._load_to_table(
            file_path,
            self.staging_table_ref,
            bigquery.WriteDisposition.WRITE_TRUNCATE,
            limit,
        )
        print(f"Staged {job.output_rows} rows.")

        # 2 — Build MERGE from staging schema (avoids hardcoding columns)
        staging_table = self.client.get_table(self.staging_table_ref)
        columns = [
            field.name for field in staging_table.schema if field.name != "registration"
        ]
        update_clause = ", ".join(f"main.{c} = delta.{c}" for c in columns)

        merge_sql = f"""
        MERGE `{self.main_table_ref}` AS main
        USING `{self.staging_table_ref}` AS delta
        ON main.registration = delta.registration
        WHEN MATCHED AND delta.modification = 'DELETED' THEN
            DELETE
        WHEN MATCHED AND delta.modification IN ('UPDATED', 'CREATED') THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED AND delta.modification IN ('CREATED', 'UPDATED') THEN
            INSERT ROW
        """

        print("Running MERGE...")
        merge_job = self.client.query(merge_sql)
        merge_job.result()
        stats = merge_job.num_dml_affected_rows
        print(f"MERGE complete — {stats} rows affected.")

        # 3 — Drop staging table
        self.client.delete_table(self.staging_table_ref, not_found_ok=True)
        print(f"Dropped staging table {self.staging_table_ref}.")

    def create_table(self, file_path, limit=None):
        """Create the main table from a bulk file with daily partitioning on lastMotTestDate."""
        partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="lastMotTestDate",
        )
        job = self._load_to_table(
            file_path,
            self.main_table_ref,
            bigquery.WriteDisposition.WRITE_TRUNCATE,
            limit,
            time_partitioning=partitioning,
        )
        print(
            f"Created {self.main_table_ref} with {job.output_rows} rows (partitioned daily on lastMotTestDate)."
        )


if __name__ == "__main__":
    dataset_id = "motwot_v2"
    table_id = "motwot_main"

    uploader = GCPUploader(dataset_id=dataset_id, main_table_id=table_id)
    uploader.create_table("data/bulk-light-vehicle_02-02-2026.json", limit=10000)
    # uploader.fetch_and_load("data/delta-light-vehicle_06-02-2026.json", limit=3000)
