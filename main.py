import os
from pathlib import Path
from fastapi import FastAPI, BackgroundTasks
from data_runner.data_puller import DataPuller
from data_runner.data_processor import DataProcessor
from data_runner.gcp_upload import GCPUploader
from data_runner._base import DATA_DIR

DATASET_ID = "motwot_v2"
TABLE_ID = "motwot_main_enriched"

app = FastAPI()


def perform_weekly_delta_merge():
    # 1 - Pull data
    puller = DataPuller()
    puller.download_deltas()

    # 2 - Process data
    processor = DataProcessor(data_dir=Path(DATA_DIR))
    filepaths = processor.run()

    # 3 - Upload data
    uploader = GCPUploader(dataset_id=DATASET_ID, main_table_id=TABLE_ID)
    for i, fp in enumerate(filepaths):
        if i == 0:
            uploader.merge_delta(fp, no_merge=True, append_staging=False)
        else:
            uploader.merge_delta(fp, no_merge=True, append_staging=True)
        os.remove(fp)


@app.get("/weekly-delta-merge")
def weekly_delta_merge(background_tasks: BackgroundTasks):
    background_tasks.add_task(perform_weekly_delta_merge)
    return {"status": "started"}
