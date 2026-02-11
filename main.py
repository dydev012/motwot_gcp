from pathlib import Path
from data_runner.data_puller import DataPuller
from data_runner.data_processor import DataProcessor
from data_runner.gcp_upload import GCPUploader
from data_runner._base import DATA_DIR

DATASET_ID = "motwot_v2"
TABLE_ID = "motwot_main"

# daily run job to pull latest delta


def main():
    # 1 - Pull data
    puller = DataPuller()
    puller.download_latest_delta()
    
    # 2 - Process data
    processor = DataProcessor(data_dir=Path(DATA_DIR))
    files = processor.run()
    fp = str(files[0])

    # # 3 - Upload data
    uploader = GCPUploader(dataset_id=DATASET_ID, main_table_id=TABLE_ID)
    uploader.merge_delta(fp)


if __name__ == "__main__":
    main()
