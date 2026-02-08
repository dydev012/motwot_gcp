from data_puller import DataPuller
from data_processor import DataProcessor
from gcp_upload import GCPUploader

DATASET_ID = "motwot_v2"
TABLE_ID = "motwot_main"


def main():
    # # # 1 - Pull data
    # puller = DataPuller()
    # puller.download_bulk()

    # 2 - Process data
    # processor = DataProcessor()
    # ndjson_files = processor.run()

    # # # 3 - Upload data
    # uploader = GCPUploader(dataset_id=DATASET_ID, table_id=TABLE_ID)
    # uploader.fetch_and_load()
    pass


if __name__ == "__main__":
    main()
