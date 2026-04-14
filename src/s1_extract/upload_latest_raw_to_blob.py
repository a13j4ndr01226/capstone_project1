from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

from src.utils.find_latest_file import find_latest_raw_nested
from src.utils.logger_config import get_logger


load_dotenv(Path("config/.env"))

logger = get_logger("Upload_Raw_CSV_to_Blob")


def upload_latest_raw_to_blob() -> str:
    """
    Finds the latest local enriched CSV:
      data/raw/YYYY_MM_DD/spotify_rising_with_trends_YYYY_MM_DD.csv

    Uploads it to Azure Blob at:
      wasbs://<container>@<account>.blob.core.windows.net/data/raw/YYYY_MM_DD/spotify_rising_with_trends_YYYY_MM_DD.csv

    Returns the blob path string to use as TRANSFORM_ONE_OFF_INPUT.
    """

    raw_root = Path("data/raw")
    latest_file, batch_date = find_latest_raw_nested(
        raw_root,
        expected_template="spotify_rising_with_trends_{date}.csv",
        logger=logger,
    )

    if latest_file is None or batch_date is None:
        raise FileNotFoundError(
            "No local enriched raw CSV found under data/raw/{YYYY_MM_DD}/"
        )

    account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
    account_key = os.getenv("AZURE_STORAGE_KEY")
    container_name = os.getenv("AZURE_CONTAINER", "oe-container")

    if not account_name or not account_key:
        raise EnvironmentError(
            "AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY must be set"
        )

    blob_name = f"data/raw/{batch_date}/{latest_file.name}"

    account_url = f"https://{account_name}.blob.core.windows.net"
    service_client = BlobServiceClient(
        account_url=account_url,
        credential=account_key,
    )
    blob_client = service_client.get_blob_client(
        container=container_name,
        blob=blob_name,
    )

    logger.info(f"Uploading local file: {latest_file}")
    logger.info(f"Uploading to blob: {blob_name}")

    with open(latest_file, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

    wasbs_path = (
        f"wasbs://{container_name}@{account_name}.blob.core.windows.net/{blob_name}"
    )

    logger.info(f"Upload complete. Spark input path: {wasbs_path}")
    return wasbs_path


if __name__ == "__main__":
    path = upload_latest_raw_to_blob()
    print(path)