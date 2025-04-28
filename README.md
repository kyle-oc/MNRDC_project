# MNRDC Data Processing Project

## Overview
This project extracts, cleans, and uploads data from various sources including RDS databases, PDFs, and S3 buckets into a local PostgreSQL database.

## Project Structure
- `database_utils.py`: Handles database connections and data uploads.
- `data_extraction.py`: Extracts data from RDS, PDF files, APIs, and S3.
- `data_cleaning.py`: Cleans the extracted data for consistency and quality.
- `main.py`: The main driver script that orchestrates extraction, cleaning, and uploading.

## Setup Instructions
1. Clone this repository.
2. Install required packages:
    ```bash
    pip install pandas sqlalchemy psycopg2-binary boto3 tabula-py PyMuPDF
    ```
3. Update the database credentials in:
    - `db_creds.yaml` for source database (RDS)
    - `local_db_creds.yaml` for target local database

4. Run the main script:
    ```bash
    python main.py
    ```

## How It Works
- Choose the dataset you want to process from the menu.
- The script will extract, clean, and ask whether you want to upload it.
- Data will be stored into the correct dimension tables in your local database.

## Requirements
- Python 3.9+
- PostgreSQL database locally installed or running in a container

## Notes
- Some paths are hard-coded for testing purposes; adapt as needed.

## Author
- Kyle O'Callaghan

## Licence
- MIT Licence. Please see details in MNRDC_project repository.
