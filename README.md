# MNRDC Data Processing Project

## Overview
This project extracts, cleans, and uploads data from various sources including RDS databases, PDFs, and S3 buckets into a local PostgreSQL database for analysis.

## Project Structure
- `database_utils.py`: Handles database connections and data uploads.
- `data_extraction.py`: Extracts data from RDS, PDF files, APIs, and S3.
- `data_cleaning.py`: Cleans the extracted data for consistency and quality.
- `main.py`: The main driver script that orchestrates extraction, cleaning, and uploading.
- `mnrdc_project.session.sql`: Deals with conversion of data types, restructuring and cleaning, adds constraints, and sets up schema relationships.
- `mnrdc_queries.session.sql`: Contains analytical SQL queries for the database.

## Setup Instructions
1. Clone this repository.
2. Install required packages:
    ```bash
    pip install pandas sqlalchemy psycopg2-binary boto3 tabula-py
    ```
3. Update the database credentials in:
    - `db_creds.yaml` for source database (RDS)
    - `local_db_creds.yaml` for target local database

4. Run the main script:
    ```bash
    python main.py
    ```
5. Run the MNRDC Project SQL script:
   `mnrdc_project.session.sql`
6. Run the MNRDC Queries SQL script:
   `mnrdc_queries.session.sql`

## How It Works
- Choose the dataset you want to process from the menu.
- The script will extract, clean, and ask whether you want to upload it.
- Data will be stored into the correct dimension tables in your local database.
- The SQL project script will update database accordingly.
- The SQL queries script will return results.

## Requirements
- Python 3.9+
- PostgreSQL database locally installed or running in a container

## Notes
- Some paths are hard-coded for testing purposes; adapt as needed.

## Author
- Kyle O'Callaghan

## License
- This project is licensed under the MIT License. Please see details in MNRDC_project repository.
