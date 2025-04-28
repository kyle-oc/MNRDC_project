import pandas as pd
import sqlalchemy
import tabula
import requests
import boto3, botocore

class DataExtractor:
    def __init__(self, engine=None): 
        """Initialise DataExtractor with engine."""
        self.engine = engine
        
        
    def read_rds_table(self, table_name):
        """
        Reads a table from the RDS database using the provided engine.

        Args:
            db_engine (sqlalchemy.Engine): Database engine.
            table_name (str): Name of the table to read.

        Returns:
            pd.DataFrame: The extracted table as a DataFrame.
        """
        with self.engine.connect() as connection:
            query = sqlalchemy.text("SELECT * FROM {}".format(table_name))
            result = connection.execute(query)
            user_df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return user_df
    
    @staticmethod
    def retrieve_pdf_data(pdf_url):
        """
        Downloads and extracts tabular data from a PDF link.

        Args:
            link (str): URL of the PDF file.

        Returns:
            pd.DataFrame: Extracted data from the PDF.
        """
        card_df = tabula.read_pdf(pdf_url, output_format='dataframe', pages='all')
        card_df = pd.concat(card_df, ignore_index=True)
        return card_df
        
    @staticmethod    
    def list_number_of_stores(endpoint, headers):
        """
        Sends a request to the API endpoint to retrieve the number of stores.

        Args:
            endpoint (str): API endpoint URL.
            headers (dict): Request headers including authorization token.

        Returns:
            int: Total number of stores.
        """
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        return response.json()['NUMBER_OF_STORES_KEY'] # Need to confirm correct key so that total number of stores is returned
        
    @staticmethod    
    def return_stores_data(total_stores, store_endpoint, headers):
        """
        Fetches store data from the API for a given number of stores.

        Args:
            endpoint (str): API endpoint URL.
            headers (dict): Request headers including authorization token.
            number_of_stores (int): Total number of stores to retrieve.

        Returns:
            pd.DataFrame: Store data as a DataFrame.
        """
        stores = []
        for store_number in range(total_stores):
            url = f"{store_endpoint}{store_number}"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            stores.append(response.json())
        return pd.DataFrame(stores)
    
    @staticmethod    
    def extract_from_s3(bucket_name, bucket_file, local_path):
        """
        Reads a CSV file stored in an S3 bucket.

        Args:
            bucket_name (str): Full name of bucket.
            bucket_file (str): Full name of file within bucket.
            local_path (str): Local directory to specify save location for file.

        Returns:
            pd.DataFrame: Extracted data from S3.
        """
        s3 = boto3.client('s3')
                
        try:
            s3.download_file(bucket_name, bucket_file, local_path)
        except botocore.NoCredentialsError:
            print("AWS credentials not found. Please configure your credentials.")
            return None
        except botocore.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print("The specified bucket does not exist.")
            else:
                print("An error occurred:", e)
            return None
        
        if local_path.endswith('.csv'):
            df = pd.read_csv(local_path, usecols=lambda x: x != 'Unnamed: 0')
            return df
        
        elif local_path.endswith('.json'):
            df = pd.read_json(local_path)
            return df
        
        else:
            print("Error loading file. Please ensure file is .csv or .json file type")
            return None