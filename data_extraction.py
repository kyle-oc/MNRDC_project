import pandas as pd
import sqlalchemy
from sqlalchemy import text
import tabula
import numpy as np
# from database_utils import DatabaseConnector


class DataExtractor: # will work as a utility class, containing methods that help extract data from different data sources (CSV, API, S3 bucket):
    def __init__(self, engine): # TODO: this removed from __init__ arguments > ", engine, table_name" RE ADD 
        """Initialise DataExtractor with a DatabaseConnector instance."""
        # self.db_connector = DatabaseConnector(r"C:\Users\comma\VS Code projects\Python projects\mnrdc_project\MNRDC_project\db_creds.yaml")
        self.engine = engine
        # self.pdf_df = self.retrieve_pdf_data

        
    def read_rds_table(self, table_name):
        """Fetches data from specified table and returns a DataFrame."""
        with self.engine.connect() as connection:
            query = sqlalchemy.text("SELECT * FROM {}".format(table_name))
            result = connection.execute(query)
            user_df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return user_df
    
    def retrieve_pdf_data():
        """Fetches data from .pdf file given as an argument and returns a Dataframe."""
        card_df = tabula.read_pdf(r'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', output_format='dataframe', pages='all')
        card_df = pd.concat(card_df, ignore_index=True)
        return card_df
        

if __name__ =='__main__':
    # extractor = DataExtractor()
    # connector = DatabaseConnector(r"C:\Users\comma\VS Code projects\Python projects\mnrdc_project\MNRDC_project\db_creds.yaml")
    # table_names = connector.list_db_tables()
    # print(table_names)
    # legacy_users = extractor.read_rds_table("legacy_users")
    # card_df = DataExtractor.retrieve_pdf_data()
    # DatabaseConnector.upload_to_db(card_df, 'dim_card_details')
    pass