import yaml
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import text
import psycopg2
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

class DatabaseConnector:
    def __init__(self, yaml_file):
        """Initialise with the path to a YAML file and calls read_db_creds method.""" # TODO UPDATE DOCSTRING
        self.yaml_file = yaml_file
        self.db_creds = self.read_db_creds()
        self.engine = self.init_db_engine()
        ## self.table_names = self.list_db_tables()
        ## table_name = str(input("Please select a table from the above options: "))
        table_name = "legacy_users"
        initialiser = DataExtractor(self.engine)
        self.user_df = initialiser.read_rds_table(table_name)
        initialiser = DataCleaning(self.user_df)
        self.cleaned_user_df = initialiser.clean_user_data()
        print("User data successfully cleaned")
        # self.upload_to_db()
        self.card_df = DataExtractor.retrieve_pdf_data()
        initialiser = DataCleaning()
        self.cleaned_card_df = initialiser.clean_card_data(self.card_df)


    def read_db_creds(self):
        """Reads database credentials from the specified YAML file."""
        try:
            with open(self.yaml_file, 'r') as y:
                return yaml.safe_load(y)
        except FileNotFoundError:
            print(f"Error: The file {self.yaml_file} was not found.")
        except yaml.YAMLError as e:
            print(f"Error: Problem parsing the YAML file - {e}")
            return {} # Return empty dict if error occurs 
            
    def init_db_engine(self):
        """Initialises and returns an sqlalchemy database engine."""
        if not self.db_creds:
            print("Error: No database credentials found.")
            return None

        db_type = "postgresql"
        dbapi = "psycopg2"
        user = self.db_creds.get("RDS_USER")
        password = self.db_creds.get("RDS_PASSWORD")
        host = self.db_creds.get("RDS_HOST")
        port = self.db_creds.get("RDS_PORT")
        database = self.db_creds.get("RDS_DATABASE")
        
        connection_string = f"{db_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}"
     
        try:
            engine = sqlalchemy.create_engine(connection_string)
            return engine
        except Exception as e:
            print(f"Error: Failed to create database engine - {e}")
            return None
        
    def list_db_tables(self):
        """Returns table names from database using sqlalchemy inspect"""
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names
    
    def upload_to_db(df_to_upload, new_db_name):
        """Takes a pandas dataframe and uploads to the dim_users sales_data database."""

        # df_to_upload = self.df_to_upload
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'Bertie'
        DATABASE = 'sales_data'
        PORT = 5432

        engine = sqlalchemy.create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        df_to_upload.to_sql(new_db_name, engine, if_exists='replace', index=False)
        print("table uploaded successfully.")
                
        return None
        
    
if __name__=='__main__':
    db_connector = DatabaseConnector(r"C:\Users\comma\VS Code projects\Python projects\mnrdc_project\MNRDC_project\db_creds.yaml")
    # card_df = DataExtractor.retrieve_pdf_data()
    # print(card_df.head(10))
    
