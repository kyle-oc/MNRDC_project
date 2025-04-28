import yaml
import sqlalchemy
from sqlalchemy import inspect
import psycopg2
import pandas as pd

class DatabaseConnector:
    def __init__(self, yaml_file):
        """
        Initializes the DatabaseConnector with a path to the database credentials YAML file.

        Args:
            db_creds_path (str): Path to the YAML file containing database credentials.
        """
        self.yaml_file = yaml_file
        self.db_creds = self.read_db_creds()


    def read_db_creds(self):
        """
        Reads database credentials from the YAML file.

        Returns:
            dict: A dictionary containing database connection credentials.
        """
        try:
            with open(self.yaml_file, 'r') as y:
                return yaml.safe_load(y)
        except FileNotFoundError:
            print(f"Error: The file {self.yaml_file} was not found.")
        except yaml.YAMLError as e:
            print(f"Error: Problem parsing the YAML file - {e}")
            return {} # Return empty dict if error occurs 
            
    def init_db_engine(self):
        """
        Initialises and returns a SQLAlchemy database engine using the credentials.

        Returns:
            sqlalchemy.Engine: SQLAlchemy engine connected to the database.
        """
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
        
    def list_db_tables(self, engine):
        """Returns table names from database using sqlalchemy inspect
        
        Args:
            sqlalchemy engine initialised in init_db_engine method
            
        Returns:
            None
            """
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        print("Available tables:")
        for idx, table in enumerate(table_names, 1):
            print(f"{idx}. {table}")
        return None
    
    def upload_to_db(self, df_to_upload, new_db_name, engine=None):
        """
        Uploads a DataFrame to the database into the specified table.

        Args:
            df (pd.DataFrame): The DataFrame to upload.
            table_name (str): Name of the target table in the database.

        Returns:
            None
        """        
        if engine is None:
            engine = self.init_db_engine()
        
        try:
            df_to_upload.to_sql(new_db_name, engine, if_exists='replace', index=False)
            print("table uploaded successfully.")
        except Exception as e:
            print(f"Error uploading table: {e}")
        return None