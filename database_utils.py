import yaml
import sqlalchemy

class DatabaseConnector:
    def __init__(self, yaml_file):
        """Initialise with the path to a YAML file and calls read_db_creds method."""
        self.yaml_file = yaml_file
        self.db_creds = self.read_db_creds()


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
            print("engine set up successfuly, giblet")
            return engine
        except Exception as e:
            print(f"Error: Failed to create database engine - {e}")
            return None
     
db_connector = DatabaseConnector(r"C:\Users\comma\VS Code projects\Python projects\mnrdc_project\MNRDC_project\db_creds.yaml")
engine = db_connector.init_db_engine()
