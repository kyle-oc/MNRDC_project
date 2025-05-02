from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import sys

yaml_directory = r"C:\Users\comma\VS Code projects\Python projects\mnrdc_project\MNRDC_project\db_creds.yaml"
local_yaml_directory = r"C:\Users\comma\VS Code projects\Python projects\mnrdc_project\MNRDC_project\local_db_creds.yaml"
pdf_url = r'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'


def run_user_data():
    """
    Extracts, cleans, and optionally uploads user data to the local database.
    """
    db_connector = DatabaseConnector(yaml_directory)
    engine = db_connector.init_db_engine()
    table_name = 'legacy_users'       
    extractor = DataExtractor(engine)
    user_df = extractor.read_rds_table(table_name)    
    cleaner = DataCleaning()
    cleaned_user_df = cleaner.clean_user_data(user_df)
    print("User data successfully cleaned")
    ask_and_upload(cleaned_user_df, "dim_users")
    
def run_card_data():
    """
    Extracts, cleans, and optionally uploads card data from a PDF to the local database.
    """
    pdf_url=r'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    card_df = DataExtractor.retrieve_pdf_data(pdf_url)
    cleaned_card_df = DataCleaning.clean_card_data(card_df)
    print("Card data successfully cleaned")
    ask_and_upload(cleaned_card_df, "dim_card_details")

def run_store_data():
    """
    Extracts, cleans, and optionally uploads store data to the local database.
    """
    db_connector = DatabaseConnector(yaml_directory)
    engine = db_connector.init_db_engine()
    table_name = 'legacy_store_details'       
    extractor = DataExtractor(engine)
    stores_df = extractor.read_rds_table(table_name)
    cleaned_stores_df = DataCleaning.clean_stores_data(stores_df)
    print("Store data successfully cleaned")
    ask_and_upload(cleaned_stores_df, "dim_store_details")
        
def run_products_data():
    """
    Extracts, cleans, and optionally uploads product data from S3 to the local database.
    """
    local_path = r'C:\Users\comma\VS Code projects\Python projects\mnrdc_project\MNRDC_project\products.csv'
    products_df = DataExtractor.extract_from_s3('data-handling-public', 'products.csv', local_path)
    cleaned_products_df = DataCleaning.clean_product_data(products_df)
    fixed_weights_df = DataCleaning.convert_product_weights(cleaned_products_df)
    print("Products data successfully cleaned")
    ask_and_upload(fixed_weights_df, "dim_products")
        
def run_orders_data():
    """
    Extracts, cleans, and optionally uploads order data to the local database.
    """
    table_name = 'orders_table'
    connector = DatabaseConnector(yaml_directory)
    engine = connector.init_db_engine()
    extractor = DataExtractor(engine)
    orders_df = extractor.read_rds_table(table_name)
    cleaned_orders_df = DataCleaning.clean_orders_data(orders_df)
    print("Orders data successfully cleaned")
    ask_and_upload(cleaned_orders_df, "dim_orders_details")
    
def run_events_data():
    """
    Extracts, cleans, and optionally uploads event data from S3 to the local database.
    """
    local_path = r"C:\Users\comma\VS Code projects\Python projects\mnrdc_project\MNRDC_project\date_details.json"
    events_df = DataExtractor.extract_from_s3('data-handling-public', 'date_details.json', local_path)
    cleaned_events_df = DataCleaning.clean_events_data(events_df)
    print("Events data successfully cleaned")    
    ask_and_upload(cleaned_events_df, "dim_date_times")
 
def ask_and_upload(df, table_name):
    """
    Asks the user if they want to upload the cleaned DataFrame to the local database.

    Args:
        df (pd.DataFrame): The cleaned data to be uploaded.
        table_name (str): The target table name in the database.
    """
    while True:
        upload_choice = input("Would you like to upload cleaned data? Y or N: ").casefold()
        if upload_choice == "y":
            connector = DatabaseConnector(local_yaml_directory)
            connector.upload_to_db(df, table_name)
            break
        elif upload_choice == "n":
            break
        else:
            print("Please enter 'Y' or 'N'. ")    

def display_menu():
    """
    Displays the main menu for selecting which dataset to process.
    """
    print("\n Please select the dataset you want to process:")
    print("1  User Data")
    print("2  Card Data")
    print("3  Store Data")
    print("4  Product Data")
    print("5  Orders Data")
    print("6  Events Data")
    print("0  Exit\n")

def main():
    """
    Main execution loop:
    Displays the menu, handles user input, and runs the corresponding data processing function.
    """
    runners = {
        "1": run_user_data,
        "2": run_card_data,
        "3": run_store_data,
        "4": run_products_data,
        "5": run_orders_data,
        "6": run_events_data,
    }
    
    while True:
        display_menu()
        choice = input("Enter choice number: ")
        
        if choice == "0":
            print("\nExiting program.")
            sys.exit()

        runner_func = runners.get(choice)
        if runner_func:
            runner_func()
        else:
            print("\nInvalid choice. Please select a valid number.\n")
            
if __name__ == "__main__":
    main()