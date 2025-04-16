import pandas as pd
import numpy as np

class DataCleaning:
    def __init__(self):
        """Initialise with a DataExtractor instance, fetches rds table, and cleans data."""
        pass
        
    
    def clean_user_data(self):
        """Cleans data by performing the following:
        - Converts "NULL" string values to NaN
        - Removes rows with missing 'join_date'
        - Detects and removes invalid values
        - Standardises date formats
        - Converts 'join_date' column to datetime format
        
        Returns:
            None (modifies self.user_df in-place)
            """
        month_dict = {
            'January': '01', 'February': '02', 'March': '03', 'April': '04',
            'May': '05', 'June': '06', 'July': '07', 'August': '08',
            'September': '09', 'October': '10', 'November': '11', 'December': '12'
        }
        
        # Drop 'index' column
        self.user_df.drop(columns='index', inplace=True)
        
        # Convert "NULL" strings to NaN type
        self.user_df.replace("NULL", np.nan, inplace=True)

        # Remove NULL values
        self.user_df.dropna(inplace=True)

        # Replace '/' date separator with '-'
        # join_date
        self.user_df['join_date'] = self.user_df['join_date'].str.replace('/', '-')
        # date_of_birth
        self.user_df['date_of_birth'] = self.user_df['date_of_birth'].str.replace('/', '-')

        # Use dictionary to replace month names with month numbers
        # join_date
        for month, number in month_dict.items(): 
            self.user_df['join_date'] = self.user_df['join_date'].str.replace(month, number)
        # date_of_birth
        for month, number in month_dict.items(): 
            self.user_df['date_of_birth'] = self.user_df['date_of_birth'].str.replace(month, number)

        # Replace white space (left over from month name conversion) with '-'
        # join_date
        self.user_df['join_date'] = self.user_df['join_date'].str.replace(' ', '-', regex=False)
        # date_of_birth
        self.user_df['date_of_birth'] = self.user_df['date_of_birth'].str.replace(' ', '-', regex=False)

        # Identify and drop invalid date values
        # join_date
        invalid_dates = self.user_df[~self.user_df['join_date'].str.match(r'^\d{2}-\d{2}-\d{4}$|^\d{2}-\d{4}-\d{2}$|^\d{4}-\d{2}-\d{2}$', na=False)]
        print("The following invalid date entries were detected and will be removed:\n", invalid_dates, "\n")
        self.user_df.drop(invalid_dates.index, inplace=True)
        print()
        
        # Fix date formats that are in MM-YYYY-DD before global conversion
        # join_date
        regex_filter = r'^\d{2}-\d{4}-\d{2}$'  # Matches 'MM-YYYY-DD'
        mask = self.user_df['join_date'].str.match(regex_filter, na=False)
        self.user_df.loc[mask, 'join_date'] = pd.to_datetime(self.user_df.loc[mask, 'join_date'], format='%m-%Y-%d', errors='coerce')
        # date_of_birth
        mask = self.user_df['date_of_birth'].str.match(regex_filter, na=False)
        self.user_df.loc[mask, 'date_of_birth'] = pd.to_datetime(self.user_df.loc[mask, 'date_of_birth'], format='%m-%Y-%d', errors='coerce')
        
        # Convert "join_data" column into a datetime data type
        # join_date
        self.user_df['join_date'] = pd.to_datetime(self.user_df['join_date'], errors="coerce")
        # date_of_birth
        self.user_df['date_of_birth'] = pd.to_datetime(self.user_df['date_of_birth'], errors="coerce")
        
        return self.user_df
    
    def clean_card_data(self, card_df):
        """Cleans card data by doing the following:
        - Converts "card_number" (NULL) string values to NULL data type
        - Removes NULL values
        - Removes duplicate values
        - Removes non-numerical card numbers
        - Converts "date_payment_confirmed" column into a datetime data type
        
        Returns:
            None (modifies self.card_df in-place)"""
                

        # Convert "card_number" (NULL) string values to NULL data type
        card_df['card_number'] = card_df['card_number'].replace('card_number', np.nan)

        # Remove NULL values
        card_df = card_df.dropna()
        
        # Remove '?' from card_numbers
        card_df['card_number'] = card_df['card_number'].str.strip("?")

        # Drop duplicate values
        card_df = card_df.drop_duplicates()

        # Detect invalid card providers
        valid_card_providers = ['VISA 16 digit', 'JCB 16 digit', 'VISA 13 digit', 'JCB 15 digit', 'VISA 19 digit', 'Diners Club / Carte Blanche',
                                'American Express', 'Maestro', 'Discover', 'Mastercard']

        invalid_cards = card_df['card_provider'][~card_df['card_provider'].isin(valid_card_providers)]
        print("The following invalid card providers were detected and will be removed:\n", invalid_cards, "\n")
        card_df.drop(invalid_cards.index, inplace=True)

        # Convert date strings in date_payment_confirmed column
        month_dict = {
                    'January': '01', 'February': '02', 'March': '03', 'April': '04',
                    'May': '05', 'June': '06', 'July': '07', 'August': '08',
                    'September': '09', 'October': '10', 'November': '11', 'December': '12'
                }

        for month, number in month_dict.items(): 
            card_df['date_payment_confirmed'] = card_df['date_payment_confirmed'].str.replace(month, number)
                
        # Replace white space (left over from month name conversion) with '-'
        card_df['date_payment_confirmed'] = card_df['date_payment_confirmed'].str.replace(' ', '-', regex=False)

        # Replace '/' with '-' to prevent issues during datetime conversion
        card_df['date_payment_confirmed'] = card_df['date_payment_confirmed'].str.replace('/', '-', regex=False)

        # Fix date formats that are in MM-YYYY-DD before global conversion
        regex_filter = r'^\d{2}-\d{4}-\d{2}$'  # Matches 'MM-YYYY-DD'
        mask = card_df['date_payment_confirmed'].str.match(regex_filter, na=False)
        card_df.loc[mask, 'date_payment_confirmed'] = pd.to_datetime(card_df.loc[mask, 'date_payment_confirmed'], format='%m-%Y-%d', errors='coerce')

        # Convert 'date_payment_confirmed' column to datetime
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], errors='coerce')

        
# if __name__=='__main__':
# initialiser = DataCleaning("legacy_users")
    # initialiser.clean_user_data()
    # df = initialiser.read_rds_table("legacy_users")
    # upload_df = DatabaseConnector.upload_to_db()
    





