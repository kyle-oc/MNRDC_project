import pandas as pd
import numpy as np
import re

class DataCleaning:
    @staticmethod
    def clean_user_data(user_df):
        """Cleans data by performing the following:
        - Converts "NULL" string values to NaN.
        - Removes rows with missing 'join_date'.
        - Detects and removes invalid values.
        - Standardises date formats.
        - Converts 'join_date' column to datetime format.
        
        Args:
            user_df (pd.DataFrame): Raw user data.
        
        Returns:
            pd.DataFrame: cleaned user_df.
            """
        # Drop obsolete index columns
        user_df = user_df.drop(['level_0', 'Unnamed: 0'],axis=1)
        
        month_dict = {
            'January': '01', 'February': '02', 'March': '03', 'April': '04',
            'May': '05', 'June': '06', 'July': '07', 'August': '08',
            'September': '09', 'October': '10', 'November': '11', 'December': '12'
        }
        
        # Drop 'index' column
        user_df.drop(columns='index', inplace=True)
        
        # Convert "NULL" strings to NaN type
        user_df.replace("NULL", np.nan, inplace=True)

        # Remove NULL values
        user_df.dropna(inplace=True)

        # Replace '/' date separator with '-'
        # join_date
        user_df['join_date'] = user_df['join_date'].str.replace('/', '-')
        # date_of_birth
        user_df['date_of_birth'] = user_df['date_of_birth'].str.replace('/', '-')

        # Use dictionary to replace month names with month numbers
        # join_date
        for month, number in month_dict.items(): 
            user_df['join_date'] = user_df['join_date'].str.replace(month, number)
        # date_of_birth
        for month, number in month_dict.items(): 
            user_df['date_of_birth'] = user_df['date_of_birth'].str.replace(month, number)

        # Replace white space (left over from month name conversion) with '-'
        # join_date
        user_df['join_date'] = user_df['join_date'].str.replace(' ', '-', regex=False)
        # date_of_birth
        user_df['date_of_birth'] = user_df['date_of_birth'].str.replace(' ', '-', regex=False)

        # Identify and drop invalid date values
        # join_date
        invalid_dates = user_df[~user_df['join_date'].str.match(r'^\d{2}-\d{2}-\d{4}$|^\d{2}-\d{4}-\d{2}$|^\d{4}-\d{2}-\d{2}$', na=False)]
        print("The following invalid date entries were detected and will be removed:\n", invalid_dates, "\n")
        user_df.drop(invalid_dates.index, inplace=True)
        print()
        
        # Fix date formats that are in MM-YYYY-DD before global conversion
        # join_date
        regex_filter = r'^\d{2}-\d{4}-\d{2}$'  # Matches 'MM-YYYY-DD'
        mask = user_df['join_date'].str.match(regex_filter, na=False)
        user_df.loc[mask, 'join_date'] = pd.to_datetime(user_df.loc[mask, 'join_date'], format='%m-%Y-%d', errors='coerce')
        # date_of_birth
        mask = user_df['date_of_birth'].str.match(regex_filter, na=False)
        user_df.loc[mask, 'date_of_birth'] = pd.to_datetime(user_df.loc[mask, 'date_of_birth'], format='%m-%Y-%d', errors='coerce')
        
        # Convert "join_data" column into a datetime data type
        # join_date
        user_df['join_date'] = pd.to_datetime(user_df['join_date'], errors="coerce")
        # date_of_birth
        user_df['date_of_birth'] = pd.to_datetime(user_df['date_of_birth'], errors="coerce")
        
        return user_df
    
    @staticmethod
    def clean_card_data(card_df):
        """Cleans card data by doing the following:
        - Converts "card_number" (NULL) string values to NULL data type.
        - Removes NULL values.
        - Removes duplicate values.
        - Removes non-numerical card numbers.
        - Converts "date_payment_confirmed" column into a datetime data type.
        
        Args:
            card_df (pd.DataFrame): Raw card data.
        
        Returns:
            pd.DataFrame: cleaned card_df"""
                
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
        
        return card_df
        
    @staticmethod
    def clean_stores_data(store_df):
        """
        Cleans store data by:
        - Fixing country names.
        - Handling missing values.
        - Converting opening dates to datetime.

        Args:
            stores_df (pd.DataFrame): Raw store data.

        Returns:
            pd.DataFrame: Cleaned store data.
        """        
        
        # Drop obsolete index columns
        store_df = store_df.drop(['level_0','Unnamed: 0', 'index'], axis=1)
        
        # Drop lat column
        store_df = store_df.drop('lat', axis=1)
        
        # Replace '\n' with comma
        store_df['address'] = store_df['address'].str.replace('\n', ', ')
        
        # Remove 'ee' in continent columns
        store_df['continent'] = store_df['continent'].str.replace('ee', '')
                
        # Detect invalid card providers (remove NULL values)
        valid_continents = ['Europe', 'America']

        invalid_continents = store_df['continent'][~store_df['continent'].isin(valid_continents)]
        print("The following invalid entries in the continent column were detected and will be removed:\n", invalid_continents, "\n")
        store_df.drop(invalid_continents.index, inplace=True)
        
        # Convert date strings in date_payment_confirmed column
        month_dict = {
                    'January': '01', 'February': '02', 'March': '03', 'April': '04',
                    'May': '05', 'June': '06', 'July': '07', 'August': '08',
                    'September': '09', 'October': '10', 'November': '11', 'December': '12'
                }

        for month, number in month_dict.items(): 
            store_df['opening_date'] = store_df['opening_date'].str.replace(month, number)
                
        # Replace white space (left over from month name conversion) with '-'
        store_df['opening_date'] = store_df['opening_date'].str.replace(' ', '-', regex=False)
        
        # Fix date formats that are in MM-YYYY-DD before global conversion
        regex_filter = r'^\d{2}-\d{4}-\d{2}$'  # Matches 'MM-YYYY-DD'
        mask = store_df['opening_date'].str.match(regex_filter, na=False)
        store_df.loc[mask, 'opening_date'] = pd.to_datetime(store_df.loc[mask, 'opening_date'], format='%m-%Y-%d', errors='coerce')
        
        # Convert all opening_date rows to string, then remove timestamp to prevent to_datetime conversion errors        
        store_df['opening_date'] = store_df['opening_date'].astype(str)
        store_df['opening_date'] = store_df['opening_date'].str.replace(' 00:00:00', '', regex=False)

        # Replace '/' with '-' to prevent issues during datetime conversion
        store_df['opening_date'] = store_df['opening_date'].str.replace('/', '-', regex=False)
        
        # Convert "opening_date" column into a datetime data type
        store_df['opening_date'] = pd.to_datetime(store_df['opening_date'], errors='raise')
        
        # Strip away symbols, letters, and white spaces from "staff_number" column
        store_df['staff_numbers'] = store_df['staff_numbers'].str.replace('[^0-9]','', regex=True)
                
        return store_df
    
    @staticmethod
    def convert_product_weights(products_df):
        """
        Converts the 'weight' column in product data to kilograms.

        Args:
            products_df (pd.DataFrame): Product data with various weight formats.

        Returns:
            pd.DataFrame: Product data with standardized weight in kilograms.
        """
        # Remove errant '.' from end of 'weight' row before processing
        products_df['weight'] = products_df['weight'].str.rstrip('.')
        
        # kg weights (remove 'kg'):
        products_df['weight'] = products_df['weight'].str.replace('kg', '', regex=False)
        
        # Multipack weights:        
        def process_multipack(w):
            parts = re.findall(r'\d+', w)
            if len(parts) == 2:
                return round(int(parts[0]) * int(parts[1]) / 1000, 2)
            return None
        
        mask = products_df['weight'].str.contains('x', na=False)
        products_df.loc[mask, 'weight'] = products_df.loc[mask, 'weight'].apply(process_multipack)
        
        # Convert oz weights:        
        def process_oz(w):
            match = re.search(r'\d+', w)
            if match:
                oz_val = int(match.group())
                return round(oz_val / 35.274, 2)
            return w
        
        mask = products_df['weight'].str.contains('oz', na=False)
        products_df.loc[mask, 'weight'] = products_df.loc[mask, 'weight'].apply(process_oz)
        
        # Convert g and ml weights:
        def process_g_or_ml(w):
            match = re.search(r'\d+\.?\d*', w) # regex 
            if match:
                g_val = float(match.group())
                return g_val / 1000
            return w
        
        mask = products_df['weight'].str.contains(r'g|ml', na=False)
        products_df.loc[mask, 'weight'] = products_df.loc[mask, 'weight'].apply(process_g_or_ml)
        
        # Correcting products with incorrect weight formats in original dataset (labelled as 'g', rather than 'kg')
        mask = products_df['weight'].astype(float) < 0.01
        products_df.loc[mask, 'weight'] = products_df.loc[mask, 'weight'] * 1000

        # Round and convert entire column to float:
        products_df['weight'] = products_df['weight'].astype(dtype=float, errors='raise').round(2)
        return products_df
    
    @staticmethod
    def clean_product_data(products_df):
        """
        Cleans products data by:
        - Fixing date formats.
        - Correcting spelling mistakes.
        - Removing null rows.

        Args:
            products_df (pd.DataFrame): Raw products data.

        Returns:
            pd.DataFrame: Cleaned products data.
        """
        # Convert date strings in 'date_added' column
        month_dict = {
                    'January': '01', 'February': '02', 'March': '03', 'April': '04',
                    'May': '05', 'June': '06', 'July': '07', 'August': '08',
                    'September': '09', 'October': '10', 'November': '11', 'December': '12'
                }

        for month, number in month_dict.items(): 
            products_df['date_added'] = products_df['date_added'].str.replace(month, number)
            
        # Replace white space (left over from month name conversion) with '-'
        products_df['date_added'] = products_df['date_added'].str.replace(' ', '-', regex=False)
        
        # Fix date formats that are in MM-YYYY-DD before global conversion
        regex_filter = r'^\d{2}-\d{4}-\d{2}$'  # Matches 'MM-YYYY-DD'
        mask = products_df['date_added'].str.match(regex_filter, na=False)
        products_df.loc[mask, 'date_added'] = pd.to_datetime(products_df.loc[mask, 'date_added'], format='%m-%Y-%d', errors='coerce')

        # Convert entire 'date_added' column to datetime
        products_df['date_added'] = pd.to_datetime(products_df['date_added'], errors='coerce')
        
        # Drop NULLS
        products_df.dropna(inplace=True)
        
        # Fix spelling error:
        products_df['removed'] = products_df['removed'].str.replace('Still_avaliable', 'Still_available', regex=False)
        
        return products_df
    
    @staticmethod
    def clean_orders_data(orders_df):
        """
        Cleans orders data by:
        - Dropping unnecessary columns.

        Args:
            orders_df (pd.DataFrame): Raw orders data.

        Returns:
            pd.DataFrame: Cleaned orders data.
        """
        orders_df = orders_df.drop(['Unnamed: 0', 'index', 'level_0', 'first_name', 'last_name', '1'], axis=1)
        return orders_df
     
    @staticmethod
    def clean_events_data(events_df):
        """
        Cleans events data by:
        - Removing invalid 'time_period' entries.
        - Replacing 'NULL' strings with NaN.
        - Dropping null rows.

        Args:
            events_df (pd.DataFrame): Raw events data.

        Returns:
            pd.DataFrame: Cleaned events data.
        """
        # Detect and remove invalid time_period entries:
        valid_time_periods = ['Evening', 'Midday', 'Morning', 'Late_Hours', 'NULL']
        invalid_time_periods = events_df['time_period'][~events_df['time_period'].isin(valid_time_periods)]
        print("The following invalid entries were detected and will be removed:\n", invalid_time_periods, "\n")
        events_df.drop(invalid_time_periods.index, inplace=True)
                
        # Convert 'NULL' strings to NULL, then drop:
        events_df = events_df.replace('NULL', np.nan)
        events_df = events_df.dropna()
        
        return events_df