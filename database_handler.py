import psycopg2 as psycopg2
import pandas as pd
import time

class DatabaseHandler:
    def __init__(self, connection_info_dict):
        self.connection_info_dict = connection_info_dict
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = psycopg2.connect(**self.connection_info_dict)
        self.cursor = self.connection.cursor()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()



    def execute_query(self, query, column_names, start_timestamp, end_timestamp):
        for attempt in range(3):  # Try up to three times
            try:
                if not self.connection:
                    self.connect()

                cursor = self.connection.cursor()
                print("Connected to DB")
                cursor.execute(query, (start_timestamp, end_timestamp))
                rows = cursor.fetchall()

                # Assuming you want the result in a DataFrame
                output_df = pd.DataFrame(rows, columns=column_names)
                return output_df  # Successfully fetched results, exit the function

            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(1)  # Optional delay between retries

                if attempt == 2:  # After the third attempt, print error and raise
                    print("Failed to connect to the database after 3 attempts.")
                    raise

    def convert_to_numeric(self, df, columns):
        for column in columns:
            df[column] = pd.to_numeric(df[column], errors='coerce')
        return df

    def convert_to_datetime(self, df, columns):
        for column in columns:
            df[column] = pd.to_datetime(df[column], errors='coerce')
        return df

    def save_to_pickle(self, df, pickle_path):
        df.to_pickle(pickle_path)
        print(f"DataFrame saved to {pickle_path}")

    def process_and_save_df(self, query, column_names, start_timestamp, end_timestamp, pickle_path, numeric_columns=None, date_columns=None, bool_columns=None):
        df = self.execute_query(query, column_names, start_timestamp, end_timestamp)
        if numeric_columns:
            df = self.convert_to_numeric(df, numeric_columns)
        if date_columns:
            df = self.convert_to_datetime(df, date_columns)
        if bool_columns:
            df = self.convert_to_boolean(df, bool_columns)
        self.save_to_pickle(df, pickle_path)
        return df
