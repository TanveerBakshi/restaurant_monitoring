import pandas as pd
import sqlite3

def load_csv_to_db(csv_file, table_name, columns):
    try:
        conn = sqlite3.connect('restaurant_monitoring.db')
        df = pd.read_csv(csv_file, usecols=columns)
        print(f"Loaded data from {csv_file} into DataFrame:\n{df.head()}")
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Data loaded into table {table_name}.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    load_csv_to_db('store status.csv', 'status_logs', ['store_id', 'timestamp_utc', 'status'])
    load_csv_to_db('Menu hours.csv', 'restaurants', ['store_id', 'day', 'start_time_local', 'end_time_local'])
    load_csv_to_db('bq-results-20230125-202210-1674678181880.csv', 'timezones', ['store_id', 'timezone_str'])