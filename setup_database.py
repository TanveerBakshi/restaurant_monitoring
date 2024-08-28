import sqlite3

def recreate_table():
    conn = sqlite3.connect('restaurant_monitoring.db')
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS status_logs')
    cursor.execute('DROP TABLE IF EXISTS restaurants')
    cursor.execute('DROP TABLE IF EXISTS timezones')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS status_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            store_id INTEGER,
            timestamp_utc TEXT,
            status TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS restaurants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            store_id INTEGER,
            day INTEGER,
            start_time_local TEXT,
            end_time_local TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS timezones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            store_id INTEGER,
            timezone_str TEXT
        )
    ''')

    conn.commit()  
    conn.close()   

if __name__ == '__main__':
    recreate_table()  
