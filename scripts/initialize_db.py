# scripts/initialize_db.py

import sqlite3
import os
from scripts.config import config  # Absolute import from the scripts package

def initialize_database():
    """
    Initialize the SQLite database.

    This function creates a 'sales' table in the SQLite database if it doesn't exist.
    The table includes columns for order_id, customer_id, product_id, quantity, price, and order_date.

    AWS Parallel:
    This SQLite database emulates an AWS RDS (Relational Database Service) instance.
    """
    # Connect to the SQLite database (emulating AWS RDS)
    conn = sqlite3.connect(config.DATABASE_PATH)
    cursor = conn.cursor()

    # Create the 'sales' table with order_id as PRIMARY KEY if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sales (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT,
        product_id TEXT,
        quantity INTEGER,
        price REAL,
        order_date TEXT
    )
    ''')

    # Commit changes and close the connection
    conn.commit()
    conn.close()

if __name__ == "__main__":
    # Run the database initialization function when the script is executed
    initialize_database()
