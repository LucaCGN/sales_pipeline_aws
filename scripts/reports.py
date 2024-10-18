# scripts/reports.py

import sqlite3
import pandas as pd
from scripts.config import config  # Absolute import from the scripts package

# Database path emulating AWS RDS
DATABASE_PATH = config.DATABASE_PATH

def get_top_customers():
    """
    Generate a report of the top 5 customers with the highest revenue.

    AWS Parallel:
    - Similar to executing a query on AWS RDS to retrieve top customers based on sales data.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    query = '''
    SELECT customer_id, SUM(price * quantity) AS total_revenue
    FROM sales
    GROUP BY customer_id
    ORDER BY total_revenue DESC
    LIMIT 5
    '''
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_top_products():
    """
    Generate a report of the top 5 best-selling products by quantity sold.

    AWS Parallel:
    - Analogous to querying AWS RDS to identify top-selling products based on sales volume.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    query = '''
    SELECT product_id, SUM(quantity) AS total_quantity_sold
    FROM sales
    GROUP BY product_id
    ORDER BY total_quantity_sold DESC
    LIMIT 5
    '''
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_top_products_by_revenue():
    """
    Generate a report of the top 5 products by total revenue.

    AWS Parallel:
    - Equivalent to querying AWS RDS to determine products generating the most revenue.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    query = '''
    SELECT product_id, SUM(price * quantity) AS total_revenue
    FROM sales
    GROUP BY product_id
    ORDER BY total_revenue DESC
    LIMIT 5
    '''
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_top_customers_by_quantity():
    """
    Generate a report of the top 5 customers by total quantity purchased.

    AWS Parallel:
    - Similar to analyzing customer purchase patterns using AWS RDS queries.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    query = '''
    SELECT customer_id, SUM(quantity) AS total_quantity_purchased
    FROM sales
    GROUP BY customer_id
    ORDER BY total_quantity_purchased DESC
    LIMIT 5
    '''
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_monthly_revenue():
    """
    Generate a report of total revenue per month, including months with zero revenue.

    AWS Parallel:
    - Mirrors monthly revenue aggregation tasks that might be handled by AWS Glue or AWS Athena.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    query = '''
    SELECT strftime('%Y-%m', order_date) AS month, SUM(price * quantity) AS total_revenue
    FROM sales
    GROUP BY month
    ORDER BY month
    '''
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Convert 'month' to datetime for complete date range
    df['month'] = pd.to_datetime(df['month'], format='%Y-%m')

    # Create a complete date range to include months with zero revenue
    start_month = df['month'].min()
    end_month = df['month'].max()
    all_months = pd.date_range(start=start_month, end=end_month, freq='MS')  # MS: month start

    # Reindex the DataFrame to include all months, filling missing with zero revenue
    df = df.set_index('month').reindex(all_months).fillna(0).rename_axis('month').reset_index()

    # Convert 'month' back to string format for consistency
    df['month'] = df['month'].dt.strftime('%Y-%m')

    return df

def get_all_reports_as_text():
    """
    Gather all reports and concatenate them into a single string for AI processing.

    AWS Parallel:
    - Similar to aggregating multiple AWS Athena query results for comprehensive analysis.
    """
    from io import StringIO

    output = StringIO()

    output.write("Top 5 Customers by Revenue:\n")
    top_customers_df = get_top_customers()
    output.write(top_customers_df.to_string(index=False))
    output.write("\n\n")

    output.write("Top 5 Best-Selling Products by Quantity:\n")
    top_products_df = get_top_products()
    output.write(top_products_df.to_string(index=False))
    output.write("\n\n")

    output.write("Top 5 Products by Revenue:\n")
    top_products_by_revenue_df = get_top_products_by_revenue()
    output.write(top_products_by_revenue_df.to_string(index=False))
    output.write("\n\n")

    output.write("Top 5 Customers by Quantity Purchased:\n")
    top_customers_by_quantity_df = get_top_customers_by_quantity()
    output.write(top_customers_by_quantity_df.to_string(index=False))
    output.write("\n\n")

    output.write("Monthly Revenue:\n")
    monthly_revenue_df = get_monthly_revenue()
    output.write(monthly_revenue_df.to_string(index=False))
    output.write("\n")

    return output.getvalue()

if __name__ == "__main__":
    # Example usage: Generate and print reports
    print("Top 5 Customers by Revenue:")
    print(get_top_customers())
    print("\n")

    print("Top 5 Best-Selling Products by Quantity:")
    print(get_top_products())
    print("\n")

    print("Top 5 Products by Revenue:")
    print(get_top_products_by_revenue())
    print("\n")

    print("Top 5 Customers by Quantity Purchased:")
    print(get_top_customers_by_quantity())
    print("\n")

    print("Monthly Revenue:")
    print(get_monthly_revenue())
