import sqlite3
import pandas as pd

try:
    # Step 1: Connect to SQLite database
    conn = sqlite3.connect(r'D:\sample_space\zoblik\zoblike-task\Data Engineer_ETL Assignment.db')

    # Step 2: Read data into Pandas DataFrames
    df_customer = pd.read_sql_query("SELECT * FROM Customers", conn)
    df_sales = pd.read_sql_query("SELECT * FROM Sales", conn)
    df_orders = pd.read_sql_query("SELECT * FROM Orders", conn)
    df_items = pd.read_sql_query("SELECT * FROM Items", conn)

    # Step 3: Merge tables and filter data
    df_merged = pd.merge(df_customer, df_sales, on='customer_id')
    df_merged = pd.merge(df_merged, df_orders, on='sales_id')
    df_merged = pd.merge(df_merged, df_items, on='item_id')

    # Filter customers aged 18-35 and calculate total quantities
    df_filtered = df_merged[(df_merged['age'] >= 18) & (df_merged['age'] <= 35)]

    df_grouped = df_filtered.groupby(['customer_id', 'item_name', 'age']).agg({'quantity': 'sum'}).reset_index()

    df_final = df_grouped[df_grouped['quantity'] > 0]

    df_final['quantity'] = df_final['quantity'].astype(int)
    df_final['customer_id'] = df_final['customer_id'].astype(int)
    df_final['age'] = df_final['age'].astype(int)

    # Step 4: Store results in a CSV file with semicolon delimiter
    df_final.to_csv('output_pandas_solution.csv', sep=';', index=False, header=['customer_id', 'item_name', 'quantity', 'age'])

    # Step 5: Close database connection
    conn.close()

except Exception as e:
    print(f'Failed due to {e}')

print({"status_code":200,
      "status_messgae":"Ran Successfully"})