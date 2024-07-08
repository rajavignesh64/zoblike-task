import sqlite3
import pandas as pd

# Step 1: Connect to SQLite database
conn = sqlite3.connect('your_database_name.db')

# Step 2: Read data into Pandas DataFrames
df_customer = pd.read_sql_query("SELECT * FROM Customer", conn)
df_sales = pd.read_sql_query("SELECT * FROM Sales", conn)
df_orders = pd.read_sql_query("SELECT * FROM Orders", conn)
df_items = pd.read_sql_query("SELECT * FROM Items", conn)

# Step 3: Merge tables and filter data
df_merged = pd.merge(df_customer, df_sales, on='customer_id')
df_merged = pd.merge(df_merged, df_orders, on='sales_id')
df_merged = pd.merge(df_merged, df_items, on='item_id')

# Filter customers aged 18-35 and calculate total quantities
df_filtered = df_merged[(df_merged['age'] >= 18) & (df_merged['age'] <= 35)]
df_grouped = df_filtered.groupby(['customer_id', 'item_name']).agg({'quantity': 'sum'}).reset_index()
df_final = df_grouped[df_grouped['quantity'] > 0]

# Step 4: Store results in a CSV file with semicolon delimiter
df_final.to_csv('output_pandas_solution.csv', sep=';', index=False, header=['Customer', 'Age', 'Item', 'Quantity'])

# Step 5: Close database connection
conn.close()
