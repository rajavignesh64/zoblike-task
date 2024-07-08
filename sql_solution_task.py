import sqlite3
import csv

# Step 1: Connect to SQLite database
conn = sqlite3.connect('sqllite.db')
cursor = conn.cursor()

# Step 2: SQL query to get total quantities of each item bought per customer aged 18-35
sql_query = """
SELECT c.customer_id, c.age, i.item_name, COALESCE(SUM(o.quantity), 0) AS total_quantity
FROM Customer c
JOIN Sales s ON c.customer_id = s.customer_id
JOIN Orders o ON s.sales_id = o.sales_id
JOIN Items i ON o.item_id = i.item_id
WHERE c.age BETWEEN 18 AND 35
GROUP BY c.customer_id, i.item_name
HAVING total_quantity > 0
"""

# Step 3: Execute the query
cursor.execute(sql_query)

# Step 4: Fetch all results
results = cursor.fetchall()

# Step 5: Store results in a CSV file with semicolon delimiter
with open('output_sql_solution.csv', 'w', newline='') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(['Customer', 'Age', 'Item', 'Quantity'])
    for row in results:
        writer.writerow(row)

# Step 6: Close database connection
conn.close()
