from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(
    "mysql+mysqlconnector://root:@localhost:3306/nepalmart"
)
print("Database connected")

print("\n Tables in database:")
tables_df = pd.read_sql("SHOW TABLES;", engine)
print(tables_df)

print("\n Customers preview:")
customers_df = pd.read_sql("SELECT * FROM customers LIMIT 5;", engine)
print(customers_df)

print("\n Row counts:")
tables = ["customers", "orders", "order_items", "products", "payments"]
for table in tables:
    count_df = pd.read_sql(
        f"SELECT COUNT(*) AS total_rows FROM {table};", engine
    )
    print(f"{table}: {count_df['total_rows'][0]} rows")


describe_customers = pd.read_sql("DESCRIBE customers;", engine)
print(describe_customers)

query = """
SELECT
    COUNT(*) - COUNT(name) AS missing_name,
    COUNT(*) - COUNT(gender) AS missing_gender,
    COUNT(*) - COUNT(city) AS missing_city   
FROM customers;
"""
df = pd.read_sql(query, engine)
print(df)


Describe_orders = pd.read_sql("DESCRIBE orders;", engine)
print(Describe_orders)

query = """
SELECT
    COUNT(*) - COUNT(order_date) AS missing_order_date,
    COUNT(*) - COUNT(order_status) AS missing_order_status,
    COUNT(*) - COUNT(customer_id) AS missing_customer_id
FROM orders;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT
    customer_id,
    COUNT(*) AS cnt
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1;
"""
df = pd.read_sql(query, engine)
if not df.empty:
    print("❌ Duplicate records detected")
else:
    print("✅ No duplicates found")

query = """
SELECT
    customer_id,
    order_date,
    COUNT(*) AS cnt
FROM orders
WHERE customer_id IS NOT NULL
GROUP BY customer_id, order_date
HAVING COUNT(*) > 1;
"""
df = pd.read_sql(query, engine)
if not df.empty:
    print("❌ Duplicate records detected")
else:
    print("✅ No duplicates found")

query = '''
SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id, order_date
        ORDER BY order_id
    ) AS rn
FROM orders;
'''
df = pd.read_sql(query, engine)
print(df)


order_item = pd.read_sql("DESCRIBE order_items;", engine)
print(order_item)

query = """
SELECT *
FROM order_items
WHERE quantity <= 0 OR quantity IS NULL;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT
    MIN(quantity) AS min_qty,
    MAX(quantity) AS max_qty,
    AVG(quantity) AS avg_qty
FROM order_items;
"""
print(pd.read_sql(query, engine))

query = """
SELECT
    MIN(price) AS min_price,
    MAX(price) AS max_price
FROM products;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT COUNT(*) AS orphan_orders
FROM orders o
LEFT JOIN customers c
ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT COUNT(*) AS orphan_items
FROM order_items oi
LEFT JOIN products p
ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT DISTINCT city
FROM customers;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT
    customer_id,
    name,
    LOWER(TRIM(city)) AS City
FROM customers;
"""
df = pd.read_sql(query, engine)
print(df.head(5))



















