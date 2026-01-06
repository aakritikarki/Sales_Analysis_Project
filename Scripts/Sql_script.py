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

Describe_payments = pd.read_sql("DESCRIBE payments;", engine)
print(Describe_payments)

Describe_products = pd.read_sql("DESCRIBE products;", engine)
print(Describe_products)

query = """
SELECT p.payment_id, p.order_id
FROM payments p
LEFT JOIN orders o
  ON p.order_id = o.order_id
WHERE p.order_id IS NOT NULL
  AND o.order_id IS NULL;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT o.order_id
FROM orders o
LEFT JOIN payments p
  ON o.order_id = p.order_id
WHERE o.order_status = 'Completed'
  AND p.order_id IS NULL;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT
    oi.order_id,
    SUM(oi.quantity * oi.item_price) AS order_total
FROM order_items oi
GROUP BY oi.order_id;
"""
order_totals = pd.read_sql(query, engine)

query = """
SELECT
    o.order_id,
    SUM(p.amount) AS paid_amount
FROM orders o
LEFT JOIN payments p
  ON o.order_id = p.order_id
GROUP BY o.order_id;
"""
payments_total = pd.read_sql(query, engine)

df = order_totals.merge(payments_total, on="order_id", how="left")
df["paid_amount"] = df["paid_amount"].fillna(0)

df_invalid = df[df["paid_amount"] != df["order_total"]]
print(df_invalid)

query = """
SELECT p.payment_id, p.payment_date, o.order_date
FROM payments p
JOIN orders o
  ON p.order_id = o.order_id
WHERE p.payment_date < o.order_date;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT *
FROM payments
WHERE amount <= 0 OR amount IS NULL;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT oi.product_id
FROM order_items oi
LEFT JOIN products p
  ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT *
FROM products
WHERE price <= 0 OR price IS NULL;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT o.order_id, p.payment_id
FROM orders o
JOIN payments p
  ON o.order_id = p.order_id
WHERE o.order_status = 'Cancelled';
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT
    SUM(oi.quantity * oi.item_price) AS total_revenue
FROM order_items oi
JOIN orders o
  ON oi.order_id = o.order_id
WHERE o.order_status = 'Completed';
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT
    SUM(amount) AS total_paid
FROM payments;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT
    order_status,
    COUNT(*) AS order_count
FROM orders
GROUP BY order_status;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT
    order_status,
    COUNT(*) AS order_count
FROM orders
GROUP BY order_status;
"""
df = pd.read_sql(query, engine)
print(df)


query = """
SELECT COUNT(DISTINCT customer_id) AS repeat_customers
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 1;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT
    p.product_name,
    SUM(oi.quantity) AS total_quantity_sold
FROM order_items oi
JOIN products p
  ON oi.product_id = p.product_id
GROUP BY p.product_name;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT
    o.order_date,
    SUM(oi.quantity * oi.item_price) AS daily_revenue
FROM orders o
JOIN order_items oi
  ON o.order_id = oi.order_id
GROUP BY o.order_date;
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS active_customers,
    SUM(oi.quantity * oi.item_price) AS total_revenue,
    AVG(order_totals.order_total) AS avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN (
    SELECT order_id, SUM(quantity * item_price) AS order_total
    FROM order_items
    GROUP BY order_id
) order_totals ON o.order_id = order_totals.order_id
WHERE o.order_status = 'Completed';
"""
df = pd.read_sql(query, engine)
print(df)

query = '''
WITH order_totals AS (
    SELECT
        oi.order_id,
        SUM(oi.quantity * oi.item_price) AS order_total
    FROM order_items oi
    GROUP BY oi.order_id
)
SELECT
    o.order_id,
    o.customer_id,
    ot.order_total,
    SUM(ot.order_total) OVER (PARTITION BY o.customer_id) AS customer_lifetime_value
FROM orders o
JOIN order_totals ot
  ON o.order_id = ot.order_id;
'''
df = pd.read_sql(query, engine)
print(df.head(5))

query = """
WITH order_totals AS (
    SELECT
        oi.order_id,
        SUM(oi.quantity * oi.item_price) AS order_total
    FROM order_items oi
    GROUP BY oi.order_id
)
SELECT
    o.customer_id,
    o.order_id,
    ot.order_total,
    RANK() OVER (
        PARTITION BY o.customer_id
        ORDER BY ot.order_total DESC
    ) AS order_rank
FROM orders o
JOIN order_totals ot
  ON o.order_id = ot.order_id;
  """
df = pd.read_sql(query, engine)
print(df)

query = '''
WITH daily_revenue AS (
    SELECT
        o.order_date,
        SUM(oi.quantity * oi.item_price) AS daily_revenue
    FROM orders o
    JOIN order_items oi
      ON o.order_id = oi.order_id
    WHERE o.order_status = 'Completed'
    GROUP BY o.order_date
)
SELECT
    order_date,
    daily_revenue,
    SUM(daily_revenue) OVER (
        ORDER BY order_date
    ) AS running_revenue
FROM daily_revenue;
'''
df = pd.read_sql(query, engine)
print(df.head(5))

query = '''
WITH daily_revenue AS (
    SELECT
        o.order_date,
        SUM(oi.quantity * oi.item_price) AS daily_revenue
    FROM orders o
    JOIN order_items oi
      ON o.order_id = oi.order_id
    WHERE o.order_status = 'Completed'
    GROUP BY o.order_date
)
SELECT
    order_date,
    daily_revenue,
    AVG(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d
FROM daily_revenue;
'''
df = pd.read_sql(query, engine)
print(df.head(5))

query = '''
WITH ranked_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, order_date
            ORDER BY order_id
        ) AS rn
    FROM orders
)
SELECT *
FROM ranked_orders
WHERE rn > 1;
'''
df = pd.read_sql(query, engine)
print(df.head(5))

query = """
SELECT 
    DATE_FORMAT(payment_date, '%Y-%m') AS month,
    SUM(amount) AS revenue
FROM payments
GROUP BY month
ORDER BY month;
"""
monthly_df = pd.read_sql(query, engine)
monthly_df

import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(10,5))
sns.lineplot(data=monthly_df, x="month", y="revenue", marker="o")
plt.title("Monthly Revenue Trend")
plt.xlabel("Month")
plt.ylabel("Revenue")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
























