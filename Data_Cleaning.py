import pandas as pd
import os

# Create cleaned folder
os.makedirs("data/cleaned", exist_ok=True)

# LOAD RAW DATA
customers = pd.read_csv("data/raw/customers.csv")
products = pd.read_csv("data/raw/products.csv")
orders = pd.read_csv("data/raw/orders.csv")
order_items = pd.read_csv("data/raw/order_items.csv")
payments = pd.read_csv("data/raw/payments.csv")

# CUSTOMERS CLEANING
customers["signup_date"] = pd.to_datetime(customers["signup_date"])
customers = customers.drop_duplicates(subset="customer_id")

# PRODUCTS CLEANING
products = products[products["price"] > 0]

# ORDERS CLEANING
orders["order_date"] = pd.to_datetime(orders["order_date"])
orders = orders[orders["customer_id"].isin(customers["customer_id"])]

# ORDER ITEMS CLEANING
order_items = order_items[
    order_items["order_id"].isin(orders["order_id"]) &
    order_items["product_id"].isin(products["product_id"])
]

# PAYMENTS CLEANING
completed_orders = orders[orders["order_status"] == "Completed"]["order_id"]
payments = payments[payments["order_id"].isin(completed_orders)]

# SAVE CLEANED DATA
customers.to_csv("data/cleaned/customers_cleaned.csv", index=False)
products.to_csv("data/cleaned/products_cleaned.csv", index=False)
orders.to_csv("data/cleaned/orders_cleaned.csv", index=False)
order_items.to_csv("data/cleaned/order_items_cleaned.csv", index=False)
payments.to_csv("data/cleaned/payments_cleaned.csv", index=False)

print("Data cleaned and validated successfully!")
