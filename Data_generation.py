import pandas as pd
import random
import os
from datetime import datetime, timedelta

os.makedirs("data/raw", exist_ok=True)
random.seed(42)

# CONFIG

NUM_CUSTOMERS = 5000
NUM_PRODUCTS = 200
NUM_ORDERS = 20000

cities = ["Kathmandu", "Lalitpur", "Bhaktapur", "Pokhara"]
categories = ["Electronics", "Clothing", "Household", "Groceries"]
payment_methods = ["Cash on Delivery", "Digital Wallet", "Bank Transfer"]

# CUSTOMERS
customers = []
for i in range(1, NUM_CUSTOMERS + 1):
    customers.append({
        "customer_id": i,
        "name": f"Customer_{i}",
        "gender": random.choice(["Male", "Female"]),
        "city": random.choice(cities),
        "signup_date": datetime(2021, 1, 1) + timedelta(days=random.randint(0, 900))
    })

customers_df = pd.DataFrame(customers)

# PRODUCTS

products = []
for i in range(1, NUM_PRODUCTS + 1):
    products.append({
        "product_id": i,
        "product_name": f"Product_{i}",
        "category": random.choice(categories),
        "price": random.randint(300, 50000)
    })

products_df = pd.DataFrame(products)

# ORDERS
orders = []
for i in range(1, NUM_ORDERS + 1):
    orders.append({
        "order_id": i,
        "customer_id": random.randint(1, NUM_CUSTOMERS),
        "order_date": datetime(2021, 1, 1) + timedelta(days=random.randint(0, 900)),
        "order_status": random.choice(["Completed", "Cancelled", "Returned"])
    })

orders_df = pd.DataFrame(orders)

# ORDER ITEMS
order_items = []
for order_id in orders_df["order_id"]:
    for _ in range(random.randint(1, 3)):
        product = products_df.sample(1).iloc[0]
        order_items.append({
            "order_id": order_id,
            "product_id": product["product_id"],
            "quantity": random.randint(1, 3),
            "item_price": product["price"]
        })

order_items_df = pd.DataFrame(order_items)

# PAYMENTS

payments = []
for order in orders_df.itertuples():
    if order.order_status == "Completed":
        payments.append({
            "payment_id": order.order_id,
            "order_id": order.order_id,
            "payment_method": random.choice(payment_methods),
            "payment_date": order.order_date,
            "amount": random.randint(500, 150000)
        })

payments_df = pd.DataFrame(payments)

# SAVE FILES

customers_df.to_csv("data/raw/customers.csv", index=False)
products_df.to_csv("data/raw/products.csv", index=False)
orders_df.to_csv("data/raw/orders.csv", index=False)
order_items_df.to_csv("data/raw/order_items.csv", index=False)
payments_df.to_csv("data/raw/payments.csv", index=False)

print("Raw data generated successfully!")
