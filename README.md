#  NepalMart – Retail Data Analysis Pipeline

##  Project Overview

NepalMart is a comprehensive data analysis project simulating a retail e-commerce platform in Nepal. This project demonstrates a complete data pipeline from synthetic data generation to advanced SQL analysis, showcasing industry-standard data engineering practices.

**Key Features:**
- Synthetic data generation simulating Nepali e-commerce transactions
- Robust data cleaning and validation pipeline
- SQL database integration with Python
- Comprehensive data quality checks
- Analytical queries for business insights

## Architecture
Data_Analysis/
├── Data_generation.py # Synthetic data generation
├── Data_Cleaning.py # Data cleaning pipeline
├── data/
│ ├── raw/ # Raw generated data
│ └── cleaned/ # Cleaned data for analysis
├── scripts/
│ ├── sql_connection_test.py # Database connection test
│ └── sql_analysis.py # Advanced SQL queries
├── test.ipynb # Jupyter notebook for exploration
└── README.md # Project documentation

## Tech Stack

| Technology | Purpose |
|------------|---------|
| **Python 3.8+** | Core programming language |
| **Pandas** | Data manipulation and analysis |
| **MySQL 8.0** | Relational database management |
| **SQLAlchemy** | Python-SQL integration |
| **Jupyter** | Interactive data exploration |
| **Git** | Version control |

##  Dataset Description

The dataset contains synthetic but realistic Nepali e-commerce data across 5 relational tables:

| Table | Records | Description | Key Fields |
|-------|---------|-------------|------------|
| **customers** | 5,000 | Customer demographics | customer_id, city, signup_date |
| **products** | 200 | Product catalog | product_id, category, price |
| **orders** | 20,000 | Order transactions | order_id, customer_id, status |
| **order_items** | ~40,000 | Line item details | order_id, product_id, quantity |
| **payments** | ~13,300 | Payment records | payment_id, order_id, amount |

**Geographic Coverage:** Kathmandu, Lalitpur, Bhaktapur, Pokhara

## Data Quality Framework

### Data Validation Checks Performed:

1. **Schema Validation**
   - Data type consistency
   - Constraint verification

2. **Completeness Checks**
   - NULL value detection
   - Required field validation

3. **Consistency Checks**
   - Foreign key integrity
   - Orphan record detection

4. **Business Rule Validation**
   - Price > 0 validation
   - Order-payment consistency
   - Temporal logic (signup_date ≤ order_date)

### Results:
- ✅ No duplicate customer records
- ✅ No orphan order items
- ✅ All prices validated (> 0)
- ✅ Referential integrity maintained

## 📈 Analytical Capabilities

### SQL Analysis Examples:
```sql
-- Customer lifetime value
SELECT customer_id, COUNT(order_id) as order_count, 
       SUM(amount) as total_spent
FROM orders o
JOIN payments p ON o.order_id = p.order_id
GROUP BY customer_id;

-- Monthly revenue trends
SELECT DATE_FORMAT(order_date, '%Y-%m') as month,
       SUM(amount) as monthly_revenue
FROM orders o
JOIN payments p ON o.order_id = p.order_id
WHERE order_status = 'Completed'
GROUP BY month
ORDER BY month;
