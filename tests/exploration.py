import pandas as pd

# Load the CSV
df = pd.read_csv('data/outputs/stripe_transaction_data.csv')

# Parse the date column
df['Date'] = pd.to_datetime(df['Date'])

# Add a 'month' column for grouping
df['month'] = df['Date'].dt.to_period('M')

# Ensure 'Total Amount' is numeric
df['Total Amount'] = pd.to_numeric(df['Total Amount'], errors='coerce')

# 1. By month: total revenue
monthly_revenue = df.groupby('month')['Total Amount'].sum().reset_index()
print("Total Revenue by Month:")
print(monthly_revenue)
print()

# 2. By month and category: total revenue
monthly_category_revenue = df.groupby(['month', 'revenue_category'])['Total Amount'].sum().reset_index()
print("Total Revenue by Month and Category:")
print(monthly_category_revenue)
