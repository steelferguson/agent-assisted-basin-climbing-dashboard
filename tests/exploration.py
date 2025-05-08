import pandas as pd

# Load both CSVs
stripe_df = pd.read_csv('data/outputs/stripe_transaction_data.csv')
square_df = pd.read_csv('data/outputs/square_transaction_data.csv')

# Parse dates with ISO8601 format and UTC timezone
stripe_df['Date'] = pd.to_datetime(stripe_df['Date'], format='ISO8601', utc=True)
square_df['Date'] = pd.to_datetime(square_df['Date'], format='ISO8601', utc=True)

# Combine the dataframes
df = pd.concat([stripe_df, square_df], ignore_index=True)

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
