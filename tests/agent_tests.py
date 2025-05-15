# test the functions in investigate.py
from agent.investigate import *
import pandas as pd
from agent.data_loader import load_all_dataframes, initialize_data_uploader

'''
This file is used to test the functions in investigate.py
'''

def test_summarize_date_range(df: pd.DataFrame):
    print(summarize_date_range(df, '2025-01-01', '2025-01-31'))

def test_compare_categories(df: pd.DataFrame):
    print(compare_categories(df, ('2025-01-01', '2025-01-31')))

def test_detect_anomalies(df: pd.DataFrame):    
    print(detect_anomalies(df, ('2025-01-01', '2025-01-31')))

if __name__ == "__main__":
    uploader = initialize_data_uploader()
    dfs = load_all_dataframes(uploader)
    df = dfs['transactions']
    print(df.head())
    df.to_csv('data/outputs/transactions_temp.csv', index=False)
    test_summarize_date_range(df)
    test_compare_categories(df)
    test_detect_anomalies(df)