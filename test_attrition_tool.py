"""Quick test to verify attrition tool is working correctly."""

from data_pipeline import upload_data, config
from agent.analytics_tools import create_get_attrition_tool
import pandas as pd

# Load data
print("Loading data from S3...")
uploader = upload_data.DataUploader()

csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_memberships)
df_memberships = uploader.convert_csv_to_df(csv_content)
df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

print(f"\nTotal memberships: {len(df_memberships)}")
print(f"Memberships with status='END': {len(df_memberships[df_memberships['status'] == 'END'])}")

# Create the tool
attrition_tool = create_get_attrition_tool(df_memberships)

# Test for November 2025
print("\n" + "="*70)
print("Testing attrition for November 2025:")
print("="*70)
result = attrition_tool.func("2025-11-01", "2025-11-30")
print(result)

# Also manually verify
ended_nov = df_memberships[
    (df_memberships['status'] == 'END') &
    (df_memberships['end_date'] >= pd.to_datetime('2025-11-01')) &
    (df_memberships['end_date'] <= pd.to_datetime('2025-11-30'))
]

print("\n" + "="*70)
print("Manual verification:")
print("="*70)
print(f"Ended memberships in November: {len(ended_nov)}")
print(f"\nTheir end dates:")
for _, row in ended_nov.iterrows():
    print(f"  {row['end_date'].date()} - {row['size']} - status: {row['status']}")
