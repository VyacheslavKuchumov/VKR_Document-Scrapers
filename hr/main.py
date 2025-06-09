import pandas as pd
import requests

def load_and_aggregate(csv_path: str) -> pd.DataFrame:
    """
    Loads the CSV, groups by entry_date and professional_roles_name,
    and returns a DataFrame with columns:
      - entry_date
      - professional_role
      - vacancies_num
    """
    # Read the CSV
    df = pd.read_csv(csv_path, parse_dates=['entry_date'], dayfirst=True)
    
    # Group and count
    grouped = (
        df
        .groupby(['entry_date', 'professional_roles_name'])
        .size()
        .reset_index(name='vacancies_num')
    )
    
    # Rename columns for clarity
    grouped.rename(columns={
        'professional_roles_name': 'professional_role'
    }, inplace=True)
    
    # Optionally format the date as string YYYY-MM-DD
    grouped['entry_date'] = grouped['entry_date'].dt.strftime('%Y-%m-%d')
    
    return grouped

def send_data_to_api(df: pd.DataFrame, api_url: str):
    """
    Sends each row of the DataFrame to the specified FastAPI endpoint using POST.
    Expects the API to accept JSON with fields:
      - entry_date (str, YYYY-MM-DD)
      - professional_role (str)
      - vacancies_num (int)
    """
    headers = {'Content-Type': 'application/json'}
    success_count = 0
    fail_count = 0

    for idx, row in df.iterrows():
        payload = {
            "entry_date": row["entry_date"],
            "professional_role": row["professional_role"],
            "vacancies_num": int(row["vacancies_num"])
        }

        response = requests.post(api_url, json=payload, headers=headers)
        if response.status_code == 201:
            success_count += 1
        else:
            fail_count += 1
            print(f"❌ Failed at row {idx}: {response.status_code} — {response.text}")

    print(f"\n✅ Sent {success_count} rows successfully.")
    if fail_count:
        print(f"⚠️ Failed to send {fail_count} rows.")

if __name__ == "__main__":
    # 1. Path to your CSV file
    csv_file_path = "perm_krai.csv"
    
    # 2. Your FastAPI endpoint
    api_endpoint = "http://localhost:8000/api/hh-ru-dataset/"
    
    # 3. Load, aggregate, and preview
    df_summary = load_and_aggregate(csv_file_path)
    print("Grouped Data Preview:")
    print(df_summary, "\n")
    
    # 4. Send to API
    send_data_to_api(df_summary, api_endpoint)
