import pandas as pd
import requests

def load_kcp_csv(csv_path: str) -> pd.DataFrame:
    """
    Reads the KCP CSV and returns a clean DataFrame with required columns:
    - year
    - study_field_code
    - study_field_name
    - kcp_num
    """
    df = pd.read_csv(csv_path, encoding='utf-8')
    df.dropna(subset=['year', 'study_field_code', 'study_field_name', 'kcp_num'], inplace=True)
    df = df.astype(str).apply(lambda col: col.str.strip())

    # Ensure year and kcp_num are integers
    df['year'] = df['year'].astype(int)
    df['kcp_num'] = df['kcp_num'].astype(int)

    return df

def send_kcp_to_api(df: pd.DataFrame, api_url: str):
    """
    Sends each KCP row to the specified FastAPI endpoint using POST.
    Expected payload:
    {
        "kcp_num": int,
        "study_field_code": str,
        "study_field_name": str,
        "year": int
    }
    """
    headers = {'Content-Type': 'application/json'}
    success_count = 0
    fail_count = 0

    for idx, row in df.iterrows():
        payload = {
            "kcp_num": row["kcp_num"],
            "study_field_code": row["study_field_code"],
            "study_field_name": row["study_field_name"],
            "year": row["year"]
        }

        try:
            response = requests.post(api_url, json=payload, headers=headers)
            if response.status_code == 201:
                success_count += 1
            else:
                fail_count += 1
                print(f"❌ Row {idx}: Status {response.status_code}, Response: {response.text}")
        except requests.RequestException as e:
            fail_count += 1
            print(f"❌ Row {idx}: Exception occurred — {e}")

    print(f"\n✅ Uploaded: {success_count} records")
    if fail_count:
        print(f"⚠️ Failed: {fail_count} records")


# Step 1: CSV file path
csv_file_path = "kcp.csv"

# Step 2: Your FastAPI endpoint
api_endpoint = "http://localhost:8000/api/kcp-datasets/"

# Step 3: Load the CSV
kcp_df = load_kcp_csv(csv_file_path)
print(f"Loaded {len(kcp_df)} KCP records.\n")

# Step 4: Send to API
send_kcp_to_api(kcp_df, api_endpoint)
