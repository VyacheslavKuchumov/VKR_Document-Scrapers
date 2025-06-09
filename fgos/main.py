import pandas as pd
import requests

def load_fgos_csv(csv_path: str) -> pd.DataFrame:
    """
    Reads the FGOS CSV and returns a clean DataFrame with required columns:
    - fgos_code
    - fgos_name
    - fgos_prikaz
    """
    df = pd.read_csv(csv_path, encoding='utf-8')
    df.dropna(subset=['fgos_code', 'fgos_name', 'fgos_prikaz'], inplace=True)
    df = df.astype(str).apply(lambda col: col.str.strip())  # Clean whitespace
    return df

def send_fgos_to_api(df: pd.DataFrame, api_url: str):
    """
    Sends each FGOS row to the specified FastAPI endpoint using POST.
    Expected payload:
    {
        "fgos_code": str,
        "fgos_name": str,
        "fgos_prikaz": str
    }
    """
    headers = {'Content-Type': 'application/json'}
    success_count = 0
    fail_count = 0

    for idx, row in df.iterrows():
        payload = {
            "fgos_code": row["fgos_code"],
            "fgos_name": row["fgos_name"],
            "fgos_prikaz": row["fgos_prikaz"]
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
csv_file_path = "fgos.csv"

# Step 2: Your FastAPI endpoint
api_endpoint = "http://localhost:8000/api/fgos-dataset/"

# Step 3: Load the CSV
fgos_df = load_fgos_csv(csv_file_path)
print(f"Loaded {len(fgos_df)} FGOS records.\n")

# Step 4: Send to API
send_fgos_to_api(fgos_df, api_endpoint)
