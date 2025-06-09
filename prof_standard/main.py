import pandas as pd
import requests

def load_profstandards_csv(csv_path: str) -> pd.DataFrame:
    """
    Reads the professional standards CSV and returns a clean DataFrame with required columns:
    - prof_standard_code
    - prof_standard_sphere
    - prof_standard_type
    - prof_standard_name
    """
    df = pd.read_csv(csv_path, encoding='utf-8')
    
    required_columns = [
        'prof_standard_code',
        'prof_standard_sphere',
        'prof_standard_type',
        'prof_standard_name'
    ]
    
    # Validate column existence
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
    
    df.dropna(subset=required_columns, inplace=True)
    df = df.astype(str).apply(lambda col: col.str.strip())  # Clean whitespace
    return df

def send_profstandards_to_api(df: pd.DataFrame, api_url: str):
    """
    Sends each professional standard row to the specified FastAPI endpoint using POST.
    Expected payload:
    {
        "prof_standard_code": str,
        "prof_standard_sphere": str,
        "prof_standard_type": str,
        "prof_standard_name": str
    }
    """
    headers = {'Content-Type': 'application/json'}
    success_count = 0
    fail_count = 0

    for idx, row in df.iterrows():
        payload = {
            "prof_standard_code": row["prof_standard_code"],
            "prof_standard_sphere": row["prof_standard_sphere"],
            "prof_standard_type": row["prof_standard_type"],
            "prof_standard_name": row["prof_standard_name"]
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

# === CONFIGURATION ===

# Step 1: CSV file path
csv_file_path = "prof_standard.csv"  # Replace with your actual file path

# Step 2: FastAPI endpoint
api_endpoint = "http://localhost:8000/api/prof-standard-datasets/"  # Replace with your actual endpoint

# Step 3: Load and clean data
prof_df = load_profstandards_csv(csv_file_path)
print(f"📄 Loaded {len(prof_df)} professional standards records.\n")

# Step 4: Send to API
send_profstandards_to_api(prof_df, api_endpoint)
