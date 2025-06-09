
import pandas as pd
import requests

def load_classificator_csv(csv_path: str) -> pd.DataFrame:
    """
    Load and validate classificator-prof-dataset CSV file.

    Required columns:
    - prof_code
    - prof_name
    """
    df = pd.read_csv(csv_path, encoding='utf-8')

    required_columns = ['prof_code', 'prof_name']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    df.dropna(subset=required_columns, inplace=True)
    df = df.astype(str).apply(lambda col: col.str.strip())
    return df

def post_prof_dataset_to_api(df: pd.DataFrame, api_url: str):
    """
    Post each row of classificator data to FastAPI backend.

    Payload example:
    {
        "prof_code": "10003",
        "prof_name": "Авербандщик"
    }
    """
    headers = {'Content-Type': 'application/json'}
    success_count = 0
    fail_count = 0

    for idx, row in df.iterrows():
        payload = {
            "prof_code": row["prof_code"],
            "prof_name": row["prof_name"]
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
            print(f"❌ Row {idx}: Exception — {e}")

    print(f"\n✅ Uploaded: {success_count} records")
    if fail_count:
        print(f"⚠️ Failed: {fail_count} records")

# === CONFIGURATION ===

csv_file_path = "professions.csv"  # Update if needed
api_endpoint = "http://localhost:8000/classificator-prof-datasets/"  # Adjust as needed

# === EXECUTION ===

df = load_classificator_csv(csv_file_path)
print(f"📄 Loaded {len(df)} records from CSV.")

post_prof_dataset_to_api(df, api_endpoint)
