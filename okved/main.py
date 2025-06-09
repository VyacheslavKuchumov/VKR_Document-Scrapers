import pandas as pd
import requests

def load_okved_csv(csv_path: str) -> pd.DataFrame:
    """
    Считывает CSV с колонками:
    - okved_code
    - okved_name
    Очищает данные и возвращает DataFrame.
    """
    df = pd.read_csv(csv_path, encoding='utf-8')
    # Удаляем строки с пустыми обязательными полями
    df.dropna(subset=['okved_code', 'okved_name'], inplace=True)
    # Убираем пробелы вокруг текста
    df = df.astype(str).apply(lambda col: col.str.strip())
    return df

def send_okved_to_api(df: pd.DataFrame, api_url: str):
    """
    Отправляет каждую строку в FastAPI эндпоинт POST /okved-datasets/
    Формат JSON:
    {
      "okved_code": str,
      "okved_name": str
    }
    """
    headers = {'Content-Type': 'application/json'}
    success_count = 0
    fail_count = 0

    for idx, row in df.iterrows():
        payload = {
            "okved_code": row["okved_code"],
            "okved_name": row["okved_name"]
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

if __name__ == "__main__":
    # Укажи путь к CSV с данными OKVED
    csv_file_path = "okved_sections.csv"

    # Укажи URL твоего FastAPI эндпоинта (поменяй на свой хост и порт)
    api_endpoint = "http://localhost:8000/api/okved-datasets/"

    # Загружаем CSV
    okved_df = load_okved_csv(csv_file_path)
    print(f"Loaded {len(okved_df)} OKVED records.\n")

    # Отправляем данные на сервер
    send_okved_to_api(okved_df, api_endpoint)
