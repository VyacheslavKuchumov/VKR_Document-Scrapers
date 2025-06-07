import pandas as pd
import requests

# OKVED_API = "http://localhost:8000/okved_sections/"
# EMPLOYMENT_API = "http://127.0.0.1:8000/employment_minstat/"

OKVED_API = "https://vkr-api.vyachik-dev.ru/okved_sections/"
EMPLOYMENT_API = "https://vkr-api.vyachik-dev.ru/employment_minstat/"

def push_okveds_and_employment(df: pd.DataFrame):
    """
    Sends OKVEDs to FastAPI and inserts employment data per year.
    """
    # Create a mapping from okved_group to section ID
    okved_to_id = {}

    for okved_group in df["okved_group"].unique():
        payload = {
            "okved_section_name": str(okved_group),
            "okved_section_code": "",  # optional
            "img_url": ""  # optional
        }
        response = requests.post(OKVED_API, json=payload)
        if response.status_code == 200:
            okved_id = response.json()["id"]
            okved_to_id[okved_group] = okved_id
        else:
            print(f"Failed to post OKVED: {okved_group}, {response.text}")

    resp = requests.get(OKVED_API)
    resp.raise_for_status()
    existing = resp.json()  # [{ "id": 1, "code":"", "name":"A" }, …]
    okved_to_id = {e["okved_section_name"]: e["id"] for e in existing}

    # Send employment data
    for _, row in df.iterrows():
        okved_id = okved_to_id.get(row["okved_group"])
        if not okved_id:
            print(f"Missing ID for {row['okved_group']}, skipping")
            continue

        payload = {
            "year": int(row["year"]),
            "number_of_employees": float(row["worker_num"]),
            "okved_section_id": okved_id
        }
        resp = requests.post(EMPLOYMENT_API, json=payload)
        if resp.status_code != 200:
            print(f"Failed to insert employment for {row['okved_group']} in {row['year']}:",
                  resp.text)

import pandas as pd


def parse_okved(file_path: str) -> pd.DataFrame:
    """
    Reads the Excel file, extracts and reshapes the data from sheets '1' and '2',
    and returns a DataFrame with columns [year, worker_num, okved_group].
    """
    xls = pd.ExcelFile(file_path)
    data_frames = []


    for sheet in xls.sheet_names:
        if sheet.lower() == "содержание":
            continue

        # чтение и переименование колонок
        if sheet == '1' or sheet == '2':
            df = pd.read_excel(xls, sheet_name=sheet, header=6)
            df.rename(columns={df.columns[0]: "okved_group"}, inplace=True)
            years = ["2010", "2011", "2012", "2013", "2014", "2015", "2016"] if sheet == '1' \
                else ["2017", "2018", "2019", "2020", "2021", "2022", "2023"]
            for idx, year in enumerate(years, start=1):
                df.rename(columns={df.columns[idx]: year}, inplace=True)

            # Оставляем только первые 19 строк (далше идут проценты)
            df_cleaned = df.iloc[:19, :]

            desired_name = "сельское, лесное хозяйство, охота, рыболовство и рыбоводство"
            # если это второй лист — нормализуем название группы
            if sheet == '2':
                df_cleaned['okved_group'] = df_cleaned['okved_group'] \
                    .replace(
                    regex=r'(?i)^сельское.*',
                    value=desired_name
                )

            # Переводим в длинный формат
            df_long = df_cleaned.melt(
                id_vars="okved_group",
                value_vars=years,
                var_name="year",
                value_name="worker_num"
            )
            df_long["year"] = df_long["year"].astype(int)
            df_long["worker_num"] = df_long["worker_num"].astype(float)
            data_frames.append(df_long)

    # Объединяем все года и сохраняем
    result_df = pd.concat(data_frames, ignore_index=True)
    result_df.to_csv('jobs_minstat_out.csv', index=False)
    return result_df


# Запуск
df = parse_okved("Среднегодовая_численность_занятых_по_видам_деятельности_в_Пермском.xlsx")

push_okveds_and_employment(df)