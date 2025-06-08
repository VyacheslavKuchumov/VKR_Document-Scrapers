import pandas as pd
import requests


def send_data_to_api(df: pd.DataFrame, api_url: str):
    """
    Sends each row of the DataFrame to the specified FastAPI endpoint using POST.
    """
    headers = {'Content-Type': 'application/json'}
    success_count, fail_count = 0, 0

    for idx, row in df.iterrows():
        payload = {
            "okved_group": row["okved_group"],
            "worker_num": round(row["worker_num"], 3),
            "year": int(row["year"])
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

    # Приводим первую букву каждой группы к верхнему регистру
    result_df["okved_group"] = result_df["okved_group"].str.capitalize()

    result_df.to_csv('jobs_minstat_out.csv', index=False)
    return result_df


# Запуск
df = parse_okved("Среднегодовая_численность_занятых_по_видам_деятельности_в_Пермском.xlsx")

print(df.head())  # Вывод первых строк для проверки

# Отправка данных на API
api_url = "http://localhost:8000/api/minstat-workers/"
send_data_to_api(df, api_url)


