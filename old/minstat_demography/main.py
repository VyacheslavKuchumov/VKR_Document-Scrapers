import pandas as pd


def parse_demograph(file_path: str) -> pd.DataFrame:
    """
    Reads the Excel file, extracts and reshapes the data from sheets '1' and '2',
    and returns a DataFrame with columns [year, worker_num, okved_group].
    """
    xls = pd.ExcelFile(file_path)
    data_frames = []


    for sheet in xls.sheet_names:

        # Общая часть: чтение и переименование колонок
        if sheet.lower() == "лист1":
            df = pd.read_excel(xls, sheet_name=sheet, header=23)
            df.rename(columns={df.columns[0]: "age_group"}, inplace=True)
            years = ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]
            for idx, year in enumerate(years, start=1):
                df.rename(columns={df.columns[idx]: year}, inplace=True)

            # Оставляем только первые 19 строк (группы)
            df_cleaned = df.iloc[:3, :]

            desired_name = "трудоспособного"

            df_cleaned['age_group'] = df_cleaned['age_group'] \
                .replace(
                regex=r'(?i)^трудоспособном.*',
                value=desired_name
            )

            # Переводим в длинный формат
            df_long = df_cleaned.melt(
                id_vars="age_group",
                value_vars=years,
                var_name="year",
                value_name="people_num"
            )

            df_long["year"] = df_long["year"].astype(int)
            df_long["people_num"] = df_long["people_num"].astype(float)
            data_frames.append(df_long)

    # Объединяем все годы и сохраняем
    result_df = pd.concat(data_frames, ignore_index=True)
    result_df.to_csv('demography_minstat_out.csv', index=False)
    return result_df


# Запуск
parse_demograph("Распределение_населения_Пермского_края_по_возрастным_группам_в_2010.xlsx")
