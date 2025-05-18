#!/usr/bin/env python3
"""
Parser for average annual number of workers by OKVED groups
in Perm region. Outputs a CSV with columns: year, worker_num, okved_group.
"""

import pandas as pd
import argparse
import sys

def parse_okved(file_path: str) -> pd.DataFrame:
    """
    Reads the Excel file, extracts and reshapes the data from sheets '1' and '2',
    and returns a DataFrame with columns [year, worker_num, okved_group].
    """
    # Load workbook
    xls = pd.ExcelFile(file_path)
    data_frames = []

    for sheet in xls.sheet_names:
        # Skip table of contents
        if sheet.lower() == "содержание":
            continue

        # Read with header on the third row (index 2)
        df = pd.read_excel(xls, sheet_name=sheet, header=2)

        # Rename the first column to 'okved_group'
        df.rename(columns={df.columns[0]: "okved_group"}, inplace=True)

        # Drop rows where okved_group is NaN
        df = df[df["okved_group"].notna()]

        # Identify all year columns
        year_cols = [col for col in df.columns if col != "okved_group"]

        # Melt to long format
        df_long = df.melt(
            id_vars=["okved_group"],
            value_vars=year_cols,
            var_name="year",
            value_name="worker_num"
        )

        data_frames.append(df_long)

    # Combine both sheets
    result = pd.concat(data_frames, ignore_index=True)

    # Convert year to integer (some may be floats from Excel)
    result["year"] = result["year"].astype(float).astype(int)

    # Ensure worker_num is numeric (thousands of persons)
    result["worker_num"] = pd.to_numeric(result["worker_num"], errors="coerce")

    # Drop any rows where worker_num is missing
    result.dropna(subset=["worker_num"], inplace=True)

    # Reorder columns
    result = result[["year", "worker_num", "okved_group"]]

    return result

def main():
    parser = argparse.ArgumentParser(
        description="Parse OKVED worker counts from an Excel file."
    )
    parser.add_argument(
        "input_excel",
        help="Path to the input Excel file (e.g. Среднегодовая_численность_занятых_по_видам_деятельности_в_Пермском.xlsx)"
    )
    parser.add_argument(
        "output_csv",
        help="Path where to save the output CSV (e.g. okved_workers.csv)"
    )
    args = parser.parse_args()

    try:
        df_parsed = parse_okved(args.input_excel)
        df_parsed.to_csv(args.output_csv, index=False)
        print(f"✔ Parsed data saved to {args.output_csv}")
    except Exception as e:
        print(f"✖ Error parsing file: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
