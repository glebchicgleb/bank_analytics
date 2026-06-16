import os
import sqlite3
from dbfread import DBF
from simpledbf import Dbf5
import pandas as pd

DB_PATH = "bank_data.db"

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print("Старая база удалена")

conn = sqlite3.connect(DB_PATH)

all_items = os.listdir("data")

periods = []
for item in all_items:
    item_path = os.path.join("data", item)

    if os.path.isdir(item_path):
        periods.append(item)

periods = sorted(periods)

print(f"Периоды: {periods}")

for period in periods:
    period_path = os.path.join("data", period)
    files = os.listdir(period_path)

    b1_file = None
    n1_file = None
    names_file = None

    for file_name in files:
        upper_name = file_name.upper()

        if upper_name.endswith("B1.DBF"):
            b1_file = file_name

        elif upper_name.endswith("N1.DBF"):
            n1_file = file_name

        elif upper_name.endswith("NAMES.DBF"):
            names_file = file_name

    print(f"Обработка периода {period}")

    if b1_file:
        b1_path = os.path.join(period_path, b1_file)
        b1 = DBF(b1_path, encoding="cp866")
        df_b1 = pd.DataFrame(iter(b1))
        df_b1["report_date"] = period
        df_b1.to_sql("accounts", conn, if_exists="append", index=False)
        print(f"Загружен accounts: {b1_file}, строк: {len(df_b1)}")

    if n1_file:
        n1_path = os.path.join(period_path, n1_file)
        n1 = Dbf5(n1_path, codec="cp866")
        df_n1 = n1.to_dataframe()
        df_n1["report_date"] = period
        df_n1.to_sql("banks", conn, if_exists="append", index=False)
        print(f"Загружен banks: {n1_file}, строк: {len(df_n1)}")

    if names_file:
        names_path = os.path.join(period_path, names_file)
        names = DBF(names_path, encoding="cp866")
        df_names = pd.DataFrame(iter(names))
        df_names["report_date"] = period
        df_names.to_sql("accounts_dict", conn, if_exists="append", index=False)
        print(f"Загружен accounts_dict: {names_file}, строк: {len(df_names)}")

conn.close()

print("Загрузка завершена")