import os
import sqlite3
from dbfread import DBF
from simpledbf import Dbf5
import pandas as pd

conn = sqlite3.connect('bank_data.db')

all_items = os.listdir('data')
periods = []
for item in all_items:
    item_path = os.path.join('data', item)
    if os.path.isdir(item_path):
        periods.append(item)
print(f"Периоды:{periods}")

for period in periods:
    period_path = os.path.join('data', period)
    files = os.listdir(period_path)
    b1_file=None
    n1_file=None
    names_file=None
    for f in files:
        if f.endswith('B1.dbf'):
            b1_file=f
        elif f.endswith('N1.dbf'):
            n1_file=f
        elif f.endswith('NAMES.dbf'):
            names_file=f
    if b1_file:
        b1_path = os.path.join(period_path, b1_file)
        b1=DBF(b1_path, encoding='cp866')
        dfb1 = pd.DataFrame(iter(b1))
        dfb1['report_date'] = period
        dfb1.to_sql('accounts', conn, if_exists='append', index=False)
    
    if n1_file:
        n1_path = os.path.join(period_path, n1_file)
        n1 = Dbf5(n1_path, codec='cp866')
        df_n1 = n1.to_dataframe()
        df_n1['report_date'] = period
        df_n1.to_sql('banks', conn, if_exists='append', index=False)
    
    if names_file:
        names_path = os.path.join(period_path, names_file)
        names = DBF(names_path, encoding='cp866')
        df_names = pd.DataFrame(iter(names))
        df_names['report_date'] = period
        df_names.to_sql('accounts_dict', conn, if_exists='append', index=False)
conn.close()