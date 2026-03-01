import os
import sqlite3

if os.path.exists('bank_data.db'):
    os.remove('bank_data.db')
    print("База удалена")
else:
    print("База не найдена")