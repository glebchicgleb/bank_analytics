import sqlite3
import pandas as pd

DB_PATH = 'bank_data.db'

def get_connection():
    return sqlite3.connect(DB_PATH)

def load_all_data():
    con = get_connection()
    accounts = pd.read_sql("SELECT * FROM accounts", con)
    banks = pd.read_sql("SELECT * FROM banks", con)
    con.close()
    return accounts, banks

ACCOUNTS, BANKS = load_all_data()
print('Данные загружены')


def get_bank_data(regn, month):
    data = ACCOUNTS[ACCOUNTS['REGN'] == regn]
    data = data[data['report_date'] == month]
    return data

def get_all_banks():
    unique_regn = ACCOUNTS[['REGN']].drop_duplicates()
    
    banks_dict = dict(zip(BANKS['REGN'], BANKS['NAME_B']))
    
    names = []
    for regn in unique_regn['REGN']:
        if regn in banks_dict:
            names.append(banks_dict[regn])
        else:
            names.append("Банк " + str(regn))
    
    unique_regn['bank_name'] = names
    unique_regn = unique_regn.sort_values('REGN')
    unique_regn = unique_regn.reset_index(drop=True)
    
    return unique_regn

def find_bank_by_code(code):
    if not str(code).isdigit():
        return None
    code = int(code)
    all_banks = get_all_banks()
    result = all_banks[all_banks['REGN'] == code]
    if len(result) == 0:
        return None
    return result.to_dict('records')

def get_months():
    months = ACCOUNTS['report_date'].unique()
    months = sorted(months)
    return months

# Активы
def get_bank_assets(regn, month):
    data = get_bank_data(regn, month)
    assets = data[data['A_P'] == '1']
    return assets['IITG'].sum()

# Кредиты физлицам
def get_bank_loans_people(regn, month):
    data = get_bank_data(regn, month)
    
    mask = (data['NUM_SC'].str.startswith('455') |
            data['NUM_SC'].str.startswith('457') |
            data['NUM_SC'].str.startswith('458') |
            data['NUM_SC'].str.startswith('478'))
    
    return data[mask]['IITG'].sum()

# Кредиты юрлицам
def get_bank_loans_companies(regn, month):
    data = get_bank_data(regn, month)
    
    mask = (data['NUM_SC'].str.startswith('446') |
            data['NUM_SC'].str.startswith('447') |
            data['NUM_SC'].str.startswith('449') |
            data['NUM_SC'].str.startswith('450') |
            data['NUM_SC'].str.startswith('452') |
            data['NUM_SC'].str.startswith('453') |
            data['NUM_SC'].str.startswith('456') |
            data['NUM_SC'].str.startswith('465') |
            data['NUM_SC'].str.startswith('466') |
            data['NUM_SC'].str.startswith('468') |
            data['NUM_SC'].str.startswith('469') |
            data['NUM_SC'].str.startswith('471') |
            data['NUM_SC'].str.startswith('472') |
            data['NUM_SC'].str.startswith('473') |
            data['NUM_SC'].str.startswith('478'))
    
    return data[mask]['IITG'].sum()

#Вклады клиентов
def get_bank_deposits(regn, month):
    data = get_bank_data(regn, month)
    
    mask = (data['NUM_SC'].str.startswith('423') |
            data['NUM_SC'].str.startswith('426') |
            data['NUM_SC'].str.startswith('476') |
            data['NUM_SC'].str.startswith('522') |
            data['NUM_SC'].str.startswith('524'))
    
    return data[mask]['IITG'].sum()

# Капитал банка
def get_bank_capital(regn, month):
    data = get_bank_data(regn, month)
    
    capital = 0
    capital_accounts = ['102', '106', '107', '108']
    for acc in capital_accounts:
        acc_data = data[data['NUM_SC'].str.startswith(acc) & (data['A_P'] == '2')]
        capital += acc_data['IITG'].sum()
    
    shares = data[data['NUM_SC'].str.startswith('105') & (data['A_P'] == '1')]
    capital -= shares['IITG'].sum()
    
    return capital

#Прибыль банка
def get_bank_profit(regn, month):
    data = get_bank_data(regn, month)
    
    # Доходы
    income = data[data['NUM_SC'].str.startswith('706') & (data['A_P'] == '2')]['IITG'].sum()
    
    # Расходы
    expense = data[data['NUM_SC'].str.startswith('706') & (data['A_P'] == '1')]['IITG'].sum()
    
    return income - expense

#Рентабельность активов
def get_bank_roa(regn, month):
    profit = get_bank_profit(regn, month)
    assets = get_bank_assets(regn, month)
    
    if assets > 0:
        roa = (profit / assets) * 100
    else:
        roa = 0
    
    return roa

#Отношение кредитов к вкладам
def get_bank_ltd(regn, month):
    loans = get_bank_loans_people(regn, month) + get_bank_loans_companies(regn, month)
    deposits = get_bank_deposits(regn, month)
    
    if deposits > 0:
        ltd = (loans / deposits) * 100
    else:
        ltd = 0
    
    return ltd

#Достаточность капитала
def get_bank_capital_ratio(regn, month):
    capital = get_bank_capital(regn, month)
    assets = get_bank_assets(regn, month)
    
    if assets > 0:
        ratio = (capital / assets) * 100
    else:
        ratio = 0
    
    return ratio

#Коэффициент ликвидности
def get_bank_liquidity_ratio(regn, month):
    data = get_bank_data(regn, month)
    
    # Денежные средства
    cash = data[data['NUM_SC'].str.startswith('202')]
    
    # Корсчета в банках
    corr_banks = data[data['NUM_SC'].str.startswith('301')]
    
    # Средства в ЦБ
    cbr_mask = (data['NUM_SC'].str.startswith('30102') |
                data['NUM_SC'].str.startswith('30104') |
                data['NUM_SC'].str.startswith('30106') |
                data['NUM_SC'].str.startswith('30125') |
                data['NUM_SC'].str.startswith('30417') |
                data['NUM_SC'].str.startswith('30419') |
                data['NUM_SC'].str.startswith('30208') |
                data['NUM_SC'].str.startswith('30210') |
                data['NUM_SC'].str.startswith('30213') |
                data['NUM_SC'].str.startswith('30224') |
                data['NUM_SC'].str.startswith('30228') |
                data['NUM_SC'].str.startswith('30235'))
    
    # Депозиты в ЦБ
    deposits_cbr = data[data['NUM_SC'].str.startswith('319')]
    
    # Резервы
    reserves = data[data['NUM_SC'].str.startswith('30126')]
    
    liquid = (cash['IITG'].sum() + 
              corr_banks['IITG'].sum() + 
              data[cbr_mask]['IITG'].sum() + 
              deposits_cbr['IITG'].sum() - 
              reserves['IITG'].sum())
    
    assets = get_bank_assets(regn, month)
    
    if assets > 0:
        ratio = (liquid / assets) * 100
    else:
        ratio = 0
    
    return ratio


def get_bank_all_metrics(regn, month):
    bank_info = find_bank_by_code(regn)
    if bank_info is not None and len(bank_info) > 0:
        bank_name = bank_info.iloc[0]['bank_name']
    else:
        bank_name = f'Банк {regn}'
    
    metrics = {
        'Код банка': regn,
        'Название банка': bank_name,
        'Месяц': month,
        'Активы': get_bank_assets(regn, month),
        'Кредиты физлицам': get_bank_loans_people(regn, month),
        'Кредиты юрлицам': get_bank_loans_companies(regn, month),
        'Вклады': get_bank_deposits(regn, month),
        'Капитал': get_bank_capital(regn, month),
        'Прибыль': get_bank_profit(regn, month),
        'ROA (рентабельность)': get_bank_roa(regn, month),
        'LTD (кредиты/вклады)': get_bank_ltd(regn, month),
        'Достаточность капитала': get_bank_capital_ratio(regn, month),
        'Ликвидность': get_bank_liquidity_ratio(regn, month)
    }
    
    return metrics