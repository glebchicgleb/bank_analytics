import sqlite3
import pandas as pd

DB_PATH = "bank_data.db"


def normalize_regn(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    value = str(value).strip()

    if value.endswith(".0"):
        value = value[:-2]

    return value


def normalize_code(value):
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    value = str(value).strip()

    if value.endswith(".0"):
        value = value[:-2]

    return value


def get_connection():
    return sqlite3.connect(DB_PATH)


def load_all_data():
    con = get_connection()
    accounts = pd.read_sql("SELECT * FROM accounts", con)
    banks = pd.read_sql("SELECT * FROM banks", con)
    con.close()
    return accounts, banks


ACCOUNTS, BANKS = load_all_data()

ACCOUNTS["REGN"] = ACCOUNTS["REGN"].apply(normalize_regn)
ACCOUNTS["report_date"] = ACCOUNTS["report_date"].astype(str).str.strip()
ACCOUNTS["NUM_SC"] = ACCOUNTS["NUM_SC"].apply(normalize_code)
ACCOUNTS["A_P"] = ACCOUNTS["A_P"].apply(normalize_code)
ACCOUNTS["IITG"] = pd.to_numeric(ACCOUNTS["IITG"], errors="coerce").fillna(0)

BANKS["REGN"] = BANKS["REGN"].apply(normalize_regn)
BANKS["NAME_B"] = BANKS["NAME_B"].astype(str).str.strip()
BANKS["report_date"] = BANKS["report_date"].astype(str).str.strip()

print("Данные загружены")


def sum_by_prefix(data, prefixes, ap=None):
    prefixes = tuple(prefixes)

    mask = data["NUM_SC"].str.startswith(prefixes)

    if ap is not None:
        mask = mask & (data["A_P"] == str(ap))

    return data[mask]["IITG"].sum()


def get_bank_data(regn, month):
    regn = normalize_regn(regn)
    month = str(month).strip()

    data = ACCOUNTS[ACCOUNTS["REGN"] == regn]
    data = data[data["report_date"] == month]

    return data

def get_clean_bank_data(regn, month):
    data = get_bank_data(regn, month).copy()

    exclude_prefixes = (
        "ITGAP",
        "999",
        "90.",
        "933", "934", "935", "936", "937", "939",
        "940", "941",
        "963", "964", "965", "966", "967",
        "969", "970", "971"
    )

    data = data[
        ~data["NUM_SC"].str.startswith(
            exclude_prefixes,
            na=False
        )
    ]

    return data


def get_all_banks():
    unique_regn = ACCOUNTS[["REGN"]].drop_duplicates().copy()
    unique_regn["REGN_key"] = unique_regn["REGN"].apply(normalize_regn)

    banks_copy = BANKS.copy()
    banks_copy["REGN_key"] = banks_copy["REGN"].apply(normalize_regn)

    banks_copy = banks_copy.dropna(subset=["REGN_key"])
    banks_copy = banks_copy.sort_values("report_date")
    banks_copy = banks_copy.drop_duplicates(subset=["REGN_key"], keep="last")

    result = unique_regn.merge(
        banks_copy[["REGN_key", "NAME_B"]],
        on="REGN_key",
        how="left"
    )

    result = result.dropna(subset=["NAME_B"])

    result["NAME_B"] = result["NAME_B"].astype(str).str.strip()
    result = result[result["NAME_B"] != ""]
    result = result[result["NAME_B"].str.lower() != "nan"]

    result["bank_name"] = result["NAME_B"]

    result = result[["REGN_key", "bank_name"]]
    result = result.rename(columns={"REGN_key": "REGN"})
    result = result.dropna(subset=["REGN"])
    result = result.sort_values("bank_name")
    result = result.reset_index(drop=True)

    return result


def find_bank_by_code(code):
    code = normalize_regn(code)

    all_banks = get_all_banks()
    all_banks["REGN"] = all_banks["REGN"].apply(normalize_regn)

    result = all_banks[all_banks["REGN"] == code]

    if result.empty:
        return None

    return result.to_dict("records")


def get_bank_name_safe(regn):
    bank_info = find_bank_by_code(regn)

    if bank_info is not None:
        return bank_info[0]["bank_name"]

    return f"Банк {regn}"


def get_available_months():
    months = ACCOUNTS["report_date"].dropna().unique()
    return sorted(months)


def get_bank_assets(regn, month):
    data = get_clean_bank_data(regn, month)

    if data.empty:
        return 0

    return data[data["A_P"] == "1"]["IITG"].sum()


def get_bank_liabilities(regn, month):
    data = get_clean_bank_data(regn, month)

    if data.empty:
        return 0

    return data[data["A_P"] == "2"]["IITG"].sum()


def get_bank_loans_people(regn, month):
    data = get_clean_bank_data(regn, month)

    if data.empty:
        return 0

    prefixes = [
        "455",
        "457",
        "458"
    ]

    return sum_by_prefix(data, prefixes, ap="1")


def get_bank_loans_companies(regn, month):
    data = get_clean_bank_data(regn, month)

    if data.empty:
        return 0

    prefixes = [
        "441", "442", "443", "444", "445",
        "446", "447", "448", "449",
        "450", "451", "452", "453", "454",
        "456",
        "460", "461", "462", "463", "464",
        "465", "466", "467", "468", "469",
        "470", "471", "472", "473"
    ]

    return sum_by_prefix(data, prefixes, ap="1")


def get_bank_loans_total(regn, month):
    return (
        get_bank_loans_people(regn, month)
        + get_bank_loans_companies(regn, month)
    )


def get_bank_deposits(regn, month):
    data = get_clean_bank_data(regn, month)

    if data.empty:
        return 0

    prefixes = [
        "401", "402", "403", "404", "405",
        "406", "407", "408", "409",
        "410", "411", "412", "413", "414",
        "415", "416", "417", "418", "419", "420",
        "421", "422", "423", "424", "425", "426",
        "427", "428", "429", "430", "431", "432",
        "433", "434", "435", "436", "437", "438",
        "439", "440"
    ]

    return sum_by_prefix(data, prefixes, ap="2")


def get_bank_capital(regn, month):
    data = get_clean_bank_data(regn, month)

    if data.empty:
        return 0

    capital = 0

    capital += sum_by_prefix(data, ["102"], ap="2")
    capital += sum_by_prefix(data, ["106"], ap="2")
    capital += sum_by_prefix(data, ["107"], ap="2")
    capital += sum_by_prefix(data, ["108"], ap="2")

    own_shares = sum_by_prefix(data, ["105"], ap="1")
    capital -= own_shares

    return capital


def get_bank_profit(regn, month):
    data = get_clean_bank_data(regn, month)

    if data.empty:
        return 0

    income = sum_by_prefix(data, ["706", "707"], ap="2")
    expense = sum_by_prefix(data, ["706", "707"], ap="1")

    return income - expense


def get_bank_roa(regn, month):
    profit = get_bank_profit(regn, month)
    assets = get_bank_assets(regn, month)

    if assets == 0:
        return 0

    return (profit / assets) * 100


def get_bank_ltd(regn, month):
    loans = get_bank_loans_total(regn, month)
    deposits = get_bank_deposits(regn, month)

    if deposits == 0:
        return 0

    return (loans / deposits) * 100


def get_bank_capital_ratio(regn, month):
    capital = get_bank_capital(regn, month)
    assets = get_bank_assets(regn, month)

    if assets == 0:
        return 0

    return (capital / assets) * 100


def get_bank_liquidity_ratio(regn, month):
    data = get_clean_bank_data(regn, month)

    if data.empty:
        return 0

    liquid_prefixes = [
        "301",
        "302",
        "304",
        "319"
    ]

    liquid_assets = sum_by_prefix(data, liquid_prefixes, ap="1")

    liabilities = get_bank_liabilities(regn, month)

    if liabilities == 0:
        return 0

    return (liquid_assets / liabilities) * 100

def get_bank_all_metrics(regn, month):
    bank_name = get_bank_name_safe(regn)

    metrics = {
        "Код банка": normalize_regn(regn),
        "Название банка": bank_name,
        "Месяц": month,

        "Активы": get_bank_assets(regn, month),
        "Кредиты физлицам": get_bank_loans_people(regn, month),
        "Кредиты юрлицам": get_bank_loans_companies(regn, month),
        "Кредиты всего": get_bank_loans_total(regn, month),
        "Вклады": get_bank_deposits(regn, month),
        "Капитал": get_bank_capital(regn, month),
        "Прибыль": get_bank_profit(regn, month),

        "ROA (рентабельность)": get_bank_roa(regn, month),
        "LTD (кредиты/вклады)": get_bank_ltd(regn, month),
        "Достаточность капитала": get_bank_capital_ratio(regn, month),
        "Ликвидность": get_bank_liquidity_ratio(regn, month)
    }

    return metrics


def get_growth(regn, month, month_previous, metric_func):
    metric1 = metric_func(regn, month)
    metric2 = metric_func(regn, month_previous)

    if metric2 == 0:
        return 0

    return ((metric1 / metric2) - 1) * 100


def get_prev_month(month):
    months = get_available_months()
    month = str(month)

    if month not in months:
        return None

    index = months.index(month)

    if index == 0:
        return None

    return months[index - 1]


def get_all_growth(regn, month, month_previous):
    metrics = [
        ("Активы", get_bank_assets),
        ("Кредиты физлицам", get_bank_loans_people),
        ("Кредиты юрлицам", get_bank_loans_companies),
        ("Вклады", get_bank_deposits),
        ("Капитал", get_bank_capital),
        ("Прибыль", get_bank_profit),
        ("ROA (рентабельность)", get_bank_roa),
        ("LTD (кредиты/вклады)", get_bank_ltd),
        ("Достаточность капитала", get_bank_capital_ratio),
        ("Ликвидность", get_bank_liquidity_ratio)
    ]

    result = {}

    for name, func in metrics:
        result[name] = get_growth(regn, month, month_previous, func)

    return result


def get_assets_history(regn):
    months = get_available_months()

    result = []

    for month in months:
        result.append({
            "month": month,
            "assets": get_bank_assets(regn, month)
        })

    return pd.DataFrame(result)


def get_bank_status(regn, month):
    score = 0

    roa = get_bank_roa(regn, month)
    liquidity = get_bank_liquidity_ratio(regn, month)
    capital_ratio = get_bank_capital_ratio(regn, month)
    ltd = get_bank_ltd(regn, month)

    if roa > 0.5:
        score += 1

    if liquidity > 0.3:
        score += 1

    if capital_ratio > 0.5:
        score += 1

    if 20 <= ltd <= 150:
        score += 1

    if score >= 3:
        return "Надежный"

    if score == 2:
        return "Удовлетворительный"

    return "Рискованный"


def search_banks(query):
    banks = get_all_banks()

    if query is None or query.strip() == "":
        return banks

    query = query.strip().lower()

    mask = (
        banks["REGN"].astype(str).str.lower().str.contains(query, na=False)
        |
        banks["bank_name"].astype(str).str.lower().str.contains(query, na=False)
    )

    return banks[mask].reset_index(drop=True)


def get_metric_function(metric_key):
    metric_map = {
        "assets": ("Активы", get_bank_assets),
        "capital": ("Капитал", get_bank_capital),
        "profit": ("Прибыль", get_bank_profit),
        "loans": ("Кредиты всего", get_bank_loans_total),
        "deposits": ("Вклады", get_bank_deposits),
        "roa": ("ROA", get_bank_roa),
        "ltd": ("Кредиты / вклады", get_bank_ltd),
        "capital_ratio": ("Достаточность капитала", get_bank_capital_ratio),
        "liquidity": ("Ликвидность", get_bank_liquidity_ratio),
    }

    return metric_map.get(metric_key, metric_map["assets"])


def get_top_banks(metric_key, month, limit=10):
    metric_name, metric_func = get_metric_function(metric_key)

    banks = get_all_banks()
    result = []

    for _, bank in banks.iterrows():
        regn = bank["REGN"]
        bank_name = bank["bank_name"]

        try:
            value = metric_func(regn, month)

            if value is None:
                continue

            result.append({
                "REGN": regn,
                "bank_name": bank_name,
                "metric_value": value
            })

        except Exception:
            continue

    df = pd.DataFrame(result)

    if df.empty:
        return df, metric_name

    df = df.sort_values("metric_value", ascending=False)
    df = df.head(limit).reset_index(drop=True)
    df["place"] = df.index + 1

    return df, metric_name


def get_metric_history(regn, metric_key):
    """Получает историю конкретной метрики для графика"""
    months = get_available_months()

    _, metric_func = get_metric_function(metric_key)
    
    result = []
    for month in months:
        value = metric_func(regn, month)
        result.append({
            "month": month,
            "value": value
        })
    
    return pd.DataFrame(result)


def get_available_banks_list():
    """Получает список всех банков для выпадающих списков"""
    banks = get_all_banks()
    return banks.to_dict("records")