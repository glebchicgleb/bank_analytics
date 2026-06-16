from flask import Flask, render_template, request
from bank_analytics import *

app = Flask(__name__)

def format_month(month_str):
    months_names = {
        '01': 'январь', '02': 'февраль', '03': 'март',
        '04': 'апрель', '05': 'май', '06': 'июнь',
        '07': 'июль', '08': 'август', '09': 'сентябрь',
        '10': 'октябрь', '11': 'ноябрь', '12': 'декабрь'
    }
    
    year = month_str[:4]
    month_num = month_str[4:6]
    
    return f"{months_names[month_num]} {year}"

@app.template_filter('format_month')
def format_month_filter(month_str):
    return format_month(month_str)

@app.route("/")
def home():
    query = request.args.get("q", "")

    banks = search_banks(query).to_dict("records")

    months = get_available_months()
    latest_month = months[-1]

    return render_template(
        "simple.html",
        banks=banks,
        latest_month=latest_month,
        query=query
    )


@app.route("/bank/<regn>")
def bank_page(regn):
    months = get_available_months()

    month = request.args.get("month")
    if month is None:
        month = months[-1]

    metrics = get_bank_all_metrics(regn, month)
    status = get_bank_status(regn, month)

    history_df = get_assets_history(regn)
    history = history_df.to_dict("records")

    chart_months = history_df["month"].tolist()
    chart_assets = history_df["assets"].tolist()

    return render_template(
        "bank.html",
        regn=regn,
        month=month,
        months=months,
        metrics=metrics,
        status=status,
        history=history,
        chart_months=chart_months,
        chart_assets=chart_assets
    )

# Добавьте фильтр форматирования
@app.template_filter('format_metric_value')
def format_metric_value(value, metric_key):
    """Форматирует значение метрики в зависимости от её типа"""
    money_metrics = ["assets", "capital", "profit", "loans", "deposits"]
    percent_metrics = ["roa", "ltd", "capital_ratio", "liquidity"]
    
    if metric_key in money_metrics:
        return "{:,.0f}".format(value).replace(",", ".") + " руб"
    elif metric_key in percent_metrics:
        return "{:.2f}%".format(value)
    else:
        return "{:,.2f}".format(value)


# Новый маршрут для сравнения
@app.route("/compare")
def compare_page():
    months = get_available_months()
    
    # Получаем параметры из URL
    bank_regns = request.args.getlist("banks")
    month = request.args.get("month", months[-1])
    metric = request.args.get("metric", "assets")
    
    # Загружаем список всех доступных банков
    all_banks = get_available_banks_list()
    
    # Получаем название метрики используя существующую функцию
    metric_name, _ = get_metric_function(metric)
    
    # Если банки выбраны, загружаем данные для сравнения
    compare_data = None
    chart_data = None
    
    if bank_regns and len(bank_regns) >= 2:
        compare_data = []
        chart_data = {
            "labels": [],
            "datasets": []
        }
        
        # Получаем все месяцы для графиков
        all_months_list = get_available_months()
        chart_data["labels"] = all_months_list
        
        # Цвета для графиков
        colors = [
            {"bg": "rgba(139, 60, 44, 0.7)", "border": "#8b3c2c"},
            {"bg": "rgba(61, 90, 60, 0.7)", "border": "#3d5a3c"},
            {"bg": "rgba(70, 80, 120, 0.7)", "border": "#465078"},
            {"bg": "rgba(180, 140, 60, 0.7)", "border": "#b48c3c"}
        ]
        
        for i, regn in enumerate(bank_regns[:4]):
            # Используем существующую функцию get_bank_all_metrics
            metrics = get_bank_all_metrics(regn, month)
            
            compare_data.append({
                "regn": regn,
                "name": metrics["Название банка"],
                "metrics": metrics
            })
            
            # Получаем историю для графика
            history = get_metric_history(regn, metric)
            values = history["value"].tolist()
            
            color = colors[i % len(colors)]
            chart_data["datasets"].append({
                "label": metrics["Название банка"],
                "data": values,
                "borderColor": color["border"],
                "backgroundColor": color["bg"],
                "borderWidth": 2,
                "tension": 0.1
            })
    
    return render_template(
        "compare.html",
        months=months,
        month=month,
        metric=metric,
        metric_name=metric_name,
        all_banks=all_banks,
        selected_banks=bank_regns,
        compare_data=compare_data,
        chart_data=chart_data
    )

@app.route("/top")
def top_page():
    months = get_available_months()

    month = request.args.get("month")
    if month is None:
        month = months[-1]

    metric = request.args.get("metric", "assets")

    top_df, metric_name = get_top_banks(metric, month, limit=10)
    top_banks = top_df.to_dict("records")

    return render_template(
        "top.html",
        months=months,
        month=month,
        metric=metric,
        metric_name=metric_name,
        top_banks=top_banks
    )


if __name__ == "__main__":
    app.run(debug=True)