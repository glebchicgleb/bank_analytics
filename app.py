from flask import Flask
from bank_analytics import find_bank_by_code, get_bank_all_metrics

app = Flask(__name__)

@app.route('/')
def home():
    return "Это главная страница"

@app.route('/test')
def test():
    return "Это тестовая страница"

@app.route('/bank/<int:code>')
def bank_page(code):
    bank_info = find_bank_by_code(code)
    if bank_info is None:
        return f"Банк с кодом {code} не найден"
    return f"Найден банк: {bank_info[0]['bank_name']}"

if __name__ == '__main__':
    app.run()