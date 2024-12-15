import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import logging
from random import choice
from flask import Flask, request, jsonify
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of User-Agent strings to randomize requests
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
]

# Flask app initialization
app = Flask(__name__)

# SQLite in-memory database
conn = sqlite3.connect(":memory:", check_same_thread=False)
cursor = conn.cursor()

# Create a table to store forex data
cursor.execute("""
CREATE TABLE IF NOT EXISTS forex_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    date TEXT,
    open TEXT,
    high TEXT,
    low TEXT,
    close TEXT,
    adj_close TEXT,
    volume TEXT
)
""")
conn.commit()

def fetch_historical_exchange_data(quote, from_date, to_date):
    """
    Fetches historical exchange data from Yahoo Finance and returns it as a Pandas DataFrame.

    Parameters:
        quote (str): The currency pair quote, e.g., 'EURUSD=X'.
        from_date (int): The start date in Unix timestamp.
        to_date (int): The end date in Unix timestamp.

    Returns:
        pd.DataFrame: The historical exchange data as a Pandas DataFrame.
    """
    try:
        url = f"https://finance.yahoo.com/quote/{quote}/history/?period1={from_date}&period2={to_date}&interval=1d"
        headers = {"User-Agent": choice(USER_AGENTS)}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table')

        if not table:
            raise ValueError("No historical data table found on the web page.")

        # Extract column headers and rows
        headers = [header.get_text(strip=True) for header in table.find_all('th')]
        rows = [[cell.get_text(strip=True) for cell in row.find_all('td')] for row in table.find_all('tr') if row.find_all('td')]

        df = pd.DataFrame(rows, columns=headers)
        df.dropna(inplace=True)

        # Standardize column names
        def clean_column_name(col):
            col = col.lower().strip()
            col = re.sub(r"close.*", "close", col)
            col = re.sub(r"adj close.*", "adj_close", col)
            col = re.sub(r"volume.*", "volume", col)
            return col

        df.rename(columns=lambda x: clean_column_name(x), inplace=True)

        # Keep only valid columns
        required_columns = ["date", "open", "high", "low", "close", "adj_close", "volume"]
        df = df[[col for col in df.columns if col in required_columns]]

        # Convert 'date' column to datetime
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df.dropna(subset=["date"], inplace=True)

        # Handle missing or invalid values
        if "volume" in df.columns:
            df["volume"] = df["volume"].replace("-", 0).astype(float, errors="ignore")

        return df

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching the URL: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error parsing the table: {e}")
        return pd.DataFrame()

def store_data_in_sqlite(dataframe, key):
    """
    Stores a Pandas DataFrame into the SQLite database.

    Parameters:
        dataframe (pd.DataFrame): The DataFrame to store.
        key (str): The key under which the data will be stored.
    """
    try:
        dataframe['key'] = key
        dataframe.to_sql('forex_data', conn, if_exists='append', index=False)
        logging.info(f"Data stored in SQLite database under key '{key}'.")
    except Exception as e:
        logging.error(f"Error storing data in SQLite: {e}")

def parse_period_to_timestamps(period):
    """
    Converts a period like '1M' or '1Y' into start and end Unix timestamps.

    Parameters:
        period (str): The period to parse, e.g., '1M' or '1Y'.

    Returns:
        tuple: (start_date_unix, end_date_unix)
    """
    end_date = datetime.now()
    if period.endswith('M'):
        start_date = end_date - timedelta(days=30 * int(period[:-1]))
    elif period.endswith('Y'):
        start_date = end_date - timedelta(days=365 * int(period[:-1]))
    else:
        raise ValueError("Invalid period format. Use 'XM' for months or 'XY' for years.")

    return int(start_date.timestamp()), int(end_date.timestamp())

@app.route('/')
def home():
    return "Forex data API"

@app.route('/api/forex-data', methods=['POST'])
def get_forex_data():
    try:
        from_currency = request.args.get('from')
        to_currency = request.args.get('to')
        period = request.args.get('period')

        if not all([from_currency, to_currency, period]):
            return jsonify({"error": "Missing required fields: 'from', 'to', 'period'."}), 400

        from_date, to_date = parse_period_to_timestamps(period)
        quote = f"{from_currency}{to_currency}=X"

        historical_data = fetch_historical_exchange_data(quote, from_date, to_date)

        if not historical_data.empty:
            key = f"{from_currency}_{to_currency}_{period}"
            store_data_in_sqlite(historical_data, key)
            return historical_data.to_json(orient='records')

        return jsonify({"error": "No data found for the specified query."}), 404

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "An unexpected error occurred."}), 500

@app.route('/api/forex-data-range', methods=['POST'])
def get_forex_data_range():
    try:
        quote = request.args.get('quote')
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        if not all([quote, start_date_str, end_date_str]):
            return jsonify({"error": "Missing required fields: 'quote', 'start_date', 'end_date'."}), 400

        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({"error": "Invalid date format. Use 'YYYY-MM-DD'."}), 400

        from_date, to_date = int(start_date.timestamp()), int(end_date.timestamp())
        historical_data = fetch_historical_exchange_data(quote, from_date, to_date)

        if not historical_data.empty:
            key = f"{quote}_{start_date_str}_to_{end_date_str}"
            store_data_in_sqlite(historical_data, key)
            return historical_data.to_json(orient='records')

        return jsonify({"error": "No data found for the specified query."}), 404

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "An unexpected error occurred."}), 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)

