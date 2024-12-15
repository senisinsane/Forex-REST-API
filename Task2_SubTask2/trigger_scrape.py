import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta
import logging
from random import choice
import schedule
from concurrent.futures import ThreadPoolExecutor

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of User-Agent strings to randomize requests
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
]

def fetch_historical_exchange_data(quote, from_date, to_date):
    """
    Fetches historical exchange data from Yahoo Finance and returns it as a Pandas DataFrame.

    Parameters:
        quote (str): The currency pair quote, e.g., 'GBPINR=X'.
        from_date (int): The start date in Unix timestamp.
        to_date (int): The end date in Unix timestamp.

    Returns:
        pd.DataFrame: The historical exchange data as a Pandas DataFrame.
    """
    try:
        # Construct the URL
        url = f"https://finance.yahoo.com/quote/{quote}/history/?period1={from_date}&period2={to_date}&interval=1d"
        headers = {"User-Agent": choice(USER_AGENTS)}

        # Fetch the content of the URL
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')

        # Locate the table containing historical data
        table = soup.find('table')
        if not table:
            raise ValueError("No historical data table found on the web page.")

        # Extract table headers
        headers = [header.get_text(strip=True) for header in table.find_all('th')]

        # Extract table rows
        rows = []
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if cells:
                rows.append([cell.get_text(strip=True) for cell in cells])

        # Create a DataFrame
        df = pd.DataFrame(rows, columns=headers if headers else None)

        # Data cleaning: remove rows with missing or malformed data
        df.dropna(inplace=True)

        # Convert date column to datetime if it exists
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df.dropna(subset=['Date'], inplace=True)

        return df

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching the URL: {e}")
    except Exception as e:
        logging.error(f"Error parsing the table: {e}")


def store_data_in_memory_db(dataframe, db_name="exchange_data.db", table_name="exchange_rates"):
    """
    Stores a Pandas DataFrame into an SQLite database.

    Parameters:
        dataframe (pd.DataFrame): The DataFrame to store.
        db_name (str): The name of the SQLite database file (this can be a file, not just in-memory).
        table_name (str): The name of the table to store the data.
    """
    try:
        with sqlite3.connect(db_name) as conn:
            dataframe.to_sql(table_name, conn, if_exists='append', index=False)
            logging.info(f"Data stored in SQLite database '{db_name}' table '{table_name}'.")
    except Exception as e:
        logging.error(f"Error storing data in SQLite: {e}")


def get_period_timestamps(period):
    """
    Get Unix timestamps for the given period.
    Example: '1W' -> 1 week ago, '1M' -> 1 month ago, etc.
    """
    end_date = datetime.now()
    if period == '1W':
        start_date = end_date - timedelta(weeks=1)
    elif period == '1M':
        start_date = end_date - timedelta(days=30)
    elif period == '3M':
        start_date = end_date - timedelta(days=90)
    elif period == '6M':
        start_date = end_date - timedelta(days=180)
    elif period == '1Y':
        start_date = end_date - timedelta(days=365)
    else:
        raise ValueError(f"Invalid period: {period}")

    # Convert to Unix timestamps
    from_timestamp = int(time.mktime(start_date.timetuple()))
    to_timestamp = int(time.mktime(end_date.timetuple()))

    return from_timestamp, to_timestamp


def scrape_and_store(pair, period):
    """
    Scrapes and stores data for a given currency pair and period.

    Parameters:
        pair (str): Currency pair (e.g., 'GBPINR=X').
        period (str): Time period (e.g., '1W', '1M').
    """
    logging.info(f"Scraping data for {pair} for the period {period}")
    try:
        from_date, to_date = get_period_timestamps(period)
        historical_data = fetch_historical_exchange_data(pair, from_date, to_date)

        if historical_data is not None and not historical_data.empty:
            table_name = f"exchange_rates_{pair}_{period}".replace('=', '').replace('X', '')
            store_data_in_memory_db(historical_data, table_name=table_name)  # Store to in-memory DB or file DB
            logging.info(f"Data for {pair} for {period} successfully stored.")
        else:
            logging.warning(f"No data found for {pair} for {period}.")
    except Exception as e:
        logging.error(f"Error scraping data for {pair} for {period}: {e}")


def schedule_scraping():
    """
    Schedules the scraping task for multiple currency pairs and periods.
    """
    currency_pairs = ["GBPINR=X", "AEDINR=X"]
    periods = ['1W', '1M', '3M', '6M', '1Y']

    with ThreadPoolExecutor(max_workers=5) as executor:
        for pair in currency_pairs:
            for period in periods:
                executor.submit(scrape_and_store, pair, period)


# Schedule the scraping task periodically (e.g., every 5 minutes and daily at midnight)
schedule.every(5).minutes.do(schedule_scraping)         # Every 5 minutes
schedule.every().day.at("00:00").do(schedule_scraping)  # Daily at midnight

if __name__ == "__main__":
    logging.info("Starting the scheduled scraping job.")
    while True:
        schedule.run_pending()
        time.sleep(1)
