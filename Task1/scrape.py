import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time
from datetime import datetime
import logging
from random import choice

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
        quote (str): The currency pair quote, e.g., 'EURUSD=X'.
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


def store_data_in_memory(dataframe, table_name):
    """
    Stores a Pandas DataFrame into an in-memory SQLite database.

    Parameters:
        dataframe (pd.DataFrame): The DataFrame to store.
        table_name (str): The name of the table to store the data.
    """
    try:
        # Create an in-memory SQLite database
        with sqlite3.connect(":memory:") as conn:
            # Store the DataFrame in the database
            dataframe.to_sql(table_name, conn, if_exists='replace', index=False)
            logging.info(f"Data stored in in-memory SQLite database table '{table_name}'.")
            
            # For demonstration purposes, query and display the data
            result = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            logging.info(f"Queried data from in-memory database:\n{result}")
    except Exception as e:
        logging.error(f"Error storing data in in-memory SQLite database: {e}")


if __name__ == "__main__":
    # Example usage
    quote = input("Enter the currency pair quote (e.g., EURUSD=X): ").strip()
    from_date_str = input("Enter the start date (YYYY-MM-DD): ").strip()
    to_date_str = input("Enter the end date (YYYY-MM-DD): ").strip()

    try:
        # Convert dates to Unix timestamps
        from_date = int(time.mktime(datetime.strptime(from_date_str, "%Y-%m-%d").timetuple()))
        to_date = int(time.mktime(datetime.strptime(to_date_str, "%Y-%m-%d").timetuple()))

        # Fetch historical exchange data
        historical_data = fetch_historical_exchange_data(quote, from_date, to_date)

        if historical_data is not None and not historical_data.empty:
            print("Extracted Historical Data:")
            print(historical_data)

            # Store the data in an in-memory SQLite database
            table_name = "exchange_rates"
            store_data_in_memory(historical_data, table_name)
        else:
            logging.warning("No valid data was extracted.")

    except ValueError as e:
        logging.error(f"Date conversion error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
