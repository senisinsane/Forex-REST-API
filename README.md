# Forex Data Scraping and API Project

## Overview
This project is designed to fetch historical foreign exchange (forex) data from Yahoo Finance, store it in-memory in a SQLite database, and provide access via a Flask-based REST API. The project consists of three main tasks:

- **Task 1**: Scraping historical forex data.
- **Task 2**: Providing forex data via a REST API.
  - **Subtask 1**: Implementing the API to fetch forex data.
  - **Subtask 2**: Scheduling the scraping task at regular intervals.

## Task 1: Scraping Historical Forex Data (`scrape.py`)

### Description
This script fetches historical exchange rate data from Yahoo Finance for a given currency pair between two specified dates and stores it in an in-memory SQLite database.

### Setup

1. Clone or navigate to the project directory.
2. Install the required dependencies by running:

   ```bash
   pip install -r requirements.txt
   ```
Change to the Task1 directory.

Run the following command to start scraping:
bash
```
python scrape.py
```
Input the following when prompted:

Currency pair: e.g., EURUSD=X
Start date: The start date in format YYYY-MM-DD.
End date: The end date in format YYYY-MM-DD.
The script will fetch the historical forex data and store it in an in-memory SQLite database.

## Task 2: Forex API (forex_api.py)
### Setup
Subtask 1: Implementing the Flask API
Change to the Task2_SubTask1/api/ directory.

Run the following command to start the Flask app:

bash
```
python forex_api.py
```
The server will start running, and you can access the API documentation via Postman to send requests.

Subtask 2: Scheduling the Scraping Task
Change to the Task2_SubTask2 directory.

Run the following command to start the scheduled scraping job:

bash
```
python trigger_scrape.py
```
This will trigger the scraping task at regular intervals, ensuring the forex data is updated regularly.
