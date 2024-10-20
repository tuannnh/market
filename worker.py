import requests
from bs4 import BeautifulSoup
import psycopg2
from dotenv import load_dotenv
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import re

# Load environment variables from .env file
load_dotenv()

# Retrieve database connection settings from environment variables
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
GOLD_URL = os.getenv('GOLD_URL')
CURRENCY_URL = os.getenv('CURRENCY_URL')


def convert_currency_date(date_str):
    # Parse the original string into a datetime object
    datetime_object = datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")

    # Format the datetime object to the desired format
    formatted_date_str = datetime_object.strftime("%Y-%m-%d")

    return formatted_date_str


def convert_gold_date(date_str):
    # Parse the original string into a datetime object
    datetime_object = datetime.strptime(date_str, "%d/%m/%Y")

    # Format the datetime object to the desired format
    formatted_date_str = datetime_object.strftime("%Y-%m-%d")

    return formatted_date_str


def fetch_gold_data():
    # Fetch Gold price data
    gold_response = requests.get(GOLD_URL)
    html = gold_response.text

    # Parse the HTML and extract data from the table
    soup = BeautifulSoup(html, 'html.parser')
    extracted_date = soup.find('span', {'class': 'update-time size-14'}).text
    date_match = re.search(r"\d{2}/\d{2}/\d{4}", extracted_date)
    date_str = date_match.group()
    capture_date = convert_gold_date(date_str)
    sg_div = soup.find('div', {'class': 'hcm'})
    table = sg_div.find('table')
    gold_data = []
    for row in table.find_all('tr'):
        cols = row.find_all('td')
        gold_data.append(
            [
                col for col in [capture_date] + [ele.text.strip() for ele in cols]
            ]
        )
    # remove header
    gold_data.pop(0)
    return gold_data


def fetch_currency_data():
    # Fetch Currency data
    currency_response = requests.get(CURRENCY_URL)
    currency_xml_data = currency_response.content

    # Parse the XML data
    root = ET.fromstring(currency_xml_data)
    # print(root.ExrateList)
    currency_data = []
    # Ignore the first and last child as it contains metadata
    capture_date = convert_currency_date(root.find('DateTime').text)

    for exrate in root.findall('Exrate'):
        currency_data.append(
            [
                capture_date,
                exrate.attrib['CurrencyCode'],
                exrate.attrib['CurrencyName'],
                exrate.attrib['Buy'].replace(",", "").replace("-", "-1"),
                exrate.attrib['Transfer'].replace(",", "").replace("-", "-1"),
                exrate.attrib['Sell'].replace(",", "").replace("-", "-1")
            ]
        )

    return currency_data


# Function to store data in PostgreSQL
def store_gold_data(gold_data):
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cur = conn.cursor()

    upsert_query = """
        INSERT INTO gold_rates (id, product_name, buy, sell)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id, product_name) DO UPDATE SET buy = EXCLUDED.buy, sell = EXCLUDED.sell
    """

    for row in gold_data:
        cur.execute(upsert_query, row)

    conn.commit()
    cur.close()
    conn.close()


def store_currency_data(currency_data):
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cur = conn.cursor()

    upsert_query = """
        INSERT INTO currency_rates (id, currency_code, currency_name, buy, transfer, sell)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id, currency_code) DO UPDATE SET buy = EXCLUDED.buy, transfer = EXCLUDED.transfer, sell = EXCLUDED.sell
    """

    for row in currency_data:
        cur.execute(upsert_query, row)

    conn.commit()
    cur.close()
    conn.close()


def do_tasks():
    gold_data = fetch_gold_data()
    currency_data = fetch_currency_data()
    store_gold_data(gold_data)
    store_currency_data(currency_data)
