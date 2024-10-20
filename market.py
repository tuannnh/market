# import streamlit as st
# import pandas as pd
# import numpy as np
import time

from numpy.ma.core import product

# Import necessary libraries
import streamlit as st
import plotly.graph_objs as go
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd
from millify import prettify
from dotenv import load_dotenv
import os
from worker import do_tasks

# Load environment variables from .env file
load_dotenv()

# Retrieve database connection settings from environment variables
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# URL-encode the password
encoded_password = quote_plus(DB_PASSWORD)

connection_string = f'postgresql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}'


def format_weekly_date(date):
    week_number = date.isocalendar()[1] - date.replace(day=1).isocalendar()[1] + 1
    month_year = date.strftime('%b %Y')
    return f'Week {week_number}/{month_year}'


# Function to load data from PostgreSQL
def load_data():
    engine = create_engine(connection_string)
    gold_query = "SELECT * FROM gold_rates WHERE product_name IN ('Nữ trang 99.99', 'Nữ trang 99.99 - Bán Lẻ') ORDER BY id DESC"
    gold_data = pd.read_sql(gold_query, engine)

    currency_query = "SELECT * FROM currency_rates WHERE currency_code = 'USD' ORDER BY id DESC"
    currency_data = pd.read_sql(currency_query, engine)

    return gold_data, currency_data


def aggregate_gold_data(gold_data, granularity):
    gold_data['id'] = pd.to_datetime(gold_data['id'])  # Ensure the date column is datetime
    if granularity == "Daily":
        gold_data['date'] = gold_data['id'].dt.strftime('%d %b %Y')
        return gold_data
    elif granularity == "Weekly":
        resampled_gold_data = gold_data.resample('W-Mon', on='id').agg({'buy': 'mean', 'sell': 'mean'}).reset_index()
        resampled_gold_data['date'] = resampled_gold_data['id'].apply(format_weekly_date)
        return resampled_gold_data
    elif granularity == "Monthly":
        resampled_gold_data = gold_data.resample('ME', on='id').agg({'buy': 'mean', 'sell': 'mean'}).reset_index()
        resampled_gold_data['date'] = resampled_gold_data['id'].dt.strftime('%b %Y')
        return resampled_gold_data


def aggregate_currency_data(currency_data, granularity):
    currency_data['id'] = pd.to_datetime(currency_data['id'])  # Ensure the date column is datetime
    if granularity == "Daily":
        currency_data['date'] = currency_data['id'].dt.strftime('%d %b %Y')
        return currency_data
    elif granularity == "Weekly":
        resampled_currency_data = currency_data.resample('W-Mon', on='id').agg(
            {'buy': 'mean', 'transfer': 'mean', 'sell': 'mean'}).reset_index()
        resampled_currency_data['date'] = resampled_currency_data['id'].apply(format_weekly_date)
        return resampled_currency_data
    elif granularity == "Monthly":
        resampled_currency_data = currency_data.resample('ME', on='id').agg(
            {'buy': 'mean', 'transfer': 'mean', 'sell': 'mean'}).reset_index()
        resampled_currency_data['date'] = resampled_currency_data['id'].dt.strftime('%b %Y')
        return resampled_currency_data


def load_updated_date():
    today = pd.Timestamp.now().date().strftime('%d/%m/%Y')
    st.markdown(f'### Data Updated: <span style="color:lightblue;">{today}</span>', unsafe_allow_html=True)


def load_metrics(gold_data, currency_data):
    latest_gold_buy_data = gold_data.head(1)['buy'].values[0]
    latest_gold_sell_data = gold_data.head(1)['sell'].values[0]

    delta_gold_buy = int(gold_data.head(1)['buy'].values[0] - gold_data.head(2)['buy'].values[1])
    delta_gold_sell = int(gold_data.head(1)['sell'].values[0] - gold_data.head(2)['sell'].values[1])

    latest_currency_buy_data = currency_data.head(1)['buy'].values[0]
    latest_currency_transfer_data = currency_data.head(1)['transfer'].values[0]
    latest_currency_sell_data = currency_data.head(1)['sell'].values[0]

    delta_currency_buy = currency_data.head(1)['buy'].values[0] - currency_data.head(2)['buy'].values[1]
    delta_currency_transfer = currency_data.head(1)['transfer'].values[0] - currency_data.head(2)['transfer'].values[1]
    delta_currency_sell = currency_data.head(1)['sell'].values[0] - currency_data.head(2)['sell'].values[1]

    gold_col, currency_col = st.columns(2)
    gold_col.markdown(f"### Gold Rate")

    gold_sub_col1, gold_sub_col2 = gold_col.columns(2)
    gold_sub_col1.metric(label=f"Buy",
                         value=prettify(latest_gold_buy_data),
                         delta=delta_gold_buy,
                         delta_color='normal',
                         )

    gold_sub_col2.metric(label=f"Sell",
                         value=prettify(latest_gold_sell_data),
                         delta=delta_gold_sell,
                         delta_color='normal',
                         )

    currency_col.markdown("### USD Rate")
    currency_sub_col1, currency_sub_col2, currency_sub_col3 = currency_col.columns(3)
    currency_sub_col1.metric(label="Buy",
                             value=prettify(latest_currency_buy_data),
                             delta=delta_currency_buy,
                             delta_color='normal',
                             )
    currency_sub_col2.metric(label="Transfer",
                             value=prettify(latest_currency_transfer_data),
                             delta=delta_currency_transfer,
                             delta_color='normal',
                             )
    currency_sub_col3.metric(label="Sell",
                             value=prettify(latest_currency_sell_data),
                             delta=delta_currency_sell,
                             delta_color='normal',
                             )


def prepare_gold_chart(aggregated_gold_data):
    aggregated_gold_data = aggregated_gold_data.sort_values(by='date')
    trace_green = go.Scatter(
        x=aggregated_gold_data['date'],
        y=aggregated_gold_data['buy'],
        mode='lines+markers',
        name='Buy',
        line=dict(color='lightgreen'),
        hovertemplate='%{y}')
    trace_red = go.Scatter(
        x=aggregated_gold_data['date'],
        y=aggregated_gold_data['sell'],
        mode='lines+markers',
        name='Sell',
        line=dict(color='lightcoral'),
        hovertemplate='%{y}',
    )
    gold_layout = go.Layout(title='Gold Rates',
                            xaxis=dict(title='Date', showgrid=True),
                            yaxis=dict(title='Price (Thousand VND)'),
                            hovermode='closest')
    return go.Figure(data=[trace_green, trace_red], layout=gold_layout)


def prepare_currency_chart(aggregated_currency_data):
    aggregated_currency_data = aggregated_currency_data.sort_values(by='date')
    trace_green = go.Scatter(
        x=aggregated_currency_data['date'],
        y=aggregated_currency_data['buy'],
        mode='lines+markers',
        name='Buy',
        line=dict(color='lightgreen'),
        hovertemplate='%{y}')
    trace_blue = go.Scatter(
        x=aggregated_currency_data['date'],
        y=aggregated_currency_data['transfer'],
        mode='lines+markers',
        name='Transfer',
        line=dict(color='lightblue'),
        hovertemplate='%{y}'
    )
    trace_red = go.Scatter(
        x=aggregated_currency_data['date'],
        y=aggregated_currency_data['sell'],
        mode='lines+markers',
        name='Sell',
        line=dict(color='lightcoral'),
        hovertemplate='%{y}'
    )
    usd_layout = go.Layout(title='USD Rates',
                           xaxis=dict(title='Date', showgrid=True),
                           yaxis=dict(title='Price (Thousand VND)'),
                           hovermode='closest')
    return go.Figure(data=[trace_green, trace_red, trace_blue], layout=usd_layout)


def load_tab_charts(gold_data, currency_data):
    daily, weekly, monthly = st.tabs(["Daily", "Weekly", "Monthly"])
    with daily:
        daily_gold_col, daily_currency_col = st.columns(2)
        aggregated_gold_data = aggregate_gold_data(gold_data, 'Daily')
        aggregate_currency_data(currency_data, 'Daily')
        gold_fig = prepare_gold_chart(aggregated_gold_data)
        currency_fig = prepare_currency_chart(aggregate_currency_data(currency_data, 'Daily'))
        daily_gold_col.plotly_chart(gold_fig)
        daily_currency_col.plotly_chart(currency_fig)
    with weekly:
        weekly_gold_col, weekly_currency_col = st.columns(2)
        aggregate_gold_data(gold_data, 'Weekly')
        aggregate_currency_data(currency_data, 'Weekly')
        weekly_gold_fig = prepare_gold_chart(aggregate_gold_data(gold_data, 'Weekly'))
        weekly_currency_fig = prepare_currency_chart(aggregate_currency_data(currency_data, 'Weekly'))
        weekly_gold_col.plotly_chart(weekly_gold_fig)
        weekly_currency_col.plotly_chart(weekly_currency_fig)
    with monthly:
        monthly_gold_col, monthly_currency_col = st.columns(2)
        aggregate_gold_data(gold_data, 'Monthly')
        aggregate_currency_data(currency_data, 'Monthly')
        monthly_gold_fig = prepare_gold_chart(aggregate_gold_data(gold_data, 'Monthly'))
        monthly_currency_fig = prepare_currency_chart(aggregate_currency_data(currency_data, 'Monthly'))
        monthly_gold_col.plotly_chart(monthly_gold_fig)
        monthly_currency_col.plotly_chart(monthly_currency_fig)


def load_main_page():
    st.title(' Viet Nam Market Data')
    load_updated_date()
    gold_data, currency_data = load_data()
    load_metrics(gold_data, currency_data)
    load_tab_charts(gold_data, currency_data)


def main():
    # Set page config
    st.set_page_config(
        page_title="Market Data",
        page_icon="💻",
        layout="wide",
    )
    load_main_page()


if __name__ == "__main__":
    main()
    while True:
        do_tasks()
        time.sleep(3600)
