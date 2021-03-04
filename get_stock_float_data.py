from bs4 import BeautifulSoup
import requests
from globals import cxn
from mysql.connector import Error
import time
from tqdm import tqdm

baseurl = 'https://finviz.com/quote.ashx?t='
redirecturl='https://finviz.com/'

def get_ticker_list():
    tickers=[]
    asofDate=None
    try:
        select_query = 'select max(asof_date) as maxDate from arkdb.holdings'
        with cxn.cursor() as cursor:
            cursor.execute(select_query)
            for row in cursor.fetchall():
                asofDate = row[0]
        select_query='select distinct ticker, asof_date from arkdb.holdings where asof_date = (select max(asof_date) from arkdb.holdings)'
        with cxn.cursor() as cursor:
            cursor.execute(select_query)
            for row in cursor.fetchall():
                tickers.append(row[0])
    except Error as e:
        print(e)
    return tickers, asofDate

def delete_data_for_stocks_for_date(asofDate):
    try:
        delete_query = "delete from stock_data WHERE asof_date='" + asofDate + "'"
        with cxn.cursor() as cursor:
            cursor.execute(delete_query)
            cxn.commit()
    except Error as e:
        print(e)

def insert_data_for_for_stocks_and_date(data):
    try:
        insert_query = """
        INSERT INTO stock_data
        (ticker, asof_date, shares_outstanding, shares_float)
        VALUES (%s, %s, %s, %s)
        """

        with cxn.cursor() as cursor:
            cursor.executemany(insert_query, data)
            cxn.commit()
    except Error as e:
        print(e)

def populateHoldingForAllTickers():
    tickers, asofDate = get_ticker_list()
    sDate=asofDate.strftime('%Y-%m-%d')
    delete_data_for_stocks_for_date(sDate)
    data=[]
    for item in tqdm(tickers):
        sharesOutstanding, sharesFloat = get_holdings_information(item)
        data.append((item, sDate, sharesOutstanding, sharesFloat))
        time.sleep(1)
    insert_data_for_for_stocks_and_date(data)

def get_holdings_information(ticker):
    sharesOutstanding = "-1"
    sharesFloat = "-1"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
    }
    response = requests.get(baseurl + ticker, headers=headers)
    if response.status_code==200:
        html = BeautifulSoup(response.content, 'html.parser')
        if 'No results found for "'+ticker+'"' in html.text:
            print("No data found for ticker " + ticker)
            return sharesOutstanding,sharesFloat

        temp=html.find(text='Shs Outstand')
        if temp is not None:
            sharesOutstanding=temp.parent.nextSibling.text
        temp = html.find(text='Shs Float')
        if temp is not None:
            sharesFloat = temp.parent.nextSibling.text
        return sharesOutstanding,sharesFloat
    else:
        print("Could not retrieve data for ticker " + ticker + " response " + str(response.status_code))
        return sharesOutstanding, sharesFloat

if __name__ == "__main__":
    populateHoldingForAllTickers()