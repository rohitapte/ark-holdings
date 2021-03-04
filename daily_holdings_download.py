import os
import requests
from mysql.connector import Error
from pathlib import Path
import csv
import datetime
from globals import cxn

def get_etf_list():
    data={}
    try:
        select_query='SELECT id, ticker, description, cusip, isin, url, csv_url from etf'
        with cxn.cursor() as cursor:
            cursor.execute(select_query)
            for row in cursor.fetchall():
                row_data={
                    'id': row[0],
                    'ticker': row[1],
                    'description': row[2],
                    'cusip': row[3],
                    'isin': row[4],
                    'url': row[5],
                    'csv_url': row[6],
                }
                data[row_data['ticker']]=row_data
    except Error as e:
        print(e)
    return data

def delete_data_for_etf_and_date(etfname, asofDate):
    try:
        delete_query = "delete from holdings WHERE etfname='" + etfname + "' and asof_date='" + asofDate + "'"
        with cxn.cursor() as cursor:
            cursor.execute(delete_query)
            cxn.commit()
    except Error as e:
        print(e)

def insert_daily_data_for_for_etf(data):
    try:
        insert_query = """
        INSERT INTO holdings
        (etfname, asof_date, company, ticker, cusip, shares, market_value, weight)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        with cxn.cursor() as cursor:
            cursor.executemany(insert_query, data)
            cxn.commit()
    except Error as e:
        print(e)

def download_daily_holdings():
    data = get_etf_list()
    for key, value in data.items():
        response = requests.get(value['csv_url'])
        asofDate = list(csv.reader(response.text.split('\n'), delimiter=','))[1][0]
        formatted_date = datetime.datetime.strptime(asofDate, '%m/%d/%Y').strftime('%Y-%m-%d')
        filename = Path('holdings_data/' + value['csv_url'][value['csv_url'].rfind('/') + 1:].replace('.csv',
                                                                                                      '_' + formatted_date + '.csv'))
        filename.write_bytes(response.content)
        filename = Path('temp/' + value['csv_url'][value['csv_url'].rfind('/') + 1:])
        filename.write_bytes(response.content)


def upload_etf_data():
    etf_data = get_etf_list()
    for root, dirs, files in os.walk("temp/"):
        for filename in files:
            with open(os.path.join(root,filename), 'r') as file:
                reader = list(csv.reader(file))
                data=[]
                sDate=reader[1][0]
                asofDate=datetime.datetime.strptime(sDate, '%m/%d/%Y').strftime('%Y-%m-%d')
                etfname=reader[1][1]
                delete_data_for_etf_and_date(etfname,asofDate)
                for row in reader[1:]:
                    if row[0]!='':
                        assert(row[0]==sDate)
                        assert(etfname in etf_data)
                        data.append((etfname,datetime.datetime.strptime(row[0], '%m/%d/%Y'),row[2],row[3],row[4],row[5],row[6],row[7]))
                    else:
                        break
                insert_daily_data_for_for_etf(data)


if __name__ == "__main__":
    download_daily_holdings()
    upload_etf_data()
