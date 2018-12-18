#!/usr/bin/env python
# coding: utf-8

# In[5]:


import re
import requests
from io import StringIO
from datetime import datetime, timedelta
import pandas as pd
import urllib
import gzip


# In[6]:


# credit:
# https://stackoverflow.com/questions/44225771/scraping-historical-data-from-yahoo-finance-with-python?rq=1


# In[7]:


class YFH: #YFH = YahooFinanceHistory
    timeout = 2
    crumb_link = 'https://finance.yahoo.com/quote/{0}/history?p={0}'
    crumble_regex = r'CrumbStore":{"crumb":"(.*?)"}'
    quote_link = 'https://query1.finance.yahoo.com/v7/finance/download/{quote}?period1={dfrom}&period2={dto}&interval=1d&events=history&crumb={crumb}'
    
    def __init__(self, symbol, days_back=7):
        self.symbol = symbol
        self.session = requests.Session()
        self.dt = timedelta(days=days_back)
        
    def get_crumb(self):
        response = self.session.get(self.crumb_link.format(self.symbol), timeout=self.timeout)
        response.raise_for_status()
        match = re.search(self.crumble_regex, response.text)
        if not match:
            raise ValueError('Could not get crumb from Yahoo Finance')
        else: 
            self.crumb = match.group(1)
        
    def get_quote(self):
        if not hasattr(self, 'crumb') or len(self.session.cookies) == 0:
            self.get_crumb()
        now = datetime.utcnow()
        dateto = int(now.timestamp())
        datefrom = int((now - self.dt).timestamp())
        url = self.quote_link.format(quote=self.symbol, dfrom=datefrom, dto=dateto, crumb=self.crumb)
        response = self.session.get(url)
        response.raise_for_status()
        return pd.read_csv(StringIO(response.text), parse_dates=['Date'])
        


# In[8]:


# df = YFH('AAPL', days_back=30).get_quote()
# df['Symbol'] = 'AAPL'
# cols = df.columns.tolist()
# cols = cols[-1:] + cols[:-1]
# df = df[cols]
# df


# In[9]:


import csv
nasdaq_url = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
nyse_url = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download'
amex_url = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download'

nasdaq_csv = urllib.request.urlopen(nasdaq_url)
nyse_csv = urllib.request.urlopen(nyse_url)
amex_csv = urllib.request.urlopen(amex_url)

nasdaq = pd.read_csv(nasdaq_csv)
nyse = pd.read_csv(nyse_csv)
amex = pd.read_csv(amex_csv)

nasdaq['Exchange'] = 'nasdaq'
nyse['Exchange'] = 'nyse'
amex['Exchange'] = 'amex'

symbols = pd.DataFrame(columns=nyse.columns.tolist())


# In[10]:


symbols = nasdaq
# cols = symbols.columns.tolist()
# cols = cols[-1:] + cols[:-1]
# symbols = symbols[cols]

symbols.append(nyse)
symbols.append(amex)

exchanges = list(symbols['Exchange'])
symbols = list(symbols['Symbol'])

# print(symbols[0:9], exchanges[0:9])


# In[33]:


df = pd.DataFrame()

for i in symbols:
    print(i)
    
    try:
        df_i = YFH(i, days_back=365*10).get_quote()
        df_i['Symbol'] = i
        df = df.append(df_i)
        print(i)
    except: ## skipping when there is a cookie / crumb issue with yahoo finance...
        pass

cols = df.columns.tolist()
cols = cols[-1:] + cols[:-1]
df = df[cols]


# In[34]:


df.to_csv("/home/david/stockPortfolioGen/data/stocks.csv.gz", compression='gzip')


# In[35]:


df = pd.read_csv('/home/david/stockPortfolioGen/data/stocks.csv.gz', compression='gzip')
# df

