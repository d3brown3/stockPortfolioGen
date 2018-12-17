#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import requests
from io import StringIO
from datetime import datetime, timedelta
import pandas as pd


# In[2]:


# credit:
# https://stackoverflow.com/questions/44225771/scraping-historical-data-from-yahoo-finance-with-python?rq=1


# In[3]:


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
        


# In[4]:


df = YFH('AAPL', days_back=365*5).get_quote()
df['Symbol'] = 'AAPL'
cols = df.columns.tolist()
cols = cols[-1:] + cols[:-1]
df = df[cols]
df

