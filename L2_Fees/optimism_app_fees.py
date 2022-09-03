#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests as r

#handle weird kaleido error
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
pio.kaleido.scope.chromium_args = tuple([arg for arg in pio.kaleido.scope.chromium_args if arg != "--disable-dev-shm-usage"])

from datetime import datetime, timedelta, date
import numpy as np
import time
import os
import asyncio, aiohttp, nest_asyncio
from aiohttp_retry import RetryClient, ExponentialRetry
nest_asyncio.apply()
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0'}


# In[ ]:


#https://stackoverflow.com/questions/23267409/how-to-implement-retry-mechanism-into-python-requests-library

import logging
import requests

from requests.adapters import HTTPAdapter, Retry

# logging.basicConfig(level=logging.DEBUG)

s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 404, 502, 503, 504 ])
s.mount('http://', HTTPAdapter(max_retries=retries))

pwd = os.getcwd()
if 'L2_Fees' in pwd:
    prepend = ''
else:
    prepend = 'L2_Fees/'


# In[ ]:


trailing_num_days = 365#90

start_date = date.today()-timedelta(days=trailing_num_days +1)

start_date = max(start_date, date(2021,11,12))
print(start_date)
# start_date = datetime.strptime('2022-07-13', '%Y-%m-%d').date()


# In[ ]:


eth_prices = r.get(
                'https://api.coingecko.com/api/v3/coins/ethereum/market_chart?vs_currency=usd&days='
                + str(trailing_num_days)
                + '&interval=daily'
                )
ethp = pd.DataFrame(eth_prices.json()['prices'])
ethp.rename(columns={0:'date',1:'eth_price'},inplace=True)
ethp['date'] = pd.to_datetime(ethp['date'], unit ='ms')
ethp['date'] = ethp['date'].dt.strftime("%Y-%m-%d")
ethp.drop_duplicates(subset=['date'],inplace=True,keep='first')


# In[ ]:


statuses = {x for x in range(100, 600)}
statuses.remove(200)
statuses.remove(429)


# In[ ]:


async def get_cryptostats_api(api_core_string,dt_string ):
        dt_string = dt_string.strftime("%Y-%m-%d")
        # print(dt_string)
        i = 0
        res = pd.DataFrame()
        # #try 5 times
        # while i < 5:
        #         try: 
        # async with aiohttp.ClientSession() as session:
        retry_client = RetryClient()
        async with retry_client.get(api_core_string + dt_string, retry_options=ExponentialRetry(attempts=10), raise_for_status=statuses) as response:
                try:
                        prot_req = await response.json()
                        res = pd.json_normalize(prot_req['data']).reset_index()
                        res['date'] = dt_string
                except Exception as e:
                        raise Exception("Could not convert json")
        await retry_client.close()
                
                # prot_req = await retry_client.get(api_core_string + dt_string, headers=header)
                # prot_req = await prot_req.json()
        
                # except:
                #         i = i+1
                #         time.sleep(1)
                #         continue
                # break
        return res

# async def gather_with_concurrency(n, *tasks):
#     semaphore = asyncio.Semaphore(n)
#     async def sem_task(task):
#         async with semaphore:
#             return await task
        


# In[ ]:


# ALL Relevant API Strings
# https://cryptostats.community/discover/fees
# This is dependent on crypto stats, eventually maybe we mod this to pull from TheGraph, but this is simplest por ahora
# https://docs.cryptostats.community/tutorial/how-to-query-with-rest-api/making-get-requests
        #How to make a single query
        #To make a single query on a collection. Send a GET request to
        #https://api.cryptostats.community/api/v1/<collection-id>/<query>/<arg>
        
        # How to make multiple queries
        # To make multiple queries on a collection, send a GET request to
        # https://api.cryptostats.community/api/v1/<collection-id>/<query-1>,<query-2>/<query-1-arg>,<query-2-arg>

api_core = 'https://api.cryptostats.community/api/v1/fees/oneDayTotalFees/' #Can only pull everything all at once. Odd.
date_rng = pd.date_range(start=start_date, end=date.today()-timedelta(days=1))
date_rng_str = ','.join(x.strftime("%Y-%m-%d") for x in date_rng)
# print(date_rng[0].strftime("%Y-%m-%d"))


# In[ ]:


def get_range(date_range):
        data_dfs = []
        fee_df = []
        # for dt in date_range:
        #         await asyncio.gather()
        #         data_dfs.append(res_df)
        #         # res.columns
        # try:
        #         loop.close()
        # except:
        #         #nothing
        loop = asyncio.get_event_loop()
        tasks = [get_cryptostats_api(api_core,dt) for dt in date_range]
        # print(tasks)
        data_dfs = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        # print(len(data_dfs))
        data_dfs = [value for value in data_dfs if type(value) == pd.DataFrame]
        # print(len(data_dfs))
        # print(date_range)
        # loop.close()
        # print(data_dfs)
        fee_df = pd.concat(data_dfs)
        return fee_df


# In[ ]:




fdf = get_range(date_rng)
# r.get('https://api.cryptostats.community/api/v1/fees/oneDayTotalFees/2022-05-21')
# print(r)


# In[ ]:


fdf['results.oneDayTotalFees'] = fdf['results.oneDayTotalFees'].fillna(0)
# display(fdf)


# In[ ]:


data_fdf = fdf[['id','metadata.blockchain','results.oneDayTotalFees','metadata.name','metadata.category','date']]
data_fdf = data_fdf.merge(ethp,on='date',how='left')

data_fdf = data_fdf.groupby(['date','metadata.blockchain','metadata.name']).sum()
data_fdf.reset_index(inplace=True)
# Exclude the chain's fees
data_fdf = data_fdf[data_fdf['metadata.blockchain'] != data_fdf['metadata.name']]

data_fdf['oneDayTotalFees_ETH'] = data_fdf['results.oneDayTotalFees']/data_fdf['eth_price']


data_fdf


# In[ ]:


data_fdf_op = data_fdf[data_fdf['metadata.blockchain'] == 'Optimism']
data_fdf_op['chain_app_id'] = data_fdf_op['metadata.blockchain'] + '_' + data_fdf_op['metadata.name']

data_fdf_op.sort_values(by =['date','results.oneDayTotalFees','metadata.blockchain'], ascending = [True,False,False],inplace = True)
data_fdf_op.reset_index(inplace=True,drop=True)

data_fdf_op['fees_7_day_avg'] = data_fdf_op.groupby('chain_app_id')['results.oneDayTotalFees'].rolling(7,min_periods=1).mean().reset_index(0,drop=True)
data_fdf_op['fees_30_day_avg'] = data_fdf_op.groupby('chain_app_id')['results.oneDayTotalFees'].rolling(30,min_periods=1).mean().reset_index(0,drop=True)
data_fdf_op['fees_90_day_avg'] = data_fdf_op.groupby('chain_app_id')['results.oneDayTotalFees'].rolling(90,min_periods=1).mean().reset_index(0,drop=True)

data_fdf_op['fees_7_day_avg_eth'] = data_fdf_op.groupby('chain_app_id')['oneDayTotalFees_ETH'].rolling(7,min_periods=1).mean().reset_index(0,drop=True)
data_fdf_op['fees_30_day_avg_eth'] = data_fdf_op.groupby('chain_app_id')['oneDayTotalFees_ETH'].rolling(30,min_periods=1).mean().reset_index(0,drop=True)
data_fdf_op['fees_90_day_avg_eth'] = data_fdf_op.groupby('chain_app_id')['oneDayTotalFees_ETH'].rolling(90,min_periods=1).mean().reset_index(0,drop=True)


# display(data_fdf_op)


# In[ ]:


data_fdf_chain = data_fdf.groupby(['date','metadata.blockchain']).sum()
data_fdf_chain.reset_index(inplace=True)

data_fdf_chain = data_fdf_chain[data_fdf_chain['metadata.blockchain'] == 'Optimism']
data_fdf_chain.reset_index(inplace=True,drop=True)

data_fdf_chain.sort_values(by =['date','results.oneDayTotalFees'], ascending = [True,False],inplace = True)
data_fdf_chain['fees_30_day_avg'] = data_fdf_chain.groupby('metadata.blockchain')['results.oneDayTotalFees'].rolling(30,min_periods=1).mean().reset_index(0,drop=True)
data_fdf_chain['fees_90_day_avg'] = data_fdf_chain.groupby('metadata.blockchain')['results.oneDayTotalFees'].rolling(90,min_periods=1).mean().reset_index(0,drop=True)

data_fdf_chain['fees_30_day_avg_eth'] = data_fdf_chain.groupby('metadata.blockchain')['oneDayTotalFees_ETH'].rolling(30,min_periods=1).mean().reset_index(0,drop=True)
data_fdf_chain['fees_90_day_avg_eth'] = data_fdf_chain.groupby('metadata.blockchain')['oneDayTotalFees_ETH'].rolling(90,min_periods=1).mean().reset_index(0,drop=True)


#compound monthly growth rates
# https://velawoodlaw.com/glossary-term/compounded-monthly-growth-rate-cmgr/#:~:text=Compounded%20Monthly%20Growth%20Rate%20(CMGR)%20is%20a%20calculation%20that%20helps,of%20Months)%20-1%5D.
# CMGR = (Latest Month/ First Month)^(1/# of Months) -1].
data_fdf_chain['fees_1q_cmgr'] = (
                                data_fdf_chain['fees_30_day_avg']
                                / data_fdf_chain.groupby('metadata.blockchain')['fees_30_day_avg'].shift(30*3*1)
                                ) ** (1/(3*1)) - 1

data_fdf_chain['fees_3q_cmgr'] = (
                                data_fdf_chain['fees_30_day_avg']
                                / data_fdf_chain.groupby('metadata.blockchain')['fees_30_day_avg'].shift(30*3*3)
                                ) ** (1/(3*3)) - 1
data_fdf_chain['fees_1q_cmgr_eth'] = (
                                data_fdf_chain['fees_30_day_avg_eth']
                                / data_fdf_chain.groupby('metadata.blockchain')['fees_30_day_avg_eth'].shift(30*3*1)
                                ) ** (1/(3*1)) - 1

data_fdf_chain['fees_3q_cmgr_eth'] = (
                                data_fdf_chain['fees_30_day_avg_eth']
                                / data_fdf_chain.groupby('metadata.blockchain')['fees_30_day_avg_eth'].shift(30*3*3)
                                ) ** (1/(3*3)) - 1
# #if focus on op

# display(data_fdf_chain)


# In[ ]:


fig = px.line(data_fdf_op, x="date", y="results.oneDayTotalFees", color='metadata.name', title = 'Fees Earned on Optimism (USD)')
fig.update_layout(yaxis_tickprefix = '$')
# fig.show()

fig.write_image(prepend + "img_outputs/svg/app_fees_on_op.svg") #prepend + 
fig.write_image(prepend + "img_outputs/png/app_fees_on_op.png") #prepend + 
fig.write_html(prepend + "img_outputs/app_fees_on_op.html", include_plotlyjs='cdn')


# In[ ]:


fig_7d = px.line(data_fdf_op, x="date", y="fees_7_day_avg", color='metadata.name', title = 'Fees Earned on Optimism (USD) - 7 Day Moving Average')
fig_7d.update_layout(yaxis_tickprefix = '$')
# fig_7d.show()

fig_7d.write_image(prepend + "img_outputs/svg/app_fees_on_op_7dma.svg") #prepend + 
fig_7d.write_image(prepend + "img_outputs/png/app_fees_on_op_7dma.png") #prepend + 
fig_7d.write_html(prepend + "img_outputs/app_fees_on_op_7dma.html", include_plotlyjs='cdn')

fig_30d = px.line(data_fdf_op, x="date", y="fees_30_day_avg", color='metadata.name', title = 'Fees Earned on Optimism (USD) - 30 Day Moving Average')
fig_30d.update_layout(yaxis_tickprefix = '$')
# fig_30d.show()

fig_30d.write_image(prepend + "img_outputs/svg/app_fees_on_op_30dma.svg") #prepend + 
fig_30d.write_image(prepend + "img_outputs/png/app_fees_on_op_30dma.png") #prepend + 
fig_30d.write_html(prepend + "img_outputs/app_fees_on_op_30dma.html", include_plotlyjs='cdn')


# In[ ]:


fig_chain = px.line(data_fdf_chain, x="date", y="results.oneDayTotalFees", color = "metadata.blockchain", title = 'Sum Fees Earned on apps by Chain (USD)')
fig_chain.update_layout(yaxis_tickprefix = '$')
# fig_chain.show()

fig_chain.write_image(prepend + "img_outputs/svg/app_fees_by_chain.svg") #prepend + 
fig_chain.write_image(prepend + "img_outputs/png/app_fees_by_chain.png") #prepend + 
fig_chain.write_html(prepend + "img_outputs/app_fees_by_chain.html", include_plotlyjs='cdn')


# In[ ]:


fig_chain_30d = px.line(data_fdf_chain, x="date", y="results.oneDayTotalFees", color = "metadata.blockchain", title = 'Sum Fees Earned on apps by Chain (USD)')
fig_chain_30d.update_layout(yaxis_tickprefix = '$')
fig_chain_30d.show()


# In[ ]:


# ! jupyter nbconvert --to python optimism_app_fees.ipynb

