#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests as r
import plotly.express as px
import plotly.graph_objects as go
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
retries = Retry(total=10, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
s.mount('http://', HTTPAdapter(max_retries=retries))

pwd = os.getcwd()
if 'L2 TVL' in pwd:
    prepend = ''
else:
    prepend = 'L2 TVL/'


# In[ ]:


trailing_num_days = 90

start_date = date.today()-timedelta(days=trailing_num_days +1)

# start_date = datetime.strptime('2022-07-13', '%Y-%m-%d').date()


# In[ ]:


#get all apps > 10 m tvl
min_tvl = 10_000_000
all_api = 'https://api.llama.fi/protocols'
res = pd.DataFrame( r.get(all_api, headers=header).json() )
res = res[res['tvl'] > min_tvl] ##greater than 10mil
# print(len(res))
# print(res.columns)
# display(res)


# In[ ]:



protocols = res[['slug','chainTvls']]
# print(protocols)
re = res['chainTvls']
# r[1].keys()
protocols['chainTvls'] = protocols['chainTvls'].apply(lambda x: list(x.keys()) )
# protocols[protocols['chainTvls'].map(set(['Arbitrum']).issubset)]


# In[ ]:


# protocols = protocols[ protocols['slug'] == 'uniswap-v3' ]
# api_str = 'https://api.llama.fi/protocol/'
# ad = pd.DataFrame( r.get(api_str).json()['chainTvls'] ).T[['tokens']]
# ad


# In[ ]:


# api_str = 'https://api.llama.fi/protocol/uniswap'
# prot_req = r.get(api_str, headers=header).json()['chainTvls']
# # prot_req['Ethereum']
# prot_req


# In[ ]:


statuses = {x for x in range(100, 600)}
statuses.remove(200)
statuses.remove(429)


# In[ ]:


async def get_tvl(apistring, header, statuses, chains, prot):
        prod = []
        retry_client = RetryClient()
        async with retry_client.get(apistring, retry_options=ExponentialRetry(attempts=10), raise_for_status=statuses) as response:
                try:
                        prot_req = await response.json()
                        prot_req = prot_req['chainTvls']
                        for ch in chains:
                                ad = pd.json_normalize( prot_req[ch]['tokens'] )
                                ad_usd = pd.json_normalize( prot_req[ch]['tokensInUsd'] )
                        #             ad = ad.merge(how='left')
                                if not ad.empty:
                                        ad = pd.melt(ad,id_vars = ['date'])
                                        ad = ad.rename(columns={'variable':'token','value':'token_value'})
                                        ad_usd = pd.melt(ad_usd,id_vars = ['date'])
                                        ad_usd = ad_usd.rename(columns={'variable':'token','value':'usd_value'})
                                        ad = ad.merge(ad_usd,on=['date','token'])
                                        
                                        ad['date'] = pd.to_datetime(ad['date'], unit ='s') #convert to days
                                        ad['token'] = ad['token'].str.replace('tokens.','', regex=False)
                                        ad['protocol'] = prot
                                        ad['chain'] = ch
                                #         ad['start_date'] = pd.to_datetime(prot[1])
                                        # ad['date'] = ad['date'] - timedelta(days=1) #change to eod vs sod
                                        prod.append(ad)
                except Exception as e:
                        raise Exception("Could not convert json")
        await retry_client.close()
        
        return prod


# In[ ]:


def get_range(protocols):
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
        #get by app
        api_str = 'https://api.llama.fi/protocol/'
        # print(protocols)
        prod = []
        tasks = []
        for index,proto in protocols.iterrows():
                #     print(proto)
                prot = proto['slug']
                chains = proto['chainTvls']
                apic = api_str + prot
                #     time.sleep(0.1)
                tasks.append( get_tvl(apic, header, statuses, chains, prot) )
        # print(tasks)
        data_dfs = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        # print(date_range)
        # loop.close()
        # print(data_dfs)
        # fee_df = pd.concat(data_dfs)
        # return fee_df
        return data_dfs


# In[ ]:


df_df = get_range(protocols)
# print (typeof(df_df_all) )


# In[ ]:


df_list = []
for dat in df_df:
        if isinstance(dat,list):
                # print(dat)
                for pt in dat: #each list within the list (i.e. multiple chains)
                        try:
                                tempdf = pd.DataFrame(pt)
                                if not tempdf.empty:
                                        # print(tempdf)
                                        df_list.append(tempdf)
                        except:
                                continue
# df_df_all = pd.DataFrame()
df_df_all = pd.concat(df_list)


# In[ ]:


# df_df_all


# In[ ]:



# df_df_all = pd.concat(df_df_all)
# print(df_df_all[2])
print("done api")


# In[ ]:


#filter down a bit so we can do trailing comp w/o doing every row
df_df = df_df_all[df_df_all['date'].dt.date >= start_date-timedelta(days=1) ]

#trailing comp
df_df['last_token_value'] = df_df.groupby(['token','protocol','chain'])['token_value'].shift(1)
#now actually filter
df_df = df_df[df_df['date'].dt.date >= start_date ]


# In[ ]:



data_df = df_df.copy()
data_df['token_value'] = data_df['token_value'].replace(0, np.nan)
# price = usd value / num tokens
data_df['price_usd'] = data_df['usd_value']/data_df['token_value']

data_df.sort_values(by='date',inplace=True)

# net token change
data_df['net_token_flow'] = data_df['token_value'] - data_df['last_token_value']
# net token change * current price
data_df['net_dollar_flow'] = data_df['net_token_flow'] * data_df['price_usd']


data_df = data_df[abs(data_df['net_dollar_flow']) < 50_000_000_000] #50 bil error bar for bad prices
data_df = data_df[~data_df['net_dollar_flow'].isna()]


# In[ ]:


netdf_df = data_df[['date','protocol','chain','net_dollar_flow','usd_value']]
netdf_df = netdf_df.fillna(0)
netdf_df = netdf_df.groupby(['date','protocol','chain']).sum(['net_dollar_flow','usd_value']) ##agg by app

#usd_value is the TVL on a given day
netdf_df = netdf_df.groupby(['date','protocol','chain','usd_value']).sum(['net_dollar_flow'])
netdf_df.reset_index(inplace=True)

netdf_df['cumul_net_dollar_flow'] = netdf_df[['protocol','chain','net_dollar_flow']]                                    .groupby(['protocol','chain']).cumsum()
netdf_df.reset_index(inplace=True)
netdf_df.drop(columns=['index'],inplace=True)


# In[ ]:


#get latest
netdf_df['rank_desc'] = netdf_df.groupby(['protocol', 'chain'])['date'].                            rank(method='dense',ascending=False).astype(int)


# In[ ]:


summary_df = netdf_df[  ( netdf_df['rank_desc'] == 1 ) &                        (~netdf_df['chain'].str.contains('-borrowed')) &                        (~netdf_df['chain'].str.contains('-staking')) &                        (~netdf_df['chain'].str.contains('-pool2')) &                            (~netdf_df['chain'].str.contains('-treasury')) &                        (~( netdf_df['chain'] == 'treasury') ) &                        (~( netdf_df['chain'] == 'borrowed') ) &                        (~( netdf_df['chain'] == 'staking') ) &                            (~( netdf_df['chain'] == 'treasury') ) &                        (~( netdf_df['chain'] == 'pool2') ) &                        (~( netdf_df['protocol'] == 'polygon-bridge-&-staking') ) 
#                         & (~( netdf_df['chain'] == 'Ethereum') )
                        ]
summary_df = summary_df.sort_values(by='cumul_net_dollar_flow',ascending=False)
summary_df['pct_of_tvl'] = 100* summary_df['net_dollar_flow'] / summary_df['usd_value']
summary_df['flow_direction'] = np.where(summary_df['cumul_net_dollar_flow']>=0,1,-1)
summary_df['abs_cumul_net_dollar_flow'] = abs(summary_df['cumul_net_dollar_flow'])

print(summary_df[summary_df['chain']=='Arbitrum'])
print(summary_df[summary_df['chain']=='Optimism'])
# display(summary_df)
fig = px.treemap(summary_df[summary_df['abs_cumul_net_dollar_flow'] !=0],                  path=[px.Constant("all"), 'chain', 'protocol'], #                  path=[px.Constant("all"), 'token', 'chain', 'protocol'], \
                 values='abs_cumul_net_dollar_flow', color='flow_direction'
#                 ,color_discrete_map={'-1':'red', '1':'green'})
                ,color_continuous_scale='Spectral'
                     , title = "App Net Flows Change by App -> Chain - Last " + str(trailing_num_days) + \
                            " Days - (Apps with > $" + str(min_tvl/1e6) + "M TVL Shown)"
                )
# fig.data[0].textinfo = 'label+text+value'
fig.update_traces(root_color="lightgrey")
fig.update_layout(margin = dict(t=50, l=25, r=25, b=25))
# fig.update_layout(tickprefix = '$')

fig.show()
fig_app = px.treemap(summary_df[summary_df['abs_cumul_net_dollar_flow'] !=0],                 #  path=[px.Constant("all"), 'chain', 'protocol'], \
#                  path=[px.Constant("all"), 'token', 'chain', 'protocol'], \
                        path=[px.Constant("all"), 'protocol','chain'], \
                 values='abs_cumul_net_dollar_flow', color='flow_direction'
#                 ,color_discrete_map={'-1':'red', '1':'green'})
                ,color_continuous_scale='Spectral'
                , title = "App Net Flows Change by Chain -> App - Last " + str(trailing_num_days) + \
                            " Days - (Apps with > $" + str(min_tvl/1e6) + "M TVL Shown)"
                )
# fig.data[0].textinfo = 'label+text+value'
fig_app.update_traces(root_color="lightgrey")
fig_app.update_layout(margin = dict(t=50, l=25, r=25, b=25))
fig.show()


# In[ ]:


fig.write_image(prepend + "img_outputs/svg/net_app_flows.svg") #prepend + 
fig.write_image(prepend + "img_outputs/png/net_app_flows.png") #prepend + 
fig.write_html(prepend + "img_outputs/net_app_flows.html", include_plotlyjs='cdn')

fig_app.write_image(prepend + "img_outputs/svg/net_app_flows_by_app.svg") #prepend + 
fig_app.write_image(prepend + "img_outputs/png/net_app_flows_by_app.png") #prepend + 
fig_app.write_html(prepend + "img_outputs/net_app_flows_by_app.html", include_plotlyjs='cdn')


# In[28]:


# ! jupyter nbconvert --to python total_app_net_flows_async.ipynb

