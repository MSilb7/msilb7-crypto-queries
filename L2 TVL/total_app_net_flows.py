#!/usr/bin/env python
# coding: utf-8

# In[50]:


import pandas as pd
import requests as r
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
import numpy as np
import time
import os
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0'}


# In[51]:


#https://stackoverflow.com/questions/23267409/how-to-implement-retry-mechanism-into-python-requests-library

import logging
import requests

from requests.adapters import HTTPAdapter, Retry

logging.basicConfig(level=logging.DEBUG)

s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
s.mount('http://', HTTPAdapter(max_retries=retries))

pwd = os.getcwd()
if 'L2 TVL' in pwd:
    prepend = ''
else:
    prepend = 'L2 TVL/'


# In[52]:


trailing_num_days = 7


# In[53]:


#get all apps > 10 m tvl
all_api = 'https://api.llama.fi/protocols'
res = pd.DataFrame( r.get(all_api, headers=header).json() )
res = res[res['tvl'] > 10_000_000] ##greater than 10mil
print(len(res))
# print(res.columns)


# In[54]:



protocols = res[['slug','chainTvls']]
re = res['chainTvls']
# r[1].keys()
protocols['chainTvls'] = protocols['chainTvls'].apply(lambda x: list(x.keys()) )
protocols


# In[55]:


# protocols = protocols[ protocols['slug'] == 'uniswap-v3' ]
# api_str = 'https://api.llama.fi/protocol/'
# ad = pd.DataFrame( r.get(api_str).json()['chainTvls'] ).T[['tokens']]
# ad


# In[56]:


# api_str = 'https://api.llama.fi/protocol/uniswap'
# prot_req = r.get(api_str, headers=header).json()['chainTvls']
# # prot_req['Ethereum']
# prot_req


# In[57]:


#get by app
api_str = 'https://api.llama.fi/protocol/'
# print(protocols)
prod = []
for index,proto in protocols.iterrows():
#     print(proto)
    prot = proto['slug']
    chains = proto['chainTvls']
    apic = api_str + prot
#     time.sleep(0.1)
    try:
        prot_req = s.get(apic, headers=header).json()['chainTvls']
#         print(apic)
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
                ad['date'] = ad['date'] - timedelta(days=1) #change to eod vs sod
                prod.append(ad)
    except:
        print('err')


# In[58]:


df_df_all = pd.concat(prod)
# df_df_all
print("done api")


# In[59]:


# df_df[df_df['protocol']=='perpetual-protocol']
#filter down a bit so we can do trailing comp w/o doing every row
df_df = df_df_all[df_df_all['date'].dt.date >= date.today()-timedelta(days=trailing_num_days +2) ]
# display(df_df['date'].drop_duplicates())
#trailing comp
df_df['last_token_value'] = df_df.groupby(['token','protocol','chain'])['token_value'].shift(1)
#now actually filter
df_df = df_df[df_df['date'].dt.date >= date.today()-timedelta(days=trailing_num_days +1) ]
# display(df_df['date'].drop_duplicates())
# df_df[(df_df['protocol'] == 'uniswap') & (df_df['token'] != 'USDT') & (df_df['token_value'] > 0)]


# In[ ]:





# In[60]:


# # DISTINCT TOKENS

# token_list = df_df.groupby(['token']).count()
# token_list = token_list[token_list['token_value']>=1000]
# token_list.sort_values(by='token_value',inplace=True, ascending=False)
# token_list.reset_index(inplace=True)
# missing_token_list = token_list[~token_list['token'].isin(cg_token_list)]
# missing_token_list
# missing_token_list


# In[61]:


# data_df = df_df.merge(cg_df, on=['date','token'],how='inner')
data_df = df_df.copy()
data_df['token_value'] = data_df['token_value'].replace(0, np.nan)
data_df['price_usd'] = data_df['usd_value']/data_df['token_value']

# print(len(data_df))
data_df.sort_values(by='date',inplace=True)
# print(len(data_df))
# data_df['net_token_flow'] = data_df.groupby(['token','protocol','chain'])['token_value'].apply(lambda x: x- x.shift(1))
data_df['net_token_flow'] = data_df['token_value'] - data_df['last_token_value']

# print(len(data_df))
data_df['net_dollar_flow'] = data_df['net_token_flow'] * data_df['price_usd']


# data_df = data_df[data_df['token'] != 'RVRS'] #Harmony error
# data_df = data_df[data_df['token'] != 'usn'] #Harmony error
# data_df = data_df[data_df['protocol'] != 'shibaswap'] #Harmony error
data_df = data_df[abs(data_df['net_dollar_flow']) < 50_000_000_000] #50 bil error bar
data_df = data_df[~data_df['net_dollar_flow'].isna()] #50 bil error bar
# display(data_df[
# #         (data_df['protocol'] == 'sushiswap') & (data_df['chain'] == 'Harmony')
#         (data_df['net_dollar_flow'].notna()) & (data_df['net_dollar_flow'] != 0)
#         ].sort_values('net_dollar_flow'))


# data_df.sort_values('net_dollar_flow')


# In[62]:


# data_df[['date']].drop_duplicates().sort_values('date')
# data_df[data_df['date'].dt.date != data_df['date']].sort_values('date')


# In[63]:


# data_df[data_df['protocol']=='perpetual-protocol'].sort_values(by='date')

# segment_cols = ['date','protocol','chain','token']


# In[64]:


netdf_df = data_df[['date','protocol','chain','net_dollar_flow','usd_value']]
netdf_df = netdf_df.fillna(0)
netdf_df = netdf_df.groupby(['date','protocol','chain']).sum(['net_dollar_flow','usd_value']) ##agg by app
# display(netdf_df)
#usd_value is the TVL on a given day
netdf_df = netdf_df.groupby(['date','protocol','chain','usd_value']).sum(['net_dollar_flow'])
netdf_df.reset_index(inplace=True)
# display(netdf_df)
netdf_df['cumul_net_dollar_flow'] = netdf_df[['protocol','chain','net_dollar_flow']]                                    .groupby(['protocol','chain']).cumsum()
netdf_df.reset_index(inplace=True)
netdf_df.drop(columns=['index'],inplace=True)


# In[65]:


#get latest
netdf_df['rank_desc'] = netdf_df.groupby(['protocol', 'chain'])['date'].                            rank(method='dense',ascending=False).astype(int)
# netdf_df[netdf_df['chain'] == 'Klaytn']


# In[ ]:





# In[71]:


summary_df = netdf_df[  ( netdf_df['rank_desc'] == 1 ) &                        (~netdf_df['chain'].str.contains('-borrowed')) &                        (~netdf_df['chain'].str.contains('-staking'))  
                        (~netdf_df['chain'].str.contains('-pool2')) 
                        (~netdf_df['chain'] == 'treasury')
                        ]
summary_df = summary_df.sort_values(by='cumul_net_dollar_flow',ascending=False)
summary_df['pct_of_tvl'] = 100* summary_df['net_dollar_flow'] / summary_df['usd_value']
summary_df['flow_direction'] = np.where(summary_df['cumul_net_dollar_flow']>=0,1,-1)
summary_df['abs_cumul_net_dollar_flow'] = abs(summary_df['cumul_net_dollar_flow'])
# display(summary_df)
fig = px.treemap(summary_df[summary_df['abs_cumul_net_dollar_flow'] !=0],                  path=[px.Constant("all"), 'chain', 'protocol'], #                  path=[px.Constant("all"), 'token', 'chain', 'protocol'], \
                 values='abs_cumul_net_dollar_flow', color='flow_direction'
#                 ,color_discrete_map={'-1':'red', '1':'green'})
                ,color_continuous_scale='Spectral'
                )
# fig.data[0].textinfo = 'label+text+value'
fig.update_traces(root_color="lightgrey")
fig.update_layout(margin = dict(t=50, l=25, r=25, b=25))
# fig.update_layout(tickprefix = '$')

# fig.show()
fig.write_image(prepend + "img_outputs/svg/net_app_flows.svg") #prepend + 
fig.write_image(prepend + "img_outputs/png/net_app_flows.png") #prepend + 
fig.write_html(prepend + "img_outputs/net_app_flows.html", include_plotlyjs='cdn')


# In[67]:


# netdf_df_plot = netdf_df[netdf_df['chain']=='Ethereum']

# fig = px.line(netdf_df_plot, x="date", y="net_dollar_flow", color="protocol", \
#              title="Daily Net Dollar Flow since Program Announcement",\
#             labels={
#                      "date": "Day",
#                      "net_dollar_flow": "Net Dollar Flow (N$F)"
#                  }
#             )
# fig.update_layout(
#     legend_title="App Name"
# )
# fig.update_layout(yaxis_tickprefix = '$')
# fig.show()

# # cumul_fig = px.area(netdf_df, x="date", y="cumul_net_dollar_flow", color="protocol", \
# #              title="Cumulative Dollar Flow since Program Announcement",\
# #                    labels={
# #                      "date": "Day",
# #                      "cumul_net_dollar_flow": "Cumulative Net Dollar Flow (N$F)"
# #                  }
# #             ,areamode='group')
# # cumul_fig.update_layout(yaxis_tickprefix = '$')
# # cumul_fig.show()


# cumul_fig = go.Figure()
# proto_names = netdf_df_plot['protocol'].drop_duplicates()
# print(proto_names)
# for p in proto_names:
#     cumul_fig.add_trace(go.Scatter(x=netdf_df_plot[netdf_df_plot['protocol'] == p]['date'] \
#                                    , y=netdf_df_plot[netdf_df_plot['protocol'] == p]['cumul_net_dollar_flow'] \
#                                     ,name = p\
#                                   ,fill='tozeroy')) # fill down to xaxis

# cumul_fig.update_layout(yaxis_tickprefix = '$')
# cumul_fig.update_layout(
#     title="Cumulative Dollar Flow since Program Announcement",
#     xaxis_title="Day",
#     yaxis_title="Cumulative Net Dollar Flow (N$F)",
#     legend_title="App Name",
# #     color_discrete_map=px.colors.qualitative.G10
# )
# cumul_fig.show()


# In[75]:


# ! jupyter nbconvert --to python total_app_net_flows.ipynb

