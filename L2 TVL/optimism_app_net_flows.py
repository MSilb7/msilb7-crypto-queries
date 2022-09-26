#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# ! pip install pandas
# ! pip install requests
# ! pip install plotly
# ! pip install datetime
# ! pip install os
# ! pip freeze = requirements.txt


# In[ ]:


import pandas as pd
import requests as r
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
import os


# In[ ]:


pwd = os.getcwd()
if 'L2 TVL' in pwd:
    prepend = ''
else:
    prepend = 'L2 TVL/'


# In[ ]:



api_str = 'https://api.llama.fi/protocol/'

# Protocol Incentive Start Dates
# NOTE: This should be when the in-app incentives began, not any external incentives (i.e. DEX pools)
protocols = [
    # name, incentive start date
         ['velodrome',          '2022-07-13']
        ,['pooltogether',       '2022-07-14']
        ,['lyra',               '2022-08-02']
        ,['rubicon',            '2022-07-15']
        ,['perpetual-protocol', '2022-07-14']
        ,['thales',             '2022-07-14'] #TVL not relevant
        ,['aave-v3',            '2022-08-04']
        ,['wepiggy',            '2022-08-03']
        ,['stargate',           '2022-08-05']
        ,['pika-protocol',      '2022-08-29']
        ,['synthetix',          '2022-08-25'] #This is when Curve incentives started, so not really 1:1
        ,['pickle',             '2022-09-09']
        ,['aelin',              '2022-09-12']
        ,['polynomial-protocol','2022-09-14']
        ,['xtoken',             '2022-09-19']
        ,['hop-protocol',       '2022-09-22']
        ]
# print(protocols[0])
prod = []
for prot in protocols:
    tp = r.get(api_str + prot[0]).json()['chainTvls']['Optimism']
    ad = pd.json_normalize( tp['tokens'] )
    ad_usd = pd.json_normalize( tp['tokensInUsd'] )
    if not ad.empty:
        ad = pd.melt(ad,id_vars = ['date'])
        ad = ad.rename(columns={'variable':'token','value':'token_value'})
        ad_usd = pd.melt(ad_usd,id_vars = ['date'])
        ad_usd = ad_usd.rename(columns={'variable':'token','value':'usd_value'})
        ad = ad.merge(ad_usd,on=['date','token'])
        
        ad['date'] = pd.to_datetime(ad['date'], unit ='s') #convert to days

        ad['token'] = ad['token'].str.replace('tokens.','', regex=False)
        ad['protocol'] = prot[0]
        ad['start_date'] = pd.to_datetime(prot[1])
        # ad['date'] = ad['date'] - timedelta(days=1) #change to eod vs sod
        prod.append(ad)

df_df = pd.concat(prod)


# In[ ]:


# df_df


# In[ ]:


data_df = df_df.copy()#merge(cg_df, on=['date','token'],how='inner')
data_df.sort_values(by='date',inplace=True)
data_df['token_value'] = data_df['token_value'].replace(0, np.nan)
data_df['price_usd'] = data_df['usd_value']/data_df['token_value']

data_df.sort_values(by='date',inplace=True)

data_df['last_token_value'] = data_df.groupby(['token','protocol'])['token_value'].shift(1)
data_df['last_price_usd'] = data_df.groupby(['token','protocol'])['price_usd'].shift(1)
data_df['last_token_value'] = data_df['last_token_value'].fillna(0)

data_df['net_token_flow'] = data_df['token_value'] - data_df['last_token_value']
data_df['net_price_change'] = data_df['price_usd'] - data_df['last_price_usd']

data_df['net_dollar_flow'] = data_df['net_token_flow'] * data_df['price_usd']

data_df['net_price_stock_change'] = data_df['last_token_value'] * data_df['net_price_change']


# display(data_df)


# In[ ]:


# data_df[data_df['protocol']=='perpetual-protocol'].sort_values(by='date')
data_df.head()


# In[ ]:


netdf_df = data_df[data_df['date']>= data_df['start_date']][['date','protocol','net_dollar_flow','net_price_stock_change','usd_value']]


netdf_df = netdf_df.groupby(['date','protocol']).sum(['net_dollar_flow','net_price_stock_change','usd_value'])


netdf_df['tvl_change'] = netdf_df['usd_value'] - netdf_df.groupby(['protocol'])['usd_value'].shift(1)
netdf_df['error'] = netdf_df['tvl_change'] - (netdf_df['net_dollar_flow'] + netdf_df['net_price_stock_change'])

netdf_df['cumul_net_dollar_flow'] = netdf_df['net_dollar_flow'].groupby(['protocol']).cumsum()
netdf_df['cumul_net_price_stock_change'] = netdf_df['net_price_stock_change'].groupby(['protocol']).cumsum()
netdf_df.reset_index(inplace=True)


# In[ ]:


netdf_df[netdf_df['protocol'] == 'velodrome'].head()


# In[ ]:


fig = px.line(netdf_df, x="date", y="net_dollar_flow", color="protocol",              title="Daily Net Dollar Flow since Program Announcement",            labels={
                     "date": "Day",
                     "net_dollar_flow": "Net Dollar Flow (N$F)"
                 }
            )
fig.update_layout(
    legend_title="App Name"
)
fig.update_layout(yaxis_tickprefix = '$')
fig.write_image(prepend + "img_outputs/svg/daily_ndf.svg")
fig.write_image(prepend + "img_outputs/png/daily_ndf.png")
fig.write_html(prepend + "img_outputs/daily_ndf.html", include_plotlyjs='cdn')

# cumul_fig = px.area(netdf_df, x="date", y="cumul_net_dollar_flow", color="protocol", \
#              title="Cumulative Dollar Flow since Program Announcement",\
#                    labels={
#                      "date": "Day",
#                      "cumul_net_dollar_flow": "Cumulative Net Dollar Flow (N$F)"
#                  }
#             ,areamode='group')
# cumul_fig.update_layout(yaxis_tickprefix = '$')
# cumul_fig.show()


cumul_fig = go.Figure()
proto_names = netdf_df['protocol'].drop_duplicates()
print(proto_names)
for p in proto_names:
    cumul_fig.add_trace(go.Scatter(x=netdf_df[netdf_df['protocol'] == p]['date']                                    , y=netdf_df[netdf_df['protocol'] == p]['cumul_net_dollar_flow']                                     ,name = p                                  ,fill='tozeroy')) # fill down to xaxis

cumul_fig.update_layout(yaxis_tickprefix = '$')
cumul_fig.update_layout(
    title="Cumulative Net Dollar Flow since Program Announcement",
    xaxis_title="Day",
    yaxis_title="Cumulative Net Dollar Flow (N$F)",
    legend_title="App Name",
#     color_discrete_map=px.colors.qualitative.G10
)
cumul_fig.write_image(prepend + "img_outputs/svg/cumul_ndf.svg") #prepend + 
cumul_fig.write_image(prepend + "img_outputs/png/cumul_ndf.png") #prepend + 
cumul_fig.write_html(prepend + "img_outputs/cumul_ndf.html", include_plotlyjs='cdn')
# cumul_fig.show()


# In[ ]:


# fig.show()
# cumul_fig.show()
print("yay")


# In[ ]:


# ! jupyter nbconvert --to python optimism_app_net_flows.ipynb

