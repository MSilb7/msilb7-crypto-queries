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

protocols = [
    # name, incentive start date
         ['velodrome','2022-07-13']
        ,['pooltogether','2022-07-14']
        ,['lyra','2022-08-02']
        ,['rubicon','2022-07-15']
        ,['perpetual-protocol','2022-07-14']
        ,['thales','2022-07-14'] #TVL not relevant
        ,['aave-v3','2022-08-04']
        ,['wepiggy','2022-08-03']
        ,['stargate','2022-08-05']
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
df_df


# In[ ]:


# df_df[df_df['protocol']=='perpetual-protocol']


# In[ ]:


# #defillama api feedback - only token symbols come through, makes it hard to map w/o doing it manually
# coingecko_token_map = [
#     ['LINK','chainlink']
#     ,['USDT','tether']
#     ,['USDC','usd-coin']
#     ,['WETH','weth']
#     ,['SUSD','nusd']
#     ,['DAI','dai']
#     ,['AAVE','aave']
#     ,['WBTC','wrapped-bitcoin']
#     ,['SNX','havven']
#     ,['OP','optimism']
#     ,['SETH','seth']
#     ,['FXS','frax-share']
#     ,['THALES','thales']
#     ,['FRAX','frax']
#     ,['ALUSD','alchemix-usd']
#     ,['PERP','perpetual-protocol']
#     ,['LUSD','liquity-usd']
#     ,['LYRA','lyra-finance']
#     ,['HND','hundred-finance']
#     ,['BITANT','bitant']
#     ,['UNI','uniswap']
#     ,['SLINK','slink']
#     ,['VELO','velodrome-finance']
#     ,['DOLA','dola-usd']
#     ,['CRV','curve-dao-token']
#     ,['SBTC','sbtc']
#     ,['KROM','kromatika']
#     ,['DF','dforce-token']
#     ,['STG','stargate-finance']
#     ,['AELIN','aelin']
#     ,['RAI','rai']
#     ,['RETH','rocket-pool-eth']
# ]


# In[ ]:


# cg_token_list = [i[0] for i in coingecko_token_map]
# # print(cg_token_list)


# In[ ]:


# # DISTINCT TOKENS

# token_list = df_df[['token']].drop_duplicates()
# missing_token_list = token_list[~token_list['token'].isin(cg_token_list)]
# missing_token_list


# In[ ]:



# cg_pds = []
# for t in coingecko_token_map:
#     time.sleep(0.1)
#     cg_api = 'https://api.coingecko.com/api/v3/coins/'\
#             + t[1] + '/market_chart?vs_currency=usd&days=max&interval=daily'
#     print(cg_api)
#     pr = pd.DataFrame( r.get(cg_api).json()['prices'] )
#     pr['token'] = t[0]
#     pr['cg_slug'] = t[1]
#     pr = pr.rename(columns={0:'date',1:'price_usd'})
#     pr['date'] = pd.to_datetime(pr['date'], unit ='ms') #convert to days
#     cg_pds.append(pr)

# cg_df = pd.concat(cg_pds)
# # cg_df


# In[ ]:


data_df = df_df.copy()#merge(cg_df, on=['date','token'],how='inner')
data_df.sort_values(by='date',inplace=True)
data_df['token_value'] = data_df['token_value'].replace(0, np.nan)
data_df['price_usd'] = data_df['usd_value']/data_df['token_value']

data_df.sort_values(by='date',inplace=True)

data_df['last_token_value'] = data_df.groupby(['token','protocol'])['token_value'].shift(1)
data_df['last_token_value'] = data_df['last_token_value'].fillna(0)

data_df['net_token_flow'] = data_df['token_value'] - data_df['last_token_value']

data_df['net_dollar_flow'] = data_df['net_token_flow'] * data_df['price_usd']


# display(data_df)


# In[ ]:


data_df[data_df['protocol']=='perpetual-protocol'].sort_values(by='date')


# In[ ]:


netdf_df = data_df[data_df['date']>= data_df['start_date']][['date','protocol','net_dollar_flow']]
netdf_df = netdf_df.groupby(['date','protocol']).sum(['net_dollar_flow'])
netdf_df['cumul_net_dollar_flow'] = netdf_df.groupby(['protocol']).cumsum()
netdf_df.reset_index(inplace=True)

netdf_df


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


# In[ ]:


# fig.show()
cumul_fig.show()
print("yay")


# In[ ]:


# ! jupyter nbconvert --to python optimism_app_net_flows.ipynb

