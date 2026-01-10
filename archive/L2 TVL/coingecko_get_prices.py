#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# ! jupyter nbconvert --to python coingecko_get_prices.ipynb


# In[ ]:


import pandas as pd
import coingecko_utils as cg
import numpy as np
import plotly.express as px

rolling_periods = [30,90,180,365]


# In[ ]:


op_price = cg.get_daily_token_data('optimism')[['date','prices','total_volumes','symbol']]
# op_price = op_price.fillna(0)
prices_cols = []
# display(op_price)
# print( op_price.dtypes )
for i in rolling_periods:
    pcol = 'prices_'+str(i)+'dma'
    vwapcol = 'prices_'+str(i)+'d_vwap'
    op_price[pcol] = op_price[['date','symbol','prices']]\
                                    .groupby(['symbol'])['prices']\
                                    .transform(lambda x: x.rolling(i, min_periods=1).mean() )
    
    vol_rolling_sum = op_price['total_volumes'].rolling(i, min_periods=1).sum()

    op_price[vwapcol] = (op_price['prices'] * op_price['total_volumes']).rolling(i, min_periods=1).sum() / vol_rolling_sum
    
    prices_cols.append(pcol)
    prices_cols.append(vwapcol)

op_price = op_price.sort_values(by='date',ascending=False)
op_price.to_csv('csv_outputs/op_market_price_avgs.csv')
# display(op_price)
print(op_price)


# In[ ]:


fig = px.line(op_price, x="date", y=prices_cols, title='OP Price - Moving Averages (Source: Coingecko)')
fig.show()

