# streamlit run "L2 TVL/img_outputs/dashboards/op_summer.py"
import streamlit as st
import pandas as pd
import plotly.express as px
import seaborn as sns
import os

st.set_page_config(
    page_title="OP Summer - Latest Stats",
    page_icon="✅",
    layout="wide",
)

# load latest file
pwd = os.getcwd()
print(pwd)
if 'L2 TVL/img_outputs' in pwd:
    prepend = ''
else:
    prepend = 'L2 TVL/img_outputs'
os.chdir(prepend + '/dashboards')
# pwd2 = os.getcwd()
# print(pwd2)
df = pd.read_csv('../app/op_summer_latest_stats.csv')

st.table(df)
# print('weee')