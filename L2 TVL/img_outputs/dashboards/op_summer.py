import streamlit as st
import pandas as pd
import plotly.express as px
import seaborn as sns
import os

pwd = os.getcwd()
# print(pwd)
if 'L2 TVL/img_outputs' in pwd:
    prepend = ''
else:
    prepend = 'L2 TVL/img_outputs'
os.chdir(prepend + '/dashboards')
# pwd2 = os.getcwd()
# print(pwd2)
df = pd.read_csv('../app/op_summer_latest_stats.csv')

print(df)
# print('weee')