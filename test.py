import pandas as pd
import streamlit as st
import requests, zipfile, io,logging

url1="https://www1.nseindia.com/content/historical/DERIVATIVES/2022/OCT/fo31OCT2022bhav.csv.zip"
st.write(url1)
#ls,df_ns,df_nf=req(temp_zip_file_url,df_ns,df_nf)
r = requests.post(url1)
status_code=r.status_code
st.write(status_code)
