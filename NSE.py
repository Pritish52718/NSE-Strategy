import nsepy
import streamlit as st
import pandas as pd
from datetime import date


with st.sidebar.header('Choose your input type'):
    check_type = st.sidebar.selectbox('Select your input type here:',('NSE_stocks','NSE_filter'))
df=pd.read_csv('FOSett_prce_31102022.csv')
st.sidebar.write('Your selected input type:', check_type)
if check_type=='NSE_stocks':
    col1,col2,col3,col4,col5,col6=st.columns([1.3,1.3,1.3,1.3,1.3,1.3])
    option = st.selectbox('Please select you stock',(df.SYMBOL))
    strike_price=st.text_input('Please enter strike price')
    start=st.date_input("Enter start date")
    end=st.date_input("Enter end date")
    option_type=st.radio("Option Type",('CE', 'PE'))
    expiry=st.date_input("Enter expiry date")

    if option and strike_price:
        df=nsepy.get_history(symbol=option,strike_price=strike_price,start=date(start),end=date(end),option_type =option_type,expiry_date=date(expiry))
        st.dataframe(df.style.highlight_min(axis=0))
