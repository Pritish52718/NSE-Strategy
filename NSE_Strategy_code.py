#!/usr/bin/env python
# coding: utf-8



import pandas as pd
import streamlit as st
import os

st.set_page_config(layout="wide")
with st.sidebar.header('Choose your input type'):
        check_type = st.sidebar.selectbox('Select your input type here:',('NSE_stocks','NSE_filter'))
nse_start(check_type)



today='op03112022'
yesterday='op02112022'
daybefore='op01112022'
expiry='24/11/2022'

col1,col2,col3=st.columns([2,2,2])

INSTRUMENT=col1.radio('Select Index option or Stock options',("OPTIDX","OPTSTK"))



min_inv=int(col2.radio('Enter minimum Investments',(100,3000,5000,10000)))
max_inv=int(col3.radio('Enter maximum Investments',(3000,5000,10000)))

col1,buff,col2=st.columns([2,2,2])
close_price=col1.text_input('Minumum price',4)
contr=col2.text_input('Minumum contracts',200)
op_int=buff.text_input('Minimum OPEN INTEREST',100000)





Data_names=os.listdir('Data')



Data_names.remove(today+'.csv')






lot_size=pd.read_csv('fo_mktlots.csv')
lot_size.columns=lot_size.columns.str.strip()
lot_size=lot_size[['SYMBOL','NOV-22']]
lot_size = lot_size.applymap(lambda x: x.strip() if type(x)==str else x)



def get_df(name,expiry,INSTRUMENT):
    df = pd.read_csv('Data/'+name)
    df.columns = df.columns.str.strip()
    df = df.applymap(lambda x: x.strip() if type(x)==str else x)
    df = df[df.INSTRUMENT==INSTRUMENT]
    df = df[df.EXP_DATE==expiry]
    df=df[['INSTRUMENT', 'SYMBOL', 'EXP_DATE', 'STR_PRICE', 'OPT_TYPE',
           'OPEN_PRICE', 'HI_PRICE', 'LO_PRICE', 'CLOSE_PRICE', 'OPEN_INT*', 'NO_OF_CONT']]
    
    return df



def drop_y(df,i):
    # list comprehension of the cols that end with '_y'
    to_drop = [x for x in df if x.endswith('_y')]
    if "LO_PRICE_y" in to_drop:
        to_drop.remove("LO_PRICE_y")
        to_drop.remove('OPEN_INT*_y')
    df.drop(to_drop, axis=1, inplace=True)
    rename_x(df,i)
def rename_x(df,i):
    for col in df:
        if col.endswith('_x'):
            df.rename(columns={col:col.rstrip('_x')}, inplace=True)
        elif col.endswith('_y'):
            df.rename(columns={col:col.rstrip('_y')+'_'+i[2:6]},inplace=True)



df=get_df(today+".csv",expiry,INSTRUMENT)
df.rename(columns={'NO_OF_CONT':'Contracts'+'_'+today[2:6]},inplace=True)
#print(df.columns)
for i in Data_names:
    df1=get_df(i,expiry,INSTRUMENT)
    df1.rename(columns={'NO_OF_CONT':'Contracts'+'_'+i[2:6]},inplace=True)
    df=pd.merge(df,df1,on=['INSTRUMENT', 'SYMBOL', 'EXP_DATE', 'STR_PRICE', 'OPT_TYPE'],how='left')
    drop_y(df,i)
    #print(i)

mtm=pd.read_csv('FOSett_prce_'+today[2:]+'.csv')
mtm=mtm[mtm.INSTRUMENT=="OPTSTK"]
df=pd.merge(df,mtm[['UNDERLYING','MTM SETTLEMENT PRICE']],left_on="SYMBOL",right_on='UNDERLYING',how="left").drop('UNDERLYING',axis=1)
df=df.fillna(0)



lows=df.columns[df.columns.str.contains('LO_PRICE')]
contracts=df.columns[df.columns.str.contains('Contracts')]
OI=df.columns[df.columns.str.contains('OPEN_INT')]



#df=df[(df.STR_PRICE>df['MTM SETTLEMENT PRICE'])&(df.LO_PRICE==df[lows].min(axis=1))]
df=df[df.LO_PRICE==df[lows].min(axis=1)]


if INSTRUMENT=='OPTSTK':
    df_ce=df[(df.OPT_TYPE=='CE')&((df.STR_PRICE>df['MTM SETTLEMENT PRICE']))]
    df_pe=df[(df.OPT_TYPE=='PE')&((df.STR_PRICE<df['MTM SETTLEMENT PRICE']))]
    df2=pd.concat([df_ce, df_pe], ignore_index=True, axis=0)
else:
    df2=df



today_con_name="Contracts_"+today[2:6]
yest_con_name="Contracts_"+yesterday[2:6]
daybef_con_name="Contracts_"+daybefore[2:6]
df4=df2[(df2[today_con_name]<df2[yest_con_name])&(df2[yest_con_name]<df2[daybef_con_name])]


today_OI_name="OPEN_INT*"
yest_OI_name="OPEN_INT*_"+yesterday[2:6]

df4=df4.merge(lot_size,on='SYMBOL',how="left")
df4.rename(columns={'NOV-22':'Lot_size'},inplace=True)

df4.Lot_size=df4.Lot_size.astype(int)

df4['Investment']=df4['HI_PRICE']*df4['Lot_size']

if (close_price) and (not contr) and (not op_int):
    close_price=int(close_price)
    df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4.CLOSE_PRICE>close_price)].reset_index(drop=True)
elif (contr) and (not close_price) and (not op_int):
    contr=int(contr)
    df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4[today_con_name]>contr)].reset_index(drop=True)
elif (op_int) and (not close_price) and (not contr):
    op_int=int(op_int)
    df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4['OPEN_INT*']>op_int)].reset_index(drop=True)
elif (op_int) and (close_price) and (contr):
    close_price=int(close_price)
    contr=int(contr)
    op_int=int(op_int)
    df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4['OPEN_INT*']>op_int)&(df4.CLOSE_PRICE>close_price)&(df4[today_con_name]>contr)].reset_index(drop=True)
else:
    df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)].reset_index(drop=True)
    
    

#df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4['OPEN_INT*']>op_int)&(df4.CLOSE_PRICE>close_price)&(df4[today_con_name]>contr)].reset_index(drop=True)

df_ce1=df10[df10.OPT_TYPE=='CE'].drop_duplicates(subset=['SYMBOL','OPT_TYPE'],keep='first',ignore_index=True)
df_pe1=df10[df10.OPT_TYPE=='PE'].drop_duplicates(subset=['SYMBOL','OPT_TYPE'],keep='last',ignore_index=True)
df11=pd.concat([df_ce1, df_pe1], ignore_index=True, axis=0)




#style.highlight_max(axis=0)
df11=df11[['SYMBOL', 'EXP_DATE', 'STR_PRICE', 'OPT_TYPE',
       'OPEN_PRICE', 'HI_PRICE', 'LO_PRICE', 'CLOSE_PRICE', 'OPEN_INT*','MTM SETTLEMENT PRICE', 'Lot_size', 'Investment']]

st.dataframe(df11.style.set_precision(0))

reports_csv=df11.to_csv().encode('utf-8')
st.download_button(label="Export Report",data=reports_csv,file_name='Report.csv',mime='text/csv')




