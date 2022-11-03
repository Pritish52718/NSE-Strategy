#!/usr/bin/env python
# coding: utf-8

# In[33]:


import pandas as pd
import streamlit as st
import os

st.set_page_config(layout="wide")


# In[34]:


today='op03112022'
yesterday='op02112022'
daybefore='op01112022'
expiry='24/11/2022'
INSTRUMENT=st.radio('Select Index option or Stock options',("OPTIDX","OPTSTK"))


# In[35]:


min_inv=int(st.radio('Enter minimum Investments',(100,3000,5000,10000)))
max_inv=int(st.radio('Enter maximum Investments',(3000,5000,10000)))
close_price=st.text_input('Minumum price')
contr=st.text_input('Minumum contracts')

if close_price:
    close_price=int(close_price)
if contr:
    contr=int(contr)


# In[36]:


Data_names=os.listdir('Data')


# In[37]:


Data_names.remove(today+'.csv')


# In[38]:





lot_size=pd.read_csv('fo_mktlots.csv')
lot_size.columns=lot_size.columns.str.strip()
lot_size=lot_size[['SYMBOL','NOV-22']]
lot_size = lot_size.applymap(lambda x: x.strip() if type(x)==str else x)


# In[40]:


def get_df(name,expiry,INSTRUMENT):
    df = pd.read_csv('Data/'+name)
    df.columns = df.columns.str.strip()
    df = df.applymap(lambda x: x.strip() if type(x)==str else x)
    df = df[df.INSTRUMENT==INSTRUMENT]
    df = df[df.EXP_DATE==expiry]
    df=df[['INSTRUMENT', 'SYMBOL', 'EXP_DATE', 'STR_PRICE', 'OPT_TYPE',
           'OPEN_PRICE', 'HI_PRICE', 'LO_PRICE', 'CLOSE_PRICE', 'OPEN_INT*', 'NO_OF_CONT']]
    
    return df


# In[41]:


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


# In[42]:


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


# In[43]:


#df.columns


# In[44]:


#df3[df3.today_contracts<df3.yesterday_contracts].to_csv('Index_data.csv')


# In[45]:


lows=df.columns[df.columns.str.contains('LO_PRICE')]
contracts=df.columns[df.columns.str.contains('Contracts')]
OI=df.columns[df.columns.str.contains('OPEN_INT')]


# In[46]:


#df=df[(df.STR_PRICE>df['MTM SETTLEMENT PRICE'])&(df.LO_PRICE==df[lows].min(axis=1))]
df=df[df.LO_PRICE==df[lows].min(axis=1)]


# In[47]:


if INSTRUMENT=='OPTSTK':
    df_ce=df[(df.OPT_TYPE=='CE')&((df.STR_PRICE>df['MTM SETTLEMENT PRICE']))]
    df_pe=df[(df.OPT_TYPE=='PE')&((df.STR_PRICE<df['MTM SETTLEMENT PRICE']))]
    df2=pd.concat([df_ce, df_pe], ignore_index=True, axis=0)
else:
    df2=df


# In[48]:


#OI


# In[49]:


today_con_name="Contracts_"+today[2:6]
yest_con_name="Contracts_"+yesterday[2:6]
daybef_con_name="Contracts_"+daybefore[2:6]
df4=df2[(df2[today_con_name]<df2[yest_con_name])&(df2[yest_con_name]<df2[daybef_con_name])]


# In[50]:


today_OI_name="OPEN_INT*"
yest_OI_name="OPEN_INT*_"+yesterday[2:6]
#df4=df4[(df4[today_OI_name]>df4[yest_OI_name])]


# In[51]:


#df4.columns


# In[52]:


df4=df4.merge(lot_size,on='SYMBOL',how="left")
df4.rename(columns={'NOV-22':'Lot_size'},inplace=True)


# In[53]:


df4.Lot_size=df4.Lot_size.astype(int)


# In[54]:


df4['Investment']=df4['HI_PRICE']*df4['Lot_size']


# In[55]:


#df4


# In[56]:


#df4=df4[(df4[today_OI_name]>df4[yest_OI_name])]


# In[57]:


#.to_csv('Stock_options_200_updatedv3.csv',index=False)
df5=df4[(df4[today_con_name]>200)&(df4.Investment<=8000)]


# In[58]:


#df5= df5.drop_duplicates(subset=['SYMBOL','OPT_TYPE'],keep='first',ignore_index=True)


# In[59]:


df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4['OPEN_INT*']>100000)&(df4.CLOSE_PRICE>close_price)&(df4[today_con_name]>contr)].reset_index(drop=True)


# In[60]:


df_ce1=df10[df10.OPT_TYPE=='CE'].drop_duplicates(subset=['SYMBOL','OPT_TYPE'],keep='first',ignore_index=True)
df_pe1=df10[df10.OPT_TYPE=='PE'].drop_duplicates(subset=['SYMBOL','OPT_TYPE'],keep='last',ignore_index=True)
df11=pd.concat([df_ce1, df_pe1], ignore_index=True, axis=0)


# In[61]:


#df11= df10.drop_duplicates(subset=['SYMBOL','OPT_TYPE'],keep='first',ignore_index=True)


# In[65]:


df11[['SYMBOL', 'EXP_DATE', 'STR_PRICE', 'OPT_TYPE',
       'OPEN_PRICE', 'HI_PRICE', 'LO_PRICE', 'CLOSE_PRICE', 'OPEN_INT*','MTM SETTLEMENT PRICE', 'Lot_size', 'Investment']]


# In[63]:


len(df10.SYMBOL.unique())


# In[64]:


#df11.to_csv('Stocks_invst10k_OI1lkh_cont50_price1_20_10_2022.csv',index=False)


# In[ ]:




