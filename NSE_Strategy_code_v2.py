#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import streamlit as st
import requests, zipfile, io,logging
import shutil
import os
from datetime import datetime,timedelta


# In[104]:


#expiry='24/11/2022'
#expiry=datetime.strptime(expiry, '%d/%m/%Y').strftime("%d-%b-%Y")
#INSTRUMENT='OPTSTK'


# In[67]:


Start_date=""
End_date=""
d_path=r"Data"
e_path=r"Other_Data"


# In[68]:

for file in os.scandir(d_path):
    os.remove(file.path)
for file in os.scandir(e_path):
    os.remove(file.path)


# In[70]:


global No_of_download,Working_day,Non_Work_day
No_of_download=0
Working_day=0
Non_Work_day=0


# In[173]:


def req(zip_file_url,path):
    global No_of_download
    r = requests.post(zip_file_url)
    status_code=r.status_code
    #print(status_code)
    #If status code is <> 200, it indicates that no data is present for that date. For example, week-end, or trading holiday.
    if status_code==200:
        No_of_download=No_of_download+1
        logger.info("File Available.Downloading")
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(path=path)
    else:
        logger.info("******File Not Available.Moving to next date.")
    return status_code
    


# In[179]:


today_date=datetime.now().strftime("%Y%b%d")
logging.basicConfig(filename="Log_"+today_date+".log", format='%(asctime)s %(message)s', filemode='w') 
logger=logging.getLogger() 
logger.setLevel(logging.INFO) 

#Populating today's date as default, if the stat_date and/or End_date is not provided.
if Start_date=="" or Start_date=="enter_start_date_in_DDMMMYYYY":
    Start_date=(datetime.now()-timedelta(days=14)).strftime("%Y%b%d")
    End_date=today_date
if End_date=="" or End_date=="enter_start_date_in_DDMMMYYYY":
        End_date=today_date

daterange = pd.date_range(datetime.strptime(Start_date, "%Y%b%d"),datetime.strptime(End_date, "%Y%b%d"))
lis=[]
#Looping through each date, and downloading the file.
for single_date in daterange:
    loop_date=single_date.strftime("%Y-%b-%d")
    year,month,date=loop_date.split('-')
    month=month.upper()
    weekday=single_date.weekday()
    #If day is not Saturday or Sunday,then proceed to download the file.
    if weekday not in [5,6]:
        Working_day=Working_day+1
        logger.info("Trying to download File of :"+loop_date)
        temp_zip_file_url = 'https://www1.nseindia.com/content/historical/DERIVATIVES/'+year+'/'+month+'/fo'+date+month+year+'bhav.csv.zip'
        ls=req(zip_file_url=temp_zip_file_url,path=d_path)
        if ls==200:
            lis.append(single_date)
        #print(temp_zip_file_url)
    else:
        Non_Work_day=Non_Work_day+1
if single_date.weekday()==5:
    new_date=single_date-timedelta(1)
elif single_date.weekday()==6:
    new_date=single_date-timedelta(1)
loop1_date=new_date.strftime("%Y-%b-%d")
year,month,date=loop1_date.split('-')
month=month.upper()
temp_zip_file_url = 'https://www1.nseindia.com/content/historical/EQUITIES/'+year+'/'+month+'/cm'+date+month+year+'bhav.csv.zip'
ls2=req(zip_file_url=temp_zip_file_url,path=e_path)
filename='fo'+date+month+year+'bhav.csv'
cm_filename='cm'+date+month+year+'bhav.csv'
lis.sort()
#print("Number of files downloaded:"+str(No_of_download))
logger.info("****************************************************************************************") 
logger.info("No. of files downloaded="+str(No_of_download)) 
logger.info("Span= " + Start_date+ " to " + End_date )
logger.info("No. of weekdays in the given time span="+str(Working_day)) 
logger.info("****************************************************************************************") 
logging.shutdown()


# In[73]:


Data_names=os.listdir(d_path)
Data_names.remove(filename)


# In[74]:


lot_size=pd.read_csv('fo_mktlots.csv')
lot_size.columns=lot_size.columns.str.strip()
lot_size=lot_size[['SYMBOL','JAN-23']]
lot_size = lot_size.applymap(lambda x: x.strip() if type(x)==str else x)


# In[119]:


def get_df(name,expiry,INSTRUMENT):
    df = pd.read_csv(d_path+'/'+name)
    df.columns = df.columns.str.strip()
    df = df.applymap(lambda x: x.strip() if type(x)==str else x)
    df = df[df.INSTRUMENT==INSTRUMENT]
    df = df[df.EXPIRY_DT==expiry]
    df=df[['TIMESTAMP','SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP',
           'OPEN', 'HIGH', 'LOW', 'CLOSE', 'OPEN_INT', 'CONTRACTS']]
    df.reset_index(drop=True,inplace=True)
    
    return df


# In[76]:


def drop_y(df,i):
    # list comprehension of the cols that end with '_y'
    to_drop = [x for x in df if x.endswith('_y')]
    if "LOW_y" in to_drop:
        to_drop.remove("LOW_y")
        to_drop.remove('OPEN_INT_y')
    df.drop(to_drop, axis=1, inplace=True)
    rename_x(df,i)
def rename_x(df,i):
    for col in df:
        if col.endswith('_x'):
            df.rename(columns={col:col.rstrip('_x')}, inplace=True)
        elif col.endswith('_y'):
            df.rename(columns={col:col.rstrip('_y')+'_'+i[2:7]},inplace=True)


# In[ ]:


with st.sidebar.header('Choose your input type'):
    check_type = st.sidebar.selectbox('Select your input type here:',('NSE_stocks','NSE_filter'))

st.sidebar.write('Your selected input type:', check_type)


# In[69]:


if check_type=='NSE_stocks':
    col1,col2,col3,col4,col5,col6=st.columns([1.3,1.3,1.3,1.3,1.3,1.3])
    option = st.selectbox('Please select you stock',(df.SYMBOL))
    strike_price=st.text_input('Please enter strike price')
    #start=st.date_input("Enter start date")
    #end=st.date_input("Enter end date")
    option_type=st.radio("Option Type",('CE', 'PE'))
    expiry=st.date_input("Enter expiry date")
    expiry=datetime.strptime(expiry, '%d/%m/%Y').strftime("%d-%b-%Y")
    df=get_df(filename,expiry,INSTRUMENT)
    for i in Data_names:
        df1=get_df(i,expiry,INSTRUMENT)
        #df1.rename(columns={'NO_OF_CONT':'Contracts'+'_'+i[2:7]},inplace=True)
        #df=pd.merge(df,df1,on=['INSTRUMENT', 'SYMBOL', 'EXP_DATE', 'STR_PRICE', 'OPT_TYPE'],how='left')
        #drop_y(df,i)
        #print(i)
        df=pd.concat([df,df1],ignore_index=True,axis=0)

    if option and strike_price and option_type and expiry:
        df1=df[(df.SYMBOL==option)and(df.STRIKE_PR==strike_price)and(df.OPTION_TYP=option_type)and(df.EXPIRY_DT==expiry)]
        st.dataframe(df.style.highlight_min(axis=0))
    else:
        st.subheader('Please enter all inputs')
elif check_type=='NSE_filter':
    col1,col2,col3,col4=st.columns([2,2,2,2])

    INSTRUMENT=col1.radio('Select Index option or Stock options',("OPTIDX","OPTSTK"))

    co=int(col4.radio('1-Day or 2-Days decreasing Contracts',(1,2)))


    min_inv=int(col2.radio('Enter minimum Investments',(100,3000,5000,10000)))
    max_inv=int(col3.radio('Enter maximum Investments',(3000,5000,10000)))

    col1,buff,col2,col3=st.columns([2,2,2,2])
    close_price=col1.text_input('Minumum price',4)
    contr=col2.text_input('Minumum contracts',200)
    op_int=buff.text_input('Minimum OPEN INTEREST',100000)
    expiry=col4.date_input("Enter expiry date")
    expiry=datetime.strptime(expiry, '%d/%m/%Y').strftime("%d-%b-%Y")
    
    df=get_df(filename,expiry,INSTRUMENT)
    df.drop(['TIMESTAMP'], axis=1,inplace=True)
    
    for i in Data_names:
        df1=get_df(i,expiry,INSTRUMENT)
        df1.rename(columns={'CONTRACTS':'Contracts'+'_'+i[2:7]},inplace=True)
        df=pd.merge(df,df1,on=['SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP'],how='left')
        drop_y(df,i)
    #print(i)
    
    mtm=pd.read_csv(e_path+'/'+cm_filename)
    mtm=mtm[mtm.SERIES=='EQ']
    df=pd.merge(df,mtm[['SYMBOL','CLOSE']],on="SYMBOL",how="left")
    df.rename(columns={"CLOSE_y":"EQ_price","CLOSE_x":"CLOSE"},inplace=True)
    
    lows=df.columns[df.columns.str.contains('LOW')]
    contracts=df.columns[df.columns.str.contains('CONTRACTS')]
    OI=df.columns[df.columns.str.contains('OPEN_INT')]
    
    df=df[df.LOW==df[lows].min(axis=1)]
    
    if INSTRUMENT=='OPTSTK':
        df_ce=df[(df.OPTION_TYP=='CE')&((df.STRIKE_PR>df.EQ_price))]
        df_pe=df[(df.OPTION_TYP=='PE')&((df.STRIKE_PR<df.EQ_price))]
        df2=pd.concat([df_ce, df_pe], ignore_index=True, axis=0)
    else:
        df2=df
        
    today_con_name="CONTRACTS"
    yest_con_name="Contracts_"+lis[-3:][1].strftime('%d%b').upper()
    daybef_con_name="Contracts_"+lis[-3:][0].strftime('%d%b').upper()
    
    
    #Add butooon **************************************
    if co==1:
        df4=df2[(df2[today_con_name]<df2[yest_con_name])]
    else:
        df4=df2[(df2[today_con_name]<df2[yest_con_name])&(df2[yest_con_name]<df2[daybef_con_name])]
    
    
    today_OI_name="OPEN_INT"
    yest_OI_name="OPEN_INT_"+lis[-3:][1].strftime('%d%b').upper()
    
    df4=df4.merge(lot_size,on='SYMBOL',how="left")
    df4.rename(columns={'JAN-23':'Lot_size'},inplace=True)
    df4.Lot_size=df4.Lot_size.astype(int)
    df4['Investment']=df4['HI_PRICE']*df4['Lot_size']

    
    
    if (close_price) and (not contr) and (not op_int):
        close_price=int(close_price)
        df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4.CLOSE>close_price)].reset_index(drop=True)
    elif (contr) and (not close_price) and (not op_int):
        contr=int(contr)
        df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4[CONTRACTS]>contr)].reset_index(drop=True)
    elif (op_int) and (not close_price) and (not contr):
        op_int=int(op_int)
        df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4['OPEN_INT']>op_int)].reset_index(drop=True)
    elif (op_int) and (close_price) and (contr):
        close_price=int(close_price)
        contr=int(contr)
        op_int=int(op_int)
        df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4['OPEN_INT']>op_int)&(df4.CLOSE>close_price)&(df4[today_con_name]>contr)].reset_index(drop=True)
    else:
        df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)].reset_index(drop=True)
        
    df_ce1=df10[df10.OPT_TYPE=='CE'].drop_duplicates(subset=['SYMBOL','OPT_TYPE'],keep='first',ignore_index=True)
    df_pe1=df10[df10.OPT_TYPE=='PE'].drop_duplicates(subset=['SYMBOL','OPT_TYPE'],keep='last',ignore_index=True)
    df11=pd.concat([df_ce1, df_pe1], ignore_index=True, axis=0)




    #style.highlight_max(axis=0)
    df11=df11[['SYMBOL', 'EXP_DATE', 'STR_PRICE', 'OPT_TYPE',
           'OPEN_PRICE', 'HI_PRICE', 'LO_PRICE', 'CLOSE_PRICE', 'OPEN_INT*','MTM SETTLEMENT PRICE', 'Lot_size', 'Investment']]

    st.dataframe(df11.style.set_precision(0))

    reports_csv=df11.to_csv().encode('utf-8')
    st.download_button(label="Export Report",data=reports_csv,file_name='Report.csv',mime='text/csv')

