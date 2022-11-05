#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import streamlit as st
import requests, zipfile, io,logging
import shutil
import os
from datetime import datetime,date,timedelta

st.set_page_config(layout="wide")

# In[104]:


#expiry='24/11/2022'
#expiry=datetime.strptime(expiry, '%d/%m/%Y').strftime("%d-%b-%Y")
#INSTRUMENT='OPTSTK'


# In[67]:


from dateutil.relativedelta import relativedelta, TH


nthu = datetime.today()
while (nthu + relativedelta(weekday=TH(2))).month == datetime.today().month:
    nthu += relativedelta(weekday=TH(2))

d_path=r"Data"
e_path=r"Other_Data"


# In[68]:




# In[70]:





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
        z.extractall(path=path+"/")
    else:
        logger.info("******File Not Available.Moving to next date.")
    return status_code
    


# In[179]:


today_date=datetime.now().strftime("%Y%b%d")
logging.basicConfig(filename="Log_"+today_date+".log", format='%(asctime)s %(message)s', filemode='w') 
logger=logging.getLogger() 
logger.setLevel(logging.INFO) 



#Populating today's date as default, if the stat_date and/or End_date is not provided.
@st.cache
def downld_data(d_path,e_path):
    for file in os.scandir(d_path+"/"):
        os.remove(file.path)
    for file in os.scandir(e_path+"/"):
        os.remove(file.path)
    global No_of_download,Working_day,Non_Work_day
    No_of_download=0
    Working_day=0
    Non_Work_day=0
    Start_date=""
    End_date=""
    check=True
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
            print(temp_zip_file_url)
            ls=req(zip_file_url=temp_zip_file_url,path=d_path)
            if ls==200:
                lis.append(single_date)
            #print(temp_zip_file_url)
        else:
            Non_Work_day=Non_Work_day+1
    if daterange[-1].weekday()==5:
        new_date=daterange[-1]-timedelta(1)
    elif daterange[-1].weekday()==6:
        new_date=daterange[-1]-timedelta(2)
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
    
    Data_names=os.listdir(d_path)
    if filename in Data_names:
        Data_names.remove(filename)


    # In[74]:

    lot_size=pd.read_csv('fo_mktlots.csv')
    mtm=pd.read_csv(e_path+'/'+cm_filename)

    return(filename,cm_filename,lis,Data_names,lot_size,mtm)


#if st.button('Download Data'):



# In[119]:


def get_df(name):
    df = pd.read_csv(d_path+'/'+name)
    df.columns = df.columns.str.strip()
    df = df.applymap(lambda x: x.strip() if type(x)==str else x)
#     df = df[df.INSTRUMENT==INSTRUMENT]
#     df = df[df.EXPIRY_DT==expiry]
    df=df[['TIMESTAMP','INSTRUMENT','SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP',
           'OPEN', 'HIGH', 'LOW', 'CLOSE', 'OPEN_INT', 'CONTRACTS']]
    df.reset_index(drop=True,inplace=True)

    return df


# In[76]:


def drop_y(df,i):
    # list comprehension of the cols that end with '_y'
    to_drop = [x for x in df if x.endswith('_y')]
    if "LOW_y" in to_drop:
        to_drop.remove("LOW_y")
        to_drop.remove('CONTRACTS_y')
    df.drop(to_drop, axis=1, inplace=True)
    rename_x(df,i)
def rename_x(df,i):
    for col in df:
        if col.endswith('_x'):
            df.rename(columns={col:col.rstrip('_x')}, inplace=True)
        elif col.endswith('_y'):
            df.rename(columns={col:col.rstrip('_y')+'_'+i[2:7]},inplace=True)


# In[ ]:
@st.cache
def read_data(filename,Data_names,lot_size):
    df=get_df(filename)
    df_nf=df
    dfns=df

    for i in Data_names:
        df1=get_df(i)
        #dfnf=df1.rename(columns={'CONTRACTS':'Contracts_'+i[2:7]})
        df_nf=pd.merge(df_nf,df1,on=['SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP'],how='left')
        drop_y(df_nf,i)
        dfns=pd.concat([dfns,df1],ignore_index=True,axis=0)
    #print(i)
    df_nf.drop(['TIMESTAMP'], axis=1,inplace=True)
    lot_size.columns=lot_size.columns.str.strip()
    lot_size=lot_size[['SYMBOL','JAN-23']]
    lot_size = lot_size.applymap(lambda x: x.strip() if type(x)==str else x)
    for i in lot_size['JAN-23']:
        try:
            int(i)
        except ValueError:
            #print(i)
            lot_size.drop(lot_size[lot_size['JAN-23']==i].index, inplace = True)
    lot_size['JAN-23']=lot_size['JAN-23'].astype(int)
    return df_nf,dfns,lot_size




filename,cm_filename,lis,Data_names,lot_size,mtm=downld_data(d_path,e_path)
df_nf,dfns,lot_size=read_data(filename,Data_names,lot_size)

mtm=mtm[mtm.SERIES=='EQ']
df_nf=pd.merge(df_nf,mtm[['SYMBOL','CLOSE']],on="SYMBOL",how="left")
df_nf.rename(columns={"CLOSE_y":"EQ_price","CLOSE_x":"CLOSE"},inplace=True)

dfns=pd.merge(dfns,mtm[['SYMBOL','CLOSE']],on="SYMBOL",how="left")
dfns.rename(columns={"CLOSE_y":"EQ_price","CLOSE_x":"CLOSE"},inplace=True)




with st.sidebar.header('Choose your input type'):
    check_type = st.sidebar.selectbox('Select your input type here:',('NSE_stocks','NSE_filter'))

st.sidebar.write('Your selected input type:', check_type)





if check_type=='NSE_stocks':
    col1,col2,col3,col4,col5=st.columns([1.6,1.6,1.6,1.6,1.6])
    INSTRUMENT=col1.radio('Select Stock option or Index option',("OPTSTK","OPTIDX"))
    expiry=col5.date_input("Enter expiry date",nthu)
    expiry=expiry.strftime("%d-%b-%Y")

    dfns=dfns[dfns.INSTRUMENT==INSTRUMENT]
    dfns=dfns[dfns.EXPIRY_DT==expiry]

    l=list(dfns.SYMBOL)
    if INSTRUMENT=="OPTIDX":
        option = col3.selectbox('Please select an index',['BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTY'])
    else:        
        option = col3.selectbox('Please select a stock',['AARTIIND', 'ABB', 'ABBOTINDIA', 'ABCAPITAL', 'ABFRL', 'ACC', 'ADANIENT', 'ADANIPORTS', 'ALKEM', 'AMARAJABAT', 'AMBUJACEM', 'APOLLOHOSP', 'APOLLOTYRE', 'ASHOKLEY', 'ASIANPAINT', 'ASTRAL', 'ATUL', 'AUBANK', 'AUROPHARMA', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJFINANCE', 'BALKRISIND', 'BALRAMCHIN', 'BANDHANBNK', 'BANKBARODA', 'BATAINDIA', 'BEL', 'BERGEPAINT', 'BHARATFORG', 'BHARTIARTL', 'BHEL', 'BIOCON', 'BOSCHLTD', 'BPCL', 'BRITANNIA', 'BSOFT', 'CANBK', 'CANFINHOME', 'CHAMBLFERT', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COFORGE', 'COLPAL', 'CONCOR', 'COROMANDEL', 'CROMPTON', 'CUB', 'CUMMINSIND', 'DABUR', 'DALBHARAT', 'DEEPAKNTR', 'DELTACORP', 'DIVISLAB', 'DIXON', 'DLF', 'DRREDDY', 'EICHERMOT', 'ESCORTS', 'EXIDEIND', 'FEDERALBNK', 'FSL', 'GAIL', 'GLENMARK', 'GMRINFRA', 'GNFC', 'GODREJCP', 'GODREJPROP', 'GRANULES', 'GRASIM', 'GSPL', 'GUJGASLTD', 'HAL', 'HAVELLS', 'HCLTECH', 'HDFC', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDCOPPER', 'HINDPETRO', 'HINDUNILVR', 'HONAUT', 'IBULHSGFIN', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'IDEA', 'IDFC', 'IDFCFIRSTB', 'IEX', 'IGL', 'INDHOTEL', 'INDIACEM', 'INDIAMART', 'INDIGO', 'INDUSINDBK', 'INDUSTOWER', 'INFY', 'INTELLECT', 'IOC', 'IPCALAB', 'IRCTC', 'ITC', 'JINDALSTEL', 'JKCEMENT', 'JSWSTEEL', 'JUBLFOOD', 'KOTAKBANK', 'L&TFH', 'LALPATHLAB', 'LAURUSLABS', 'LICHSGFIN', 'LT', 'LTI', 'LTTS', 'LUPIN', 'M&M', 'M&MFIN', 'MANAPPURAM', 'MARICO', 'MARUTI', 'MCDOWELL-N', 'MCX', 'METROPOLIS', 'MFSL', 'MGL', 'MINDTREE', 'MOTHERSON', 'MPHASIS', 'MRF', 'MUTHOOTFIN', 'NATIONALUM', 'NAUKRI', 'NAVINFLUOR', 'NESTLEIND', 'NMDC', 'NTPC', 'OBEROIRLTY', 'OFSS', 'ONGC', 'PAGEIND', 'PEL', 'PERSISTENT', 'PETRONET', 'PFC', 'PIDILITIND', 'PIIND', 'PNB', 'POLYCAB', 'POWERGRID', 'PVR', 'RAIN', 'RAMCOCEM', 'RBLBANK', 'RECLTD', 'RELIANCE', 'SAIL', 'SBICARD', 'SBILIFE', 'SBIN', 'SHREECEM', 'SIEMENS', 'SRF', 'SRTRANSFIN', 'SUNPHARMA', 'SUNTV', 'SYNGENE', 'TATACHEM', 'TATACOMM', 'TATACONSUM', 'TATAMOTORS', 'TATAPOWER', 'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'TORNTPHARM', 'TORNTPOWER', 'TRENT', 'TVSMOTOR', 'UBL', 'ULTRACEMCO', 'UPL', 'VEDL', 'VOLTAS', 'WHIRLPOOL', 'WIPRO', 'ZEEL', 'ZYDUSLIFE'])
    s=list(dfns[dfns.SYMBOL==option].STRIKE_PR.unique())
    strike_price=col4.selectbox('Please select strike price',s)
    #start=st.date_input("Enter start date")
    #end=st.date_input("Enter end date")
    option_type=col2.radio("Option Type",('CE', 'PE'))



    if option and strike_price and option_type and expiry:
        df1=dfns[(dfns.SYMBOL==option)&(dfns.STRIKE_PR==strike_price)&(dfns.OPTION_TYP==option_type)&(dfns.EXPIRY_DT==expiry)]
        df1.TIMESTAMP=pd.to_datetime(df1.TIMESTAMP)
        df1=df1.sort_values("TIMESTAMP",ascending=False).reset_index(drop=True)
        st.dataframe(df1.style.set_precision(2))
#         print(df1.style.apply(lambda x: ['highlight_min: lightblue' if x.name == 'LOW'
#                           else '' for i in x]))

    else:
        st.subheader('Please enter all inputs')
        
        
        
        
        
elif check_type=='NSE_filter':
    col1,col2,col3,col4=st.columns([2,2,2,2])

    INSTRUMENT=col1.radio('Select Stock option or Index option',("OPTSTK","OPTIDX"))

    co=int(col4.radio('1-Day or 2-Days decreasing Contracts',(2,1)))


    min_inv=int(col2.radio('Enter minimum Investments',(1000,3000,5000,10000)))
    max_inv=int(col3.radio('Enter maximum Investments',(10000,5000,3000,)))

    col1,buff,col2,col3=st.columns([2,2,2,2])
    close_price=col1.text_input('Minumum price',4)
    contr=col2.text_input('Minumum contracts',200)
    op_int=buff.text_input('Minimum OPEN INTEREST',100000)
    expiry=col4.date_input("Enter expiry date",nthu)
    expiry=expiry.strftime("%d-%b-%Y")


    df_nf=df_nf[df_nf.INSTRUMENT==INSTRUMENT]
    df_nf=df_nf[df_nf.EXPIRY_DT==expiry]


    lows=df_nf.columns[df_nf.columns.str.contains('LOW')]
    contracts=df_nf.columns[df_nf.columns.str.contains('CONTRACTS')]
    OI=df_nf.columns[df_nf.columns.str.contains('OPEN_INT')]

    df_nf=df_nf[df_nf.LOW==df_nf[lows].min(axis=1)]

    if INSTRUMENT=='OPTSTK':
        df_ce=df_nf[(df_nf.OPTION_TYP=='CE')&(df_nf.STRIKE_PR>df_nf.EQ_price)]
        df_pe=df_nf[(df_nf.OPTION_TYP=='PE')&(df_nf.STRIKE_PR<df_nf.EQ_price)]
        df2=pd.concat([df_ce, df_pe], ignore_index=True, axis=0)
    else:
        df2=df_nf

    today_con_name="CONTRACTS"
    yest_con_name="CONTRACTS_"+lis[-3:][1].strftime('%d%b').upper()
    daybef_con_name="CONTRACTS_"+lis[-3:][0].strftime('%d%b').upper()

    #print(yest_con_name)
    #Add butooon **************************************
    if co==1:
        df4=df2[(df2[today_con_name]<df2[yest_con_name])]
    else:
        df4=df2[(df2[today_con_name]<df2[yest_con_name])&(df2[yest_con_name]<df2[daybef_con_name])]


    today_OI_name="OPEN_INT"
    yest_OI_name="OPEN_INT_"+lis[-3:][1].strftime('%d%b').upper()



    df4=df4.merge(lot_size,on='SYMBOL',how="left")
    df4.rename(columns={'JAN-23':'Lot_size'},inplace=True)
    #df4.Lot_size=df4.Lot_size.astype('int64')
    df4['Investment']=df4['HIGH']*df4['Lot_size']



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

    df_ce1=df10[df10.OPTION_TYP=='CE'].drop_duplicates(subset=['SYMBOL','OPTION_TYP'],keep='first',ignore_index=True)
    df_pe1=df10[df10.OPTION_TYP=='PE'].drop_duplicates(subset=['SYMBOL','OPTION_TYP'],keep='last',ignore_index=True)
    df11=pd.concat([df_ce1, df_pe1], ignore_index=True, axis=0)




    #style.highlight_max(axis=0)
    df11=df11[['SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP',
           'OPEN', 'HIGH', 'LOW', 'CLOSE', 'OPEN_INT','EQ_price', 'Lot_size', 'Investment']]

    st.dataframe(df11.style.set_precision(2))

    reports_csv=df11.to_csv().encode('utf-8')
    st.download_button(label="Export Report",data=reports_csv,file_name='Report.csv',mime='text/csv')
    # else:
    #     st.subheader("Please click on 'Download Data'")
