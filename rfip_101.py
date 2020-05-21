import os
import sys
import pandas as pd
import numpy as np
import pandasql
import xlrd
import pymssql
from pandasql import sqldf
from datetime import datetime, timedelta

Budget_season='yes'

print('ROLLING FORECAST\n')

def excelToSQL(excelPath,excelSheetName,tableName):
    if excelSheetName=="":
        excelSheetName="Sheet1"
    print(revenue)
    if int(pd.__version__.split('.')[0])==0 and int(pd.__version__.split('.')[1])<21:
        df=pd.read_excel(excelPath,sheetname=excelSheetName)
    else:
        df=pd.read_excel(excelPath,sheet_name=excelSheetName)
    df=df.fillna("")
    df['SITA']=sita
    df['today']=snapshot
    df['last_update']=1
    df=df[['DAY','SITA','MKT','ROLL_RNS','ROLL_REV','today','last_update']]
    df['today']=pd.to_datetime(df['today'])
    df['datediff']=(df['DAY']-df['today'])/np.timedelta64(1,'D')
    df=df[(df['DAY']>=df['today'])]
    df=df[df['datediff']<364]
    df=df[['DAY','SITA','MKT','ROLL_RNS','ROLL_REV','today','last_update']]
    #print(len(df))
    df2=df.copy(deep=True)
    df2['DAY']=df2['DAY']+timedelta(days=364)
    df2['ROLL_REV']=df2['ROLL_REV']*1.03
    df2['ROLL_RNS']=round(df2['ROLL_RNS']*1.015) 
    df2['ROLL_RNS']=df2['ROLL_RNS'].astype(int)
    df_union=pd.concat([df,df2],ignore_index=True)
    df_union['ts'] = datetime.now()
    table_name="[revgen].[dbo]."+ tableName
    print("\nDeleting today snapshot, if there is any...\n")
    cur.execute("""DELETE FROM """ +table_name+""" WHERE sita='""" + sita + """' and SNAPSHOT_DATE>='"""+snapshot+"""'""" );conn.commit() 
    cur.execute("""UPDATE """ +table_name+""" SET LAST_UPDATE=0 WHERE sita='""" + sita +"""' and SNAPSHOT_DATE<'"""+snapshot+"""'""" );conn.commit() 
    x=len(df_union) ##number of rows
    print(x, "records are now loading")
    data = [tuple(x) for x in df_union.values]  ##turn each row into a tuple
    wildcard='%s,'*(len(df_union.columns)-1)
    cur.executemany("INSERT INTO "+ table_name+" VALUES("+wildcard+"%s)", data)
    ##This has two arguments: one is the query and the other is a list of tuples. This code executes the operation repeatedly for each element in the list  
    conn.commit() ##commits current transaction
    print(x, "records loaded")

def excelToSQL_BudgetSeason(excelPath,excelSheetName,tableName):
    if excelSheetName=="":
        excelSheetName="Sheet1"
    print(excelPath)
    if int(pd.__version__.split('.')[0])==0 and int(pd.__version__.split('.')[1])<21:
        df=pd.read_excel(excelPath,sheetname=excelSheetName)
    else:
        df=pd.read_excel(excelPath,sheet_name=excelSheetName)
    df=df.fillna("")
    q  = """SELECT day, sita, mkt, cast(roll_rns as integer),roll_rev, '""" + snapshot +"""',1 FROM df WHERE DAy>='"""+snapshot +"""'"""
    df=((pandasql.sqldf(q, locals())))
    df['ts'] = datetime.now()
    x=len(df)
    table_name="[revgen].[dbo]."+ tableName
    print("\nDeleting today snapshot, if there is any...\n")
    cur.execute("""DELETE FROM """ +table_name+""" WHERE sita='""" + sita + """' and SNAPSHOT_DATE>='"""+snapshot+"""'""" );conn.commit() 
    cur.execute("""UPDATE """ +table_name+""" SET LAST_UPDATE=0 WHERE sita='""" + sita +"""' and SNAPSHOT_DATE<'"""+snapshot+"""'""" );conn.commit() 
    print(x, "records are now loading...")
    conn.commit() 
    data = [tuple(x) for x in df.values]    
    wildcard='%s,'*(len(df.columns)-1)
    cur.executemany("INSERT INTO "+ table_name+" VALUES("+wildcard+"%s)", data)
    conn.commit()
    print(x, "records loaded")

snapshot=str(datetime.now().strftime('%Y-%m-%d'))
starttime=datetime.now()
print(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

revenue = str(sys.argv[1])
db_ip   = str(sys.argv[2])
db_name = str(sys.argv[3])
login   = str(sys.argv[4])
passw   = str(sys.argv[5])
sita    = os.path.basename(revenue)[:5]

conn = pymssql.connect(host=db_ip, user=login, password=passw, database=db_name)
print("Logged to PULSE DB")
cur = conn.cursor()
if Budget_season=='yes':   
    print('Budget Season: uplift not applied beyond 364 days')
    excelToSQL_BudgetSeason(revenue,"OTB_DATA_V5","rev_tool_roll_forecast")
else:
    print('Script will apply uplift to rolling beyond 364 days. Any changes you have made after 364 days will be overriden by this script')
    excelToSQL(revenue,"OTB_DATA_V5","rev_tool_roll_forecast")
conn.close()

endtime=datetime.now()
print("END AT "+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
input("Press ENTER     to close... ")
#timetoprocess=endtime-starttime
#print(timetoprocess)
