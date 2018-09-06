import json
import requests
import pandas as pd
from sqlalchemy import create_engine
import time
import database_settings
now = time.strftime("%Y-%m-%d", time.gmtime())
con = create_engine('mssql+pyodbc://'+database_settings.User+':'+database_settings.Password+'@'+database_settings.Server+':'+database_settings.Port+'/'+database_settings.Database+'?driver=ODBC Driver 13 for SQL Server')

url = 'http://www.tpex.org.tw/web/stock/margin_trading/marginspot/mgratio_result.php?l=zh-tw&d=107/07/01'
res = requests.get(url)
result = json.loads(res.text)
result['aaData']
aaData = result['aaData']
df = pd.DataFrame(aaData)
#df = pd.DataFrame(aaData,columns=['排名','代號','名稱','月均融資餘額','月均融資市場占有率','月均融券餘額','月均融券市場占有率','月均融資融券餘額','月均融資融券市場占有率'])

#df['月均融資餘額'] = df['月均融資餘額'].str.replace(",","")
#df['月均融券餘額'] = df['月均融券餘額'].str.replace(",","")
#df['月均融資融券餘額'] = df['月均融資融券餘額'].str.replace(",","")
df[3] = df[3].str.replace(",","")
df[5] = df[5].str.replace(",","")
df[7] = df[7].str.replace(",","")


#df[['排名']] = df[['排名']].astype(int)
#df[['月均融資餘額']] = df[['月均融資餘額']].astype('int64')
#df[['月均融券餘額']] = df[['月均融券餘額']].astype('int64')
#df[['月均融資融券餘額']] = df[['月均融資融券餘額']].astype('int64')
df[[0]] = df[[0]].astype(int)
df[[3]] = df[[3]].astype('int64')
df[[5]] = df[[5]].astype('int64')
df[[7]] = df[[7]].astype('int64')


#df = df.drop(columns=['排名','月均融資市場占有率','月均融券市場占有率','月均融資融券市場占有率'])
df = df.drop(columns=[0,4,6,8],axis=1)
#df.insert(0,'年月份',10707)
df.insert(0,0,10707)
#df = df.reset_index(drop=True)
df2 = df
#df2.columns =[0,1,2,3,4,5]
#print(df2)
Table = 'tblMarginspot2'
print('Start Write Data to DB')
df2.columns = ['reportDate','Code','CodeName','Mfinance','Mbearish','MTotal']
#data2 = pd.DataFrame(data,columns=['更新日期','登記機關','登記機關名稱','商業統一編號','商業名稱','商業登記地址','資本額','組織型態','組織型態名稱','代表人姓名','核准設立日期','變更日期','商業狀態'])
df2.to_sql(Table, con, if_exists='append', index=False)
print('Write Data Done')
print('------Crawler Work Finish------')