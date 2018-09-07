import json
import requests
import pandas as pd
from sqlalchemy import create_engine
import time
import database_settings
now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
con = create_engine('mssql+pyodbc://'+database_settings.User+':'+database_settings.Password+'@'+database_settings.Server+':'+database_settings.Port+'/'+database_settings.Database+'?driver=ODBC Driver 13 for SQL Server')

year = int(time.strftime("%Y", time.localtime(time.time())))
month = int(time.strftime("%m", time.localtime(time.time())))
data= []
list1 = []
Count = 0

def Craw():
    elem_list = pd.DataFrame()
    Checklist = list1[0].values.tolist()
    for CrawlYear in range(StartYear-1911,year-1910):
            for CrawMonth in range(1,13):
                if ((CrawlYear+1911) == year and CrawMonth == month):
                    break
                else :
                    YM = str(str(CrawlYear)+"%02d" % CrawMonth)
                    if (YM in str(Checklist)):
                        print(YM,"資料庫已有資料，不爬取")
                        continue
                    else:
                        #print(CrawlYear+1911,CrawMonth)
                        print("開始爬取",str(CrawlYear)+"%02d" % CrawMonth,"的資料")
                        url = 'http://www.tpex.org.tw/web/stock/margin_trading/marginspot/mgratio_result.php?l=zh-tw&d=' + str(
                            CrawlYear) + '/' + "%02d" % CrawMonth + '/01'
                        res = requests.get(url)
                        result = json.loads(res.text)
                        aaData = result['aaData']
                        df = pd.DataFrame(aaData)
                        #消除欄位逗點
                        df[3] = df[3].str.replace(",", "")
                        df[5] = df[5].str.replace(",", "")
                        df[7] = df[7].str.replace(",", "")
                        #金錢轉換型態
                        df[[3]] = df[[3]].astype('int64')
                        df[[5]] = df[[5]].astype('int64')
                        df[[7]] = df[[7]].astype('int64')
                        #刪除不必要的欄位 & 新增時間欄位
                        df = df.drop(columns=[0, 4, 6, 8], axis=1)
                        # df.insert(0,'年月份',10707)
                        df.insert(0, 0, str(CrawlYear)+"%02d" % CrawMonth)
                        df.insert(6, 'UpdateTime', now)
                        elem_list = elem_list.append(df)
                        Count +1
    data.append(elem_list)
    #print(elem_list)
    #print(data)




def insert ():
    Table = 'tblMarginspot'
    print('Start Write Data to DB')
    #data2 = pd.DataFrame(data[0],columns = ['reportDate', 'Code', 'CodeName', 'Mfinance', 'Mbearish', 'MTotal', 'UpdateTime'])
    #data2 = pd.DataFrame(data[0],columns = ['reportDate', 'Code', 'CodeName', 'Mfinance', 'Mbearish', 'MTotal', 'UpdateTime'])
    data2 = pd.DataFrame(data[0])
    data2 = data2.reset_index(drop=True)
    data2.columns = ['reportDate', 'Code', 'CodeName', 'Mfinance', 'Mbearish', 'MTotal', 'UpdateTime']
    #data2 = pd.DataFrame(data[0],columns=['更新日期','登記機關','登記機關名稱','商業統一編號','商業名稱','商業登記地址','資本額','組織型態','組織型態名稱','代表人姓名','核准設立日期','變更日期','商業狀態'])
    #print(data2)
    data2.to_sql(Table, con, if_exists='append', index=False)
    print('Write Data Done')
    print('------Crawler Work Finish------')

#df = pd.DataFrame(aaData,columns=['排名','代號','名稱','月均融資餘額','月均融資市場占有率','月均融券餘額','月均融券市場占有率','月均融資融券餘額','月均融資融券市場占有率'])

#df['月均融資餘額'] = df['月均融資餘額'].str.replace(",","")
#df['月均融券餘額'] = df['月均融券餘額'].str.replace(",","")
#df['月均融資融券餘額'] = df['月均融資融券餘額'].str.replace(",","")



#df[['排名']] = df[['排名']].astype(int)
#df[['月均融資餘額']] = df[['月均融資餘額']].astype('int64')
#df[['月均融券餘額']] = df[['月均融券餘額']].astype('int64')
#df[['月均融資融券餘額']] = df[['月均融資融券餘額']].astype('int64')



#df = df.drop(columns=['排名','月均融資市場占有率','月均融券市場占有率','月均融資融券市場占有率'])

#df = df.reset_index(drop=True)

#df2.columns =[0,1,2,3,4,5]
#print(df2)

def select():
    sqlcmd = "select distinct reportDate from Business_list..tblMarginspot group by reportDate"
    x = pd.read_sql(sqlcmd, con)
    list1.append(x)

if __name__ == '__main__':
    StartYear = 2016
    select()
    #print(list1)
    #data.append(Craw())
    Craw()
    if (Count > 0):
        insert()