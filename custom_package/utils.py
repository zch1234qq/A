from multiprocessing import Process
import pandas as pd
import os
from tkinter import *
import datetime as dt
from datetime import timedelta
import chinese_calendar as cc
import sys
import akshare as ak
import numpy as np
import pyttsx3
import json
import tushare as ts
import redis
from io import StringIO
import datetime as dt

root="D:/emdata/"
desktop="D:/emdata/工作台/"
newdesktop="D:/emdata/new工作台/"
server="D:/emdata/server/"
start=[]
core=12
timer=True
allindustry_sub_with_singleindustryFilename="allindustry_sub_with_singleindustry"
allBankFiltered="allBankFiltered"
allBankFiltered1="allBankFiltered1"
ipMap={}

codeToDrop=["酒店","餐饮","人工景点","旅游综合","自然景点","石油加工",
                "油品石化贸易","商用载货车","商用载客车","乘用车",
                "其他酒类","啤酒","软饮料","稀土","其他小金属",
                "医疗研发外包","其他医疗服务","轨交设备","其他交运设备",
                "航空装备","航天装备","军工电子","原料药","水产养殖","海洋捕捞","宠物饲料",
                "国有大型银行","股份制银行","城商行","农商行"]
class Filters():
    month_1_4=["流通市值>5e8","净利润>0 and 营业收入>3","昨收>2"]
    month_5_12=["流通市值>5e8","收盘>2"]
    maybe_st=["流通市值>5e8","净利润<=0 or 营业收入<3","昨收>2"]
    jc=["减持!=1"]
    def SmartFilter():
        now=dt.datetime.now()
        month=now.month
        if month<=4:
            return Filters.month_1_4
        else:
            return Filters.month_5_12
def toJJcode(code:str):
    codeInt=int(code)
    prefix0=""
    prefix1="SE."
    if codeInt<400000:
        prefix0="SZ"
    if codeInt>=600000:
        prefix0="SH"
    return prefix0+prefix1+code

def is_tradeday(date=None):
    if date is None:
        date=dt.datetime.now()
    if cc.is_workday(date) and dt.datetime.weekday(date)<=4:
        return True
    else:
        return False
    
def getTradeDay(today="",dot="",dt_type=False):
    if today=="":
        today=dt.datetime.now().date()
    else:
        today=dt.datetime.strptime(today,"%Y"+dot+"%m"+dot+"%d").date()
    for i in range(20):
        if not is_tradeday(today):
            today-=timedelta(days=1)
        else:
            break
    yesterday=today-timedelta(days=1)
    for i in range(20):
        if not is_tradeday(yesterday):
            yesterday-=timedelta(days=1)
        else:
            break
    lastday=yesterday-timedelta(days=1)
    for i in range(20):
        if not is_tradeday(lastday):
            lastday-=timedelta(days=1)
        else:
            break
    if dt_type==True:
        return today,yesterday,lastday
    today=dt.datetime.strftime(today,"%Y%m%d")
    yesterday=dt.datetime.strftime(yesterday,"%Y%m%d")
    lastday=dt.datetime.strftime(lastday,"%Y%m%d")
    today=today[:4]+dot+today[4:6]+dot+today[6:8]
    yesterday=yesterday[:4]+dot+yesterday[4:6]+dot+yesterday[6:8]
    lastday=lastday[:4]+dot+lastday[4:6]+dot+lastday[6:8]
    return today,yesterday,lastday

date=getTradeDay()[0]
time0915=dt.datetime.strptime(date+" 09:15:00" ,'%Y%m%d %H:%M:%S')
time0925=dt.datetime.strptime(date+" 09:25:00" ,'%Y%m%d %H:%M:%S')
time0926=dt.datetime.strptime(date+" 09:26:00" ,'%Y%m%d %H:%M:%S')
time0930=dt.datetime.strptime(date+" 09:30:00" ,'%Y%m%d %H:%M:%S')
time1130=dt.datetime.strptime(date+" 11:30:00" ,'%Y%m%d %H:%M:%S')
time1200=dt.datetime.strptime(date+" 12:00:00" ,'%Y%m%d %H:%M:%S')
time1300=dt.datetime.strptime(date+" 13:00:00" ,'%Y%m%d %H:%M:%S')
time1450=dt.datetime.strptime(date+" 14:50:00" ,'%Y%m%d %H:%M:%S')
time1500=dt.datetime.strptime(date+" 15:01:00" ,'%Y%m%d %H:%M:%S')

def isRunning()->bool:
  now=dt.datetime.now()
  time1500=dt.datetime.strptime(date+" 15:00:00" ,'%Y%m%d %H:%M:%S')
  if now<time1500:
    return True
  else:
    return False
  
def isAuto():
  auto=redisOrder.get("auto")
  auto=int(auto)
  if auto==1:
      return True
  else:
      return False
  
def exit():
    now=dt.datetime.now()
    if now>time1500 or (now>time1130 and now<time1200):
        sys.exit()
    if not is_tradeday(dt.datetime.now()):
        sys.exit()

def reflectNow()->dt.datetime:
    now=getTradeDay_list(dtype=True)[0]
    now=dt.datetime(now.year,now.month,now.day,15,30)
    return now

dontcareConcept_list=["证金持股","新股与次新股","期货概念","REITs概念","同花顺漂亮100",
    "科创次新股","核准制次新股","注册制次新股","国家大基金持股","标普道琼斯A股","分拆上市意愿",
    "创业板重组松绑","送转填权","MSCI概念","债转股(AMC概念)","深股通","首发新股","融资融券",
    "参股券商","国家大基金持股 ","年报预增","国企改革","深圳国企改革","深圳国企改革","央企国企改革",
    "上海国企改革","沪股通","深股通","股权转让","ST板块","送转填权","参股新三板","中证500成份股",
    "上证380成份股","上证180成份股","上证50样本股","沪深300样本股","举牌"]


def getHuilv():
    df=ak.fx_spot_quote()
    row0=df.loc[0,:]
    price=0.5*(row0["买报价"]+row0["卖报价"])
    am=(price/7.1)*100-100
    data={
        'code':["美元人民币"],
        'name':["美元人民币"],
        '最新价':[price],
        '涨跌幅':[am]
    }
    dfresult=pd.DataFrame(data)
    writeToMongodb(dfresult,"汇率")
    
def getZhishu():
    df=ak.stock_zh_index_spot()
    df=df.rename(columns={"代码":"code","名称":"name"})
    df=pd.DataFrame(df,columns=["code","name","涨跌幅","成交额"])
    name0="上证指数"
    name1="创业板指"
    df0=df.query("name==@name0")
    df1=df.query("name==@name1")
    df=pd.concat([df0,df1])
    writeToMongodb(df,"指数")

def writeToRedis(key:str,value:pd.DataFrame):
    dfinfo_str=value.to_json()
    try:
        r.set(key,dfinfo_str)
    except:
        pyttsx3.speak("redis 连接失败")
    
def writeToOrderRedis(key:str,value:pd.DataFrame):
    dfinfo_str=value.to_json()
    redisOrder.set(key,dfinfo_str)
    
def writeToRedis_keyvalue(key:str,value:any):
    try:
        r.set(key,value)
    except:
        pyttsx3.speak("redis 连接失败")
    
def readFromRedis_keyvalue(key:str)->float:
    data=r.get(key)
    return float(data)

def readFromRedis(key:str)->pd.DataFrame:
    data=r.get(key)
    if data==None:
        return pd.DataFrame([])
    data = data.decode('utf-8')
    data_io = StringIO(data)

    # 现在使用 StringIO 对象而不是字符串
    dfinfo = pd.read_json(data_io, dtype={'code':str})
    # dfinfo=pd.read_json(data,dtype={'code':str})
    dfinfo.index=range(dfinfo.index.__len__())
    return dfinfo

def readFromOrderRedis(key:str)->pd.DataFrame:
    data=redisOrder.get(key)
    if data==None:
        return pd.DataFrame([])
    data = data.decode('utf-8')
    data_io = StringIO(data)
    dfinfo = pd.read_json(data_io, convert_axes=False)
    # dfinfo.index=dfinfo.index.astype(str)
    return dfinfo

def readFromRedis_hash(key:str)->dict:
    hash_data = r.hgetall(key)
    hash_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in hash_data.items()}
    for key in hash_data.keys():
        try:
            hash_data[key]=float(hash_data[key])
        except:
            pass
    return hash_data

def writeToRedis_dict(key:str,value:dict):
    dict_str=json.dumps(value)
    try:
        r.set(key,dict_str)
    except:
        pyttsx3.speak("redis 连接失败")
    
def readFromRedis_dict(key:str)->dict:
    data=r.get(key)
    try:
        data = data.decode('utf-8')
        dict=json.loads(data)
    except:
        dict={}
    return dict

def readFromRedis_dict_list(keys:list[str])->list[dict]:
    datas=r.mget(keys)
    dicts:list[dict]=[]
    for data in datas:
        try:
            data = data.decode('utf-8')
            res=json.loads(data)
        except:
            res={}
        dicts.append(res)
    return dicts

def writeToMongodb(dfinfo:pd.DataFrame,filename:str,dir:str="stockInfo"):
    dfinfo=dfinfo.to_json()
    try:
        database[dir].update_one({"filename":filename},{"$set":{"df":dfinfo,"filename":filename}},upsert=True)
    except:
        pyttsx3.speak("mongodb 连接失败")

def writeToMongodb_list(dfinfo:pd.DataFrame,filename:str,dir:str="stockInfo"):
    try:
        database[dir].update_one({"filename":filename},{"$set":{"df":dfinfo,"filename":filename}},upsert=True)
    except:
        pyttsx3.speak("mongodb 连接失败")


def writeToMongodb_record(dfinfo:pd.DataFrame,filename:str,dir:str="stockInfo"):
    dfinfo=dfinfo.to_json(orient="records")
    try:
        database[dir].update_one({"filename":filename},{"$set":{"df":dfinfo,"filename":filename}},upsert=True)
    except:
        pyttsx3.speak("mongodb 连接失败")

def readFromMongodb(filename:str,dir:str="stockInfo"):
    doc=database[dir].find_one({"filename":filename})
    if doc==None:
        return pd.DataFrame([])
    dfstr=doc["df"]
    data=json.loads(dfstr)
    dfinfo=pd.DataFrame.from_dict(data)
    dfinfo.index=range(dfinfo.index.__len__())
    return dfinfo

def stockToIndustry_sub(dfdata:pd.DataFrame):
    dfdata=pd.DataFrame(dfdata,columns=["industry_sub","custom"])
    dfdata=dfdata.sort_values(by="industry_sub")
    lastIndustry=""
    industrys=[]
    for index in dfdata.index:
        industry=dfdata.loc[index,"industry_sub"]
        if industry!=lastIndustry:
            industrys.append(index)
            lastIndustry=industry
    dfdata=pd.DataFrame(dfdata,index=industrys)
    dfdata=dfdata.rename(columns={"industry_sub":"name"})
    dfdata=reindex(dfdata)
    writeToMongodb(dfdata,"bank")

def dropColumns(df:pd.DataFrame,columns:list):
    for col in columns:
        try:
            df=df.drop(columns=[col])
        except:
            pass
    return df

def to_tscode(code:str):
    codeint=int(code)
    if codeint>=600000:
        return code+".SH"
    else:
        return code+".SZ"

def start_timer():
    if not timer:
        return
    global start
    start.append(dt.datetime.now())

def end_timer():
    if not timer:
        return
    global start
    if start.__len__()==0:
        return
    end=dt.datetime.now()
    print((end-start[-1]).total_seconds())
    start.remove(start[-1])

def format_minute(frame3:pd.DataFrame):
    xints=frame3.columns.to_series().astype(np.int16)
    xints=xints.sort_values()
    xstrs=xints.astype(str)
    frame3=pd.DataFrame(frame3,columns=xstrs)
    return frame3

class ansys:
    open=True
    def __init__(self) -> None:
        self.start:dt.datetime=0
    def begin(self):
        self.start=dt.datetime.now()
    def print(self):
        if not ansys.open:
            return
        end=dt.datetime.now()
        print((end-self.start).total_seconds())
        return (end-self.start).total_seconds()
def to_stock(df:pd.DataFrame,filename="stock"):
    df.to_csv(filename+".csv",index=None,encoding="utf_8_sig")

def set_zhuan(df:pd.DataFrame)->pd.DataFrame:
    dfcstock=pd.read_csv(root+"ConvertibleBond/list.csv",dtype={"code":str})
    dfcstock["zhuan"]=1
    dfcstock.index=dfcstock["code"]
    df.index=df["code"]
    try:
        df.drop(columns=["zhuan"],inplace=True)
    except:
        pass
    try:
        df.drop(columns=["债券代码"],inplace=True)
    except:
        pass
    dfcstock=pd.DataFrame(dfcstock,columns=["zhuan","债券代码"])
    df=df.join(dfcstock)
    reindex(df)
    return df
def while_read(name:str):
    df=None
    while(True):
        try:
            df=pd.read_csv(name,dtype={"code":str})
            return df
        except:
            continue
def while_write(df:pd.DataFrame,name:str,index=""):
    while(True):
        try:
            if index==None:
                df.to_csv(name,encoding="utf_8_sig",index=None)
            else:
                df.to_csv(name,encoding="utf_8_sig")
            return
        except:
            continue
def set_industry(dflist:pd.DataFrame)->pd.DataFrame:
    for index in dflist.index:
        code=dflist.loc[index,"code"]
        dfindustry=pd.read_csv(root+"stockindustry/"+code+".csv")
        dflist.loc[index,"industry"]=dfindustry.loc[0,"industry"]
    return dflist

def set_industry_sub(dflist:pd.DataFrame)->pd.DataFrame:
    for index in dflist.index:
        code=dflist.loc[index,"code"]
        dfindustry=pd.read_csv(root+"stockindustry_sub/"+code+".csv")
        dflist.loc[index,"industry_sub"]=dfindustry.loc[0,"industry_sub"]
    return dflist

def set_industry_em(dflist:pd.DataFrame)->pd.DataFrame:
    for index in dflist.index:
        code=dflist.loc[index,"code"]
        dfindustry=pd.read_csv(root+"stockindustry_em/"+code+".csv")
        dflist.loc[index,"industry"]=dfindustry.loc[0,"industry"]
    return dflist

def read_finished()->pd.DataFrame:
    filename=root+"data/list.csv"
    dflist=pd.read_csv(root+"data/list.csv",dtype={"code":str})
    dflist.index=dflist["code"]
    dffinished=pd.read_csv(root+"公告/财报季/finished.csv",index_col="code",dtype={"code":str})
    dflist=pd.DataFrame(dflist,index=dffinished.index)
    dflist.index=range(dflist.index.__len__())
    dflist=formatDataframeCode(dflist)
    return dflist

def read_list(index_code=False)->pd.DataFrame:
  filename=root+"data/list.csv"
  dflist=pd.read_csv(filename,dtype={"code":str})
  dflist=formatDataframeCode(dflist)
  if index_code:
    dflist=dflist.set_index("code")
  return dflist

def read_industry_sub_sorted()->pd.DataFrame:
  banks=[]
  dflist=read_list()
  for index in dflist.index:
    bank=dflist.loc[index,"industry_sub"]
    if bank not in banks:
        banks.append(bank)
  dfbanks=pd.DataFrame(banks,columns=["code"])
  dfbanks.set_index("code",inplace=True)
  return dfbanks

def write_list(dflist:pd.DataFrame):
    filename=root+"data/list.csv"
    if "code" in dflist.columns:
      dflist.to_csv(filename,index=None,encoding="utf_8_sig")
    else:
      dflist.to_csv(filename,encoding="utf_8_sig")
      
def read_list_all(index_code=False)->pd.DataFrame:
    filename=root+"data/list_all.csv"
    dflist=pd.read_csv(filename,dtype={"code":str})
    dflist=formatDataframeCode_all(dflist)
    if index_code:
        dflist=dflist.set_index("code")
    return dflist

def read_list_em()->pd.DataFrame:
    dflist=pd.read_csv(root+"data/list_em.csv",dtype={"code":str})
    return dflist

def read_cb()->pd.DataFrame:
    dflist=pd.read_csv(root+"data/list.csv",dtype={"code":str})
    formatDataframeCode(dflist)
    dflist=dflist.query("zhuan==1")
    reindex(dflist)
    return dflist

def read_qclist(uptime_min=0)->pd.DataFrame:
    dflist=pd.read_csv(root+"data/抢筹列表.csv",dtype={"code":str})
    dflist=dflist.query("uptime>@uptime_min")
    formatDataframeCode(dflist)
    reindex(dflist)
    return dflist

def read_uptime(bank:str):
    dfhist=pd.read_csv(root+'uptime/'+bank+'.csv')
    return dfhist

def read_upfocus(bank:str):
    dfhist=pd.read_csv(root+'uptime/'+bank+'_focus.csv')
    return dfhist

def read_downtime(bank:str):
    dfhist=pd.read_csv(root+'downtime/'+bank+'.csv')
    return dfhist

def read_yesterday_money():
    dfhist=readFromMongodb("money")
    return dfhist

def read_yesterday_absmoney():
    dfhist=readFromMongodb("absmoney")
    return dfhist

def read_hist(code:str,index_day=False)->pd.DataFrame:
    dfhist=pd.read_csv(root+'data/'+code+'.csv',dtype={"code":str})
    dfhist["股票代码"]=code
    if index_day:
      dfhist=dfhist.set_index("日期")
    return dfhist

def read_index(code:str,rename=False)->pd.DataFrame:
    dfhist=pd.read_csv(root+'index/'+code+'.csv')
    if rename:
      columns={}
      for col in dfhist.columns:
        if col=="date":
            continue
        columns[col]=code+"_"+col
      dfhist=dfhist.rename(columns=columns)
    dfhist=dfhist.set_index("date")
    return dfhist

def read_hist_attach(code:str)->pd.DataFrame:
    dfhist=pd.read_csv(root+'data_attach/'+code+'.csv',dtype={"code":str})
    return dfhist

def write_hist(df:pd.DataFrame,code:str):
    df.to_csv(root+'data/'+code+'.csv',encoding="utf_8_sig")

def write_hist_attach(df:pd.DataFrame,code:str):
    df.to_csv(root+'data_attach/'+code+'.csv',encoding="utf_8_sig")

def read_hist_AI_market()->pd.DataFrame:
    dfhist=pd.read_csv(root+'AI/hist_market/market.csv')
    return dfhist

def read_hist_AI_market_attach()->pd.DataFrame:
    dfhist=pd.read_csv(root+'AI/hist_market_attach/market.csv')
    return dfhist

def write_hist_AI_market_attach(dfhist:pd.DataFrame)->pd.DataFrame:
    dfhist.to_csv(root+'AI/hist_market_attach/market.csv',encoding="utf_8_sig")

def read_hist_AI_bank(code:str)->pd.DataFrame:
    dfhist=pd.read_csv(root+f'AI/hist_bank/{code}.csv')
    return dfhist

def read_hist_AI_bank_attach(code:str)->pd.DataFrame:
    dfhist=pd.read_csv(root+f'AI/hist_bank_attach/{code}.csv')
    return dfhist

def read_caiwu(code:str)->pd.DataFrame:
    dfhist=pd.read_csv(root+'财务/'+code+'.csv')
    return dfhist

def read_hist_week(code:str)->pd.DataFrame:
    dfhist=pd.read_csv(root+'data_week/'+code+'.csv')
    return dfhist

def static_read_hist(code:str)->pd.DataFrame:
    return pd.read_csv(root+"data/"+code+".csv")

def read_hist_new(code:str)->pd.DataFrame:
    dfhist=pd.read_csv(root+'newprice/'+code+'.csv')
    return dfhist

def read_hist_moneyFlow(code:str)->pd.DataFrame:
    dfhist=pd.read_csv(root+'大单净量/'+code+'.csv')
    return dfhist

def read_hist_industry(industry:str)->pd.DataFrame:
    dfhist=pd.read_csv(root+'data/'+industry+'.csv')
    return dfhist

def read_hist_concept(concept:str)->pd.DataFrame:
    filename=root+'concept/record/hist/'+concept+'.csv'
    if not os.path.exists(filename):
        return pd.DataFrame([])
    dfhist=pd.read_csv(filename)
    return dfhist
def read_cor_concept(name:str)->pd.DataFrame:
    filename=root+"concept相关性/"+name+".csv"
    dfcor=pd.read_csv(filename)
    try:
        dfcor=formatDataframeCode_all(dfcor)
    except:
        pass
    return dfcor
def read_cor_industry(name:str)->pd.DataFrame:
    filename=root+"industry相关性/"+name+".csv"
    dfcor=pd.read_csv(filename)
    try:
        dfcor=formatDataframeCode_all(dfcor)
    except:
        pass
    return dfcor

def read_allindustry()->pd.DataFrame:
    dfhist=pd.read_csv(root+'industry/allindustry.csv',dtype={"code":str})
    return dfhist
def read_filteredIndustry()->pd.DataFrame:
    return pd.read_csv(desktop+"filter/scanIndustry/"+'stock.csv')

def read_filteredIndustry_sub()->pd.DataFrame:
    return pd.read_csv(desktop+"filter/scanIndustry_sub/"+'stock.csv')

def read_stock_FilteredIndustry_sub_ByUptime()->pd.DataFrame:
    dffiltered=pd.read_csv(desktop+"uptime/industry_sub/"+'stock.csv')
    dflist=read_list()
    dffiltered=dffiltered.set_index("name")
    dffiltered["selected"]=1
    dflist=dflist.join(dffiltered["selected"],on="industry_sub")
    dflist=dflist.query("selected==1")
    return dflist

def read_stock_FilteredAll_Industry_sub_ByUptime()->pd.DataFrame:
    dffiltered=pd.read_csv(desktop+"uptime/all_industry_sub/"+'stock.csv')
    dflist=read_list()
    dffiltered=dffiltered.set_index("code")
    dffiltered["selected"]=1
    dflist=dflist.join(dffiltered["selected"],on="industry_sub")
    dflist=dflist.query("selected==1")
    return dflist

def read_stock_FilteredAll_Industry_sub_ByUptime_temp()->pd.DataFrame:
    dffiltered=pd.read_csv(desktop+"uptime/temp/"+'stock.csv')
    dflist=read_list()
    dffiltered=dffiltered.set_index("code")
    dffiltered["selected"]=1
    dflist=dflist.join(dffiltered["selected"],on="industry_sub")
    dflist=dflist.query("selected==1")
    return dflist

def read_stock_FilteredAll_Industry_sub_ByUpDown()->pd.DataFrame:
    dffiltered=pd.read_csv(desktop+"updown/all_industry_sub/"+'stock.csv')
    dflist=read_list()
    dffiltered=dffiltered.set_index("code")
    dffiltered["selected"]=1
    dflist=dflist.join(dffiltered["selected"],on="industry_sub")
    dflist=dflist.query("selected==1")
    dflist=dflist.drop(columns=["selected"])
    return dflist

def read_allindustry_sub()->pd.DataFrame:
    dfhist=pd.read_csv(root+'industry_sub/allindustry_sub_with_singleindustry.csv',dtype={"code":str})
    return dfhist
def read_stock_filteredByFilteredIndustry()->pd.DataFrame:
    dflist=read_list()
    # dflist.index=dflist["industry"]
    dffilter=read_filteredIndustry()
    dffilter=dffilter.set_index("name")
    dffilter["selected"]=1
    dflist=dflist.join(dffilter["selected"],on="industry")
    dflist=dflist.query("selected==1")
    dflist=dflist.drop(columns=["selected"])
    reindex(dflist)
    return dflist
def read_stock_filteredByFilteredIndustry_sub()->pd.DataFrame:
    dflist=read_list()
    # dflist.index=dflist["industry"]
    dffilter=read_filteredIndustry_sub()
    dffilter=dffilter.set_index("name")
    dffilter["selected"]=1
    dflist=dflist.join(dffilter["selected"],on="industry_sub")
    dflist=dflist.query("selected==1")
    dflist=dflist.drop(columns=["selected"])
    reindex(dflist)
    return dflist
def read_allindustry_all_sub()->pd.DataFrame:
    dfhist=pd.read_csv(root+'industry_sub/allindustry_all.csv',dtype={"code":str})
    return dfhist

def getDftoday(offset=0)->pd.DataFrame:
  days=getTradeDay_list(length=200,dot="-")[offset:]
  dflist=read_list(index_code=True)
  today=days[0]
  yesterday=days[1]
  dftemplet=read_hist("000001")
  days_templet=dftemplet["日期"]
  if not today in days_templet.tolist():
    dfinfo=readFromRedis("info")
    dfinfo=dfinfo.set_index("code")
    dftoday=ak.stock_zh_a_spot_em()
    dftoday=dftoday.rename(columns={"代码":"code"})
    dftoday=pd.DataFrame(dftoday,columns=["code","最高","最低","最新价","今开"])
    dftoday=dftoday.join(dfinfo["rmoney"],on="code")
    dftoday=dftoday.set_index("code")
  else:
    dftoday=pd.DataFrame([])
    for code in dflist.index:
      dfhist=read_hist(code,index_day=True)
      try:
        dftoday.loc[code,"开盘"]=dfhist.loc[today,"开盘"]
        dftoday.loc[code,"最高"]=dfhist.loc[today,"最高"]
        dftoday.loc[code,"最低"]=dfhist.loc[today,"最低"]
        dftoday.loc[code,"rmoney"]=dfhist.loc[today,"成交额"]/dfhist.loc[yesterday,"成交额"]
      except:
        continue
    dftoday["今开"]=dftoday["开盘"]
  return dftoday

def read_allbank()->pd.DataFrame:
    dfconcept=read_allconcept()
    dfindustry_sub=read_allindustry_sub()
    dfallbank=concat(dfindustry_sub,dfconcept)
    return dfallbank

def read_subIndustryOfIndustry(industry:str):
    return pd.read_csv(root+"subIndustryOfIndustry/"+industry+".csv")

def read_con_industry(industry:str)->pd.DataFrame:
    dfcon=pd.read_csv(root+'industry_sub/'+industry+'.csv',dtype={"code":str})
    return dfcon

def read_con_bank(industry:str)->pd.DataFrame:
    dfcon=readFromMongodb(industry,"allbank")
    return dfcon

def read_con_industry_sub(industry:str)->pd.DataFrame:
    dfcon=readFromMongodb(industry,"industry_sub")
    return dfcon

def read_con_industry_sub_zt2(industry:str)->pd.DataFrame:
    dfcon=readFromMongodb(industry,"industry_sub_zt2")
    return dfcon

def read_con_concept(concept:str)->pd.DataFrame:
    dfcon=pd.read_csv(root+'concept/'+concept+'.csv',dtype={"code":str})
    return dfcon

def read_allconcept()->pd.DataFrame:
    dfhist=pd.read_csv(root+'concept/allconcept.csv',dtype={"code":str})
    return dfhist

def read_allconcept_all()->pd.DataFrame:
    dfhist=pd.read_csv(root+'concept/concept_all.csv',dtype={"code":str})
    return dfhist

def read_fs(code:str)->pd.DataFrame:
    dffs=pd.read_csv(root+'分时数据/'+code+'.csv')
    return dffs

def read_fs5(code:str)->pd.DataFrame:
    dffs=pd.read_csv(root+'分时数据5/'+code+'.csv')
    return dffs

def read_newfs(code:str)->pd.DataFrame:
    dffs=pd.read_csv(root+'new分时数据/'+code+'.csv')
    return dffs

def read_attention(): 
    filename=root+"attention/stock.csv"
    dfattention=pd.read_csv(filename,dtype={"code":str})
    formatDataframeCode(dfattention)
    dfattention=set_industry(dfattention)
    dfattention=set_zhuan(dfattention)
    dfattention.to_csv(filename,encoding="utf_8_sig",index=None)
    return dfattention

def read_stock(folder="",index_col=None)->pd.DataFrame:
    if folder!="" and folder[-1]!="/":
        folder+="/"
    dfstock=pd.read_csv("stock.csv",dtype={"code":str})
    formatDataframeCode(dfstock)
    if index_col!=None:
      dfstock=dfstock.set_index(index_col)
    return dfstock

def calProfit(dfpool1:pd.DataFrame,base):
    if base==0:
        dfpool1.to_csv('stock.csv',index=None,encoding='utf_8_sig')
        return dfpool1
    else:
        dfinfo:pd.DataFrame=ak.stock_zh_a_spot_em()
        dfinfo.index=dfinfo["代码"]
        dfinfo.rename(columns={"涨跌幅":"profit"},inplace=True)
        dfpool1.index=dfpool1["code"]
        dfpool1=dfpool1.join(dfinfo["profit"])
        reindex(dfpool1)
        for index in dfpool1.index:
            code=dfpool1.loc[index,"code"]
            dfhist=pd.read_csv(root+"data/"+code+".csv")
            if base>=2:# and dfhist.loc[base-1,"涨跌幅"]>0:
                profit=dfhist.loc[base-1,"涨跌幅"]+dfhist.loc[base-2,"涨跌幅"]
            else:
                profit=dfhist.loc[base-1,"涨跌幅"]
            dfpool1.loc[index,"profit"]=profit
        dfpool1.sort_values(by="profit",ascending=False,inplace=True)
    avg=dfpool1["profit"].mean()
    dfpool1["avg_profit"]=avg
    print(avg)
    date,_=getNextTradeday(dot="-",base=base)
    try:
        dfpool1.to_csv('record/'+date+'.csv',index=None,encoding='utf_8_sig')
    except:
        pass
    return dfpool1

def commandLineArgvs():
    argvs=sys.argv
    return argvs

def getArgv_base():
    try:
        base=int(commandLineArgvs()[1])
    except:
        base=0
    return base

def isAStock(code:str)->bool:
    codeint=int(code)
    if continueAnyList([
        codeint<300000,
        codeint>600000 and codeint<680000,
    ]):
        return True
    else:
        return False

def reindex(dataframe:pd.DataFrame):
    dataframe.index=range(dataframe.index.__len__())
    return dataframe

def continueList(conditionlist:list[bool])->bool:
    for con in conditionlist:
        if not con:
            return True
    return False

def continueAnyList(conditionlist:list[bool])->bool:
    for con in conditionlist:
        if con:
            return True
    return False

def clamp(a,b,c):
    if a<b:
        a=b
    if a>c:
        a=c
    return a

def threadSBlock(threads:list[Process]):
    for thread in threads:
        thread.join()

def threadsBlock(threads:list[Process]):
    for thread in threads:
        thread.join()



def isFileNew(filetosave:str,hour:int=4):
    now=reflectNow()
    try:
        modifyTime=os.path.getmtime(filetosave)
        modify_dt=dt.datetime.fromtimestamp(modifyTime)
    except:
        return False
    if modify_dt.hour<15 and modify_dt.hour>=9 and is_tradeday(now):
        return False
    if (not is_tradeday(now)) and (not is_tradeday(modify_dt)):
        return True
    if now-modify_dt<hour*timedelta(hours=1):
        return True
    else:
        return False
    
def isFileNew_slow(filetosave:str,hour:int=4):
    now=reflectNow()
    try:
        modifyTime=os.path.getmtime(filetosave)
        time_dt=dt.datetime.fromtimestamp(modifyTime)
    except:
        return False
    if now-time_dt<hour*timedelta(hours=1):
        return True
    else:
        return False
    
def isExistAndNew(filename:str,hour:int=4,slow=False):
    if slow:
        if os.path.exists(filename) and isFileNew_slow(filename,hour):
            return True
        else:
            return False
    else:
        if os.path.exists(filename) and isFileNew(filename,hour):
            return True
        else:
            return False

def getListToWrite(dfconcept:pd.DataFrame,dirname:str,colname="name",hour=6,slow=False,check="")->pd.DataFrame:
    indexTodrop=[]
    for index in dfconcept.index:
        code=dfconcept.loc[index,"code"]
        name=dfconcept.loc[index,colname]
        haveOdd=False
        for c in name:
            if c=="/":
                haveOdd=True
        if haveOdd:
            continue
        filetosave=dirname+"/"+name+".csv"
        checkOk=False
        try:
            df=read_hist(code)
            if check=="" or (check in df.columns.to_list()):
                checkOk=True
            else:
                pass
        except:
            pass
        if slow:
            if os.path.exists(filetosave) and isFileNew_slow(filetosave,hour) and checkOk:
                indexTodrop.append(index)
                continue
        else:
            if os.path.exists(filetosave) and isFileNew(filetosave,hour) and checkOk:
                indexTodrop.append(index)
                continue

    dfconcept=dfconcept.drop(index=indexTodrop)
    reindex(dfconcept)
    return dfconcept

def getListToWrite_just_exist(dfconcept:pd.DataFrame,dirname:str,colname="name")->pd.DataFrame:
    if dirname[1]!=":":
        dirname=root+dirname
    indexTodrop=[]
    for index in dfconcept.index:
        name=dfconcept.loc[index,colname]
        filetosave=dirname+"/"+name+".csv"
        if os.path.exists(filetosave):
            indexTodrop.append(index)
    dfconcept=dfconcept.drop(index=indexTodrop)
    reindex(dfconcept)
    return dfconcept

def getDatetimeBeignEnd():
    now=dt.dt.datetime.now()
    date=dt.dt.datetime.now().strftime("%Y%m%d")
    begintime=dt.dt.datetime.strptime(date+" 09:30:00" ,'%Y%m%d %H:%M:%S')
    endtime=dt.datetime.strptime(date+" 15:00:00" ,'%Y%m%d %H:%M:%S')
    return begintime,endtime

def getDatetimeRest():
    now=dt.datetime.now()
    date=dt.datetime.now().strftime("%Y%m%d")
    rest0=dt.datetime.strptime(date+" 11:30:00" ,'%Y%m%d %H:%M:%S')
    rest1=dt.datetime.strptime(date+" 13:00:00" ,'%Y%m%d %H:%M:%S')
    return rest0,rest1

def getStrDay():
    now=dt.datetime.now()
    today=now.strftime("%Y%m%d")
    return today

def timeBlock():
    begintime,endtime=getDatetimeBeignEnd()
    while(dt.datetime.now()<begintime):
        a=1
    
def areTimeout():
    begintime,endtime=getDatetimeBeignEnd()
    return dt.datetime.now()>endtime

def areTimerest():
    rest0,rest1=getDatetimeRest()
    return dt.datetime.now()>rest0 and dt.datetime.now()<rest1

def getNextTradeday(dt_type=False,dot="",base=0):
    next=dt.datetime.now()
    if next.hour>15:
        next+=dt.timedelta(hours=9)
    next_str=dt.datetime.strftime(next,"%Y%m%d")
    next=dt.datetime.strptime(next_str,"%Y%m%d")
    # next+=dt.timedelta(days=1)
    for i in range(20):
        if is_tradeday(next.date()):
            break
        else:
            next+=timedelta(days=1)
    if base<=0:
        dir=1
    else:
        dir=-1
    while(base>0):
        next=next+dir*timedelta(days=1)
        if is_tradeday(next.date()):
            base-=1

    next2=next+dt.timedelta(days=1)
    for i in range(20):
        if not is_tradeday(next2.date()):
            next2+=timedelta(days=1)
        else:
            break
    if dt_type:
        return next,next2
    next_str=dt.datetime.strftime(next,"%Y"+dot+"%m"+dot+"%d")
    next2_str=dt.datetime.strftime(next2,"%Y"+dot+"%m"+dot+"%d")
    return next_str,next2_str


def getTradeDay_list(length=1,dtype=False,today="",dot="")->list[dt.date]|list[str]:
    if today=="":
        today_dt=dt.datetime.now().date()
    else:
        today_dt=dt.datetime.strptime(today,"%Y%d%m").date()
    # today_dt=dt.date(today_dt)
    results=[]
    while(True):
        if is_tradeday(today_dt):
            results.append(today_dt)
            length-=1
        if length==0:
            break
        today_dt-=dt.timedelta(days=1)
    if not dtype:
        result_dts=[]
        for i in range(results.__len__()):
            result_dts.append(dt.datetime.strftime(results[i],'%Y'+dot+'%m'+dot+'%d'))
        return result_dts
    else:
        return results

def getTradeDay_list_excludeToday(length=1,dtype=False,today="",dot="")->list[dt.date]|list[str]:
    if today=="":
        today_dt=dt.datetime.now()
    else:
        today_dt=dt.datetime.strptime(today,"%Y%d%m")
    today_dt-=dt.timedelta(minutes=60*15)
    today_dt=today_dt.date()
    results=[]
    while(True):
        if is_tradeday(today_dt):
            results.append(today_dt)
            length-=1
        if length==0:
            break
        today_dt-=dt.timedelta(days=1)
    if not dtype:
        result_dts=[]
        for i in range(results.__len__()):
            result_dts.append(dt.datetime.strftime(results[i],'%Y'+dot+'%m'+dot+'%d'))
        return result_dts
    else:
        return results

def getTradeDay_list_next(length=1,dtype=False,today="",dot="")->list[dt.date]|list[str]:
    if today=="":
        today_dt=dt.datetime.now().date()
    else:
        today_dt=dt.datetime.strptime(today,"%Y%d%m").date()
    # today_dt=dt.date(today_dt)
    results=[]
    while(True):
        if is_tradeday(today_dt):
            results.append(today_dt)
            length-=1
        if length==0:
            break
        today_dt+=dt.timedelta(days=1)
    if not dtype:
        result_dts=[]
        for i in range(results.__len__()):
            result_dts.append(dt.datetime.strftime(results[i],'%Y'+dot+'%m'+dot+'%d'))
        return result_dts
    else:
        return results
def convertCodeToJuejin(code:str)->str:
    codeint=int(code)
    prefix=""
    if codeint>=600000:
        prefix="SHSE."
    if codeint<600000:
        prefix="SZSE."
    return prefix+code

def formatCodeint(codeint:int)->str:
    codestr=str(int(codeint))
    prefixs=["","0","00","000","0000","00000"]
    length=len(codestr)
    codestr=prefixs[6-length]+codestr
    return codestr

def predict(dfempty:pd.DataFrame)->pd.DataFrame:
  if "预低" in dfempty.columns:
    dfempty=dfempty.drop(columns=["预低"])
  dfpredict=readFromMongodb("predict")
  dfpredict=dfpredict.set_index("code")
  dfempty=dfempty.join(dfpredict["预低"],on="code")
  return dfempty

def formatDataframeCode(dflist:pd.DataFrame)->pd.DataFrame:
    indexTodrop=[]
    for index in dflist.index:
        code=dflist.loc[index,"code"]
        codeint=int(code)
        if (codeint>300000 and codeint<600000) or codeint>680000:
            indexTodrop.append(index)
        codestr=formatCodeint(code)
        dflist.loc[index,"code"]=codestr
    dflist.drop(index=indexTodrop,inplace=True)
    return dflist
    
def formatDataframeCode_all(dflist:pd.DataFrame)->pd.DataFrame:
    indexTodrop=[]
    for index in dflist.index:
        code=dflist.loc[index,"code"]
        codestr=formatCodeint(code)
        dflist.loc[index,"code"]=codestr
    dflist.drop(index=indexTodrop,inplace=True)
    return dflist
    
def removeDRGDIP(dfconcept:pd.DataFrame):
    try:
        dfconcept.rename(columns={"概念名称":"name"},inplace=True)
    except:
        pass
    for index in dfconcept.index:
        name=dfconcept.loc[index,"name"]
        if name=="DRG/DIP" or name=="DRG/DIP概念":
            dfconcept.loc[index,"name"]="DRGDIP"
            break
    reindex(dfconcept)

def removeRepeat(df:pd.DataFrame)->pd.DataFrame:
    indexToDrop=[]
    for index in df.index:
        code0=df.loc[index,"code"]
        for j in range(index+1,len(df.index)):
            code1=df.loc[j,"code"]
            if code0==code1:
                indexToDrop.append(index)
                break
    df=df.drop(index=indexToDrop)
    df.index=range(len(df.index))
    return df

def sortByIndustry(dflist:pd.DataFrame,inemdata=False):
    dflist=set_industry(dflist)
    dfAllIndustry=pd.read_csv(root+"industry/allindustry.csv")
    dfempty=pd.DataFrame([])
    for industry_name in dfAllIndustry["name"]:
        temp=dflist.query("industry==@industry_name")
        dfempty=pd.concat([dfempty,temp])
    return dfempty

def sortByIndustry_sub(dflist:pd.DataFrame,inemdata=False):
    list_industry_sub=[]
    list_industry=[]
    for index in dflist.index:
        industry_sub=dflist.loc[index,"industry_sub"]
        if not industry_sub in list_industry_sub:
            list_industry_sub.append(industry_sub)
    
    for index in dflist.index:
        industry=dflist.loc[index,"industry"]
        if not industry in list_industry:
            list_industry.append(industry)
    if not "industry_sub" in dflist.columns:
      dflist=set_industry_sub(dflist)
      
    dfempty=pd.DataFrame([])
    for industry_name in list_industry:
        dfempty_sub=pd.DataFrame([])
        dflist_industry=dflist.query("industry==@industry_name")
        for industry_sub_name in list_industry_sub:
            temp=dflist_industry.query("industry_sub==@industry_sub_name")
            dfempty_sub=pd.concat([dfempty_sub,temp])
        if dfempty_sub.index.__len__()==0:
            dfempty_sub=dflist_industry
        dfempty=pd.concat([dfempty,dfempty_sub])
    dflist=dfempty
    return dfempty

def sortByIndustry_em(dflist:pd.DataFrame,inemdata=False):
    # left="../emdata/"
    # if inemdata:
    #     left=root
    formatDataframeCode(dflist)
    for index in dflist.index:
        code=dflist.loc[index,"code"]
        codeindustry=pd.read_csv(root+"stockindustry_em/"+code+".csv")
        industry=codeindustry.loc[0,"industry"]
        dflist.loc[index,"industry"]=industry
    dfAllIndustry=pd.read_csv(root+"industry_em/allindustry.csv")
    dfempty=pd.DataFrame([])
    for industry_name in dfAllIndustry["name"]:
        temp=dflist.query("industry==@industry_name")
        dfempty=pd.concat([dfempty,temp])
    return dfempty
    
def remind():
    dfdate=pd.read_csv(root+"美联储数据发布提示/date.csv",dtype=str)
    nextday,nextday2=getNextTradeday()
    for index in dfdate.index:
        date=dfdate.loc[index,'date']
        # date=date[:-6]
        try:
            date_dt=dt.datetime.strptime(date,"%Y/%m/%d %H:%M")
        except:
            date_dt=dt.datetime.strptime(date,"%Y/%m/%d")
        hour=date_dt.hour
        if hour<15:
            date_dt-=dt.timedelta(days=1)
        for i in range(20):
            if not is_tradeday(date_dt):
                date_dt-=dt.timedelta(days=1)
            else:
                break
        date_str=dt.datetime.strftime(date_dt,"%Y%m%d")
        name=dfdate.loc[index,"name"]
        if nextday==date_str:
            pyttsx3.speak("下个交易日是"+date_str)
            pyttsx3.speak("风险提示"+name+"数据即将发布")
        if nextday2==date_str:
            pyttsx3.speak("后天是"+date_str)
            pyttsx3.speak("风险提示"+name+"数据即将发布")
            pyttsx3.speak("注意减仓")
        date_dt=dt.datetime.strptime(date_str,"%Y%m%d")
        today,_,_=getTradeDay()
        today_dt=dt.datetime.strptime(today,"%Y%m%d")
        if today_dt>=date_dt:
            pyttsx3.speak("第"+str(index+1)+"行"+"数据"+name+"过期,请更新")
        
def getSinaCode(code:str)->str:
    codeint=int(code)
    if codeint<600000:
        return "sh"+code
    else:
        return "sz"+code
        
def mix(va:float,vb:float,a:int,b:int)->float:
    c=a+b
    return va*a/c+vb*b/c

def filterZTByIndustryWhenOpen(df:pd.DataFrame)->pd.DataFrame:
    dfinfo=ak.stock_zh_a_spot_em()
    dfinfo.index=dfinfo["代码"]
    dflist=read_list()
    dflist.index=dflist["code"]
    dflist=dflist.join(dfinfo["今开"])
    dflist["今开涨幅"]=dflist["今开"]/dflist["前收"]*100-100
    dflist=dflist.query("今开涨幅>9.5")
    industrys=[]
    for index in dflist.index:
        industry=dflist.loc[index,"industry"]
        have=False
        for ind in industrys:
            if industry==ind:
                have=True
                break
        if have:
            continue
        else:
            industrys.append(industry)
    for industry_name in industrys:
        dfempty=pd.DataFrame([])
        temp=df.query("industry==@industry_name")
        dfempty=pd.concat([dfempty,temp])
    df=dfempty.copy()
    reindex(df)
    return df

def filterZTByConceptWhenOpen(df:pd.DataFrame)->pd.DataFrame:
    dfinfo=ak.stock_zh_a_spot_em()
    dfinfo.index=dfinfo["代码"]
    dflist=read_list()
    dflist.index=dflist["code"]
    dflist=dflist.join(dfinfo["今开"])
    dflist["今开涨幅"]=dflist["今开"]/dflist["昨收"]*100-100
    dflist=dflist.query("今开涨幅>9.5")
    concepts=[]
    for index in dflist.index:
        code=dflist.loc[index,"code"]
        dfconcept=pd.read_csv(root+"stockconcept/"+code+".csv")
        concepts_=dfconcept["concept"].tolist()
        have=False
        for con_ in concepts_:
            for con in concepts:
                if con_==con:
                    have=True
                    break
            if have:
                continue
            else:
                concepts.append(con_)
    stockconcepts=[]
    df=reindex(df)
    for index in df.index:
        code=df.loc[index,"code"]
        dfconcept=pd.read_csv(root+"stockconcept/"+code+".csv")
        stockconcepts.append(dfconcept["concept"].tolist())
    indexToSave=[]
    for index in df.index:
        equal=False
        equal_count=0
        concepts_=stockconcepts[index]
        for concept_name in concepts:
            for con in concepts_:
                if con==concept_name:
                    equal_count+=1
                if equal_count>=2:
                    equal=True
                    break
            if equal:
                indexToSave.append(index)
                break
    df=pd.DataFrame(df,index=indexToSave)
    reindex(df)
    return df

def concat(df0:pd.DataFrame,df1:pd.DataFrame)->pd.DataFrame:
    df0=reindex(df0)
    df1.index=range(df0.index.__len__(),df0.index.__len__()+df1.index.__len__())
    df=pd.concat([df0,df1])
    indexToDrop=[]
    for index in df.index:
        code0=df.loc[index,"code"]
        for j in range(index+1,len(df.index)):
            code1=df.loc[j,"code"]
            if code0==code1:
                indexToDrop.append(j)
                break
    df=df.drop(index=indexToDrop)
    df=reindex(df)
    return df

def filterByLocalIndustry(df:pd.DataFrame):
    dflist.index=dflist["industry"]
    filter=pd.read_csv(root+"../工作台/filter/scanIndustry/stock.csv")
    filter.set_index("name",inplace=True)
    filter["selected"]=1
    dflist=dflist.join(filter["selected"])
    dflist=dflist.query("selected==1")
    dflist.drop(columns=["selected"],inplace=True)
    reindex(dflist)
    return dflist

def str_round(value:float)->float:
    return str(round(value))
def str_int(value:float)->int:
    return str(int(value))

def formatDf(df):
    dflist=read_list()
    dflist=dflist.set_index("code")
    for index in df.index:
        code=df.loc[index,"code"]
        dfhist=read_hist(code)
        try:
            if np.isnan(df.loc[index,"custom"]):
                df.loc[index,"custom"]=0
        except:
            df.loc[index,"custom"]=0
        try:
            if np.isnan(df.loc[index,"hold"]):
                df.loc[index,"hold"]=0
        except:
            df.loc[index,"hold"]=0
        try:
            if np.isnan(df.loc[index,"zhuan"]):
                df.loc[index,"zhuan"]=0
        except:
            df.loc[index,"zhuan"]=0

        # df.loc[index,"昨开"]=dfhist.loc[0,"开盘"]
        df.loc[index,"昨收"]=dfhist.loc[0,"收盘"]
        df.loc[index,"昨高"]=dfhist.loc[0,"最高"]
        df.loc[index,"昨低"]=dfhist.loc[0,"最低"]
        # df.loc[index,"实际披露时间"]=dflist.loc[code,"实际披露时间"]
    set_industry(df)
    set_industry_sub(df)
    return df

# def query(dfsource:pd.DataFrame,filters:list[str]):
#   dflist=read_list()
#   dflist["selected"]=1
#   dflist.set_index("code",inplace=True)
#   for filter in filters:
#     try:
#       dflist=dflist.query(filter)
#     except:
#       pyttsx3.speak(filter+"不存在!")
#   if "selected" in dfsource.columns:
#     dfsource.drop(columns=["selected"])
#   dfsource=dfsource.join(dflist["selected"],on="code",how="left")
#   dfsource=dfsource.query("selected==1")
#   dfsource=dfsource.drop(columns=["selected"])
#   return dfsource.copy()

def query(dfsource:pd.DataFrame,filters:list[str]):
  for filter in filters:
    try:
      dfsource=dfsource.query(filter)
    except:
      pyttsx3.speak(filter+"不存在!")
  if "selected" in dfsource.columns:
    dfsource.drop(columns=["selected"])
  return dfsource.copy()

