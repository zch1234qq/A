from custom_package import utils
import pandas as pd
from matplotlib import pyplot as plt
import sys
import os

def verti(offset=0):
  days=utils.getTradeDay_list(length=100,dot="-")[offset:]
  today=utils.getTradeDay_list(length=100)[offset:][0]
  yesterday_=days[1]
  dflist=utils.read_list(index_code=True)
  dflist=pd.DataFrame(dflist,columns=["昨收","涨停次数","连板次数","融资融券"])
  df=utils.read_stock()
  # df=df.iloc[:10]
  df=df.set_index("code")
  for code in df.index:
    dfhist=utils.read_hist(code)
    dfhist=dfhist.set_index("日期")
    try:
      dflist.loc[code,"昨收"]=dfhist.loc[yesterday_,"收盘"]
    except:
      continue
  dftoday=utils.getDftoday(offset)
  dftoday=dftoday.join(dflist)
  dftoday["实低"]=dftoday["最低"]/dftoday["昨收"]*100-100
  dftoday["实高"]=dftoday["最高"]/dftoday["昨收"]*100-100
  dftoday=dftoday.drop(columns=["昨收"])
  df=df.join(dftoday)
  # df["预低"]-=0.7
  for id in ["低","高"]:
    label="diff"+id
    df[label]=df["实"+id]-df["预"+id]

  if offset==0:
    df["预低1up"]=df["预低"]+0.5
    df["预低1down"]=df["预低"]-0.5
    for i in [0,1]:
      df0_up=df.query(f"实低>预低{i}up").copy()
      df0_up[f"diff{i}"]=df0_up["实低"]-df0_up[f"预低{i}up"]
      df0_down=df.query(f"实低<预低{i}down")
      df0_down[f"diff{i}"]=df0_down[f"预低{i}down"]-df0_down["实低"]
      df0=pd.concat([df0_down,df0_up])
      df=df.join(df0[f"diff{i}"])
      print(f"命中率:{100-len(df0)/len(df)*100}%")
      print(df[f"diff{i}"].sum()/len(df))

  dfresult=df.copy()
  dfresult=pd.DataFrame(dfresult,columns=["name","rmoney","预低","实低","diff低","diff0","diff1"])
  dfresult.to_csv("result/"+today+".csv",encoding="utf_8_sig")
  print(f"today是{today}")
  df=pd.DataFrame(df,columns=["name","rmoney","融资融券","预高","diff高","预低","实低","diff低"])
  df.to_csv("verify.csv",encoding="utf_8_sig")
  for id in ["低"]:
    df=df.sort_values(by="预"+id)
    print(df["diff"+id].abs().mean())
    ma=df["实"+id].rolling(window=5).mean().rolling(window=5).mean().rolling(window=5).mean().rolling(window=5).mean()
    plt.plot(df["预"+id])
    plt.scatter(df.index,df["实"+id],s=5)
    plt.plot(ma)
    plt.show()
    
argvs=sys.argv
print(os.getcwd())
offset=0
if len(argvs)>=2:
  offset=int(argvs[1])-1
  print(f"offset为{offset}")
verti(offset)