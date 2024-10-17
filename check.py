import pandas as pd
from matplotlib import pyplot as plt

df=pd.read_csv("verify.csv")
df=df.query("实低<预低").query("预低<0").query("rmoney<1")
# df=df.sort_values("rmoney").query("rmoney<1").query("实低<预低").query("预低<-1")
print(df["预低"].mean())
print(df["实低"].mean())
print(df["diff低"].abs().mean())
print(len(df))

for id in ["低"]:
  label="diff"+id
  df["diff"+id]=df["实"+id]-df["预"+id]
  # df=df.query(label+">-3")
  df=df.sort_values(by="预"+id)
  df.index=range(len(df))
  ma=df["实低"].rolling(window=5).mean().rolling(window=5).mean()
  print(df["diff"+id].abs().mean())
  plt.plot(df["预"+id])
  plt.scatter(df.index,df["实"+id],linewidth=0.5)
  plt.plot(ma)
  # plt.plot(df["diff"+id].abs())
  plt.show()
df.to_csv("check.csv",encoding="utf_8_sig",index=None)

