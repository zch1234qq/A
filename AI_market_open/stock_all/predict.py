from AI_market_open.stock_all import utils
import torch
import numpy as np
import pandas as pd

printday=True
def pres(modelFile:str,codes:list[str]):
  global printday
  model = utils.LSTM(input_size=utils.input_size,hidden_size=utils.hidden_size,num_layers=utils.num_layers,output_size=len(utils.output_columns_dicts[modelFile]))
  model=model.to("cuda")
  model.load_state_dict(torch.load(utils.root+f'model_{modelFile}.pth'))
  model.eval()
  dfhist=pd.read_csv("dfhist.csv")
  dfhists=[]
  for i in range(len(codes)):
    dfhist_slice=dfhist.iloc[i*utils.input_length:(i+1)*utils.input_length]
    if len(dfhist_slice)!=utils.input_length:
      dfhist_slice=dfhist_slice.fillna(0)
    dfhists.append(dfhist_slice.to_numpy())
  dfhists=np.stack(dfhists)
  with torch.no_grad():
    inputs=torch.from_numpy(dfhists.astype(np.float32)).to("cuda")
    outputs=model(inputs).cpu().numpy()
    results=[]
    for o in outputs:
      results.append(o[0])
    return results

def pres_all(codes:list[str],banks:list[str],offset=0)->tuple:
  return  {
    "预低":pres(utils.MODEL_LOW,codes),
  }

def pred_df(df:pd.DataFrame,offset=0):
  codes=df["code"].tolist()
  banks=df["industry_sub"].tolist()
  res=pres_all(codes,banks,offset=offset)
  for key in res.keys():
    df[key]=res[key]
  return df