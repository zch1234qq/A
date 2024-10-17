#定义模型类
import torch
import torch.nn as nn
import pandas as pd
import os

#考虑将日期转为假日属性
ratio=3000
learnRate=0.001
hidden_size=64
batch_size=128
num_layers=1
num_epochs=80
y_offset=0
clipCount=120

module_path = os.path.abspath(__file__)
root = os.path.dirname(module_path)+"\\"
if torch.cuda.is_available():
    print("CUDA is available! Training on GPU...")
else:
    print("CUDA is not available. Training on CPU...")
device = torch.device("cuda" if torch.cuda.is_available() else "cpu") 

MODEL_HIGH="high"
MODEL_RMONEY="rmoney"
MODEL_LOW="low"
MODEL_CLOSE="close"
MODEL_UP="up"
MODEL_UPMINUTE="upMinute"
MODEL_ALL="all"

output_columns_dicts={
   MODEL_HIGH:["最高"],
   MODEL_LOW:["最低"],
   MODEL_RMONEY:["成交额"],
   MODEL_CLOSE:["收盘"],
}

input_size=46
input_length=10


def hit(dfslice:pd.DataFrame)->bool:
  r4=dfslice.iloc[-4]
  r3=dfslice.iloc[-3]
  r2=dfslice.iloc[-2]
  r1=dfslice.iloc[-1]
  filters=[
     r4["up"]>4 and r4["right"]>2,
     r3["up"]>4 and r3["right"]>2,
     r2["up"]>4,
     r1["up"]>4,
  ]
  result=False
  for filter in filters:
    if filter:
      result=True
      break
  return result

class LSTM(nn.Module):
  def __init__(self,input_size,hidden_size,num_layers,output_size):
    super(LSTM, self).__init__()
    self.hidden_size = hidden_size
    self.num_layers = num_layers
    self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True,)
    self.fc = nn.Linear(hidden_size,output_size)
  def forward(self,x):
    h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
    c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
    out, _ = self.lstm(x, (h0, c0))
    out = self.fc(out[:, -1, :])
    # return torch.sigmoid(out)
    return out
