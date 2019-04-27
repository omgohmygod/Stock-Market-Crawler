import sqlite3
import numpy as np
import pandas as pd
## 技術指標
import talib
from talib import abstract

import mpl_finance
from  matplotlib import pyplot as plt
from matplotlib.pylab import date2num

# connect to sql
data_base = sqlite3.connect('./stock_dbtest.splite')
# SQL operation
df = pd.read_sql('select 證券代號, date, 開盤價, 收盤價, 最高價, 最低價, 成交股數 from daily_price where 證券代號="0050"', 
                data_base,
                ##依照日期排
                index_col=['date'],
                parse_dates=['date'])
# close data base connect
data_base.close()
# rename the columns of dataframe
df.rename(columns={'收盤價':'close', '開盤價':'open', '最高價':'high', '最低價':'low', '成交股數':'volume'}, inplace=True)
# sort by date
df = df.sort_values(by = 'date')
plt.rcParams["figure.figsize"] = (8,5)

##開啟交互模式 可以顯示多個畫圖視窗
plt.ion()

print(df['close'])
#plt.plot(abstract.STOCH(df))
# plt.plot(df['close'])

##KD值
abstract.STOCH(df).plot(secondary_y = True)
df['close'].plot(color ='black')
plt.title("KD")

## RSI
plt.figure()
abstract.RSI(df).plot()
df['close'].plot(color = 'black')
plt.title("RSI")

## 移動平均
plt.figure()
abstract.EMA(df).plot()
abstract.SMA(df).plot()
df['close'].plot(color = 'black')
plt.title("SMA/EMA")

##顯示前須要關掉交互模式，否則只會出現一瞬間
plt.ioff()
plt.show()


