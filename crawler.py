import pandas as pd
import requests
from tqdm import tqdm

from io import StringIO
from pprint import pprint
import sqlite3
import datetime
from time import sleep
import random


def main():
    # 時間日期設定
    delta = datetime.timedelta(days=1)
    # 設定開始日期與結束日期
    start_date = datetime.datetime(2017, 1, 1)
    end_date = datetime.datetime(2018, 3, 1)
    # 獲取時間差
    result =  end_date - start_date
    # 過濾六日
    dates = []
    for _ in range(result.days):
        if start_date.date().weekday() != 5 and start_date.date().weekday() != 6:
            dates.append(start_date.date())
        start_date = start_date + delta
        
    # 建立進度條物件
    pbar = tqdm(dates, ncols=75, desc='crawlering...')
    # 使用 requests.session() 建立會話物件
    request_session = requests.session()
    # 請求頭設定
    request_session.headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Host': 'www.twse.com.tw',
        'Referer': 'http://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html',
        'X-Requested-With': 'XMLHttpRequest'
    }
    # 資料處理
    for i in pbar:
        # 日期整理
        datestr = str(i).replace('-', '')
        # 進度條設定
        pbar.set_description('獲取... ' + datestr + '：..')
        # 目標網址
        url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALLBUT0999'
        # 發出請求
        response = request_session.get(url)
        # 判斷當日是否有資料
        if not response.text:
            pbar.set_description('獲取 ' + datestr + '：faild！')
            continue
        # 字串處理
        lines = response.text.split('\n')

        newlines = []
        for line in lines:
            if len(line.split('",')) == 17:
                newlines.append(line)

        newcontent = '\n'.join(newlines).replace('=', '')
        # 以 csv 讀取處理完的資料
        df = pd.read_csv(StringIO(newcontent))
        # 把所有資料轉成字串
        df = df.astype(str)
        # 數值化，因為數字中含有逗號，數值必須為純數字符號
        df = df.apply(lambda s: s.str.replace(',', ''))
        # 新增一欄存著日期的欄位
        df['date'] = pd.to_datetime(datestr) 
        # 設定 index
        df = df.set_index(['證券代號', 'date'])
        # 把是純數字號的字串，嘗試轉換成數值
        df = df.apply(lambda s: pd.to_numeric(s, errors='coerce'))
        # 把 都是 NaN 的資料欄位排除掉
        df = df[df.columns[df.isnull().sum() != len(df)]]
        # 某家公司的收盤價是空的話
        df = df.loc[~df['收盤價'].isnull()]
        # 存進 csv 檔裡面
        # df.to_csv('./stock.csv', encoding='utf_8_sig')
        # 建立資料庫物件
        coon = sqlite3.connect('stock_dbtest.splite')
        # 存進資料庫
        df.to_sql('daily_price', coon, if_exists='append')
        # 設定進度條顯示的文字
        pbar.set_description('獲取 ' + datestr + '：ok')

        sleep(25 + random.uniform(0, 10))
        del df, newlines, newcontent
# 程式進入點
if __name__ == '__main__':
    main()