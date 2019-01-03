'''
Created on Nov 28, 2017

@author: Administrator
'''
#-*- coding=utf-8 -*-
import requests
from bs4 import BeautifulSoup
import re
import urllib
# import xlrd
# import numpy as np
import pandas as pd
import os
import time

class StockInfoFetch(object):
    def __init__(self,indexCode):
        timeFormat = time.strftime("_%Y_%m_%d", time.localtime())
        self.lst = []
        self.codeList = []
        self.fileName = indexCode + timeFormat + "_day.csv"
    def getHTMLText(self,url):
        try:
            r = requests.get(url)
            r.encoding = 'utf-8'
            return r.text
        except Exception as err:
            print(err)
    
    def getStockList(self, stockUrl):
        htmlText = self.getHTMLText(stockUrl)
        soup = BeautifulSoup(htmlText,"html.parser");
        a = soup.find_all('a')
        for i in a:
            try:
                href = i.attrs['href']
                self.lst.append(re.findall("[s][hz]\d{6}",href)[0])
            except Exception as err:
                print(err)
                
    def getChinaStockList(self, url, filename):
        try:
            urllib.request.urlretrieve(url, filename)
            if os.path.exists(filename):
                df = pd.read_excel(filename, dtype = {'成分券代码Constituent Code':object},encoding="utf-8")
                
                prefixSe = df.iloc[:,-1]
                prefixSe.str.strip()
                prefixSe = prefixSe.str.replace("SHH","sh")
                prefixSe = prefixSe.str.replace("SHZ","sz")
                
                stockCode = df.iloc[:,4]
                listPre = prefixSe.values
                listCode = stockCode.values
                self.codeList = listCode
#                 print (df.dtypes)
                for index in range(len(listPre)):
                    theCode = listPre[index] + str(listCode[index])
                    self.lst.append(theCode)
            else:
                print('File not exists')
        except Exception as err:
            print(err)
                
    def getStockInfo(self, stockUrl):
        count = 0
        infoDict = {}
        indexes = []
        timeFormat = time.strftime("%Y-%m-%d", time.localtime())
        for stock in self.lst:
            url = stockUrl + stock + ".html"
            htmlText = self.getHTMLText(url)
            try:
                if htmlText == "":
                    continue
                
                soup = BeautifulSoup(htmlText, "html.parser")
                stockInfo = soup.find('div',attrs={'class':'stock-bets'})
                
                if stockInfo is None:
                    continue
                name = stockInfo.find_all(attrs={'class':'bets-name'})[0]
                stockName = name.text.split()[0]
                
                name = stockInfo.find_all(attrs={'class':'_close'})[0]
                close = name.text.split()[0]
                if(count == 0):
                    indexes.append('日期')
                    indexes.append('股票名称')
                    indexes.append('今收')
                
                keyList = stockInfo.find_all('dt')
                valueList = stockInfo.find_all('dd')
                
                values = []
                values.append(timeFormat)
                values.append(stockName)
                values.append(close)
                for i in range(len(keyList)):
                    key = keyList[i].text
                    if(count == 0):
                        indexes.append(key)
                    value = valueList[i].text
                    if(key == "跌停"):
                        m = re.match(r'\s+(\d+.*)', value)
                        value = m.group(1)
                    values.append(value)
                infoDict[stock] = values
                count = count + 1
                print("\rcurrent process: {:.2f}%".format(count*100/len(self.lst), end=""))

            
            except Exception as err:
                count = count + 1
#                 f.write(str(infoDict) + '\n')
                print("\rException: %s current process: {:.2f}%", err, format(count*100/len(self.lst), end=""))
                break
                
        df = pd.DataFrame(infoDict,index=indexes)
        df = df.T
        if(os.path.exists(self.fileName)):
            os.remove(self.fileName)
        df.to_csv(self.fileName, encoding = 'utf-8')

def getNameAndCode(url):
    m = re.match(r'.*(\d{6})(.*)', url)
    return m.group(1), m.group(1) + m.group(2)
                
if __name__ == '__main__':
    stock_info_url = 'https://gupiao.baidu.com/stock/'
    hs300StockListFileUrl  = 'http://www.csindex.com.cn/uploads/file/autofile/cons/000300cons.xls'
#     zz500StockListFileUrl  = 'http://www.csindex.com.cn/uploads/file/autofile/cons/000905cons.xls'
#     m = re.match(r'.*(\d{6})(.*)', hs300StockListFileUrl)
    indexCode, filename= getNameAndCode(hs300StockListFileUrl)
    fetcher = StockInfoFetch(indexCode)
    fetcher.getChinaStockList(hs300StockListFileUrl,filename)
#     fetcher.getStockList(stock_list_url)
    fetcher.getStockInfo(stock_info_url)
                
                
            
            
        