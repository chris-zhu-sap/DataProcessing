'''
Created on Nov 28, 2017

@author: Administrator
'''
# -*- coding=utf-8 -*-
import requests
from bs4 import BeautifulSoup
import re
import urllib
import pandas as pd
import os
import time


class StockInfoFetch(object):
    def __init__(self, index_code):
        time_format = time.strftime("_%Y_%m_%d", time.localtime())
        self.lst = []
        self.codeList = []
        self.fileName = index_code + time_format + "_day.csv"

    @staticmethod
    def getHTMLText(url):
        try:
            r = requests.get(url)
            r.encoding = 'utf-8'
            return r.text
        except Exception as err:
            print(err)

    def getStockList(self, stock_url):
        html_text = self.getHTMLText(stock_url)
        soup = BeautifulSoup(html_text, "html.parser");
        a = soup.find_all('a')
        for i in a:
            try:
                href = i.attrs['href']
                self.lst.append(re.findall("[s][hz]\d{6}", href)[0])
            except Exception as err:
                print(err)

    def get_china_stock_list(self, url, file_name):
        try:
            urllib.request.urlretrieve(url, file_name)
            if os.path.exists(file_name):
                df = pd.read_excel(file_name, dtype={'成分券代码Constituent Code': object}, encoding="utf-8")

                prefix_se = df.iloc[:, -1]
                prefix_se.str.strip()
                prefix_se = prefix_se.str.replace("SHH", "sh")
                prefix_se = prefix_se.str.replace("SHZ", "sz")

                stock_code = df.iloc[:, 4]
                list_pre = prefix_se.values
                list_code = stock_code.values
                self.codeList = list_code
                #                 print (df.dtypes)
                for index in range(len(list_pre)):
                    the_code = list_pre[index] + str(list_code[index])
                    self.lst.append(the_code)
            else:
                print('File not exists')
        except Exception as err:
            print(err)

    def getStockInfo(self, stock_url):
        count = 0
        info_dict = {}
        indexes = []
        time_format = time.strftime("%Y-%m-%d", time.localtime())
        for stock in self.lst:
            url = stock_url + stock + ".html"
            html_text = self.getHTMLText(url)
            try:
                if html_text == "":
                    continue

                soup = BeautifulSoup(html_text, "html.parser")
                stock_info = soup.find('div', attrs={'class': 'stock-bets'})

                if stock_info is None:
                    continue
                name = stock_info.find_all(attrs={'class': 'bets-name'})[0]
                stock_name = name.text.split()[0]

                name = stock_info.find_all(attrs={'class': '_close'})[0]
                close = name.text.split()[0]
                if count == 0:
                    indexes.append('日期')
                    indexes.append('股票名称')
                    indexes.append('今收')

                key_list = stock_info.find_all('dt')
                value_list = stock_info.find_all('dd')

                values = [time_format, stock_name, close]
                for i in range(len(key_list)):
                    key = key_list[i].text
                    if count == 0:
                        indexes.append(key)
                    value = value_list[i].text
                    if key == "跌停":
                        m = re.match(r'\s+(\d+.*)', value)
                        value = m.group(1)
                    values.append(value)
                info_dict[stock] = values
                count = count + 1
                print("\rcurrent process: {:.2f}%".format(count * 100 / len(self.lst), end=""))


            except Exception as err:
                count = count + 1
                #                 f.write(str(info_dict) + '\n')
                print("\rException: %s current process: {:.2f}%", err, format(count * 100 / len(self.lst), end=""))
                break

        df = pd.DataFrame(info_dict, index=indexes)
        df = df.T
        if os.path.exists(self.fileName):
            os.remove(self.fileName)
        df.to_csv(self.fileName, encoding='utf-8')


def get_name_and_code(url):
    m = re.match(r'.*(\d{6})(.*)', url)
    return m.group(1), m.group(1) + m.group(2)


if __name__ == '__main__':
    stock_info_url = 'https://gupiao.baidu.com/stock/'
    hs300StockListFileUrl = 'http://www.csindex.com.cn/uploads/file/autofile/cons/000300cons.xls'
    #     zz500StockListFileUrl  = 'http://www.csindex.com.cn/uploads/file/autofile/cons/000905cons.xls'
    #     m = re.match(r'.*(\d{6})(.*)', hs300_stock_list_file_url)
    indexCode, filename = get_name_and_code(hs300StockListFileUrl)
    fetcher = StockInfoFetch(indexCode)
    fetcher.get_china_stock_list(hs300StockListFileUrl, filename)
    #     fetcher.getStockList(stock_list_url)
    fetcher.getStockInfo(stock_info_url)
