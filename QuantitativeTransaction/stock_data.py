'''
Created on Sep 17, 2017

@author: Administrator
'''
import tushare as ts
import pandas as pd
import time
import os
import sys
import platform
import urllib
import re
import datetime
import threading
import shutil

DAY = 'D'
WEEK = 'W'
MONTH = 'M'
HOUR = '60'

FLOAT_FORMAT = '%.2f'


class SingletonBasicStockInfo(object):
    instance = None
    mutex = threading.Lock()

    def _init__(self):
        pass

    @staticmethod
    def getInstance(file_with_path):
        if SingletonBasicStockInfo.instance is None:
            SingletonBasicStockInfo.mutex.acquire()
            if SingletonBasicStockInfo.instance is None:
                SingletonBasicStockInfo.instance = SingletonBasicStockInfo()
                if os.path.exists(file_with_path):
                    SingletonBasicStockInfo.instance.basicInfoDf = pd.read_csv(file_with_path, encoding='utf-8',
                                                                               dtype={'code': str})
                    SingletonBasicStockInfo.instance.basicInfoDf.set_index('code', inplace=True)
                else:
                    SingletonBasicStockInfo.instance.basicInfoDf = ts.get_stock_basics()
                    SingletonBasicStockInfo.instance.basicInfoDf.to_csv(file_with_path, encoding='utf-8', index='code')
            SingletonBasicStockInfo.mutex.release()

        return SingletonBasicStockInfo.instance


def get_china_stock_list(url, file_name, date=None):
    file_path = get_data_file_path(date)
    file_with_path = file_path + file_name

    if os.path.exists(file_with_path) is False:
        urllib.request.urlretrieve(url, file_name)
        if os.path.exists(file_path) is False:
            os.makedirs(file_path)

        shutil.move(file_name, file_with_path)

    if os.path.exists(file_with_path):
        df = pd.read_excel(file_with_path, dtype={'成分券代码Constituent Code': object}, encoding="utf-8")
        stock_code = df.iloc[:, 4]
        return stock_code.values.tolist()
    else:
        print('[Function: %s line:%s] %s is not exists!' % (
        get_china_stock_list.__name__, sys._getframe().f_lineno, file_with_path))
        sys.exit()


def get_name_and_code(url):
    m = re.match(r'.*(\d{6})(.*)', url)
    return m.group(1), m.group(1) + m.group(2)


def is_today(date):
    today = time.strftime('%Y-%m-%d')
    if date is None or date != today:
        return False

    return True


def get_data_file_path(date=None):
    if date is None:
        dir_name_by_date = time.strftime('%Y-%m-%d')
    else:
        dir_name_by_date = date

    delimiter = get_delimiter()

    return os.getcwd() + delimiter + dir_name_by_date + delimiter


def get_df_from_basic_list(date=None):
    file_with_path = get_data_file_path(date) + "stock_basic_list.csv"
    file_path = get_data_file_path(date)
    if os.path.exists(file_path) is False:
        os.makedirs(file_path)
    instance = SingletonBasicStockInfo.getInstance(file_with_path)

    return instance.basicInfoDf


def get_stock_name_by_code(stock_code, data_date=None):
    df_basic = get_df_from_basic_list(data_date)
    return df_basic.ix[stock_code]['name']


def download_stock_data_as_csv(code_list=None):
    df = get_df_from_basic_list()

    if code_list is None:
        code_list = df.index
        print("[Function: %s] All stock in China will be download!" % download_stock_data_as_csv.__name__)
    else:
        print("[Function: %s] Stock in the code list will be download!" % download_stock_data_as_csv.__name__)

    for code in code_list:
        try:
            date_temp = df.ix[code]['timeToMarket']
            name = df.ix[code]['name']

            date = str(date_temp)
            date_to_market = date[:4] + "-" + date[4:6] + "-" + date[6:]

            my_stock = StockData(code, name)
            my_stock.getKDataAsCSV(DAY, start_date=date_to_market)
            my_stock.getKDataAsCSV(WEEK, start_date=date_to_market)
            my_stock.getKDataAsCSV(MONTH, start_date=date_to_market)
        except Exception as e:
            print("[Function: %s line:%s stock:%s Error:%s] Error happen when download the stock" % (
            download_stock_data_as_csv.__name__, sys._getframe().f_lineno, code, e))
            sys.exit()


def update_stock_data_for_list(code_list=None, date=None, k=None, update_dir=False):
    if code_list is None or not len(code_list):
        print("[Function:%s line:%s] The parameter codeList is None or empty!" % (
        update_stock_data.__name__, sys._getframe().f_lineno))
        sys.exit()

    if date is None:
        print(
            "[Function:%s line:%s] The parameter date is empty!" % (update_stock_data.__name__, sys._getframe().f_lineno))
        sys.exit()

    df = get_df_from_basic_list(date)

    for code in code_list:
        try:
            if code in df.index:
                update_stock_data(code, date, k)
            else:
                print("[Function:%s line:%s] The parameter code is not exists now!" % (
                update_stock_data.__name__, sys._getframe().f_lineno))
                sys.exit()
        except Exception as e:
            print("[Function:%s line:%s stock:%s Error:%s] Error happen when update data for the stock" % (
            update_stock_data_for_list.__name__, sys._getframe().f_lineno, code, e))
            sys.exit()

    if update_dir:
        today = time.strftime('%Y-%m-%d')
        if date is None or today != date:
            delimiter = get_delimiter()
            oldDataPath = os.getcwd() + delimiter + date
            newDataPath = os.getcwd() + delimiter + today
            if os.path.exists(oldDataPath):
                os.rename(oldDataPath, newDataPath)
            else:
                print("[Function:%s line:%s]  oldDataPath: %s is not exists!" % (
                update_stock_data_for_list.__name__, sys._getframe().f_lineno, oldDataPath))
                sys.exit()


def update_stock_data(code, date=None, k=None):
    my_stock = StockData(code, date)
    if k is None:
        my_stock.updateAllKData()
    else:
        my_stock.updateKData(k)


def get_next_date(date_str):
    if date_str is not None and date_str != '':
        new_date_str = ''
        m1 = re.match(r'(\d+)/(\d+)/(\d+)', date_str)
        m2 = re.match(r'(\d+)-(\d+)-(\d+)', date_str)
        if m1 is None and m2 is None:
            print("The format of date is unexpected!")
            sys.exit()
        else:
            if m1 is None:
                new_date_str = date_str
            else:
                new_date_str = m1.group(3) + '-' + m1.group(1) + '-' + m1.group(2)
        the_date = datetime.datetime.strptime(new_date_str, '%Y-%m-%d')
        next_date = the_date + datetime.timedelta(days=1)
        return datetime.datetime.strftime(next_date, "%Y-%m-%d")
    else:
        print("[Function: %s]: The parameter dateStr is empty!" % get_next_date.__name__)
        sys.exit()


def get_delimiter():
    if 'Windows' in platform.system():
        return "\\"
    else:
        return "/"


class StockData(object):
    def __init__(self, stock_code, stock_name=None, date=None):
        self.stockCode = stock_code
        self.lenUpdated = 0
        self.stockName = ''
        self.dirNameByDate = ''
        self.dayKDataFilePath = ''
        self.weekKDataFilePath = ''
        self.monthKDataFilePath = ''
        self.hourKDataFilePath = ''
        self.dataPath = ''
        self.basicStockInfoFilePath = ''
        self.setDirNameByDate(date)
        self.setDataPath()
        self.createDataDir()
        self.setDataFilePath()
        self.setStockName(stock_name, date)

    def setStockName(self, stock_name=None, date=None):
        if stock_name is None:
            self.stockName = get_stock_name_by_code(self.stockCode, date)
        else:
            self.stockName = stock_name

    def getStockName(self):
        return self.stockName

    def mergeData(self, df_data, period):
        df = pd.DataFrame()
        if len(df_data) > 0:
            if period == DAY and os.path.exists(self.dayKDataFilePath) is True:
                df = pd.read_csv(self.dayKDataFilePath)
            elif period == WEEK and os.path.exists(self.weekKDataFilePath) is True:
                df = pd.read_csv(self.weekKDataFilePath)
            elif period == MONTH and os.path.exists(self.monthKDataFilePath) is True:
                df = pd.read_csv(self.monthKDataFilePath)
            elif period == HOUR and os.path.exists(self.hourKDataFilePath) is True:
                df = pd.read_csv(self.hourKDataFilePath)
            else:
                print("[Function: %s line:%d]: The period:%s is not support now!" % (
                self.mergeData.__name__, sys._getframe().f_lineno, period))
                sys.exit()
        else:
            print("[Function: %s line:%d]: parameter dfData is empty!" % (
            self.mergeData.__name__, sys._getframe().f_lineno))
            sys.exit()

        return pd.concat([df, df_data])

    def getDataLenUpdated(self):
        return self.lenUpdated

    def setDirNameByDate(self, date=None):
        if date is None:
            self.dirNameByDate = time.strftime('%Y-%m-%d')
        else:
            self.dirNameByDate = date

    def setDataFilePath(self):
        self.dayKDataFilePath = self.dataPath + get_delimiter() + self.stockCode + "_k_day.csv"
        self.weekKDataFilePath = self.dataPath + get_delimiter() + self.stockCode + "_k_week.csv"
        self.monthKDataFilePath = self.dataPath + get_delimiter() + self.stockCode + "_k_month.csv"
        self.hourKDataFilePath = self.dataPath + get_delimiter() + self.stockCode + "_k_hour.csv"

    def setDataPath(self, index_dir=None):
        #         dirNameByDate = time.strftime('%Y%m%d')
        if not index_dir:
            index_dir = self.dirNameByDate
        delimiter = get_delimiter()
        self.dataPath = os.getcwd() + delimiter + index_dir + delimiter + self.stockCode
        self.basicStockInfoFilePath = os.getcwd() + delimiter + index_dir + delimiter

    def createDataDir(self):
        isExists = os.path.exists(self.dataPath)
        if not isExists:
            os.makedirs(self.dataPath)

    def getKDataAsCSV(self, k=DAY, start_date=''):
        if k == DAY:
            if os.path.exists(self.dayKDataFilePath) is False:
                df = ts.get_k_data(self.stockCode, start=start_date, ktype=k)
                # delete last column 'code'
                df.drop('code', axis=1, inplace=True)
                df.to_csv(self.dayKDataFilePath, index=False, float_format=FLOAT_FORMAT)
                print("[Function: %s]: file:%s has been download successfully!" % (
                self.getKDataAsCSV.__name__, self.dayKDataFilePath))
            else:
                print("[Function: %s]: file:%s has been download before, no need to do it again!" % (
                self.getKDataAsCSV.__name__, self.dayKDataFilePath))
        elif k == WEEK:
            if os.path.exists(self.weekKDataFilePath) is False:
                df = ts.get_k_data(self.stockCode, start=start_date, ktype=k)
                # delete last column 'code'
                df.drop('code', axis=1, inplace=True)
                df.to_csv(self.weekKDataFilePath, index=False, float_format=FLOAT_FORMAT)
                print("[Function: %s]: file:%s has been download successfully!" % (
                self.getKDataAsCSV.__name__, self.weekKDataFilePath))
            else:
                print("[Function: %s]: file:%s has been download before, no need to do it again!" % (
                self.getKDataAsCSV.__name__, self.weekKDataFilePath))
        elif k == MONTH:
            if os.path.exists(self.monthKDataFilePath) is False:
                df = ts.get_k_data(self.stockCode, start=start_date, ktype=k)
                # delete last column 'code'
                df.drop('code', axis=1, inplace=True)
                df.to_csv(self.monthKDataFilePath, index=False, float_format=FLOAT_FORMAT)
                print("[Function: %s]: file:%s has been download successfully!" % (
                self.getKDataAsCSV.__name__, self.monthKDataFilePath))
            else:
                print("[Function: %s]: file:%s has been download before, no need to do it again!" % (
                self.getKDataAsCSV.__name__, self.monthKDataFilePath))
        elif k == HOUR:
            if os.path.exists(self.hourKDataFilePath) is False:
                df = ts.get_k_data(self.stockCode, start=start_date, ktype=k)
                # delete last column 'code'
                df.drop('code', axis=1, inplace=True)
                df.to_csv(self.hourKDataFilePath, index=False, float_format=FLOAT_FORMAT)
                print("[Function: %s]: file::%s has been download successfully!" % (
                self.getKDataAsCSV.__name__, self.hourKDataFilePath))
            else:
                print("[Function: %s]: file:%s has been download before, no need to do it again!" % (
                self.getKDataAsCSV.__name__, self.hourKDataFilePath))
        else:
            print("[Function: %s line:%d]: parameter for k:%s is not support now!" % (
            self.getKDataAsCSV.__name__, sys._getframe().f_lineno, k))
            sys.exit()

    def updateAllKData(self):
        self.updateKData(DAY)
        self.updateKData(WEEK)
        self.updateKData(MONTH)

    #         self.updateKData(HOUR)

    def updateKData(self, k=None):
        if k is not None:
            if k == DAY:
                self.doUpdate(self.dayKDataFilePath, k)
            elif k == WEEK:
                self.doUpdate(self.weekKDataFilePath, k)
            elif k == MONTH:
                self.doUpdate(self.monthKDataFilePath, k)
            elif k == HOUR:
                self.doUpdate(self.hourKDataFilePath, k)
            else:
                print("[Function: %s] parameter k:%s is not support now!" % (self.updateKData.__name__, k))
                sys.exit()
        else:
            print("[Function: %s] start and k should not be empty!" % self.updateKData.__name__)
            sys.exit()

    def doUpdate(self, file_path, k):
        if file_path is None or k is None:
            print("[Function:%s line:%d]: Parameter should not be None!" % (
            self.doUpdate.__name__, sys._getframe().f_lineno))
            sys.exit()

        logStr = 'update_' + k + '_k_data'

        if os.path.exists(file_path) is True:
            df = pd.read_csv(file_path)
            last_date_str = ''
            if not df.empty:
                last_date_str = df['date'].values[-1]
                df.drop(df.index[[-1]], inplace=True)
                df_latest = ts.get_k_data(self.stockCode, start=last_date_str, ktype=k)
                self.lenUpdated = len(df_latest)
                # delete last column 'code'
                df_latest.drop('code', axis=1, inplace=True)
                if not df_latest.empty:
                    new_df = pd.concat([df, df_latest])
                    new_df.to_csv(file_path, index=False, float_format=FLOAT_FORMAT)
                    print("[Function: %s]: %s: Data of %s has been updated successfully " % (
                    self.doUpdate.__name__, logStr, str(self.stockCode)))
                else:
                    print("[Function: %s]: %s: No record need to be updated for stock %s !" % (
                    self.doUpdate.__name__, logStr, str(self.stockCode)))
            else:
                print("[Function:%s line:%d]: No record can be found in the data file!" % (
                    self.doUpdate.__name__, sys._getframe().f_lineno, file_path))
                sys.exit()

        else:
            print("[Function: %s line:%d]: %s: file %s is not exists!" % (
                self.doUpdate.__name__, logStr, file_path, sys._getframe().f_lineno))
            sys.exit()


if __name__ == '__main__':
    zz500StockListFileUrl = 'http://www.csindex.com.cn/uploads/file/autofile/cons/000905cons.xls'
    #     dataDate = '2019-01-01'
    hs300StockListFileUrl = 'http://www.csindex.com.cn/uploads/file/autofile/cons/000300cons.xls'
    indexCode, filename = get_name_and_code(hs300StockListFileUrl)
    hs300CodeList = get_china_stock_list(hs300StockListFileUrl, filename)
    print('The length of hs300CodeList is: %d!' % (len(hs300CodeList)))
    indexCode, filename = get_name_and_code(zz500StockListFileUrl)
    zz500CodeList = get_china_stock_list(zz500StockListFileUrl, filename)
    print('The length of zz500CodeList is: %d!' % (len(zz500CodeList)))
    myCodeList = ['600030', '600036', '600061', '600893', '600498', '300033', '600547', '300383', '002716', '600109',
                  '002353', '300059']
    print('The length of myCodeList is: %d %s' % (len(myCodeList)))
    codeList = hs300CodeList + zz500CodeList + myCodeList
    codeList = list(set(codeList))
    print('The length of codeList is: %d' % len(codeList))
    download_stock_data_as_csv(codeList)
#     hs300CodeList = get_china_stock_list(hs300StockListFileUrl, filename,dataDate)
#     update_stock_data_for_list(hs300CodeList,dataDate,updateDir=True)
#     update_stock_data_for_list(codeList,dataDate)
#     wanke = StockData('000002')
#     wanke.updateAllKData()
#     wanke.updateKData(DAY)
#     wanke.updateKData(WEEK)
#     wanke.updateKData(MONTH)
#     wanke.getKDataAsCSV('D', startDate='1991-01-29')
#     wanke.getKDataAsCSV('W', startDate='1991-01-29')
#     wanke.getKDataAsCSV('M', startDate='1991-01-29')
#     print("finish get HS300 data")
