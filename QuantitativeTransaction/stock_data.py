'''
Created on Sep 17, 2017

@author: Administrator
'''
import tushare as ts
import pandas as pd
import time
import os
import sys
import urllib
import re
import data_process as dp
import threading
import shutil
import util
import urllib

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
                    SingletonBasicStockInfo.instance.basicInfoDf.set_index('ts_code', inplace=True)
                else:
                    pro = get_ts_pro()
                    SingletonBasicStockInfo.instance.basicInfoDf = pro.stock_basic()
                    SingletonBasicStockInfo.instance.basicInfoDf.to_csv(file_with_path, encoding='utf-8', index='code')
            SingletonBasicStockInfo.mutex.release()

        return SingletonBasicStockInfo.instance


def get_ts_pro():
    token = '013446bdc63cac9f4ae5cf266fdaf8b617515759e85a391c3487e819'
    ts.set_token(token)
    pro = ts.pro_api()
    return pro


def get_china_stock_list(url, file_name, data_dir):
    file_with_path = os.path.join(data_dir, file_name)

    if os.path.exists(file_with_path) is False:
        urllib.request.urlretrieve(url, file_with_path)

    if os.path.exists(file_with_path):
        df = pd.read_excel(file_with_path, dtype={'成分券代码Constituent Code': object})
        stock_code = df.iloc[:, 4]
        return stock_code.values.tolist()
    else:
        print('[File: %s line:%d] %s is not exists!' % (
            sys._getframe().f_code.co_filename, sys._getframe().f_lineno, file_with_path))
        sys.exit()


def get_name_and_code(url):
    m = re.match(r'.*(\d{6})(.*)', url)
    return m.group(1) + m.group(2)


def get_date():
    return time.strftime('%Y-%m-%d')


# def get_data_file_path(data_dir):
#     if date is None:
#         dir_name_by_date = time.strftime('%Y-%m-%d')
#     else:
#         dir_name_by_date = date
#
#     delimiter = util.get_delimiter()
#
#     return os.getcwd() + delimiter + dir_name_by_date + delimiter


def get_df_from_basic_list(data_dir):
    stock_basic_info_name = 'stock_basic_list.csv'
    file_with_path = os.path.join(data_dir, stock_basic_info_name)
    if os.path.exists(data_dir) is False:
        os.makedirs(data_dir)
    instance = SingletonBasicStockInfo.getInstance(file_with_path)

    return instance.basicInfoDf


def get_stock_name_by_code(stock_code, data_dir=None):
    df_basic = get_df_from_basic_list(data_dir)
    df_search = df_basic[df_basic.ts_code == stock_code]
    if len(df_search) > 0:
        index = df_search.index.to_list()[0]
        return df_basic.loc[index, 'name']
    else:
        print("[File: %s line:%d stock:%s] can't find the name for the stock!" % (
        sys._getframe().f_code.co_filename, sys._getframe().f_lineno, stock_code))
        sys.exit()


def download_stock_data_as_csv(code_list_para=None, start_date=None):
    df = get_df_from_basic_list(start_date)

    if code_list_para is None:
        code_list_para = df.index

    length_of_code_list = len(code_list_para)
    index = 1
    for ts_code in code_list_para:
        try:
            index_list = df.loc[df.ts_code == ts_code].index.to_list()
            data_index = index_list[0]
            date_temp = df.loc[data_index, 'list_date']
            name = df.loc[data_index, 'name']

            date = str(date_temp)
            date_to_market = date[:4] + "-" + date[4:6] + "-" + date[6:]

            my_stock = StockData(ts_code, name, start_date)
            if my_stock.checkDataFilesDownloaded() is False:
                print("#### Process to get data of %d/%d ####" % (index, length_of_code_list))
                my_stock.getKDataAsCSV(k=DAY, start=date_to_market)
                my_stock.getKDataAsCSV(k=WEEK, start=date_to_market)
                my_stock.getKDataAsCSV(k=MONTH, start=date_to_market)
            else:
                print("[File: %s line:%d stock:%s] Data files has been downloaded, don't need download any more!" % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno, ts_code))
        except Exception as e:
            print("[File: %s line:%d stock:%s Error:%s] Error happen when download the stock" % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, ts_code, e))
            sys.exit()
        index = index + 1


def update_stock_data_for_list(code_list_para=None, data_dir=None, k=None):
    if code_list_para is None or not len(code_list_para):
        print("[File:%s line:%d] The parameter code_list is None or empty!" % (
            sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
        sys.exit()

    if data_dir is None:
        print(
            "[File:%s line:%d] The parameter date is empty!" % (
            sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
        sys.exit()

    df = get_df_from_basic_list(data_dir)

    length_of_code_list = len(code_list_para)
    index = 1

    for ts_code in code_list_para:
        try:
            index_list = df.loc[df.ts_code == ts_code].index.to_list()
            if len(index_list) > 0:
                update_stock_data(ts_code, data_dir, k)
                print("#### Process to update data of %d/%d ####" % (index, length_of_code_list))
            else:
                print("[File:%s line:%d] The parameter code is not exists now!" % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
                sys.exit()
        except Exception as e:
            print("[File:%s line:%d stock:%s Error:%s] Error happen when update data for the stock" % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, ts_code, e))
            sys.exit()
        index = index + 1


def update_stock_data(code, data_dir=None, k=None):
    my_stock = StockData(stock_code=code, data_dir=data_dir)
    if k is None:
        my_stock.updateAllKData()
    else:
        my_stock.updateKData(k)


# def get_next_date(date_str):
#     if date_str is not None and date_str != '':
#         new_date_str = ''
#         m1 = re.match(r'(\d+)/(\d+)/(\d+)', date_str)
#         m2 = re.match(r'(\d+)-(\d+)-(\d+)', date_str)
#         if m1 is None and m2 is None:
#             print("[File:%s line:%d] The format of date is unexpected!" % (sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
#             sys.exit()
#         else:
#             if m1 is None:
#                 new_date_str = date_str
#             else:
#                 new_date_str = m1.group(3) + '-' + m1.group(1) + '-' + m1.group(2)
#         the_date = datetime.datetime.strptime(new_date_str, '%Y-%m-%d')
#         next_date = the_date + datetime.timedelta(days=1)
#         return datetime.datetime.strftime(next_date, "%Y-%m-%d")
#     else:
#         print("[File: %s line:%d]: The parameter dateStr is empty!" % (sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
#         sys.exit()


class StockData(object):
    def __init__(self, stock_code, stock_name=None, data_dir=None):
        self.stockCode = stock_code
        self.lenUpdated = 0
        self.stockName = ''
        self.dataDir = ''
        self.dayKDataFilePath = ''
        self.weekKDataFilePath = ''
        self.monthKDataFilePath = ''
        self.hourKDataFilePath = ''
        self.dataPath = ''
        self.basicStockInfoFilePath = ''
        self.setDataDir(data_dir)
        self.setDataPath()
        self.createDataDir()
        self.setDataFilePath()
        self.setStockName(stock_name, data_dir)

    def setStockName(self, stock_name=None, data_dir=None):
        if stock_name is None:
            self.stockName = get_stock_name_by_code(self.stockCode, data_dir)
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
                print("[File: %s line:%d]: The period:%s is not support now!" % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno, period))
                sys.exit()
        else:
            print("[File: %s line:%d]: parameter dfData is empty!" % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
            sys.exit()

        return pd.concat([df, df_data])

    def getDataLenUpdated(self):
        return self.lenUpdated

    def setDataDir(self, data_dir):
        self.dataDir = data_dir

    def setDataFilePath(self):
        self.dayKDataFilePath = self.dataPath + util.get_delimiter() + self.stockCode + "_k_day.csv"
        self.weekKDataFilePath = self.dataPath + util.get_delimiter() + self.stockCode + "_k_week.csv"
        self.monthKDataFilePath = self.dataPath + util.get_delimiter() + self.stockCode + "_k_month.csv"
        self.hourKDataFilePath = self.dataPath + util.get_delimiter() + self.stockCode + "_k_hour.csv"

    def checkDataFilesDownloaded(self):
        if os.path.exists(self.dayKDataFilePath) and os.path.exists(self.weekKDataFilePath) and os.path.exists(self.monthKDataFilePath):
            return True
        return False

    def setDataPath(self, index_dir=None):
        if not index_dir:
            index_dir = self.dataDir
        self.dataPath = os.path.join(index_dir, self.stockCode)
        self.basicStockInfoFilePath = index_dir

    def createDataDir(self):
        isExists = os.path.exists(self.dataPath)
        if not isExists:
            os.makedirs(self.dataPath)

    def getKDataAsCSV(self, k=DAY, start=''):
        pro = get_ts_pro()
        if k == DAY:
            if os.path.exists(self.dayKDataFilePath) is False:
                df = ts.pro_bar(ts_code=self.stockCode, adj='qfq', start_date=start)
                # delete last column 'code'
                df.drop('ts_code', axis=1, inplace=True)
                df.sort_values(by='trade_date', ascending=True).to_csv(self.dayKDataFilePath, index=False, float_format=FLOAT_FORMAT)
                print("[File: %s line:%d]: file:%s has been download successfully!" % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.dayKDataFilePath))
            else:
                print("[File: %s line:%d]: file:%s has been download before, no need to do it again!" % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.dayKDataFilePath))
        elif k == WEEK:
            if os.path.exists(self.weekKDataFilePath) is False:
                df = ts.pro_bar(ts_code=self.stockCode, freq='W', adj='qfq', start_date=start)
                # delete last column 'code'
                df.drop('ts_code', axis=1, inplace=True)
                df.sort_values(by='trade_date', ascending=True).to_csv(self.weekKDataFilePath, index=False, float_format=FLOAT_FORMAT)
                print("[File: %s line:%d]: file:%s has been download successfully!" % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.weekKDataFilePath))
            else:
                print("[File: %s line:%d]: file:%s has been download before, no need to do it again!" % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.weekKDataFilePath))
        elif k == MONTH:
            if os.path.exists(self.monthKDataFilePath) is False:
                df = ts.pro_bar(ts_code=self.stockCode, freq='M', adj='qfq', start_date=start)
                # delete last column 'code'
                df.drop('ts_code', axis=1, inplace=True)
                df.sort_values(by='trade_date', ascending=True).to_csv(self.monthKDataFilePath, index=False, float_format=FLOAT_FORMAT)
                print("[File: %s line:%d]: file:%s has been download successfully!" % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.monthKDataFilePath))
            else:
                print("[File: %s line:%d]: file:%s has been download before, no need to do it again!" % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.monthKDataFilePath))
        # elif k == HOUR:
        #     if os.path.exists(self.hourKDataFilePath) is False:
        #         df = ts.get_k_data(ts_code=self.stockCode,, start=start_date)
        #         # delete last column 'code'
        #         df.drop('code', axis=1, inplace=True)
        #         df.to_csv(self.hourKDataFilePath, index=False, float_format=FLOAT_FORMAT)
        #         print("[File: %s line:%d]: file::%s has been download successfully!" % (
        #             sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.hourKDataFilePath))
        #     else:
        #         print("[File: %s line:%d]: file:%s has been download before, no need to do it again!" % (
        #             sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.hourKDataFilePath))
        else:
            print("[File: %s line:%d]: parameter for k:%s is not support now!" % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, k))
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
                print("[File: %s line:%d freq:%s] update data for %s complete!" % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, k, self.stockCode))
            elif k == WEEK:
                self.doUpdate(self.weekKDataFilePath, k)
                print("[File: %s line:%d freq:%s] update data for %s complete!" % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno, k, self.stockCode))
            elif k == MONTH:
                self.doUpdate(self.monthKDataFilePath, k)
                print("[File: %s line:%d freq:%s] update data for %s complete!" % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno, k, self.stockCode))
            # elif k == HOUR:
            #     self.doUpdate(self.hourKDataFilePath, k)
            else:
                print("[File: %s line:%d] parameter k:%s is not support now!" % (sys._getframe().f_code.co_filename, sys._getframe().f_lineno, k))
                sys.exit()
        else:
            print("[File: %s line:%d] start and k should not be empty!" % (sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
            sys.exit()

    def doUpdate(self, file_path, k):
        if file_path is None or k is None:
            print("[File:%s line:%d]: Parameter should not be None!" % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
            sys.exit()

        if os.path.exists(file_path) is True:
            df = pd.read_csv(file_path)
            if not df.empty:
                last_date_str = str(df['trade_date'].values[-1])
                df.drop(df.index[[-1]], inplace=True)

                df_latest = pd.DataFrame()
                if k == DAY:
                    df_latest = ts.pro_bar(ts_code=self.stockCode, adj='qfq', start_date=last_date_str)
                elif k == WEEK:
                    df_latest = ts.pro_bar(ts_code=self.stockCode, freq='W', adj='qfq', start_date=last_date_str)
                elif k == MONTH:
                    df_latest = ts.pro_bar(ts_code=self.stockCode, freq='M', adj='qfq', start_date=last_date_str)
                else:
                    print("[File: %s line:%d]: Invalid time level when request data!" % (
                        sys._getframe().f_code.co_filename, sys._getframe().f_lineno))


                if df_latest is not None:
                    self.lenUpdated = len(df_latest)
                    if self.lenUpdated > 0:
                        df_latest.drop('ts_code', axis=1, inplace=True)
                        df = pd.concat([df, df_latest.sort_values(by='trade_date', ascending=True)])
                        df.to_csv(file_path, index=False, float_format=FLOAT_FORMAT)
                        df = df.reset_index(drop=True)

                    if k == WEEK or k == MONTH:
                        index_last = len(df) - 1
                        latest_date_int = int(df.at[index_last, 'trade_date'])
                        df_day_data = pd.DataFrame()
                        if os.path.exists(self.dayKDataFilePath):
                            df_day_data = pd.read_csv(self.dayKDataFilePath)
                        else:
                            print("[File: %s line:%d]: can't find the data file for stock:%s freq:%s!" % (
                                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.stockCode, DAY))
                            sys.exit()
                        df_daily_data = df_day_data[df_day_data['trade_date'] > latest_date_int]
                        if len(df_daily_data) > 0:
                            df_daily_data = df_daily_data.reset_index(drop=True)
                            index_last = len(df_daily_data) - 1
                            open_price = df_daily_data.at[0, 'open']
                            close_price = df_daily_data.at[index_last, 'close']
                            high_price = pd.Series(df_daily_data['high']).max()
                            low_price = pd.Series(df_daily_data['low']).min()
                            trade_date = df_daily_data.at[index_last, 'trade_date']
                            data = {'trade_date': [trade_date],
                                    'open': [open_price],
                                    'high': [high_price],
                                    'low': [low_price],
                                    'close': [close_price],
                                    'pre_close': [0],
                                    'change': [0],
                                    'pct_chg': [0],
                                    'vol': [0],
                                    'amount': [0],
                                    }
                            df_data = pd.DataFrame(data)
                            df = pd.concat([df, df_data.sort_values(by='trade_date', ascending=True)])
                            df.to_csv(file_path, index=False, float_format=FLOAT_FORMAT)
            else:
                print("[File:%s line:%d]: No record can be found in the data file!" % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
                sys.exit()

        else:
            print("[File: %s line:%d]: file %s is not exists!" % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, file_path))
            sys.exit()


if __name__ == '__main__':
    zz500_stock_list_file_url = 'https://csi-web-dev.oss-cn-shanghai-finance-1-pub.aliyuncs.com/static/html/csindex/public/uploads/file/autofile/cons/000905cons.xls'
    hs300_stock_list_file_url = 'https://csi-web-dev.oss-cn-shanghai-finance-1-pub.aliyuncs.com/static/html/csindex/public/uploads/file/autofile/cons/000300cons.xls'
    file_name = get_name_and_code(hs300_stock_list_file_url)
    hs300_code_list = get_china_stock_list(hs300_stock_list_file_url, file_name, dp.DATA_DIR)
    print('The length of hs300_code_list is: %d!' % (len(hs300_code_list)))
    file_name = get_name_and_code(zz500_stock_list_file_url)
    zz500_code_list = get_china_stock_list(zz500_stock_list_file_url, file_name, dp.DATA_DIR)
    print('The length of zz500_code_list is: %d!' % (len(zz500_code_list)))

    # my_code_list = ['000002']
    # print('The length of my_code_list is: %d' % len(my_code_list))
    # code_list = hs300_code_list + zz500_code_list + my_code_list
    # code_list = list(set(my_code_list))
    # print('The length of code_list is: %d' % len(code_list))
    # util.transfer_code_as_ts_code(code_list)
    # download_stock_data_as_csv(code_list, dataDate)
#     hs300_code_list = get_china_stock_list(hs300_stock_list_file_url, file_name,dataDate)
#     update_stock_data_for_list(hs300_code_list,dataDate,updateDir=True)
      ################################################################################
      ## here dataDate is the directory(named by date) which save the old data     ###
      ################################################################################
    # update_stock_data_for_list(code_list, dataDate)

