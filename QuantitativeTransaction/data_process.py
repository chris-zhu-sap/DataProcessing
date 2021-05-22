"""
Created on Sep 18, 2017

@author: Administrator
"""
import platform
import os
import pandas as pd
import sys
import tushare as ts
import time
import stock_data as sd

DAY = 'D'
WEEK = 'W'
MONTH = 'M'
HOUR = '60'
MAX_KDJ = 80
MIN_KDJ = 15

SHORT = 12
LONG = 26
MID = 9

FLOAT_FORMAT2 = '%.2f'
FLOAT_FORMAT4 = '%.4f'

PERIOD_LIST_ALL = [DAY, WEEK, MONTH]

PERIOD_LIST_MA = [5, 10, 20, 30, 60, 120, 250]

PERIOD_LIST_DEV = [5, 10, 20, 30, 60, 120, 250]


def generate_more_data_for_all_period(stock_code=None, data_dir_by_date=None):
    if stock_code is not None and data_dir_by_date is not None:
        for period in PERIOD_LIST_ALL:
            stock_data = sd.StockData(stock_code, date=data_dir_by_date)
            stock_obj = DataProcess(stock_code, stock_data.getStockName(), data_dir_by_date, period)
            stock_obj.makeGenData()
            stock_obj.saveAsGeneratedData()
            print("[Function:%s line:%s] Message: generate data for stock:%s of period:%s has been done!" % (
                generate_more_data_for_all_period.__name__, sys._getframe().f_lineno, stock_code, period))
    else:
        print("[Function:%s line:%s] Error: Parameters should not be empty!" % (
            generate_more_data_for_all_period.__name__, sys._getframe().f_lineno))
        sys.exit()


def generate_more_data_for_all_stocks(stock_list, data_dir_by_date):
    if stock_list is None or not len(stock_list) or data_dir_by_date is None:
        print("[Function:%s line:%s] Error: Parameters should not be empty!" % (
            generate_more_data_for_all_stocks.__name__, sys._getframe().f_lineno))
        sys.exit()
    else:
        for code in stock_list:
            generate_more_data_for_all_period(code, data_dir_by_date)


def update_generated_data_for_all_period(stock_code=None, data_dir_by_date=None):
    if stock_code is not None and data_dir_by_date is not None:
        for period in PERIOD_LIST_ALL:
            stock = sd.StockData(stock_code, date=data_dir_by_date)
            stock.updateKData(period)
            length = stock.getDataLenUpdated()
            if length > 0:
                p_stock = DataProcess(stock_code, stock.getStockName(), data_dir_by_date, period)
                p_stock.readData()
                #                 pStock.addMA(PERIOD_LIST_MA)
                p_stock.addMACD()
                p_stock.saveAsGeneratedData()
                p_stock.generateDeviationByMACD(PERIOD_LIST_DEV, last_n_periods=length, update=True,
                                                update_report_for_latest_data=True)
                print(
                    "[Function:%s line:%s] Message: update generated data for stock:%s of Period:%s has been done!" % (
                        update_generated_data_for_all_period.__name__, sys._getframe().f_lineno, stock_code, period))
            else:
                print("[Function:%s line:%s] Message: No updated data for stock:%s of Period:%s!" % (
                    update_generated_data_for_all_period.__name__, sys._getframe().f_lineno, stock_code, period))
    else:
        print("[Function:%s line:%s] Error: Parameters should not be empty!" % (
            update_generated_data_for_all_period.__name__, sys._getframe().f_lineno))
        sys.exit()


def update_generated_data_for_all_stocks(stock_list=None, data_dir_by_date=None):
    if stock_list is None or not len(stock_list) or data_dir_by_date is None:
        print("[Function:%s line:%s] Error: Parameters should not be empty!" % (
            update_generated_data_for_all_period.__name__, sys._getframe().f_lineno))
        sys.exit()
    else:
        for code in stock_list:
            update_generated_data_for_all_period(code, data_dir_by_date)


def get_current_trade_report_for_all_period(stock_code=None, data_dir_by_date=None, df_data=None):
    if stock_code is not None and data_dir_by_date is not None and df_data is not None:
        for period in PERIOD_LIST_ALL:
            length = len(df_data)
            if length:
                stock = sd.StockData(stock_code, date=data_dir_by_date)
                df_merged = stock.mergeData(df_data, period)
                df_merged = df_merged.reset_index(drop=True)
                p_stock = DataProcess(stock_code, stock.getStockName(), data_dir_by_date, period)
                #                 pStock.addMA(PERIOD_LIST_MA)
                p_stock.setDfData(df_merged)
                p_stock.addMACD()
                p_stock.generateDeviationByMACD(PERIOD_LIST_DEV, last_n_periods=1, update_report_for_current_trade_data=True)
                print(
                    f"[Function:{get_current_trade_report_for_all_period.__name__} line:{sys._getframe().f_lineno}] Message: update generated data for stock:{stock_code} of Period:{period} has been done!")
    else:
        print(
            f"[Function:{get_current_trade_report_for_all_period.__name__} line:{sys._getframe().f_lineno}] Error: Parameters should not be empty!")
        sys.exit()


def get_current_trade_report(stock_list=None, data_dir_by_date=None):
    if stock_list is None or not len(stock_list) or data_dir_by_date is None:
        print(
            f"[Function:{get_current_trade_report.__name__} line:{sys._getframe().f_lineno}] Error: Parameters should not be empty!")
        sys.exit()
    else:
        df_current = ts.get_today_all()
        today = time.strftime('%Y-%m-%d')
        for code in stock_list:
            df = df_current[df_current['code'] == code]
            if len(df) > 0:
                data = {'date': [today],
                        'open': [df['open'].values[0]],
                        'close': [df['trade'].values[0]],
                        'high': [df['high'].values[0]],
                        'low': [df['low'].values[0]],
                        'volume': [df['volume'].values[0]]
                        }
                df = pd.DataFrame(data)
                get_current_trade_report_for_all_period(code, data_dir_by_date, df)
            else:
                print(
                    f"[Function:{get_current_trade_report.__name__} line:{sys._getframe().f_lineno}] Message: data for code:{code} not exists!")


class DataProcess(object):
    def __init__(self, stock_code, stock_name, data_dir_by_date, period=DAY):
        self.dataPath = data_dir_by_date + self.get_delimiter() + stock_code + self.get_delimiter()
        self.code = stock_code
        self.name = stock_name
        self.period = period
        self.dfData = 0

        if period == DAY:
            self.dataCsvFile = self.dataPath + stock_code + "_k_day.csv"
            self.dataGenCsvFile = self.dataPath + stock_code + "_k_day_gen.csv"
            self.deviationReportFile = self.dataPath + stock_code + "_deviation_day.csv"
            self.reportOfLatest = data_dir_by_date + self.get_delimiter() + 'latest_day_report.csv'
            self.reportOfCurrentTrade = data_dir_by_date + self.get_delimiter() + 'current_trade_day_report.csv'
        elif period == WEEK:
            self.dataCsvFile = self.dataPath + stock_code + "_k_week.csv"
            self.dataGenCsvFile = self.dataPath + stock_code + "_k_week_gen.csv"
            self.deviationReportFile = self.dataPath + stock_code + "_deviation_week.csv"
            self.reportOfLatest = data_dir_by_date + self.get_delimiter() + 'latest_week_report.csv'
            self.reportOfCurrentTrade = data_dir_by_date + self.get_delimiter() + 'current_trade_week_report.csv'
        elif period == MONTH:
            self.dataCsvFile = self.dataPath + stock_code + "_k_month.csv"
            self.dataGenCsvFile = self.dataPath + stock_code + "_k_month_gen.csv"
            self.deviationReportFile = self.dataPath + stock_code + "_deviation_month.csv"
            self.reportOfLatest = data_dir_by_date + self.get_delimiter() + 'latest_month_report.csv'
            self.reportOfCurrentTrade = data_dir_by_date + self.get_delimiter() + 'current_trade_month_report.csv'
        elif period == HOUR:
            self.dataCsvFile = self.dataPath + stock_code + "_k_hour.csv"
            self.dataGenCsvFile = self.dataPath + stock_code + "_k_hour_gen.csv"
            self.deviationReportFile = self.dataPath + stock_code + "_deviation_hour.csv"
            self.reportOfLatest = data_dir_by_date + self.get_delimiter() + 'latest_hour_report.csv'
            self.reportOfCurrentTrade = data_dir_by_date + self.get_delimiter() + 'current_trade_hour_report.csv'
        else:
            print("[Function:%s line:%s stock:%s] Error: input parameter period is not supported now!" % (
                self.__init__.__name__, sys._getframe().f_lineno, self.code))
            sys.exit()

    @staticmethod
    def get_delimiter():
        if 'Windows' in platform.system():
            return "\\"
        else:
            return "/"

    def getDfData(self):
        return self.dfData

    def setDfData(self, df_data_new):
        if len(df_data_new) > 0:
            self.dfData = df_data_new

    def readData(self):
        if os.path.exists(self.dataCsvFile):
            self.dfData = pd.read_csv(self.dataCsvFile)
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' % (
                self.readData.__name__, sys._getframe().f_lineno, self.code, self.dataCsvFile))
            sys.exit()

        if os.path.exists(self.dataGenCsvFile):
            os.remove(self.dataGenCsvFile)

    def saveAsGeneratedData(self):
        self.dfData.to_csv(self.dataGenCsvFile, index=False, float_format=FLOAT_FORMAT2, encoding="utf-8")

    def addMA(self, period_list):
        if period_list is None or not len(period_list):
            print('[Function:%s line:%s stock:%s] Error: Parameter is invalid or data is empty!' % (
                self.addMA.__name__, sys._getframe().f_lineno, self.code))
            sys.exit()

        for period in period_list:
            if len(self.dfData) >= period:
                maName = 'MA' + str(period)
                self.dfData[maName] = pd.Series(self.dfData['close']).rolling(period).mean()
                # pd.rolling_mean(self.dfData['close'], period)
                print('Add MA%d for %s has been done!' % (period, self.dataCsvFile))
            else:
                print('Length of data is smaller than period:%d, can not generate such MA for this period!' % (period))

        self.dfData.fillna(0, inplace=True)

    def addMACD(self, short=SHORT, long=LONG, mid=MID):
        if not self.dfData.empty and short > 0 and long > 0 and mid > 0:
            if len(self.dfData) >= long:
                # calculate short EMA
                self.dfData['sema'] = pd.Series(self.dfData['close']).ewm(span=short).mean()
                # calculate long EMA
                self.dfData['lema'] = pd.Series(self.dfData['close']).ewm(span=long).mean()
                # fill 0 if data=NA in dfData['sema'] and dfData['lema']
                self.dfData.fillna(0, inplace=True)
                # calculate diff, dea and macd
                self.dfData['dif'] = self.dfData['sema'] - self.dfData['lema']
                self.dfData['dea'] = pd.Series(self.dfData['dif']).ewm(span=mid).mean()
                self.dfData['macd'] = 2 * (self.dfData['dif'] - self.dfData['dea'])
                # fill 0 if data=NA in dfData['data_dif'],dfData['data_dea'],dfData['data_macd']
                self.dfData.fillna(0, inplace=True)

                self.dfData.drop('sema', axis=1, inplace=True)
                self.dfData.drop('lema', axis=1, inplace=True)

                print('Add MACD for %s has been done!' % self.dataCsvFile)
            else:
                print('Not enough data to generate MACD for code:%s!' % self.code)
        else:
            print('[Function:%s line:%s stock:%s!] Error: Parameter is invalid!' % (
                self.addMACD.__name__, sys._getframe().f_lineno, self.code))
            sys.exit()

    def generateDeviationByMACD(self, period_list, last_n_periods=None, update=False,
                                update_report_for_latest_data=False,
                                update_report_for_current_trade_data=False):
        if period_list is None or not len(period_list):
            print('[Function:%s line:%s stock:%s] Error: Parameter is invalid or data is empty!' % (
                self.generateDeviationByMACD.__name__, sys._getframe().f_lineno, self.code))
            sys.exit()

        df_current = pd.DataFrame()
        if update_report_for_current_trade_data:
            if os.path.exists(self.reportOfCurrentTrade):
                df_current = pd.read_csv(self.reportOfCurrentTrade, encoding="utf-8", dtype={'aCode': str})

        df_latest = pd.DataFrame()
        if update_report_for_latest_data:
            if os.path.exists(self.reportOfLatest):
                df_latest = pd.read_csv(self.reportOfLatest, encoding="utf-8", dtype={'aCode': str})

        df = pd.DataFrame()
        if update:
            if os.path.exists(self.deviationReportFile):
                df = pd.read_csv(self.deviationReportFile, encoding="utf-8", dtype={'aCode': str})
            else:
                print('[Function:%s line:%s stock:%s] Message: deviationReportFile: %s not exits!' % (
                    self.generateDeviationByMACD.__name__, sys._getframe().f_lineno, self.deviationReportFile,
                    self.code))

        old_length = len(df)

        data_df = self.dfData

        df_length = len(data_df)

        if last_n_periods is None:
            last_n_periods = len(data_df)

        if last_n_periods == 0:
            last_n_periods = 1

        if df_length < LONG:
            print('[Function:%s line:%s stock:%s] Message: Length of data is smaller than parameter LONG: %d!' % (
                self.generateDeviationByMACD.__name__, sys._getframe().f_lineno, self.code, LONG))
        else:
            array_last_n_periods = range(last_n_periods)
            for last_period in reversed(array_last_n_periods):
                duplicated_flag = False
                for period in reversed(period_list):
                    if duplicated_flag:
                        break
                    if df_length >= (period + last_period):
                        index_last_day = df_length - last_period - 1
                        # 底背离
                        price_lowest = 0
                        if last_period > 0:
                            price_lowest = pd.Series(data_df['low'][-(period + last_period):-last_period]).min()
                        else:
                            price_lowest = pd.Series(data_df['low'][-period:]).min()
                        if price_lowest == self.dfData.at[index_last_day, 'low']:
                            dif_data = pd.Series(data_df['dif'][-period:])
                            if last_period > 0:
                                dif_data = pd.Series(data_df['dif'][-(period + last_period):-last_period])
                            index = dif_data.idxmin()
                            index_start = df_length - (period + last_period)
                            if (data_df.at[index, 'low'] > data_df.at[index_last_day, 'low'] and data_df.at[
                                index, 'dif'] <
                                    data_df.at[index_last_day, 'dif'] and index != index_start):
                                data = {'aCode': [self.code],
                                        'aName': [self.name],
                                        'aType': ['底背离'],
                                        'aPeriod': [period],
                                        'dateCurrent': [data_df.at[index_last_day, 'date']],
                                        'dateOfLastDif': [data_df.at[index, 'date']],
                                        'difCurrent': [data_df.at[index_last_day, 'dif']],
                                        'difLast': [data_df.at[index, 'dif']],
                                        'deaCurrent': [data_df.at[index_last_day, 'dea']],
                                        'deaLast': [data_df.at[index, 'dea']],
                                        'macdCurrent': [data_df.at[index_last_day, 'macd']],
                                        'macdLast': [data_df.at[index, 'macd']]
                                        }
                                df_per = pd.DataFrame(data)
                                df = pd.concat([df, df_per])
                                print('Add the bottom deviation data for the stock:%s on date:%s of period %d!' % (
                                    self.code, data_df.at[index_last_day, 'date'], period))

                                if update_report_for_latest_data and last_period == 0 and update_report_for_current_trade_data is False:
                                    df_latest = pd.concat([df_latest, df_per])
                                    df_latest.to_csv(self.reportOfLatest, index=False, float_format=FLOAT_FORMAT2,
                                                     encoding="utf-8")
                                    print('Add the bottom deviation of latest data for the stock:%s has been done!' % (
                                        self.code))

                                if update_report_for_current_trade_data and last_period == 0:
                                    df_current = pd.concat([df_current, df_per])
                                    df_current.to_csv(self.reportOfCurrentTrade, index=False, float_format=FLOAT_FORMAT2,
                                                      encoding="utf-8")
                                    print(
                                        'Add the bottom deviation of current trade data for the stock:%s has been done!' % (
                                            self.code))

                                if not duplicated_flag:
                                    duplicated_flag = True

                        # 顶背离
                        price_highest = 0
                        if last_period > 0:
                            price_highest = pd.Series(data_df['high'][-(period + last_period):-last_period]).max()
                        else:
                            price_highest = pd.Series(data_df['high'][-period:]).max()
                        if price_highest == data_df.at[index_last_day, 'high']:
                            dif_data = pd.Series(data_df['dif'][-period:])
                            if last_period > 0:
                                dif_data = pd.Series(data_df['dif'][-(period + last_period):-last_period])
                            index = dif_data.idxmax()
                            index_start = df_length - (period + last_period)
                            if (data_df.at[index, 'high'] < data_df.at[index_last_day, 'high'] and data_df.at[
                                index, 'dif'] >
                                    data_df.at[index_last_day, 'dif'] and index != index_start):
                                data = {'aCode': [self.code],
                                        'aName': [self.name],
                                        'aType': ['顶背离'],
                                        'aPeriod': [period],
                                        'dateCurrent': [data_df.at[index_last_day, 'date']],
                                        'dateOfLastDif': [data_df.at[index, 'date']],
                                        'difCurrent': [data_df.at[index_last_day, 'dif']],
                                        'difLast': [data_df.at[index, 'dif']],
                                        'deaCurrent': [data_df.at[index_last_day, 'dea']],
                                        'deaLast': [data_df.at[index, 'dea']],
                                        'macdCurrent': [data_df.at[index_last_day, 'macd']],
                                        'macdLast': [data_df.at[index, 'macd']]
                                        }
                                df_per = pd.DataFrame(data)
                                df = pd.concat([df, df_per])
                                print('Add the top deviation data for the stock:%s on date:%s of period %d!' % (
                                    self.code, data_df.at[index_last_day, 'date'], period))

                                if update_report_for_latest_data and last_period == 0 and update_report_for_current_trade_data is False:
                                    df_latest = pd.concat([df_latest, df_per])
                                    df_latest.to_csv(self.reportOfLatest, index=False, float_format=FLOAT_FORMAT2,
                                                     encoding="utf-8")
                                    print('Add the top deviation of lastest data for the stock:%s has been done!' % (
                                        self.code))

                                if update_report_for_current_trade_data and last_period == 0:
                                    df_current = pd.concat([df_current, df_per])
                                    df_current.to_csv(self.reportOfCurrentTrade, index=False, float_format=FLOAT_FORMAT2,
                                                      encoding="utf-8")
                                    print(
                                        'Add the top deviation of current trade data for the stock:%s has been done!' % (
                                            self.code))

                                if not duplicated_flag:
                                    duplicated_flag = True

                    else:
                        print(
                            'The length of data for stock:%s is too small, no deviation can be found! period:%d, last_period:%d  kPeriod:%s' % (
                                self.code, period, last_period, self.period))

        if len(df) > old_length and update_report_for_current_trade_data is False:
            df.drop_duplicates(inplace=True, keep='first')
            df.to_csv(self.deviationReportFile, index=False, float_format=FLOAT_FORMAT2, encoding="utf-8")
            print('Add the deviation data for the stock:%s has been done !' % self.code)

    def makeGenData(self):
        self.readData()
        #         self.addMA(PERIOD_LIST_MA)
        self.addMACD()
        self.generateDeviationByMACD(PERIOD_LIST_DEV)


if __name__ == '__main__':
    dataDate = '2019-01-02'
    #     wanke = DataProcess('000002',dataDate,'D')
    #     wanke.makeGenData()
    #     wanke.saveAsGeneratedData()
    #     generateIncicatorForAllPeriod('000002',dataDate)

    #     hs300StockListFileUrl  = 'http://www.csindex.com.cn/uploads/file/autofile/cons/000300cons.xls'
    #     indexCode, filename= sd.get_name_and_code(hs300StockListFileUrl)
    #     hs300CodeList = sd.get_china_stock_list(hs300StockListFileUrl, filename)
    #
    #     zz500StockListFileUrl  = 'http://www.csindex.com.cn/uploads/file/autofile/cons/000905cons.xls'
    #     indexCode, filename= sd.get_name_and_code(zz500StockListFileUrl)
    #     zz500CodeList = sd.get_china_stock_list(zz500StockListFileUrl, filename)
    #
    #     myCodeList = ['600030','600036','600061','600893','600498','300033','600547','300383','002716','600109','002353','300059']
    #     codeList = hs300CodeList + zz500CodeList + myCodeList
    #     codeList = list(set(codeList))

    codeList = ['000002', '600030', '600036', '600061', '600893', '600498', '300033', '600547', '300383', '002716',
                '600109', '002353', '300059']

    sd.download_stock_data_as_csv(codeList)
    sd.update_stock_data_for_list(codeList, dataDate)
    generate_more_data_for_all_stocks(codeList, dataDate)
    update_generated_data_for_all_stocks(codeList, dataDate)
    get_current_trade_report(codeList, dataDate)
#     codeList = ['000001','000002','000063']
#     generate_more_data_for_all_stocks(codeList, dataDate)
#     update_generated_data_for_all_stocks(codeList,dataDate)
#     get_current_trade_report(codeList,dataDate)
