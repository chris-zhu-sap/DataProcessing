"""
Created on Sep 18, 2017

@author: Administrator
"""
import os
import pandas as pd
import sys
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import stock_data as sd
import util

DAY = 'D'
WEEK = 'W'
MONTH = 'M'
HOUR = '60'

SIGNAL_CROSS_BULL = 'cross_bull_band'
SIGNAL_MACD_DEVIATION = 'macd_deviation'

SHORT = 12
LONG = 26
MID = 9

KDJ_N = 9
KDJ_M1 = 3

BULL_N_DAYS = 20
BULL_N_FACTOR = 2

FLOAT_FORMAT2 = '%.2f'
FLOAT_FORMAT4 = '%.4f'

PERIOD_LIST_ALL = [DAY, WEEK, MONTH]

PERIOD_LIST_MA = [5, 10, 20, 30, 60, 120, 250]

PERIOD_LIST_DEV = [5, 10, 20, 30, 60, 120, 250]


def job():
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def generate_more_data_for_all_period(stock_code=None, data_dir_by_date=None):
    if stock_code is not None and data_dir_by_date is not None:
        for period in PERIOD_LIST_ALL:
            stock_data = sd.StockData(stock_code, date=data_dir_by_date)
            stock_obj = DataProcess(stock_code, stock_data.getStockName(), data_dir_by_date, period)
            stock_obj.makeGenData()
            stock_obj.saveAsGeneratedData()
            print("[File:%s line:%d] Message: generate data for stock:%s of period:%s has been done!" % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, stock_code, period))
    else:
        print("[File:%s line:%d] Error: Parameters should not be empty!" % (
            sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
        sys.exit()


def generate_more_data_for_all_stocks(stock_list, data_dir_by_date):
    if stock_list is None or not len(stock_list) or data_dir_by_date is None:
        print("[File:%s line:%d] Error: Parameters should not be empty!" % (
            sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
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
                p_stock.addMA(PERIOD_LIST_MA)
                p_stock.addMACD()
                p_stock.saveAsGeneratedData()
                p_stock.generateDeviationByMACD(PERIOD_LIST_DEV, last_n_periods=length, update=True,
                                                update_report_for_latest_data=True)
                print(
                    "[File:%s line:%d] Message: update generated data for stock:%s of Period:%s has been done!" % (
                        sys._getframe().f_code.co_filename, sys._getframe().f_lineno, stock_code, period))
            else:
                print("[File:%s line:%d] Message: No updated data for stock:%s of Period:%s!" % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno, stock_code, period))
    else:
        print("[File:%s line:%d] Error: Parameters should not be empty!" % (
            sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
        sys.exit()


def update_generated_data_for_all_stocks(stock_list=None, data_dir_by_date=None):
    if stock_list is None or not len(stock_list) or data_dir_by_date is None:
        print("[File:%s line:%d] Error: Parameters should not be empty!" % (
            sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
        sys.exit()
    else:
        for code in stock_list:
            update_generated_data_for_all_period(code, data_dir_by_date)


def get_current_trade_report_for_all_period(stock_code=None, data_dir_by_date=None):
    if stock_code is not None and data_dir_by_date is not None:
        for period in PERIOD_LIST_ALL:
            stock = sd.StockData(stock_code, date=data_dir_by_date)
            p_stock = DataProcess(stock_code, stock.getStockName(), data_dir_by_date, period)
            p_stock.readGenData()
            p_stock.generateDeviationByMACD(PERIOD_LIST_DEV, last_n_periods=1,
                                            update_report_for_current_trade_data=True)
            print(
                f"[File:{sys._getframe().f_code.co_filename} line:{sys._getframe().f_lineno}] Message: update generated data for stock:{stock_code} of Period:{period} has been done!")
    else:
        print(
            f"[File:{sys._getframe().f_code.co_filename} line:{sys._getframe().f_lineno}] Error: Parameters should not be empty!")
        sys.exit()


def get_current_trade_report(stock_list=None, data_dir_by_date=None):
    if stock_list is None or not len(stock_list) or data_dir_by_date is None:
        print(
            f"[File:{sys._getframe().f_code.co_filename} line:{sys._getframe().f_lineno}] Error: Parameters should not be empty!")
        sys.exit()
    else:
        for code in stock_list:
            get_current_trade_report_for_all_period(code, data_dir_by_date)


def get_trade_signal(stock_list=None, data_dir_by_date=None):
    if stock_list is None or not len(stock_list) or data_dir_by_date is None:
        print(
            f"[File:{sys._getframe().f_code.co_filename} line:{sys._getframe().f_lineno}] Error: Parameters should not be empty!")
        sys.exit()
    else:
        util.create_report_dir()
        df_day_bull_cross = pd.DataFrame()
        df_week_bull_cross = pd.DataFrame()
        df_month_bull_cross = pd.DataFrame()
        df_day_deviation = pd.DataFrame()
        df_week_deviation = pd.DataFrame()
        df_month_deviation = pd.DataFrame()
        for code in stock_list:
            for period in PERIOD_LIST_ALL:
                stock = sd.StockData(code, date=data_dir_by_date)
                stock_processed_data = DataProcess(code, stock.getStockName(), data_dir_by_date, period)
                stock_processed_data.readGenData()
                have_deviation_data = stock_processed_data.readDeviationData()
                latest_date_str = stock_processed_data.getLatestDateStr()
                if stock_processed_data.isCrossBullBandCurrently() is True:
                    df_cross = stock_processed_data.getLatestGenData()
                    df_cross = df_cross.loc[:, ['trade_date', 'flag_cross_top', 'flag_cross_bottom', 'flag_mid_up']]
                    df_cross['aCode'] = stock_processed_data.code
                    df_cross['aName'] = stock_processed_data.name
                    if period == DAY:
                        if len(df_day_bull_cross) > 0:
                            df_day_bull_cross = df_day_bull_cross.append(df_cross)
                        else:
                            df_day_bull_cross = df_cross
                    elif period == WEEK:
                        if len(df_week_bull_cross) > 0:
                            df_week_bull_cross = df_week_bull_cross.append(df_cross)
                        else:
                            df_week_bull_cross = df_cross
                    elif period == MONTH:
                        if len(df_month_bull_cross) > 0:
                            df_month_bull_cross = df_month_bull_cross.append(df_cross)
                        else:
                            df_month_bull_cross = df_cross
                    else:
                        print("[File:%s line:%d stock:%s] Error: invalid value for period!" % (
                            sys._getframe().f_code.co_filename, sys._getframe().f_lineno, stock_processed_data.code))
                        sys.exit()
                if have_deviation_data is True:
                    latest_deviation_date_str = stock_processed_data.getLatestDeviationDateStr()
                    if latest_deviation_date_str == latest_date_str:
                        latest_date = stock_processed_data.getLatestDate()
                        df_deviation = stock_processed_data.getDeviationDateData(latest_date)
                        if period == DAY:
                            if len(df_day_deviation) > 0:
                                df_day_deviation = df_day_deviation.append(df_deviation)
                            else:
                                df_day_deviation = df_deviation
                        elif period == WEEK:
                            if len(df_week_deviation) > 0:
                                df_week_deviation = df_week_deviation.append(df_deviation)
                            else:
                                df_week_deviation = df_deviation
                        elif period == MONTH:
                            if len(df_month_deviation) > 0:
                                df_month_deviation = df_month_deviation.append(df_deviation)
                            else:
                                df_month_deviation = df_deviation
                        else:
                            print("[File:%s line:%d stock:%s] Error: invalid value for period!" % (
                                sys._getframe().f_code.co_filename, sys._getframe().f_lineno,
                                stock_processed_data.code))
                            sys.exit()
        if len(df_day_bull_cross) > 0:
            file_path = util.get_signal_file_path(DAY, SIGNAL_CROSS_BULL)
            util.write_signal_into_csv(df_day_bull_cross, file_path)
        if len(df_week_bull_cross) > 0:
            file_path = util.get_signal_file_path(WEEK, SIGNAL_CROSS_BULL)
            util.write_signal_into_csv(df_week_bull_cross, file_path)
        if len(df_month_bull_cross) > 0:
            file_path = util.get_signal_file_path(MONTH, SIGNAL_CROSS_BULL)
            util.write_signal_into_csv(df_month_bull_cross, file_path)
        if len(df_day_deviation) > 0:
            file_path = util.get_signal_file_path(DAY, SIGNAL_MACD_DEVIATION)
            util.write_signal_into_csv(df_day_deviation, file_path)
        if len(df_week_deviation) > 0:
            file_path = util.get_signal_file_path(WEEK, SIGNAL_MACD_DEVIATION)
            util.write_signal_into_csv(df_week_deviation, file_path)
        if len(df_month_deviation) > 0:
            file_path = util.get_signal_file_path(MONTH, SIGNAL_MACD_DEVIATION)
            util.write_signal_into_csv(df_month_deviation, file_path)


class DataProcess(object):
    def __init__(self, stock_code, stock_name, data_dir_by_date, period=DAY):
        self.dataPath = data_dir_by_date + util.get_delimiter() + stock_code + util.get_delimiter()
        self.code = stock_code
        self.name = stock_name
        self.period = period
        self.dfData = pd.DataFrame()
        self.dfGenData = pd.DataFrame()
        self.dfDeviationData = pd.DataFrame()

        if period == DAY:
            self.dataCsvFile = self.dataPath + stock_code + "_k_day.csv"
            self.dataGenCsvFile = self.dataPath + stock_code + "_k_day_gen.csv"
            self.deviationReportFile = self.dataPath + stock_code + "_deviation_day.csv"
            self.reportOfLatest = data_dir_by_date + util.get_delimiter() + 'latest_day_report.csv'
            self.reportOfCurrentTrade = data_dir_by_date + util.get_delimiter() + 'current_trade_day_report.csv'
        elif period == WEEK:
            self.dataCsvFile = self.dataPath + stock_code + "_k_week.csv"
            self.dataGenCsvFile = self.dataPath + stock_code + "_k_week_gen.csv"
            self.deviationReportFile = self.dataPath + stock_code + "_deviation_week.csv"
            self.reportOfLatest = data_dir_by_date + util.get_delimiter() + 'latest_week_report.csv'
            self.reportOfCurrentTrade = data_dir_by_date + util.get_delimiter() + 'current_trade_week_report.csv'
        elif period == MONTH:
            self.dataCsvFile = self.dataPath + stock_code + "_k_month.csv"
            self.dataGenCsvFile = self.dataPath + stock_code + "_k_month_gen.csv"
            self.deviationReportFile = self.dataPath + stock_code + "_deviation_month.csv"
            self.reportOfLatest = data_dir_by_date + util.get_delimiter() + 'latest_month_report.csv'
            self.reportOfCurrentTrade = data_dir_by_date + util.get_delimiter() + 'current_trade_month_report.csv'
        elif period == HOUR:
            self.dataCsvFile = self.dataPath + stock_code + "_k_hour.csv"
            self.dataGenCsvFile = self.dataPath + stock_code + "_k_hour_gen.csv"
            self.deviationReportFile = self.dataPath + stock_code + "_deviation_hour.csv"
            self.reportOfLatest = data_dir_by_date + util.get_delimiter() + 'latest_hour_report.csv'
            self.reportOfCurrentTrade = data_dir_by_date + util.get_delimiter() + 'current_trade_hour_report.csv'
        else:
            print("[File:%s line:%d stock:%s] Error: input parameter period is not supported now!" % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

    @staticmethod
    def getDfData(self):
        return self.dfData

    def setDfData(self, df_data_new):
        if len(df_data_new) > 0:
            self.dfData = df_data_new

    def readData(self):
        if os.path.exists(self.dataCsvFile):
            self.dfData = pd.read_csv(self.dataCsvFile)
        else:
            print('[File:%s line:%d stock:%s] Error: File %s is not exist' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code, self.dataCsvFile))
            sys.exit()

    def deleteGenFile(self):
        if os.path.exists(self.dataGenCsvFile):
            os.remove(self.dataGenCsvFile)

    def readGenData(self):
        if os.path.exists(self.dataGenCsvFile):
            self.dfGenData = pd.read_csv(self.dataGenCsvFile)
        else:
            print('[File:%s line:%d stock:%s] Error: File %s is not exist' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code, self.dataGenCsvFile))
            sys.exit()

    def readDeviationData(self):
        if os.path.exists(self.deviationReportFile):
            self.dfDeviationData = pd.read_csv(self.deviationReportFile)
            return True
        else:
            return False

    def getLatestDeviationDateStr(self):
        index_latest = len(self.dfDeviationData) - 1
        if index_latest >= 0:
            return str(self.dfDeviationData.at[index_latest, 'dateCurrent'])
        else:
            print('[File:%s line:%d stock:%s] Error: deviation data is empty!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

    def getDeviationDateData(self, date):
        index_latest = len(self.dfDeviationData) - 1
        if index_latest >= 0:
            return self.dfDeviationData[self.dfDeviationData['dateCurrent'] == date]
        else:
            print('[File:%s line:%d stock:%s] Error: deviation data is empty!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

    def isCrossBullBandCurrently(self):
        if 'flag_cross_top' not in self.dfGenData.columns:
            return False
        index_latest = len(self.dfGenData) - 1
        if index_latest >= 0:
            bool_cross_top = self.dfGenData.at[index_latest, 'flag_cross_top']
            bool_cross_bottom = self.dfGenData.at[index_latest, 'flag_cross_bottom']
            if bool_cross_top == True or bool_cross_bottom == True:
                return True
            return False
        else:
            print('[File:%s line:%d stock:%s] Error: generated data is empty!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

    def getLatestGenData(self):
        index_latest = len(self.dfGenData) - 1
        if index_latest >= 0:
            return self.dfGenData[index_latest:]
        else:
            print('[File:%s line:%d stock:%s] Error: generated data is empty!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

    def getLatestDateStr(self):
        index_latest = len(self.dfGenData) - 1
        if index_latest >= 0:
            return str(self.dfGenData.at[index_latest, 'trade_date'])
        else:
            print('[File:%s line:%d stock:%s] Error: generated data is empty!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

    def getLatestDate(self):
        index_latest = len(self.dfGenData) - 1
        if index_latest >= 0:
            return self.dfGenData.at[index_latest, 'trade_date']
        else:
            print('[File:%s line:%d stock:%s] Error: generated data is empty!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

    def saveAsGeneratedData(self):
        self.deleteGenFile()
        self.dfData.to_csv(self.dataGenCsvFile, index=False, float_format=FLOAT_FORMAT2, encoding="utf-8")

    def addMA(self, period_list):
        if period_list is None or not len(period_list):
            print('[File:%s line:%d stock:%s] Error: Parameter is invalid or data is empty!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

        for period in period_list:
            if period <= len(self.dfData):
                maName = 'MA' + str(period)
                self.dfData[maName] = pd.Series(self.dfData['close']).rolling(period).mean()

        self.dfData.fillna(0, inplace=True)

    def addKDJ(self, n=KDJ_N, m1=KDJ_M1):
        if len(self.dfData) > 0 and n > 0 and m1 > 0:
            if len(self.dfData) >= n:
                self.dfData['llv_low'] = self.dfData['low'].rolling(n).min()
                self.dfData['hhv_high'] = self.dfData['high'].rolling(n).max()
                self.dfData['rsv'] = (self.dfData['close'] - self.dfData['llv_low']) / (
                        self.dfData['hhv_high'] - self.dfData['llv_low'])
                self.dfData['k'] = self.dfData['rsv'].ewm(adjust=False, alpha=1 / m1).mean()
                self.dfData['d'] = self.dfData['k'].ewm(adjust=False, alpha=1 / m1).mean()
                self.dfData['j'] = 3 * self.dfData['k'] - 2 * self.dfData['d']
                self.dfData['k'] = self.dfData['k'] * 100
                self.dfData['d'] = self.dfData['d'] * 100
                self.dfData['j'] = self.dfData['j'] * 100
                self.dfData.fillna(0, inplace=True)
                # only keep k d j, remove the middle data
                self.dfData.drop('llv_low', axis=1, inplace=True)
                self.dfData.drop('hhv_high', axis=1, inplace=True)
                self.dfData.drop('rsv', axis=1, inplace=True)
        else:
            print('[File:%s line:%d stock:%s!] Error: Parameter is invalid!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

    def addBullBand(self, n_days=BULL_N_DAYS, n_factor=BULL_N_FACTOR):
        if len(self.dfData) > 0 and n_days > 0:
            if len(self.dfData) >= BULL_N_DAYS:
                self.dfData['bull_mid'] = self.dfData['close'].rolling(n_days).mean()
                std = self.dfData['close'].rolling(n_days).std()
                self.dfData['bull_top'] = self.dfData['bull_mid'] + n_factor * std
                self.dfData['bull_bottom'] = self.dfData['bull_mid'] - n_factor * std
                self.dfData.fillna(0, inplace=True)
                if 'bull_top' in self.dfData.columns:
                    self.addFlagForBullBand()
        else:
            print('[File:%s line:%d stock:%s!] Error: Parameter is invalid!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

    def addFlagForBullBand(self):
        length = len(self.dfData)
        if length > 0:
            self.dfData['flag_cross_top'] = self.dfData['high'] > self.dfData['bull_top']
            self.dfData['flag_cross_bottom'] = self.dfData['low'] < self.dfData['bull_bottom']
            self.dfData['flag_mid_up'] = False
            for i in range(length):
                if i > 0 and self.dfData.at[i, 'bull_mid'] > self.dfData.at[i-1, 'bull_mid']:
                    self.dfData.at[i, 'flag_mid_up'] = True
        else:
            print('[File:%s line:%d stock:%s!] Error: Parameter is invalid!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

    def addMACD(self, short=SHORT, long=LONG, mid=MID):
        if len(self.dfData) > 0 and short > 0 and long > 0 and mid > 0:
            if len(self.dfData) >= long:
                # calculate EMA
                # self.dfData['sema'] = pd.Series(self.dfData['close']).ewm(span=short).mean()
                # self.dfData['lema'] = pd.Series(self.dfData['close']).ewm(span=long).mean()
                self.dfData['sema'] = self.dfData['close'].ewm(adjust=False, alpha=2/(short+1), ignore_na=True).mean()
                self.dfData['lema'] = self.dfData['close'].ewm(adjust=False, alpha=2/(long+1), ignore_na=True).mean()
                # fill 0 if data=NA in dfData['sema'] and dfData['lema']
                self.dfData.fillna(0, inplace=True)
                # calculate diff, dea and macd
                self.dfData['dif'] = self.dfData['sema'] - self.dfData['lema']
                # self.dfData['dea'] = pd.Series(self.dfData['dif']).ewm(span=mid).mean()
                self.dfData['dea'] = self.dfData['dif'].ewm(adjust=False, alpha=2/(mid+1), ignore_na=True).mean()
                self.dfData['macd'] = 2 * (self.dfData['dif'] - self.dfData['dea'])
                # fill 0 if data=NA in dfData['data_dif'],dfData['data_dea'],dfData['data_macd']
                self.dfData.fillna(0, inplace=True)

                self.dfData.drop('sema', axis=1, inplace=True)
                self.dfData.drop('lema', axis=1, inplace=True)
        else:
            print('[File:%s line:%d stock:%s!] Error: Parameter is invalid!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

    def generateDeviationByMACD(self, period_list, last_n_periods=None, update=False,
                                update_report_for_latest_data=False,
                                update_report_for_current_trade_data=False):
        if period_list is None or not len(period_list):
            print('[File:%s line:%d stock:%s] Error: Parameter is invalid or data is empty!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
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
                print('[File:%s line:%d stock:%s] Message: deviationReportFile: %s not exits!' % (
                    sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.deviationReportFile,
                    self.code))

        old_length = len(df)

        data_df = self.dfData

        df_length = len(data_df)

        if last_n_periods is None:
            last_n_periods = len(data_df)

        if last_n_periods <= 0:
            last_n_periods = 1

        if df_length < LONG:
            print('[File:%s line:%d stock:%s] Message: Length of data is smaller than parameter LONG: %d!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code, LONG))
        else:
            array_last_n_periods = range(last_n_periods)
            for last_period in reversed(array_last_n_periods):
                duplicated_flag = False
                for period in period_list:
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
                                        'dateCurrent': [data_df.at[index_last_day, 'trade_date']],
                                        'dateOfLastDif': [data_df.at[index, 'trade_date']],
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
                                    self.code, data_df.at[index_last_day, 'trade_date'], period))

                                if update_report_for_latest_data and last_period == 0 and update_report_for_current_trade_data is False:
                                    df_latest = pd.concat([df_latest, df_per])
                                    df_latest.to_csv(self.reportOfLatest, index=False, float_format=FLOAT_FORMAT2,
                                                     encoding="utf-8")
                                    print('Add the bottom deviation of latest data for the stock:%s has been done!' % (
                                        self.code))

                                if update_report_for_current_trade_data and last_period == 0:
                                    df_current = pd.concat([df_current, df_per])
                                    df_current.to_csv(self.reportOfCurrentTrade, index=False,
                                                      float_format=FLOAT_FORMAT2,
                                                      encoding="utf-8")
                                    print(
                                        'Add the bottom deviation of current trade data for the stock:%s has been done!' % (
                                            self.code))

                                if not duplicated_flag:
                                    duplicated_flag = True

                        # 顶背离
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
                                        'dateCurrent': [data_df.at[index_last_day, 'trade_date']],
                                        'dateOfLastDif': [data_df.at[index, 'trade_date']],
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
                                    self.code, data_df.at[index_last_day, 'trade_date'], period))

                                if update_report_for_latest_data and last_period == 0 and update_report_for_current_trade_data is False:
                                    df_latest = pd.concat([df_latest, df_per])
                                    df_latest.to_csv(self.reportOfLatest, index=False, float_format=FLOAT_FORMAT2,
                                                     encoding="utf-8")
                                    print('Add the top deviation of lastest data for the stock:%s has been done!' % (
                                        self.code))

                                if update_report_for_current_trade_data and last_period == 0:
                                    df_current = pd.concat([df_current, df_per])
                                    df_current.to_csv(self.reportOfCurrentTrade, index=False,
                                                      float_format=FLOAT_FORMAT2,
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
        self.addMA(PERIOD_LIST_MA)
        self.addMACD()
        self.addKDJ()
        self.addBullBand()
        self.generateDeviationByMACD(period_list=PERIOD_LIST_DEV, last_n_periods=1)
        # self.generateDeviationByMACD(PERIOD_LIST_DEV)


def job_update_and_generate_data_daily():
    dataDate = '2021-08-13'

    #     hs300_stock_list_file_url  = 'http://www.csindex.com.cn/uploads/file/autofile/cons/000300cons.xls'
    #     index_code, file_name= sd.get_name_and_code(hs300_stock_list_file_url)
    #     hs300_code_list = sd.get_china_stock_list(hs300_stock_list_file_url, file_name)
    #
    #     zz500StockListFileUrl  = 'http://www.csindex.com.cn/uploads/file/autofile/cons/000905cons.xls'
    #     index_code, file_name= sd.get_name_and_code(zz500StockListFileUrl)
    #     zz500_code_list = sd.get_china_stock_list(zz500StockListFileUrl, file_name

    #     code_list = hs300_code_list + zz500_code_list + code_list
    #     code_list = list(set(code_list))

    # concerned stocks
    code_list1 = ['002773', '600877', '601628', '300146', '600547', '300498']
    # cyclical stocks
    code_list2 = ['000002', '600585', '601628', '300059', '601318', '600030', '601288', '600547', '601988', '601696', '600036']
    # foreign capital
    code_list3 = ['002353', '000338', '000333', '300285', '000651', '601901', '002008', '600887', '600872', '002439', '300244', '603882', '300012', '300347', '603489', '002508', '600406', '300450', '600885', '002812']
    # tech stocks
    code_list4 = ['300346', '688012', '605111', '000158', '688561', '300339', '002371', '300373']
    # pharmaceutical stocks
    code_list5 = ['002382', '002223', '688690', '300358', '688399', '002422', '300725', '688180', '688505', '688266', '300142', '002773', '300003', '300009', '300558', '300146', '600276']
    # new energy
    code_list6 = ['002249', '601865', '300376', '002709', '002158', '002594', '601615', '002733', '002639', '688339', '600478', '002129', '603806']
    # yi mei
    code_list7 = ['000963', '300896', '688363']
    # wine
    code_list8 = ['600779', '002304', '000799', '600809', '000858', '600519']
    code_list = code_list1 + code_list2 + code_list3 + code_list4 + code_list5 + code_list6 + code_list7 + code_list8
    code_list = list(set(code_list))
    # code_list = ['000002']
    util.transfer_code_as_ts_code(code_list)
    sd.download_stock_data_as_csv(code_list, dataDate)
    sd.update_stock_data_for_list(code_list, dataDate)
    generate_more_data_for_all_stocks(code_list, dataDate)
    get_trade_signal(code_list, dataDate)


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(job_update_and_generate_data_daily, 'cron', day_of_week='1-5', hour=1, minute=0)
    scheduler.start()

    # don't use time Scheduler
    # job_update_and_generate_data_daily()


    # update_generated_data_for_all_stocks(code_list, dataDate)
    # get_current_trade_report(code_list, dataDate)
    # code_list = ['000001','000002','000063']
    # generate_more_data_for_all_stocks(code_list, dataDate)
    # update_generated_data_for_all_stocks(code_list,dataDate)
    # get_current_trade_report(code_list,dataDate)
