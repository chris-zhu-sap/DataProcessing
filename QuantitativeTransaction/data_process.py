"""
Created on Sep 18, 2017

@author: Administrator
"""
import os
import pandas as pd
import sys
import stock_data as sd
import util
import numpy as np
from apscheduler.schedulers.blocking import BlockingScheduler

DAY = 'D'
WEEK = 'W'
MONTH = 'M'
HOUR = '60'

SIGNAL_CROSS_BULL = 'cross_bull_band'
SIGNAL_MACD_DEVIATION = 'macd_deviation'

SIGNAL_MA_UP = 'ma_start_up'
SIGNAL_MA_DOWN = 'ma_start_down'

SIGNAL_UP_CROSS_MA = 'up_cross_ma'
SIGNAL_DOWN_CROSS_MA = 'down_cross_ma'

SIGNAL_AMOUNT_ENLARGE = 'amount_enlarge'

SHORT = 12
LONG = 26
MID = 9

KDJ_N = 9
KDJ_M1 = 3

BULL_N_DAYS = 20
BULL_N_FACTOR = 2

FLOAT_FORMAT2 = '%.2f'
FLOAT_FORMAT4 = '%.4f'

MA_SHORT = 20
MA_MID = 60
MA_LONG = 250

PERIOD_LIST_ALL = [DAY, WEEK, MONTH]

PERIOD_LIST_MA = [5, 10, 20, 30, 60, 120, 250]

PERIOD_LIST_DEV = [5, 10, 20, 30, 60, 120, 250]

PERIOD_LIST_MA_START_TO_UP = [MA_SHORT, MA_MID, MA_LONG]

DATA_DIR = 'data-root'


def generate_more_data_for_all_period(stock_code=None, data_dir=None):
    if stock_code is not None and data_dir is not None:
        for period in PERIOD_LIST_ALL:
            stock_data = sd.StockData(stock_code, data_dir=data_dir)
            stock_obj = DataProcess(stock_code, stock_data.getStockName(), data_dir, period)
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
        length_of_code_list = len(stock_list)
        index = 1
        for code in stock_list:
            generate_more_data_for_all_period(code, data_dir_by_date)
            print("#### Process to generate new data of %d/%d ####" % (index, length_of_code_list))
            index = index + 1


def update_generated_data_for_all_period(stock_code=None, data_dir=None):
    if stock_code is not None and data_dir is not None:
        for period in PERIOD_LIST_ALL:
            stock = sd.StockData(stock_code, data_dir=data_dir)
            stock.updateKData(period)
            length = stock.getDataLenUpdated()
            if length > 0:
                p_stock = DataProcess(stock_code, stock.getStockName(), data_dir, period)
                # p_stock.readData()
                # p_stock.addMA(PERIOD_LIST_MA)
                # p_stock.addMACD()
                # p_stock.saveAsGeneratedData()
                # p_stock.generateDeviationByMACD(PERIOD_LIST_DEV, last_n_periods=length, update=True,
                #                                 update_report_for_latest_data=True)
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


def update_generated_data_for_all_stocks(stock_list=None, data_dir=None):
    if stock_list is None or not len(stock_list) or data_dir is None:
        print("[File:%s line:%d] Error: Parameters should not be empty!" % (
            sys._getframe().f_code.co_filename, sys._getframe().f_lineno))
        sys.exit()
    else:
        for code in stock_list:
            update_generated_data_for_all_period(code, data_dir)


def get_trade_signal(stock_list=None, data_dir=None, concerned_stock_code_list=None):
    try:
        if stock_list is None or len(stock_list) == 0 or data_dir is None or concerned_stock_code_list is None \
                or len(concerned_stock_code_list) == 0:
            print(
                f"[File:{sys._getframe().f_code.co_filename} line:{sys._getframe().f_lineno}] Error: Parameters should not be empty!")
            sys.exit()
        else:
            util.transfer_code_as_ts_code(concerned_stock_code_list)
            util.create_report_dir()
            df_day_bull_cross = pd.DataFrame()
            df_week_bull_cross = pd.DataFrame()
            df_month_bull_cross = pd.DataFrame()
            df_day_deviation = pd.DataFrame()
            df_week_deviation = pd.DataFrame()
            df_month_deviation = pd.DataFrame()
            df_ma_day_short_up = pd.DataFrame()
            df_ma_day_mid_up = pd.DataFrame()
            df_ma_day_long_up = pd.DataFrame()
            df_ma_day_short_down = pd.DataFrame()
            df_ma_day_mid_down = pd.DataFrame()
            df_ma_day_long_down = pd.DataFrame()
            df_up_cross_ma_day_short = pd.DataFrame()
            df_up_cross_ma_day_mid = pd.DataFrame()
            df_up_cross_ma_day_long = pd.DataFrame()
            df_down_cross_ma_day_short = pd.DataFrame()
            df_down_cross_ma_day_mid = pd.DataFrame()
            df_down_cross_ma_day_long = pd.DataFrame()
            df_day_amount_enlarge = pd.DataFrame()
            df_week_amount_enlarge = pd.DataFrame()
            df_month_amount_enlarge = pd.DataFrame()
            length_of_code_list = len(stock_list)
            index = 1
            for code in stock_list:
                print("#### Process to generate trade signal of %d/%d, code:%s ####" % (index, length_of_code_list, code))
                index = index + 1
                for period in PERIOD_LIST_ALL:
                    stock = sd.StockData(code, data_dir=data_dir)
                    stock_processed_data = DataProcess(code, stock.getStockName(), data_dir, period)
                    stock_processed_data.readGenData()
                    have_deviation_data = stock_processed_data.readDeviationData()
                    latest_date_str = stock_processed_data.getLatestDateStr()
                    if period == DAY:
                        for period_day in PERIOD_LIST_MA_START_TO_UP:
                            start_up = stock_processed_data.isMaStartUp(period_day)
                            start_down = stock_processed_data.isMaStartDown(period_day)
                            cross_up = stock_processed_data.isClosePriceCrossUpMa(period_day)
                            cross_down = stock_processed_data.isClosePriceCrossDownMa(period_day)

                            if start_up is True:
                                key = 'is_ma_' + str(period_day) + '_start_up'
                                data = {'code': [stock_processed_data.code],
                                        'name': [stock_processed_data.name],
                                        key: [start_up]
                                        }
                                df_up = pd.DataFrame(data)
                                df_ma_day_short_up = util.save_data_into_data_frame(period_day, MA_SHORT, df_ma_day_short_up, df_up, concerned_stock_code_list)
                                df_ma_day_mid_up = util.save_data_into_data_frame(period_day, MA_MID, df_ma_day_mid_up, df_up, concerned_stock_code_list)
                                df_ma_day_long_up = util.save_data_into_data_frame(period_day, MA_LONG, df_ma_day_long_up, df_up, concerned_stock_code_list)

                            if start_down is True:
                                key = 'is_ma_' + str(period_day) + '_start_down'
                                data = {'code': [stock_processed_data.code],
                                        'name': [stock_processed_data.name],
                                        key: [start_down]
                                        }
                                df_down = pd.DataFrame(data)
                                df_ma_day_short_down = util.save_data_into_data_frame(period_day, MA_SHORT, df_ma_day_short_down, df_down, concerned_stock_code_list)
                                df_ma_day_mid_down = util.save_data_into_data_frame(period_day, MA_MID, df_ma_day_mid_down, df_down, concerned_stock_code_list)
                                df_ma_day_long_down = util.save_data_into_data_frame(period_day, MA_LONG, df_ma_day_long_down, df_down, concerned_stock_code_list)

                            if cross_up is True:
                                key = 'is_close_price_up_cross_ma_' + str(period_day)
                                data = {'code': [stock_processed_data.code],
                                        'name': [stock_processed_data.name],
                                        key: [cross_up]
                                        }
                                df_cross_up = pd.DataFrame(data)
                                df_up_cross_ma_day_short = util.save_data_into_data_frame(period_day, MA_SHORT, df_up_cross_ma_day_short, df_cross_up, concerned_stock_code_list)
                                df_up_cross_ma_day_mid = util.save_data_into_data_frame(period_day, MA_MID, df_up_cross_ma_day_mid, df_cross_up, concerned_stock_code_list)
                                df_up_cross_ma_day_long = util.save_data_into_data_frame(period_day, MA_LONG, df_up_cross_ma_day_long, df_cross_up, concerned_stock_code_list)

                            if cross_down is True:
                                key = 'is_close_price_down_cross_ma_' + str(period_day)
                                data = {'code': [stock_processed_data.code],
                                        'name': [stock_processed_data.name],
                                        key: [cross_down]
                                        }
                                df_cross_down = pd.DataFrame(data)
                                df_down_cross_ma_day_short = util.save_data_into_data_frame(period_day, MA_SHORT, df_down_cross_ma_day_short, df_cross_down, concerned_stock_code_list)
                                df_down_cross_ma_day_mid = util.save_data_into_data_frame(period_day, MA_MID, df_down_cross_ma_day_mid, df_cross_down, concerned_stock_code_list)
                                df_down_cross_ma_day_long = util.save_data_into_data_frame(period_day, MA_LONG, df_down_cross_ma_day_long, df_cross_down, concerned_stock_code_list)

                    if stock_processed_data.isCrossBullBandCurrently() is True:
                        df_cross = stock_processed_data.getLatestGenData()
                        df_cross = df_cross.loc[:, ['flag_cross_top', 'flag_cross_bottom', 'flag_mid_up']]
                        df_cross['code'] = stock_processed_data.code
                        df_cross['name'] = stock_processed_data.name
                        # df_cross = df_cross[['code', 'name', 'flag_cross_top', 'flag_cross_bottom', 'flag_mid_up']]
                        df_day_bull_cross = util.save_data_into_data_frame(period, DAY, df_day_bull_cross, df_cross, concerned_stock_code_list)
                        df_week_bull_cross = util.save_data_into_data_frame(period, WEEK, df_week_bull_cross, df_cross, concerned_stock_code_list)
                        df_month_bull_cross = util.save_data_into_data_frame(period, MONTH, df_month_bull_cross, df_cross, concerned_stock_code_list)

                    if have_deviation_data is True:
                        latest_deviation_date_str = stock_processed_data.getLatestDeviationDateStr()
                        if latest_deviation_date_str == latest_date_str:
                            latest_date = stock_processed_data.getLatestDate()
                            df_deviation = stock_processed_data.getDeviationDateData(latest_date)
                            df_day_deviation = util.save_data_into_data_frame(period, DAY, df_day_deviation, df_deviation, concerned_stock_code_list)
                            df_week_deviation = util.save_data_into_data_frame(period, WEEK, df_week_deviation, df_deviation, concerned_stock_code_list)
                            df_month_deviation = util.save_data_into_data_frame(period, MONTH, df_month_deviation, df_deviation, concerned_stock_code_list)

                    if stock_processed_data.isAmountEnlargeObviously() is True:
                        df_gen_data = stock_processed_data.getLatestGenData()
                        df_amount_enlarge = df_gen_data.loc[:, ['amount_divide_mma']]
                        df_amount_enlarge['code'] = stock_processed_data.code
                        df_amount_enlarge['name'] = stock_processed_data.name
                        # df_amount_enlarge = df_amount_enlarge[['code', 'name', 'amount_divide_mma']]
                        df_day_amount_enlarge = util.save_data_into_data_frame(period, DAY, df_day_amount_enlarge, df_amount_enlarge, concerned_stock_code_list)
                        df_week_amount_enlarge = util.save_data_into_data_frame(period, WEEK, df_week_amount_enlarge, df_amount_enlarge, concerned_stock_code_list)
                        df_month_amount_enlarge = util.save_data_into_data_frame(period, MONTH, df_month_amount_enlarge, df_amount_enlarge, concerned_stock_code_list)

            util.save_signal_into_csv(df_day_bull_cross, DAY, SIGNAL_CROSS_BULL)
            util.save_signal_into_csv(df_week_bull_cross, WEEK, SIGNAL_CROSS_BULL)
            util.save_signal_into_csv(df_month_bull_cross, MONTH, SIGNAL_CROSS_BULL)

            util.save_signal_into_csv(df_day_deviation, DAY, SIGNAL_MACD_DEVIATION)
            util.save_signal_into_csv(df_week_deviation, WEEK, SIGNAL_MACD_DEVIATION)
            util.save_signal_into_csv(df_month_deviation, MONTH, SIGNAL_MACD_DEVIATION)

            util.save_signal_into_csv(df_day_amount_enlarge, DAY, SIGNAL_AMOUNT_ENLARGE)
            util.save_signal_into_csv(df_week_amount_enlarge, WEEK, SIGNAL_AMOUNT_ENLARGE)
            util.save_signal_into_csv(df_month_amount_enlarge, MONTH, SIGNAL_AMOUNT_ENLARGE)

            util.save_signal_into_csv(df_ma_day_short_up, MA_SHORT, SIGNAL_MA_UP)
            util.save_signal_into_csv(df_ma_day_mid_up, MA_MID, SIGNAL_MA_UP)
            util.save_signal_into_csv(df_ma_day_long_up, MA_LONG, SIGNAL_MA_UP)
            util.save_signal_into_csv(df_ma_day_short_down, MA_SHORT, SIGNAL_MA_DOWN)
            util.save_signal_into_csv(df_ma_day_mid_down, MA_MID, SIGNAL_MA_DOWN)
            util.save_signal_into_csv(df_ma_day_long_down, MA_LONG, SIGNAL_MA_DOWN)

            util.save_signal_into_csv(df_up_cross_ma_day_short, MA_SHORT, SIGNAL_UP_CROSS_MA)
            util.save_signal_into_csv(df_up_cross_ma_day_mid, MA_MID, SIGNAL_UP_CROSS_MA)
            util.save_signal_into_csv(df_up_cross_ma_day_long, MA_LONG, SIGNAL_UP_CROSS_MA)
            util.save_signal_into_csv(df_down_cross_ma_day_short, MA_SHORT, SIGNAL_DOWN_CROSS_MA)
            util.save_signal_into_csv(df_down_cross_ma_day_mid, MA_MID, SIGNAL_DOWN_CROSS_MA)
            util.save_signal_into_csv(df_down_cross_ma_day_long, MA_LONG, SIGNAL_DOWN_CROSS_MA)

    except Exception as e:
        print(e)


class DataProcess(object):
    def __init__(self, stock_code, stock_name, data_dir, period=DAY):
        self.dataPath = os.path.join(data_dir, stock_code)
        self.code = stock_code
        self.name = stock_name
        self.period = period
        self.dfData = pd.DataFrame()
        self.dfGenData = pd.DataFrame()
        self.dfDeviationData = pd.DataFrame()

        if period == DAY:
            self.dataCsvFile = os.path.join(self.dataPath, stock_code + '_k_day.csv')
            self.dataGenCsvFile = os.path.join(self.dataPath, stock_code + '_k_day_gen.csv')
            self.deviationReportFile = os.path.join(self.dataPath, stock_code + '_deviation_day.csv')
        elif period == WEEK:
            self.dataCsvFile = os.path.join(self.dataPath, stock_code + '_k_week.csv')
            self.dataGenCsvFile = os.path.join(self.dataPath, stock_code + '_k_week_gen.csv')
            self.deviationReportFile = os.path.join(self.dataPath, stock_code + '_deviation_week.csv')
        elif period == MONTH:
            self.dataCsvFile = os.path.join(self.dataPath, stock_code + '_k_month.csv')
            self.dataGenCsvFile = os.path.join(self.dataPath, stock_code + '_k_month_gen.csv')
            self.deviationReportFile = os.path.join(self.dataPath, stock_code + '_deviation_month.csv')
        elif period == HOUR:
            self.dataCsvFile = os.path.join(self.dataPath, stock_code + '_k_hour.csv')
            self.dataGenCsvFile = os.path.join(self.dataPath, stock_code + '_k_hour_gen.csv')
            self.deviationReportFile = os.path.join(self.dataPath, stock_code + '_deviation_hour.csv')
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

    def deleteDevFile(self):
        if os.path.exists(self.deviationReportFile):
            os.remove(self.deviationReportFile)

    def isAmountEnlargeObviously(self):
        times = 2
        length = len(self.dfGenData)
        if length > 0:
            if self.dfGenData.at[length-1, 'amount_divide_mma'] > times:
                return True
        return False

    def isClosePriceCrossUpMa(self, period):
        length = len(self.dfGenData)
        if length > 0:
            ma_name = 'MA' + str(period)
            if ma_name in self.dfGenData.columns:
                index_last_1 = length - 1
                index_last_2 = length - 2
                if index_last_1 >= 0 and index_last_2 >= 0:
                    if self.dfGenData.at[index_last_1, 'close'] > self.dfGenData.at[index_last_1, ma_name] \
                            and self.dfGenData.at[index_last_2, 'close'] <= self.dfGenData.at[index_last_2, ma_name]:
                        return True
        else:
            print('[File:%s line:%d stock:%s] Error: no generate data in csv files!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

        return False

    def isClosePriceCrossDownMa(self, period):
        length = len(self.dfGenData)
        if length > 0:
            ma_name = 'MA' + str(period)
            if ma_name in self.dfGenData.columns:
                index_last_1 = length - 1
                index_last_2 = length - 2
                if index_last_1 >= 0 and index_last_2 >= 0:
                    if self.dfGenData.at[index_last_1, 'close'] < self.dfGenData.at[index_last_1, ma_name] \
                            and self.dfGenData.at[index_last_2, 'close'] >= self.dfGenData.at[index_last_2, ma_name]:
                        return True
        else:
            print('[File:%s line:%d stock:%s] Error: no generate data in csv files!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

        return False

    def isMaStartUp(self, period):
        length = len(self.dfGenData)
        if length > 0:
            ma_name = 'MA' + str(period)
            if ma_name in self.dfGenData.columns:
                index_last_1 = length - 1
                index_last_2 = length - 2
                index_last_3 = length - 3
                if index_last_1 >= 0 and index_last_2 >= 0 and index_last_3 >= 0:
                    if self.dfGenData.at[index_last_1, ma_name] > self.dfGenData.at[index_last_2, ma_name] \
                            and self.dfGenData.at[index_last_2, ma_name] <= self.dfGenData.at[index_last_3, ma_name]:
                        return True
                else:
                    print('[File:%s line:%d stock:%s] Error: not enough data to handle!' % (
                        sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
                    sys.exit()
        else:
            print('[File:%s line:%d stock:%s] Error: no generate data in csv files!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

        return False

    def isMaStartDown(self, period):
        length = len(self.dfGenData)
        if length > 0:
            ma_name = 'MA' + str(period)
            if ma_name in self.dfGenData.columns:
                index_last_1 = length - 1
                index_last_2 = length - 2
                index_last_3 = length - 3
                if index_last_1 >= 0 and index_last_2 >= 0 and index_last_3 >= 0:
                    if self.dfGenData.at[index_last_1, ma_name] < self.dfGenData.at[index_last_2, ma_name] \
                            and self.dfGenData.at[index_last_2, ma_name] >= self.dfGenData.at[index_last_3, ma_name]:
                        return True
                else:
                    print('[File:%s line:%d stock:%s] Error: not enough data to handle!' % (
                        sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
                    sys.exit()
        else:
            print('[File:%s line:%d stock:%s] Error: no generate data in csv files!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

        return False

    def readDeviationData(self):
        if os.path.exists(self.deviationReportFile):
            self.dfDeviationData = pd.read_csv(self.deviationReportFile)
            return True
        else:
            return False

    def getLatestDeviationDateStr(self):
        index_latest = len(self.dfDeviationData) - 1
        if index_latest >= 0:
            if 'date_current' in self.dfDeviationData.columns:
                return str(self.dfDeviationData.at[index_latest, 'date_current'])
        else:
            print('[File:%s line:%d stock:%s] Error: deviation data is empty!' % (
                sys._getframe().f_code.co_filename, sys._getframe().f_lineno, self.code))
            sys.exit()

    def getDeviationDateData(self, date):
        index_latest = len(self.dfDeviationData) - 1
        if index_latest >= 0:
            if 'date_current' in self.dfDeviationData.columns:
                return self.dfDeviationData[self.dfDeviationData['date_current'] == date]
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

    def addMMA(self, period):
        if period > 0:
            mma_name = 'MMA' + str(period)
            self.dfData[mma_name] = self.dfData['amount'].shift(1).rolling(window=5, min_periods=1).mean().fillna(0)

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

    def addAmountDividePrevious(self):
        if len(self.dfData) > 0:
            #[5,10]
            period = 5
            self.addMMA(period)
            mma_name = 'MMA' + str(period)
            self.dfData['amount_divide_mma'] = self.dfData['amount'].div(self.dfData[mma_name].shift(1)).fillna(0)
            self.dfData.loc[self.dfData.index == 0, 'amount_divide_mma'] = 0
            self.dfData.loc[self.dfData['amount'].shift(1) == 0, 'amount_divide_mma'] = 0
            self.dfData.loc[np.isinf(self.dfData['amount_divide_mma']), 'amount_divide_mma'] = 0

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
                                data = {'code': [self.code],
                                        'name': [self.name],
                                        'type': ['底背离'],
                                        'period': [period],
                                        'date_current': [data_df.at[index_last_day, 'trade_date']],
                                        'date_of_last_dif': [data_df.at[index, 'trade_date']],
                                        'dif_current': [data_df.at[index_last_day, 'dif']],
                                        'dif_last': [data_df.at[index, 'dif']],
                                        }
                                df_per = pd.DataFrame(data)
                                df_per = df_per[['code', 'name', 'type', 'period', 'date_current', 'date_of_last_dif', 'dif_current', 'dif_last']]
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
                                data = {'code': [self.code],
                                        'name': [self.name],
                                        'type': ['顶背离'],
                                        'period': [period],
                                        'date_current': [data_df.at[index_last_day, 'trade_date']],
                                        'date_of_last_dif': [data_df.at[index, 'trade_date']],
                                        'dif_current': [data_df.at[index_last_day, 'dif']],
                                        'dif_last': [data_df.at[index, 'dif']]
                                        }
                                df_per = pd.DataFrame(data)
                                df_per = df_per[['code', 'name', 'type', 'period', 'date_current', 'date_of_last_dif', 'dif_current', 'dif_last']]
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
        self.addAmountDividePrevious()
        self.deleteDevFile()
        self.generateDeviationByMACD(period_list=PERIOD_LIST_DEV, last_n_periods=1)
        # self.generateDeviationByMACD(PERIOD_LIST_DEV)


def download_data_from_list():
    hs300_stock_list_file_url = 'https://csi-web-dev.oss-cn-shanghai-finance-1-pub.aliyuncs.com/static/html/csindex/public/uploads/file/autofile/cons/000300cons.xls'
    file_name = sd.get_name_and_code(hs300_stock_list_file_url)
    hs300_code_list = sd.get_china_stock_list(hs300_stock_list_file_url, file_name, DATA_DIR)

    zz500StockListFileUrl = 'https://csi-web-dev.oss-cn-shanghai-finance-1-pub.aliyuncs.com/static/html/csindex/public/uploads/file/autofile/cons/000905cons.xls'
    file_name = sd.get_name_and_code(zz500StockListFileUrl)
    zz500_code_list = sd.get_china_stock_list(zz500StockListFileUrl, file_name, DATA_DIR)

    kc50StockListFileUrl = 'https://csi-web-dev.oss-cn-shanghai-finance-1-pub.aliyuncs.com/static/html/csindex/public/uploads/file/autofile/cons/931643cons.xls'
    file_name = sd.get_name_and_code(kc50StockListFileUrl)
    kc50_code_list = sd.get_china_stock_list(kc50StockListFileUrl, file_name, DATA_DIR)

    # concerned stocks
    code_list1 = ['688339']
    # cyclical stocks
    code_list2 = ['600489', '300033', '601288', '000975', '002142', '601998', '601658', '000002',
                  '601628', '600585', '600547', '300059', '601318', '600030','600036']
    # foreign capital
    code_list3 = ['000338', '002032', '601901', '600887', '000651', '002508', '603489', '000333', '300244']
    # tech stocks
    code_list4 = ['688327', '688318', '688787', '688012', '002371', '300782', '603986', '688396', '603501', '600584',
                  '002049', '300223', '605111', '300373']
    # pharmaceutical stocks
    code_list5 = ['000999', '688271', '002223', '300760', '688690', '603259', '300347', '300482', '603882', '603858',
                  '300358', '300725', '603392', '603087', '688399', '002287', '002603', '688626', '600276', '002317', '600211']
    # new energy
    code_list6 = ['603799', '002709', '601865', '603806', '002594', '002460', '002015', '002129', '688599', '601012',
                  '688339', '002158', '688248', '601615']
    # yi mei
    code_list7 = ['300896', '688363']
    # wine
    code_list8 = ['600779', '002304', '000799', '600809', '000858', '600519']
    # other
    code_list9 = ['300296', '603799', '300033', '601888', '600138', '600862', '300699', '600660', '300144', '300146',
                  '000998', '600195', '600388', '002505', '000156', '300251', '300413']
    # gas
    code_list10 = ['600777', '600256', '603393']
    code_list = code_list1 + code_list2 + code_list3 + code_list4 + code_list5 + code_list6 + code_list7 + code_list8 + code_list9 + code_list10 + hs300_code_list + zz500_code_list + kc50_code_list
    # code_list = ['000027']
    code_list = list(set(code_list))
    code_list.sort()
    code_list_top = code_list1 + code_list2 + code_list3 + code_list4 + code_list5 + code_list6 + code_list7 + code_list8 + code_list9 + code_list10
    code_list_top = list(set(code_list_top))
    util.transfer_code_as_ts_code(code_list)
    sd.download_stock_data_as_csv(code_list, DATA_DIR)
    return code_list, code_list_top


def job_remove_old_data_weekly():
    util.remove_create_stock_data_dir()


def job_update_and_generate_data_daily():
    code_list, code_list_top = download_data_from_list()
    sd.update_stock_data_for_list(code_list, DATA_DIR)
    generate_more_data_for_all_stocks(code_list, DATA_DIR)
    get_trade_signal(code_list, DATA_DIR, code_list_top)
    util.zip_signal_files()
    util.send_mail('chris_zhu_sap@163.com', 'WRUERXOXFUDWRBFM', 'chris_zhu_sap@163.com', 'send signal file', 'Hi Chris, \n  This is a test to send signal files!', util.get_signal_zip_file_path())


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(job_remove_old_data_weekly, 'cron', day_of_week='4', hour=10, minute=0, misfire_grace_time=3600)
    scheduler.add_job(job_update_and_generate_data_daily, 'cron', day_of_week='0-4', hour=10, minute=1, misfire_grace_time=3600)
    scheduler.start()

    # don't use time Scheduler
    # job_remove_old_data_weekly()
    # job_update_and_generate_data_daily()

