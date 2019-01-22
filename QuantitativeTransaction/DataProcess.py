'''
Created on Sep 18, 2017

@author: Administrator
'''
import platform
import os
import pandas as pd
import sys
import tushare as ts
import StockData as sd

DAY = 'D'
WEEK = 'W'
MONTH = 'M'
HOUR = '60'

SHORT = 12
LONG = 26
MID = 9

FLOAT_FORMAT2 = '%.2f'
FLOAT_FORMAT4 = '%.4f'

PERIOD_LIST_ALL = [DAY,WEEK,MONTH]

PERIOD_LIST_MA = [5,10,20,30,60,120,250]

PERIOD_LIST_VOL_MA = [5,10,20]

PERIOD_LIST_DEV = [5,10,20,30,60,120,250]

def generateMoreDataForAllPeriod(stockCode=None):
    if(stockCode is not None):
        for period in PERIOD_LIST_ALL:
            stockData = sd.StockData(stockCode)          
            stockObj = DataProcess(stockCode,stockData.getStockName(),period)
            stockObj.makeGenData()
            stockObj.saveAsGeneratedData()
            print("[Function:%s line:%s] Message: generate data for stock:%s of period:%s has been done!" % (generateMoreDataForAllPeriod.__name__, sys._getframe().f_lineno, stockCode, period))
    else:
        print("[Function:%s line:%s] Error: Parameters should not be empty!" % (generateMoreDataForAllPeriod.__name__, sys._getframe().f_lineno))
        sys.exit()

def generateMoreDataForAllStocks(stockList):
    if(stockList is None or not len(stockList)):
        print("[Function:%s line:%s] Error: Parameters should not be empty!" % (generateMoreDataForAllStocks.__name__, sys._getframe().f_lineno))
        sys.exit()          
    else:
        for code in stockList:
            generateMoreDataForAllPeriod(code)
            
def updateGeneratedDataForAllPeriod(stockCode=None):
    if(stockCode is not None):
        for period in PERIOD_LIST_ALL:
            stock = sd.StockData(stockCode)
            stock.updateKData(period)
            length = stock.getDataLenUpdated()
            if(length > 0):
                pStock = DataProcess(stockCode,stock.getStockName(),period)
                pStock.readData()
                pStock.addMA(PERIOD_LIST_MA)
                pStock.addVolumeMA(PERIOD_LIST_VOL_MA)
                pStock.addMACD()
                pStock.addKDJ()
                pStock.addBULL()
                pStock.saveAsGeneratedData()
                pStock.generateExchangeSignal(PERIOD_LIST_DEV,lastNPeriods=length,Update=True,updateReportForLatestData=True)
                print("[Function:%s line:%s] Message: update generated data for stock:%s of Period:%s has been done!" % (updateGeneratedDataForAllPeriod.__name__, sys._getframe().f_lineno, stockCode, period))
            else:
                print("[Function:%s line:%s] Message: No updated data for stock:%s of Period:%s!" % (updateGeneratedDataForAllPeriod.__name__, sys._getframe().f_lineno, stockCode, period))
    else:
        print("[Function:%s line:%s] Error: Parameters should not be empty!" % (updateGeneratedDataForAllPeriod.__name__, sys._getframe().f_lineno))
        sys.exit()
        
def updateGeneratedDataForAllStocks(stockList=None):
    if(stockList is None or not len(stockList)):
        print("[Function:%s line:%s] Error: Parameters should not be empty!" % (updateGeneratedDataForAllPeriod.__name__, sys._getframe().f_lineno))
        sys.exit()          
    else:
        for code in stockList:
            updateGeneratedDataForAllPeriod(code)
            
def getCurrentTradeReportForAllPeriod(stockCode=None, dfData=None):    
    if(stockCode is not None and dfData is not None):
        for period in PERIOD_LIST_ALL:
            length = len(dfData)
            if(length):
                stock = sd.StockData(stockCode)
                dfMerged = stock.mergeData(dfData,period)
                dfMerged = dfMerged.reset_index(drop = True)
                pStock = DataProcess(stockCode,stock.getStockName(),period)
                pStock.setDfData(dfMerged)
                pStock.addMA(PERIOD_LIST_MA)
                pStock.addVolumeMA(PERIOD_LIST_VOL_MA)
                pStock.addMACD()
                pStock.addKDJ()
                pStock.addBULL()
                pStock.generateExchangeSignal(PERIOD_LIST_DEV,lastNPeriods=1,updateReportForCurrentTradeData=True)
                print("[Function:%s line:%s] Message: update generated data for stock:%s of Period:%s has been done!" % (getCurrentTradeReportForAllPeriod.__name__, sys._getframe().f_lineno, stockCode, period))
    else:
        print("[Function:%s line:%s] Error: Parameters should not be empty!" % (getCurrentTradeReportForAllPeriod.__name__, sys._getframe().f_lineno))
        sys.exit()
            
def getCurrentTradeReport(stockList=None):
    if(stockList is None or not len(stockList)):
        print("[Function:%s line:%s] Error: Parameters should not be empty!" % (getCurrentTradeReport.__name__, sys._getframe().f_lineno))
        sys.exit()          
    else:
        dfCurrent = ts.get_today_all()
        today = sd.getCurrentShanghaiDate()

        for code in stockList:
            df = dfCurrent[dfCurrent['code'] == code]
            if(len(df) > 0):
                data = {'date':[today],
                        'open':[df['open'].values[0]],
                        'close':[df['trade'].values[0]],
                        'high':[df['high'].values[0]],
                        'low':[df['low'].values[0]],
                        'volume':[df['volume'].values[0]]
                    }
                df = pd.DataFrame(data)
                getCurrentTradeReportForAllPeriod(code, df)
            else:
                print("[Function:%s line:%s] Message: data for code:%s not exists!" % (getCurrentTradeReport.__name__, sys._getframe().f_lineno,code))  

class DataProcess(object):
    def __init__(self, stockCode, stockName,period=DAY):
        sdate = sd.SingletonDate.GetInstance()
        dataDirByDate = sdate.date
        self.dataPath = dataDirByDate + self.getDelimeter() + stockCode + self.getDelimeter()
        self.code = stockCode
        self.name = stockName
        self.period = period
        
        if period == DAY:
            self.dataCsvFile = self.dataPath + stockCode + "_k_day.csv"
            self.dataGenCsvFile = self.dataPath + stockCode + "_k_day_gen.csv"
            self.signalReportFile = self.dataPath + stockCode + "_sinal_day.csv"
            self.reportOfLatest = dataDirByDate + self.getDelimeter() + 'latest_day_report.csv'
            self.reportOfCurrentTrade = dataDirByDate + self.getDelimeter() + 'current_trade_day_report.csv'
        elif period == WEEK:
            self.dataCsvFile = self.dataPath + stockCode + "_k_week.csv"
            self.dataGenCsvFile = self.dataPath + stockCode + "_k_week_gen.csv"
            self.signalReportFile = self.dataPath + stockCode + "_sinal_week.csv"
            self.reportOfLatest = dataDirByDate + self.getDelimeter() + 'latest_week_report.csv'
            self.reportOfCurrentTrade = dataDirByDate + self.getDelimeter() + 'current_trade_week_report.csv'
        elif period == MONTH:
            self.dataCsvFile = self.dataPath + stockCode + "_k_month.csv"
            self.dataGenCsvFile = self.dataPath + stockCode + "_k_month_gen.csv"
            self.signalReportFile = self.dataPath + stockCode + "_sinal_month.csv"
            self.reportOfLatest = dataDirByDate + self.getDelimeter() + 'latest_month_report.csv'
            self.reportOfCurrentTrade = dataDirByDate + self.getDelimeter() + 'current_trade_month_report.csv'
        elif period == HOUR:
            self.dataCsvFile = self.dataPath + stockCode + "_k_hour.csv"
            self.dataGenCsvFile = self.dataPath + stockCode + "_k_hour_gen.csv"
            self.signalReportFile = self.dataPath + stockCode + "_sinal_hour.csv"
            self.reportOfLatest = dataDirByDate + self.getDelimeter() + 'latest_hour_report.csv'
            self.reportOfCurrentTrade = dataDirByDate + self.getDelimeter() + 'current_trade_hour_report.csv'
        else:
            print("[Function:%s line:%s stock:%s] Error: input parameter period is not supported now!" % (self.__init__.__name__,sys._getframe().f_lineno,self.code)) 
            sys.exit()             
        
    def getDelimeter(self):
        if 'Windows' in platform.system():
            return "\\"
        else:
            return "/"
        
    def getDfData(self):
        return self.dfData;
    
    def setDfData(self,dfDataNew):
        if(len(dfDataNew) > 0):
            self.dfData = dfDataNew;
        
    def readData(self):
        if(os.path.exists(self.dataCsvFile)):
            self.dfData = pd.read_csv(self.dataCsvFile)
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' %(self.readData.__name__, sys._getframe().f_lineno,self.code,self.dataCsvFile))
            sys.exit()
            
        if(os.path.exists(self.dataGenCsvFile)):
            os.remove(self.dataGenCsvFile)
        
    def saveAsGeneratedData(self):
        self.dfData.to_csv(self.dataGenCsvFile,index=0,float_format=FLOAT_FORMAT2,encoding="utf-8")

    def addMA(self,periodList):
        if(periodList is None or not len(periodList)):
            print('[Function:%s line:%s stock:%s] Error: Parameter is invalid or data is empty!' %(self.addMA.__name__,sys._getframe().f_lineno,self.code))
            sys.exit()
            
        for period in periodList:
            if(len(self.dfData) >= period):
                maName = 'MA' + str(period)
                self.dfData[maName] = pd.Series(self.dfData['close']).rolling(period).mean()
                # pd.rolling_mean(self.dfData['close'], period)
                print('Add MA%d for %s has been done!' %(period, self.dataCsvFile))
            else:
                print('Length of data is smaller than period:%d, can not generate such MA for this period!' %(period))
            
        self.dfData.fillna(0,inplace=True)
        
    def addVolumeMA(self,periodList):
        if(periodList is None or not len(periodList)):
            print('[Function:%s line:%s stock:%s] Error: Parameter is invalid or data is empty!' %(self.addVolumeMA.__name__,sys._getframe().f_lineno,self.code))
            sys.exit()
            
        for period in periodList:
            if(len(self.dfData) >= period):
                maName = 'VOLMA' + str(period)
                self.dfData[maName] = pd.Series(self.dfData['volume']).rolling(period).mean()
                # pd.rolling_mean(self.dfData['close'], period)
                print('Add VOL MA%d for %s has been done!' %(period, self.dataCsvFile))
            else:
                print('Length of data is smaller than period:%d, can not generate such MA for this period!' %(period))
            
        self.dfData.fillna(0,inplace=True)
        
    def addMACD(self,short=SHORT,long=LONG,mid=MID):       
        if(not self.dfData.empty and short>0 and long>0 and mid>0):
            if len(self.dfData) >= long:
                # calculate short EMA
                self.dfData['sema']=pd.Series(self.dfData['close']).ewm(span=short).mean()
                # calculate long EMA
                self.dfData['lema']=pd.Series(self.dfData['close']).ewm(span=long).mean()
                # fill 0 if data=NA in dfData['sema'] and dfData['lema']
                self.dfData.fillna(0,inplace=True)
                # calculate diff, dea and macd
                self.dfData['dif']=self.dfData['sema']-self.dfData['lema']
                self.dfData['dea']=pd.Series(self.dfData['dif']).ewm(span=mid).mean()
                self.dfData['macd']=2*(self.dfData['dif']-self.dfData['dea'])
                # fill 0 if data=NA in dfData['data_dif'],dfData['data_dea'],dfData['data_macd']
                self.dfData.fillna(0,inplace=True)
                
                self.dfData.drop('sema', axis=1,inplace=True)
                self.dfData.drop('lema', axis=1,inplace=True)
                
                print('Add MACD for %s has been done!' %(self.dataCsvFile))
            else:
                print('Not enough data to generate MACD for code:%s!' %(self.code))
        else:
            print('[Function:%s line:%s stock:%s!] Error: Parameter is invalid!' %(self.addMACD.__name__,sys._getframe().f_lineno,self.code))
            sys.exit()
    def addKDJ(self,period=9):
        if(not self.dfData.empty and period>0):
#             if len(self.dfData) >= period:
            lowList = pd.rolling_min(self.dfData['low'],period)
            lowList.fillna(value=pd.expanding_min(self.dfData['low']),inplace=True)
            highList = pd.rolling_max(self.dfData['high'],period)
            highList.fillna(value=pd.expanding_max(self.dfData['high']),inplace=True)
            rsv = (self.dfData['close']-lowList)/(highList-lowList)*100
            self.dfData['kdj_k'] = pd.ewma(rsv,com=2)
            self.dfData['kdj_d'] = pd.ewma(self.dfData['kdj_k'],com=2)
            self.dfData['kdj_j'] = 3*self.dfData['kdj_k'] - 2*self.dfData['kdj_d']
#             else:
#                 print('Not enough data to generate KDJ for code:%s!' %(self.code))
        else:
            print('[Function:%s line:%s stock:%s!] Error: Parameter is invalid!' %(self.addKDJ.__name__,sys._getframe().f_lineno,self.code))
            sys.exit()

    def addBULL(self,period=20,t=2):
        if(not self.dfData.empty and period>0 and t>0):
            self.dfData['boll_mid']=self.dfData['close'].rolling(period).mean()
            self.dfData.fillna(0,inplace=True)
            stdData=self.dfData['close'].rolling(period).std()
            stdData.fillna(0,inplace=True)
            self.dfData['boll_top']=self.dfData['boll_mid']+t*stdData
            self.dfData['boll_bottom']=self.dfData['boll_mid']-t*stdData
            self.dfData.fillna(0,inplace=True)
        else:
            print('[Function:%s line:%s stock:%s!] Error: Parameter is invalid!' %(self.addBULL.__name__,sys._getframe().f_lineno,self.code))
            sys.exit()
            
    def generateExchangeSignal(self,periodList,lastNPeriods=None,Update=False,updateReportForLatestData=False,updateReportForCurrentTradeData=False):
        if(periodList is None or not len(periodList)):
            print('[Function:%s line:%s stock:%s] Error: Parameter is invalid or data is empty!' %(self.generateExchangeSignal.__name__,sys._getframe().f_lineno,self.code))
            sys.exit()
            
        dfCurrent = pd.DataFrame()
        if(updateReportForCurrentTradeData):
            if(os.path.exists(self.reportOfCurrentTrade)):
                dfCurrent = pd.read_csv(self.reportOfCurrentTrade,encoding="utf-8",dtype={'code':str})
            
        dfLatest = pd.DataFrame()
        if(updateReportForLatestData):
            if(os.path.exists(self.reportOfLatest)):
                dfLatest = pd.read_csv(self.reportOfLatest,encoding="utf-8",dtype={'code':str})

        df = pd.DataFrame()
        if(Update):
            if(os.path.exists(self.signalReportFile)):
                df = pd.read_csv(self.signalReportFile,encoding="utf-8",dtype={'code':str})
            else:
                print('[Function:%s line:%s stock:%s] Message: signalReportFile: %s not exits!' %(self.generateExchangeSignal.__name__,sys._getframe().f_lineno,self.code,self.signalReportFile))
        elif(updateReportForLatestData is False and updateReportForCurrentTradeData is False and os.path.exists(self.signalReportFile)):
            print('[Function:%s line:%s stock:%s] Message: %s exits, no need to regenerate the file!' %(self.generateExchangeSignal.__name__,sys._getframe().f_lineno,self.code,self.signalReportFile))
            return
        
        oldLength = len(df)
        
        dataDf = self.dfData
        
        dfLength = len(dataDf)
        
        if(lastNPeriods == None):
            lastNPeriods = len(dataDf)
            
        if(lastNPeriods == 0):
            lastNPeriods = 1
        
        if(dfLength < LONG):
            print('[Function:%s line:%s stock:%s] Message: Length of data is smaller than parameter LONG: %d!' %(self.generateExchangeSignal.__name__,sys._getframe().f_lineno,self.code,LONG))
        else:
            arraylastNPeriods = range(lastNPeriods)
            for lastPeriod in reversed(arraylastNPeriods):
                duplicatedFlag = False
                for period in reversed(periodList):
                    if(duplicatedFlag):
                        break
                    if(dfLength >= (period+lastPeriod)):
                        indexLastDay = dfLength - lastPeriod -1 
                        # 底背离
                        priceLowest = 0      
                        if(lastPeriod > 0):
                            priceLowest = pd.Series(dataDf['low'][-(period+lastPeriod):-lastPeriod]).min()
                        else:
                            priceLowest = pd.Series(dataDf['low'][-period:]).min()
                        if(priceLowest == self.dfData.at[indexLastDay,'low']):
                            difData = pd.Series(dataDf['dif'][-period:])
                            if(lastPeriod > 0):
                                difData = pd.Series(dataDf['dif'][-(period+lastPeriod):-lastPeriod])
                            else:
                                difData = pd.Series(dataDf['dif'][-period:])
                            index = difData.idxmin()
                            indexStart = dfLength -(period+lastPeriod)
                            if(dataDf.at[index,'low'] > dataDf.at[indexLastDay,'low'] 
                               and dataDf.at[index,'dif'] < dataDf.at[indexLastDay,'dif'] 
                               and index != indexStart 
                               and index != indexLastDay):
                                data = {'code':[self.code],
                                        'name':[self.name],
                                        'signal':['dif底背离_'+str(period)],
                                        'aDate':[dataDf.at[indexLastDay,'date']]
                                    }
                                dfPer = pd.DataFrame(data)
                                df = pd.concat([df,dfPer])
                                print('[Function:%s line:%s stock:%s] Add the bottom deviation data on date:%s of period %d!' %(self.generateExchangeSignal.__name__,sys._getframe().f_lineno,self.code, dataDf.at[indexLastDay,'date'], period))
                                
                                if(updateReportForLatestData and lastPeriod == 0 and updateReportForCurrentTradeData == False):
                                    dfLatest = pd.concat([dfLatest,dfPer])
                                    dfLatest.to_csv(self.reportOfLatest,index=0,float_format=FLOAT_FORMAT2,encoding="utf-8")
                                    print('[Function:%s line:%s stock:%s] Add the bottom deviation of latest data has been done!' %(self.generateExchangeSignal.__name__,sys._getframe().f_lineno,self.code))
                                    
                                if(updateReportForCurrentTradeData and lastPeriod == 0):
                                    dfCurrent = pd.concat([dfCurrent,dfPer])
                                    dfCurrent.to_csv(self.reportOfCurrentTrade,index=0,float_format=FLOAT_FORMAT2,encoding="utf-8")
                                    print('[Function:%s line:%s stock:%s] Add the bottom deviation of current trade data for the stock has been done!' %(self.generateExchangeSignal.__name__,sys._getframe().f_lineno,self.code))
    
                                if(not duplicatedFlag):
                                    duplicatedFlag = True
                                
                        #顶背离
                        priceHighest = 0
                        if(lastPeriod > 0):
                            priceHighest = pd.Series(dataDf['high'][-(period+lastPeriod):-lastPeriod]).max()
                        else:
                            priceHighest = pd.Series(dataDf['high'][-period:]).max()                    
                        if(priceHighest == dataDf.at[indexLastDay,'high']):
                            difData = pd.Series(dataDf['dif'][-period:])
                            if(lastPeriod > 0):
                                difData = pd.Series(dataDf['dif'][-(period+lastPeriod):-lastPeriod])
                            else:
                                difData = pd.Series(dataDf['dif'][-period:])
                            index = difData.idxmax()
                            indexStart = dfLength -(period+lastPeriod)
                            if(dataDf.at[index,'high'] < dataDf.at[indexLastDay,'high'] 
                               and dataDf.at[index,'dif'] > dataDf.at[indexLastDay,'dif'] 
                               and index != indexStart 
                               and index != indexLastDay):
                                data = {'code':[self.code],
                                        'name':[self.name],
                                        'signal':['dif顶背离_'+str(period)],
                                        'aDate':[dataDf.at[indexLastDay,'date']]
                                    }
                                dfPer = pd.DataFrame(data)
                                df = pd.concat([df,dfPer])
                                print('[Function:%s line:%s stock:%s] Add the top deviation data on date:%s of period %d!' %(self.generateExchangeSignal.__name__,sys._getframe().f_lineno,self.code,dataDf.at[indexLastDay,'date'], period))
                                
                                if(updateReportForLatestData and lastPeriod == 0 and updateReportForCurrentTradeData == False):
                                    dfLatest = pd.concat([dfLatest,dfPer])
                                    dfLatest.to_csv(self.reportOfLatest,index=0,float_format=FLOAT_FORMAT2,encoding="utf-8")
                                    print('[Function:%s line:%s stock:%s] Add the top deviation of latest data has been done!' %(self.generateExchangeSignal.__name__,sys._getframe().f_lineno,self.code))
                                    
                                if(updateReportForCurrentTradeData and lastPeriod == 0):
                                    dfCurrent = pd.concat([dfCurrent,dfPer])
                                    dfCurrent.to_csv(self.reportOfCurrentTrade,index=0,float_format=FLOAT_FORMAT2,encoding="utf-8")
                                    print('[Function:%s line:%s stock:%s] Add the top deviation of current trade data has been done!' %(self.generateExchangeSignal.__name__,sys._getframe().f_lineno,self.code))
    
                                if(not duplicatedFlag):
                                    duplicatedFlag = True
                                    
                    else:
                        print('[Function:%s line:%s stock:%s] The length of data for stock is too small, no deviation can be found! period:%d, lastPeriod:%d  kPeriod:%s' %(self.generateExchangeSignal.__name__,sys._getframe().f_lineno,self.code,period,lastPeriod,self.period))
                # end calculate dif deviation by macd
                          
        if(len(df) > oldLength and updateReportForCurrentTradeData == False):
            df.drop_duplicates(inplace=True,keep='first')
            df.to_csv(self.signalReportFile,index=0,float_format=FLOAT_FORMAT2,encoding="utf-8")
            print('[Function:%s line:%s stock:%s] Add the deviation data has been done !' %(self.generateExchangeSignal.__name__,sys._getframe().f_lineno,self.code))
        
    def makeGenData(self):
        self.readData()
        self.addMA(PERIOD_LIST_MA)
        self.addVolumeMA(PERIOD_LIST_VOL_MA)
        self.addMACD()
        self.addKDJ()
        self.addBULL()
        self.generateExchangeSignal(PERIOD_LIST_DEV)
        

