import os
import pandas as pd
import sys
import tushare as ts
import StockData as sd
import DataProcess as dp

MA_20 = 'MA20'
MA_60 = 'MA60'

class ExchangeStrategy(object):
    def __init__(self, stockCode, stockName):
        self.code = stockCode
        self.name = stockName
        self.setDfDayData()
        self.setDfWeekData()
        self.setDfMonthData()
        
    def setDfWeekData(self):
        dpWeekObj = dp.DataProcess(self.code, self.name,period=dp.WEEK)
        if(os.path.exists(dpWeekObj.dataGenCsvFile)):
            self.dfWeekGenData = pd.read_csv(dpWeekObj.dataGenCsvFile,encoding="utf-8",dtype={'code':str})
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code,dpWeekObj.dataGenCsvFile))
            sys.exit()
            
        if(os.path.exists(dpWeekObj.signalReportFile)):
            self.dfWeekSignalData = pd.read_csv(dpWeekObj.signalReportFile,encoding="utf-8",dtype={'code':str})
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code,dpWeekObj.signalReportFile))
            sys.exit()
        
    def setDfDayData(self):
        dpDayObj = dp.DataProcess(self.code, self.name,period=dp.DAY)
        if(os.path.exists(dpDayObj.dataGenCsvFile)):
            self.dfDayGenData = pd.read_csv(dpDayObj.dataGenCsvFile,encoding="utf-8",dtype={'code':str})
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code,dpDayObj.dataGenCsvFile))
            sys.exit()
            
        if(os.path.exists(dpDayObj.signalReportFile)):
            self.dfDaySignalData = pd.read_csv(dpDayObj.signalReportFile,encoding="utf-8",dtype={'code':str})
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code,dpDayObj.signalReportFile))
            sys.exit()
        
    def setDfMonthData(self):
        dpMonthObj = dp.DataProcess(self.code, self.name,period=dp.MONTH)
        if(os.path.exists(dpMonthObj.dataGenCsvFile)):
            self.dfMonthGenData = pd.read_csv(dpMonthObj.dataGenCsvFile,encoding="utf-8",dtype={'code':str})
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code,dpMonthObj.dataGenCsvFile))
            sys.exit()
            
        if(os.path.exists(dpMonthObj.signalReportFile)):
            self.dfMonthSignalData = pd.read_csv(dpMonthObj.signalReportFile,encoding="utf-8",dtype={'code':str})
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code,dpMonthObj.signalReportFile))
            sys.exit()
            
    def exchange(self,startDate=None,endDate=None,amount=100000):
        self.amount = amount
        df = pd.DataFrame()
        if(startDate is None and endDate is None):
            df = self.dfDayGenData
        elif(startDate is None and endDate is not None):
            df = self.dfDayGenData[self.dfDayGenData['date'] < endDate]
        elif(endDate is None and startDate is not None):
            df = self.dfDayGenData[self.dfDayGenData['date'] >= startDate]
        else:
            df = self.dfDayGenData[self.dfDayGenData['date'] >= startDate and self.dfDayGenData['date'] < endDate]
            
        if(MA_20 in df.columns 
           and MA_60 in df.columns 
           and MA_20 in df.columns
           and MA_60 in df.columns):
            # add exchange condition
            for index in df.index:
#                 if(df.at[index,MA_20] > 0 
#                    and df.at[index,MA_60] > 0
#                    and ):
                self.getWeekIndex(df.at[index,'date'])
        else:
            print('[Function:%s line:%s stock:%s] not enough data to add exchange point!'%(self.exchange.__name__, sys._getframe().f_lineno,self.code))
                    
                    
    def getWeekIndex(self,date):
        df = self.dfWeekGenData[self.dfWeekGenData['date'] <= date]
        index = df.index[-1]
        print("date:%s index:%s")
            
            
if __name__ == '__main__':
    print('####################### Begin to test ExchangeStrategy ############################')
    stockExchangeStrategy = ExchangeStrategy('600030','中信证券')
    stockExchangeStrategy.exchange('2009-01-16','2019-01-16')
    print('####################### End of testing ExchangeStrategy ############################')