import os
import pandas as pd
import sys
# import tushare as ts
import StockData as sd
import DataProcess as dp
import math

MA_20 = 'MA20'
MA_60 = 'MA60'
MA_250 = 'MA250'

# VOL_MA_5 = 'VOLMA5'
# VOL_MA_5 = 'VOLMA5'
VOL_MA_20 = 'VOLMA20'

# return value when check the MA is up or not
TRUE = 1
FALSE = 0
UNDEFINED = -1

# value for action
BUY = 1
SELL = 0

ACTION_STRING ={
    1:'B',
    0:'S'
    }

class ExchangeStrategy(object):
    def __init__(self, stockCode, stockName=None,startDate=None,endDate=None):
        self.code = stockCode
        self.setName(stockName)
        self.setDfDayData(startDate,endDate)
        self.setDfWeekData(startDate,endDate)
        self.setDfMonthData(startDate,endDate)
        self.position = 0
        self.capital = 0
        self.stockAmount = 0
        self.setPosition()
        self.exchangeDf = pd.DataFrame()
        
    def saveExchangeReport(self):
        if(self.reportPath is not None):
            self.exchangeDf.to_csv(self.reportPath,index=0,float_format=dp.FLOAT_FORMAT2,encoding="utf-8")
        else:
            print('[Function:%s line:%s stock:%s] Error: File path is not exist' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code))
            sys.exit()
        
    def setPosition(self,position=1):
        # 仓位 
        self.position = position
        
    def setName(self,name):
        if(name is None):
            self.name = sd.getStockNameByCode(self.code)
        else:
            self.name = name
        
    def filterData(self,dfData,startDate=None,endDate=None):
        dfFilterData = pd.DataFrame()
        if(startDate is None and endDate is None):
            dfFilterData = dfData
        elif(startDate is None and endDate is not None):
            dfFilterData = dfData[dfData['date'] < endDate]
        elif(endDate is None and startDate is not None):
            dfFilterData = dfData[dfData['date'] >= startDate]
        else:
            dfFilterData = dfData[dfData['date'] >= startDate]
            dfFilterData = dfFilterData[dfFilterData['date'] < endDate]
            
        return dfFilterData
        
    def setDfWeekData(self,startDate=None,endDate=None):
        dpWeekObj = dp.DataProcess(self.code, self.name,period=dp.WEEK)
        if(os.path.exists(dpWeekObj.dataGenCsvFile)):
            self.dfWeekGenData = pd.read_csv(dpWeekObj.dataGenCsvFile,encoding="utf-8",dtype={'code':str})
            self.dfWeekFilterData = self.filterData(self.dfWeekGenData,startDate,endDate)
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code,dpWeekObj.dataGenCsvFile))
            sys.exit()
            
        if(os.path.exists(dpWeekObj.signalReportFile)):
            self.dfWeekSignalData = pd.read_csv(dpWeekObj.signalReportFile,encoding="utf-8",dtype={'code':str})
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code,dpWeekObj.signalReportFile))
            sys.exit()
        
        self.reportPath = dpWeekObj.dataPath + self.code + '_exchange_report.csv'

    def setDfDayData(self,startDate=None,endDate=None):
        dpDayObj = dp.DataProcess(self.code, self.name,period=dp.DAY)
        if(os.path.exists(dpDayObj.dataGenCsvFile)):
            self.dfDayGenData = pd.read_csv(dpDayObj.dataGenCsvFile,encoding="utf-8",dtype={'code':str})
            self.dfDayFilterData = self.filterData(self.dfDayGenData,startDate,endDate)
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code,dpDayObj.dataGenCsvFile))
            sys.exit()
            
        if(os.path.exists(dpDayObj.signalReportFile)):
            self.dfDaySignalData = pd.read_csv(dpDayObj.signalReportFile,encoding="utf-8",dtype={'code':str})
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code,dpDayObj.signalReportFile))
            sys.exit()
            
        self.reportPath = dpDayObj.dataPath + self.code + '_exchange_report.csv'
        
    def setDfMonthData(self,startDate=None,endDate=None):
        dpMonthObj = dp.DataProcess(self.code, self.name,period=dp.MONTH)
        if(os.path.exists(dpMonthObj.dataGenCsvFile)):
            self.dfMonthGenData = pd.read_csv(dpMonthObj.dataGenCsvFile,encoding="utf-8",dtype={'code':str})
            self.dfMonthFilterData = self.filterData(self.dfMonthGenData,startDate,endDate)
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code,dpMonthObj.dataGenCsvFile))
            sys.exit()
            
        if(os.path.exists(dpMonthObj.signalReportFile)):
            self.dfMonthSignalData = pd.read_csv(dpMonthObj.signalReportFile,encoding="utf-8",dtype={'code':str})
        else:
            print('[Function:%s line:%s stock:%s] Error: File %s is not exist' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code,dpMonthObj.signalReportFile))
            sys.exit()
            
        self.reportPath = dpMonthObj.dataPath + self.code + '_exchange_report.csv'
        
    def isUpperShadowLine(self,df,index):
        upperLineLength = 0
        if(df.at[index,'close'] > df.at[index,'open']):
            upperLineLength = df.at[index,'high'] - df.at[index,'close']
        else:
            upperLineLength = df.at[index,'high'] - df.at[index,'open']
            
        if(df.at[index,'high'] != df.at[index,'low']):
            if(upperLineLength/(df.at[index,'high']-df.at[index,'low']) > 0.5):
                return True;
            else:
                return False
            
        return False
    
    def isLowerShadowLine(self,df,index):
        lowerLineLength = 0
        if(df.at[index,'close'] > df.at[index,'open']):
            lowerLineLength = df.at[index,'open'] - df.at[index,'low']
        else:
            lowerLineLength = df.at[index,'close'] - df.at[index,'low']
            
        if(df.at[index,'high'] != df.at[index,'low']):
            if(lowerLineLength/(df.at[index,'high']-df.at[index,'low']) > 0.5):
                return True;
            else:
                return False
            
        return False
            
    def exchange(self,startDate=None,endDate=None,initCapital=100000):
        pass
#         df = pd.DataFrame()
#         if(startDate is None and endDate is None):
#             df = self.dfDayGenData
#         elif(startDate is None and endDate is not None):
#             df = self.dfDayGenData[self.dfDayGenData['date'] < endDate]
#         elif(endDate is None and startDate is not None):
#             df = self.dfDayGenData[self.dfDayGenData['date'] >= startDate]
#         else:
#             df = self.dfDayGenData[self.dfDayGenData['date'] >= startDate]
#             df = df[df['date'] < endDate]
#             
#         self.capital = initCapital*self.position 
            
#         if(MA_20 in df.columns 
#            and MA_60 in df.columns 
#            and MA_250 in df.columns):
#             # add exchange condition
#             for index in df.index:
#                 isMa20Up = self.isMaUp(self.dfDayGenData,MA_20,index)
#                 isMa60Up = self.isMaUp(self.dfDayGenData,MA_60,index)
#                 isMa250Up = self.isMaUp(self.dfDayGenData,MA_250,index)
#                 if(isMa250Up == TRUE):
#                     # 牛市， 持股待涨
#                     # J >= 90 and volumn > VOL20 and 阴线， 卖出                        
#                     if(df.at[index,'kdj_j'] >= 90 
#                        and df.at[index,'volume'] > df.at[index,VOL_MA_20] 
#                        and df.at[index,'close'] < df.at[index,'open']):
#                         self.doAction(SELL, df.at[index,'close'],df.at[index,'date'])
#                         
#                     # J < 25 或者 volume < VOL20 或者 死叉带长下影线， 买入
#                     if(df.at[index,'kdj_j'] < 25 
#                        or df.at[index,'volume'] < df.at[index,VOL_MA_20]
#                        or (df.at[index,'kdj_j'] < df.at[index,'kdj_k'] and self.isLowerShadowLine(df,index))):
#                         self.doAction(BUY, df.at[index,'close'],df.at[index,'date'])
#                 elif(isMa20Up == TRUE and isMa60Up == TRUE and isMa250Up == TRUE):
#                     pass
#                 elif(isMa20Up == FALSE and isMa60Up == TRUE and isMa250Up == TRUE):
#                     # 牛市， 低位买入
#                     pass
#                 elif(isMa20Up == TRUE and isMa60Up == FALSE and isMa250Up == TRUE):
#                     # 牛市， 高抛低吸
#                     pass
#                 elif(isMa20Up == TRUE and isMa60Up == TRUE and isMa250Up == FALSE):
#                     # 熊转牛， 高抛低吸，轻仓试盘
#                     pass
#                 else:
#                     #熊市，空仓
#                     self.doAction(SELL, df.at[index,'close'],df.at[index,'date'])
#         else:
#             print('[Function:%s line:%s stock:%s] not enough data to add exchange point!'%(self.exchange.__name__, sys._getframe().f_lineno,self.code))
            
    def doAction(self,actionType,closePrice,date):
        if(actionType == BUY):
            amount = math.floor(self.capital/closePrice/100)
            if(amount > 0):
                self.stockAmount = self.stockAmount+amount
                self.action = actionType
                stockValue = amount*closePrice*100
                self.cost = self.getExchangeCost(stockValue,self.action)
                self.capital = self.capital - stockValue - self.cost
                self.addRecord(actionType,closePrice,date)
        else:
            if(self.stockAmount > 0):
                self.action = actionType
                stockValue = self.stockAmount*closePrice*100
                self.cost = self.getExchangeCost(stockValue,self.action)
                self.capital = self.capital + stockValue - self.cost
                self.stockAmount = 0
                self.addRecord(actionType,closePrice,date)
                
    def addRecord(self,actionType,price,date):
        self.profit = 0
        self.profitRate = 0
        if(actionType == SELL):
            indexLast = len(self.exchangeDf)
            indexCurr = indexLast - 1
            while indexCurr >= 0 and indexCurr < indexLast and (self.exchangeDf.at[indexCurr,'action'] is not ACTION_STRING[actionType]):
                indexCurr = indexCurr -1
            
            # get the first buy action index before current sell
            indexCurr = indexCurr + 1
            originalCapital = self.exchangeDf.at[indexCurr,'stockAmount']*100*self.exchangeDf.at[indexCurr,'price'] + self.exchangeDf.at[indexCurr,'capital'] + self.exchangeDf.at[indexCurr,'cost']
            self.profit = self.capital - originalCapital
            self.profitRate = self.profit/originalCapital

        data = {'aDate':[date],
        'aCode':[self.code],
        'name':[self.name],
        'action':[ACTION_STRING[actionType]],
        'price':[price],
        'cost':[self.cost],
        'capital':[self.capital],
        'stockAmount':[self.stockAmount],
        'profit':[self.profit],
        'profitRate':[self.profitRate]
        }
        dfPer = pd.DataFrame(data)
        self.exchangeDf = pd.concat([self.exchangeDf,dfPer])
        self.exchangeDf.reset_index(inplace=True, drop=True)
                    
    def getExchangeCost(self,amount,action,taxRate=0.001,commissionRate=0.0002,transferRate=0.0001):
        commissionCost = amount*commissionRate
        if(commissionCost < 5):
            commissionCost = 5
        transferCost = amount*transferRate
        if(action == BUY):   
            return commissionCost+transferCost
        else:
            taxCost = amount*taxRate
            return commissionCost+transferCost+taxCost
        
                       
#     def getWeekIndex(self,date):
#         df = self.dfWeekGenData[self.dfWeekGenData['date'] <= date]       
#         return df.index[-1]
# 
#     def getMonthIndex(self,date):
#         df = self.dfMonthGenData[self.dfMonthGenData['date'] <= date]       
#         return df.index[-1]
    
    def isMaUp(self,df,ma,indexCurr):
        if(ma in df.columns):
            index = indexCurr
            while index <= indexCurr and index > 0:
                if(df.at[index,ma] >0 and df.at[index-1,ma] > 0):
                    if(df.at[index,ma] > df.at[index-1,ma]):
                        return TRUE
                    elif (df.at[index,ma] == df.at[index-1,ma]):
                        index = index -1
                    else:
                        return FALSE
                else:
                    return UNDEFINED
        else:
            print('[Function:%s line:%s] Error: ma:%s is not in the column of dataframe!' %(self.isMaUp.__name__, sys._getframe().f_lineno,ma))
            sys.exit()
            
class CyclicalStockExchangeStrategy(ExchangeStrategy):
    def exchange(self,initCapital=100000):
        pass
    
class NoneCyclicalStockExchangeStrategy(ExchangeStrategy):
    def exchange(self,initCapital=100000):
        pass
    
#具体策略类
class Context(object):
    def __init__(self,strategy):
        self.strategy = strategy
 
    def doExchange(self):
        self.strategy.exchange()
        self.strategy.saveExchangeReport()
            
if __name__ == '__main__':
    sdate = sd.SingletonDate.GetInstance('2019-01-18')
    startDate = '2009-01-16'
    endDate = '2019-01-18'
    print('####################### Begin to test ExchangeStrategy ############################')
    stockExchangeStrategy = Context(ExchangeStrategy('600030',startDate=startDate,endDate=endDate))
    stockExchangeStrategy.doExchange()
    print('####################### End of testing ExchangeStrategy ############################')