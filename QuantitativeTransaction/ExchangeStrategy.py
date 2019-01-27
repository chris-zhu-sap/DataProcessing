import os
import pandas as pd
import sys
# import tushare as ts
import StockData as sd
import DataProcess as dp
import math

MA_5 = 'MA5'
MA_10 = 'MA10'
MA_20 = 'MA20'
MA_30 = 'MA30'
MA_60 = 'MA60'
MA_120 = 'MA120'
MA_250 = 'MA250'

# VOL_MA_5 = 'VOLMA5'
# VOL_MA_5 = 'VOLMA5'
VOL_MA_20 = 'VOLMA20'

# return value when check the MA is up or not
TRUE = 1
FALSE = 0
UNDEFINED = -1

GAIN = 1
LOSS = 0


# value for action
BUY = 1
SELL = 0

ACTION_STRING ={
    1:'B',
    0:'S'
    }

SELL_REASON_MONTH_DIC = {
    0:'stop_lose'
    }

BUY_REASON_MONTH_DIC = {
    0:'ma_5_will_be_up',
    1:'cross_last_high_price',
    2:'kdj_j has been up',
    }

LOSE_RATE_MONTH = -0.1

GAIN_RATE_MONTH_BEAR = 0.3

UPPER_LOWER_LINE_RATE = 0.5

MaxIndicatorDateList = ['2001-06-29','2007-10-31','2015-06-30']
pMaxIndicatorDate = pd.Series(MaxIndicatorDateList)

def exchangeForStockList(stockList,startDate=None,endDate=None):
    if len(stockList) > 0:
        dfLastRecord = pd.DataFrame()
        dataPath = sd.getDataFilePath()
        filePath = dataPath + 'last_exchange_record.csv'
        for code in stockList:
            cyclicalStockExchangeStrategy = CyclicalStockExchangeStrategy(code,startDate=startDate,endDate=endDate)
            stockExchangeStrategy = Context(cyclicalStockExchangeStrategy)
            stockExchangeStrategy.doExchange()
            lastIndex = len(stockExchangeStrategy.strategy.exchangeDf) -1
            if lastIndex >= 0:
                dfLastRecord = pd.concat([dfLastRecord,stockExchangeStrategy.strategy.exchangeDf.loc[[lastIndex]]])
                dfLastRecord.reset_index(drop=True, inplace=True)
        
        dfLastRecord.to_csv(filePath,index=0,float_format=dp.FLOAT_FORMAT2,encoding="utf-8")
        print('######################### get the last exchange record has been done!#########################')

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
        dpWeekObj.readData()
        self.dpWeekObj = dpWeekObj
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
        dpDayObj.readData()
        self.dpDayObj = dpDayObj
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
        dpMonthObj.readData()
        self.dpMonthObj = dpMonthObj
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
        upperLineLength  = df.at[index,'high'] - df.at[index,'close']
            
        if(df.at[index,'high'] != df.at[index,'low']):
            if(upperLineLength/(df.at[index,'high']-df.at[index,'low']) > UPPER_LOWER_LINE_RATE):
                return True;
            else:
                return False
            
        return False
    
    def isLowerShadowLine(self,df,index):
        lowerLineLength = df.at[index,'close'] - df.at[index,'low']
            
        if(df.at[index,'high'] != df.at[index,'low']):
            if(lowerLineLength/(df.at[index,'high']-df.at[index,'low']) > UPPER_LOWER_LINE_RATE):
                return True;
            else:
                return False
            
        return False
    
    def gainRate(self,df,index):
        if index > 0 and df.at[index-1,'close'] > 0:
            deltaPrice = df.at[index,'close'] - df.at[index-1,'close']
            rate = deltaPrice/df.at[index-1,'close']
            return rate
        else:
            print('[Function:%s line:%s stock:%s] Error: not enough data to calculate gain rate' %(self.setDfWeekData.__name__, sys._getframe().f_lineno,self.code))
            sys.exit()
            
    def exchange(self,initCapital=100000):
        pass          

    def doAction(self,actionType,price,date,reason):
        self.profit = 0
        self.profitRate = 0
        length = len(self.exchangeDf)
        indexLast = length-1

        if(length > 0):
            self.stockAmount = self.exchangeDf.at[indexLast,'stockAmount']
            self.cash = self.exchangeDf.at[indexLast,'cash']
            self.capital = self.exchangeDf.at[indexLast,'capital']            
            
        if(actionType == BUY):
            amount = math.floor(self.cash/price/100)
            if(amount > 0):
                self.stockAmount = self.stockAmount+amount
                self.action = actionType
                stockValue = amount*price*100
                self.cost = self.getExchangeCost(stockValue,self.action)
                self.cash = self.cash - stockValue - self.cost
                self.capital = self.capital - self.cost
                self.addRecord(actionType,price,date,reason)
        else:
            if(self.stockAmount > 0):
                self.action = actionType
                stockValue = self.stockAmount*price*100
                self.cost = self.getExchangeCost(stockValue,self.action)
                self.capital = self.cash + stockValue - self.cost
                self.cash = self.capital
                self.stockAmount = 0
                
                indexCurr = indexLast
                while indexCurr >= 0 and indexCurr <= indexLast and (self.exchangeDf.at[indexCurr,'action'] is not ACTION_STRING[actionType]):
                    indexCurr = indexCurr -1
                
                indexCurr = indexCurr + 1
                originalCapital = self.exchangeDf.at[indexCurr,'stockAmount']*100*self.exchangeDf.at[indexCurr,'price'] + self.exchangeDf.at[indexCurr,'cash'] + self.exchangeDf.at[indexCurr,'cost']
                self.profit = self.capital - originalCapital
                self.profitRate = self.profit/originalCapital
                
                self.addRecord(actionType,price,date,reason)
                
    def addRecord(self,actionType,price,date,reason):
        data = {'aDate':[date],
        'aCode':[self.code],
        'name':[self.name],
        'action':[ACTION_STRING[actionType]],
        'price':[price],
        'cost':[self.cost],
        'capital':[self.capital],
        'cash':[self.cash],
        'stockAmount':[self.stockAmount],
        'profit':[self.profit],
        'profitRate':[self.profitRate],
        'xReason':[reason]
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
    
    def isIndicatorUp(self,df,indicator,indexCurr):
        if(indicator in df.columns):
            index = indexCurr
            while index <= indexCurr and index > 0:
                if(df.at[index,indicator] > df.at[index-1,indicator]):
                    return TRUE
                elif (df.at[index,indicator] == df.at[index-1,indicator]):
                    if index > 1:
                        if(df.at[index-2,indicator] > df.at[index-1,indicator]):
                            return TRUE
                        elif(df.at[index-2,indicator] < df.at[index-1,indicator]):
                            return FALSE
                        else:
                            index = index -1
                    else:
                        return UNDEFINED
                else:
                    return FALSE
                
            return UNDEFINED
        else:
            print('[Function:%s line:%s] Error: ma:%s is not in the column of dataframe!' %(self.isIndicatorUp.__name__, sys._getframe().f_lineno,indicator))
            sys.exit()
 
    def stopLose(self,dfData,index,lossRate,highPrice):
        length = len(self.exchangeDf)
        if length > 0:
            indexLast = length - 1
            # action = BUY
            actionType = self.exchangeDf.at[indexLast,'action']
            if actionType == ACTION_STRING[BUY]:
                if highPrice is not None:  
                    rate = (dfData.at[index,'close'] - highPrice)/ dfData.at[index-1,'close']
                    if rate < lossRate:
                        price = highPrice*(1+lossRate)
                        self.doAction(SELL, price,dfData.at[index,'date'],SELL_REASON_MONTH_DIC[0])
                        return True
                else:
                    print('[Function:%s line:%s] Error: parameter highPrice should not be none!' %(self.stopLose.__name__, sys._getframe().f_lineno))
                    sys.exit()
                
        return False
                
    def isDifUpCrossDeaBelowZero(self,dfData,index):
        if (index > 0
            and dfData.at[index,'dif'] < 0 
            and dfData.at[index,'dif'] > dfData.at[index,'dea'] 
            and dfData.at[index-1,'dif'] <= dfData.at[index-1,'dea'] ):
            return True
        
        return False
    
    def isDifDownCrossDeaBelowZero(self,dfData,index):        
        if (index > 0
            and dfData.at[index,'dif'] < 0 
            and dfData.at[index,'dif'] < dfData.at[index,'dea'] 
            and dfData.at[index-1,'dif'] >= dfData.at[index-1,'dea'] ):
            return True
        
        return False
    
    def isDifUpCrossDeaAboveZero(self,dfData,index):
        if (index > 0
            and dfData.at[index-1,'dif'] <= dfData.at[index-1,'dea'] 
            and dfData.at[index,'dif'] > dfData.at[index,'dea'] 
            and dfData.at[index,'dif'] >= 0):
            return True
        
        return False
    
    def isDifDownCrossDeaAboveZero(self,dfData,index):
        if (index > 0
            and dfData.at[index-1,'dif'] >= dfData.at[index-1,'dea'] 
            and dfData.at[index,'dif'] < dfData.at[index,'dea'] 
            and dfData.at[index,'dif'] >= 0):
            return True
        
        return False
    
    def maWillBeUp(self,dfData,index,maName,period):
        if (index-period+1) > 0:
            if(dfData.at[index,'close'] > dfData.at[index-period+1,'close']
               and dfData.at[index-1,'close'] <= dfData.at[index-period,'close']):
                return True
            return False
        else:
            print('[Function:%s line:%s date:%s] Not enough data to know whether ma is up!'% (self.maWillBeUp.__name__, sys._getframe().f_lineno,dfData.at[index,'date']))     
    
    def isStatusOnHaveStock(self):
        length = len(self.exchangeDf)
        if length > 0:
            if self.exchangeDf.at[length-1,'action'] == ACTION_STRING[BUY]:
                return True

        return False
    
    def regenerateWeekData(self,date):
        dfWeekData = self.dpWeekObj.getDfOriginalData()
        dfDayData = self.dpDayObj.getDfOriginalData()
        dfDayDataTemp = dfDayData
        dfWeekDataTemp = dfWeekData[dfWeekData['date'] == date]
        if(len(dfWeekDataTemp) != 0):
            self.dfWeekRegenData = self.dfWeekGenData[self.dfWeekGenData['date'] <= date]
        else:
            dfWeekDataBefore = dfWeekData[dfWeekData['date'] < date]
            lastIndex = len(dfWeekDataBefore) - 1
            if(lastIndex > 0):
                lastWeekDate = dfWeekData.at[lastIndex,'date']
                dfDayDataTemp = dfDayDataTemp[dfDayDataTemp['date'] > lastWeekDate]
                dfDayDataTemp = dfDayDataTemp[dfDayDataTemp['date'] <= date]
            else:
                dfDayDataTemp = dfDayDataTemp[dfDayDataTemp['date'] <= date]
            
            indexList = dfDayDataTemp.index
            firstIndex = indexList[0]
            lastIndex = indexList[-1]
            
            highPrice = dfDayDataTemp['high'].max()
            lowPrice = dfDayDataTemp['low'].min()
            openPrice = dfDayDataTemp.at[firstIndex,'open']
            closePrice = dfDayDataTemp.at[lastIndex,'close']
            volume = dfDayDataTemp['volume'].sum()
            data = {'date':[date],
                    'open':[openPrice],
                    'close':[closePrice],
                    'high':[highPrice],
                    'low':[lowPrice],
                    'volume':[volume]
                }
            dfWeekTemp = pd.DataFrame(data)
            
            if(lastIndex > 0):
                dfWeekTemp = pd.concat([dfWeekDataBefore,dfWeekTemp])
                
            dfWeekTemp.reset_index(drop=True,inplace=True)
            
            self.dpWeekObj.resetDfData(dfWeekTemp)
            self.dpWeekObj.addNormalIndicator()
            
            self.dfWeekRegenData = self.dpWeekObj.getDfData()
    
    def regenerateMonthData(self,date):
        dfMonthData = self.dpMonthObj.getDfOriginalData()
        dfDayData = self.dpDayObj.getDfOriginalData()
        dfDayDataTemp = dfDayData
        dfMonthDataTemp = dfMonthData[dfMonthData['date'] == date]
        if(len(dfMonthDataTemp) != 0):
            self.dfMonthRegenData = self.dfMonthGenData[self.dfMonthGenData['date'] <= date]
        else:
            dfMonthDataBefore = dfMonthData[dfMonthData['date'] < date]
            lastIndex = len(dfMonthDataBefore) - 1
            if(lastIndex > 0):
                lastMonthDate = dfMonthData.at[lastIndex,'date']
                dfDayDataTemp = dfDayDataTemp[dfDayDataTemp['date'] > lastMonthDate]
                dfDayDataTemp = dfDayDataTemp[dfDayDataTemp['date'] <= date]
            else:
                dfDayDataTemp = dfDayData[dfDayData['date'] <= date]
            
            indexList = dfDayDataTemp.index
            firstIndex = indexList[0]
            lastIndex = indexList[-1]
            
            highPrice = dfDayDataTemp['high'].max()
            lowPrice = dfDayDataTemp['low'].min()
            openPrice = dfDayDataTemp.at[firstIndex,'open']
            closePrice = dfDayDataTemp.at[lastIndex,'close']
            volume = dfDayDataTemp['volume'].sum()
            data = {'date':[date],
                    'open':[openPrice],
                    'close':[closePrice],
                    'high':[highPrice],
                    'low':[lowPrice],
                    'volume':[volume]
                }
            dfMonthTemp = pd.DataFrame(data)
            
            if(lastIndex > 0):
                dfMonthTemp = pd.concat([dfMonthDataBefore,dfMonthTemp])
                
            dfMonthTemp.reset_index(drop=True,inplace=True)
            
            self.dpMonthObj.resetDfData(dfMonthTemp)
            self.dpMonthObj.addNormalIndicator()
            
            self.dfMonthRegenData = self.dpMonthObj.getDfData()

class CyclicalStockExchangeStrategy(ExchangeStrategy):
    def exchange(self):
        # after two year of max price in bull market
        self.exchangeInMiddleTerm()

    def exchangeInMiddleTerm(self,initCapital=100000):
        self.capital = initCapital*self.position
        self.cash = self.capital
        dfData = self.dfWeekFilterData
        dfGenData = self.dfWeekGenData
        bullMarket = FALSE
        difHasBeenUpCrossDeaBelowZero = False
        difHasBeenUpCrossDeaAboveZero = False
        difHasBeenDownCrossDeaBelowZero = False
        difHasBeenDownCrossDeaAboveZero = False
        highPrice = 0
        for index in dfData.index:
            self.regenerateMonthData(dfData.at[index,'date'])
            if difHasBeenUpCrossDeaBelowZero == False:
                difHasBeenUpCrossDeaBelowZero = self.isDifUpCrossDeaBelowZero(dfGenData,index)
            if difHasBeenUpCrossDeaAboveZero == False:
                difHasBeenUpCrossDeaAboveZero = self.isDifUpCrossDeaAboveZero(dfGenData,index)
            if difHasBeenDownCrossDeaBelowZero == False:
                difHasBeenDownCrossDeaBelowZero = self.isDifDownCrossDeaBelowZero(dfGenData,index)
            if difHasBeenDownCrossDeaAboveZero == False:
                difHasBeenDownCrossDeaAboveZero = self.isDifDownCrossDeaAboveZero(dfGenData,index)
 
            if difHasBeenUpCrossDeaBelowZero:
                difHasBeenDownCrossDeaBelowZero = False
                difHasBeenDownCrossDeaAboveZero = False
                if (self.isIndicatorUp(dfGenData, MA_20, index) 
                    and self.isIndicatorUp(dfGenData, MA_10, index)
                    and dfData.at[index,'dif'] >= dfData.at[index,'dea']):
                    bullMarket = TRUE
                    #print('##########  date:%s set bull market TRUE case1#######################'% dfData.at[index,'date'])
                elif(self.isIndicatorUp(dfGenData, 'dif', index)
                     and dfData.at[index,'dif'] >= dfData.at[index,'dea']):
                    bullMarket = TRUE
                    #print('##########  date:%s set bull market TRUE case0#######################'% dfData.at[index,'date'])
                else:
                    bullMarket = UNDEFINED
                    #print('##########  date:%s set bull market UNDEFINED case2#######################'% dfData.at[index,'date'])
                 
            if difHasBeenUpCrossDeaAboveZero and dfData.at[index,'dif'] >= dfData.at[index,'dea']:
                difHasBeenDownCrossDeaBelowZero = False
                difHasBeenDownCrossDeaAboveZero = False
                if (self.isIndicatorUp(dfGenData, MA_20, index) 
                    and self.isIndicatorUp(dfGenData, MA_10, index) 
                    and dfData.at[index,'dif'] >= dfData.at[index,'dea']):
                    bullMarket = TRUE
                    #print('##########  date:%s set bull market TRUE case3#######################'% dfData.at[index,'date'])
                else:
                    bullMarket = UNDEFINED
                    #print('##########  date:%s set bull market UNDEFINED case4#######################'% dfData.at[index,'date'])                    
 
            if (difHasBeenDownCrossDeaBelowZero
                or difHasBeenDownCrossDeaAboveZero
                or dfData.at[index,'dif'] < dfData.at[index,'dea']):
                difHasBeenUpCrossDeaBelowZero =False
                difHasBeenUpCrossDeaAboveZero = False
                if dfData.at[index,'dif'] < dfData.at[index,'dea']:
                    bullMarket = FALSE
                    #print('##########  date:%s set bull market FALSE case5#######################'% dfData.at[index,'date'])
                elif self.isIndicatorUp(dfGenData, 'dif', index):
                    bullMarket = UNDEFINED
                    #print('##########  date:%s set bull market UNDEFINED case6#######################'% dfData.at[index,'date'])
                else:
                    bullMarket = FALSE
                    #print('##########  date:%s set bull market FALSE case7#######################'% dfData.at[index,'date'])

#             if bullMarket == TRUE:
#                 if self.isStatusOnHaveStock():
#                     if(dfData.at[index,'close'] < highPrice):
#                         self.stopLose(dfData, index, LOSE_RATE_MONTH,highPrice)
#                         print('[Function:%s line:%s date:%s stock:%s] bull market, status on have stock and price is decreasing, check whether need to stop lose#######################'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
#                     else:
#                         highPrice = dfData.at[index,'close']
#                         print('[Function:%s line:%s date:%s stock:%s] bull market, status on have stock and price is increading, don\'t do any exchange#######################'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
#                 else:
#                     if self.isIndicatorUp(dfGenData, 'kdj_j', index):
#                         highPrice = dfData.at[index,'close']
#                         self.doAction(BUY,dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_MONTH_DIC[2])
#                         print('[Function:%s line:%s date:%s stock:%s] bull market, status on have no stock, kdj_j has been up, buy the stock'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
#                     elif(dfData.at[index,'close'] > highPrice):
#                         if(highPrice != 0):
#                             highPrice = dfData.at[index,'close']
#                             self.doAction(BUY,highPrice,dfData.at[index,'date'],BUY_REASON_MONTH_DIC[1])
#                             print('[Function:%s line:%s date:%s stock:%s] bull market, status on have no stock, price up cross high price, buy the stock'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
#                         else:
#                             self.doAction(BUY,dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_MONTH_DIC[1])
#                             highPrice = dfData.at[index,'close']
#                             print('[Function:%s line:%s date:%s stock:%s] bull market, status on have no stock, high price equal to zero, buy the stock'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
#                     else:
#                         print('[Function:%s line:%s date:%s stock:%s] bull market, status on have no stock, current have no high price, wait for it'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
#             else:
#                 if self.maWillBeUp(dfGenData, index, MA_5, 5):
#                     if self.isStatusOnHaveStock():
#                         if(dfData.at[index,'close'] < highPrice):
#                             self.stopLose(dfData, index, LOSE_RATE_MONTH,highPrice)
#                             print('[Function:%s line:%s date:%s stock:%s] ma_5 will be up, on status of having stock, need to check whether need stop lose!'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
#                         else:
#                             highPrice = dfData.at[index,'close']
#                         print('[Function:%s line:%s date:%s stock:%s] ma_5 will be up, should have no stock before!'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
#                     else:
#                         highPrice = dfData.at[index,'close']
#                         self.doAction(BUY, dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_MONTH_DIC[0])
#                         print('[Function:%s line:%s date:%s stock:%s] ma_5 will be up, status on no stock, buy it!'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
#                 else:
#                     if self.isStatusOnHaveStock():
#                         if dfGenData.at[index,'close'] < dfGenData.at[index-1,'close']:
#                             self.stopLose(dfData, index,LOSE_RATE_MONTH, highPrice)
#                         else:
#                             highPrice = dfData.at[index,'close']
#                             print('[Function:%s line:%s date:%s stock:%s] Not bull market, status on have stock and price is increasing, don\'t do any exchange'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
#                     else:
#                         print('[Function:%s line:%s date:%s stock:%s] Not bull market, status on have cash, don\'t do any exchange'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))   

    
    def exchangeInLongTerm(self,initCapital=100000):
        self.capital = initCapital*self.position
        self.cash = self.capital
        dfData = self.dfMonthFilterData
        dfGenData = self.dfMonthGenData
        bullMarket = FALSE
        difHasBeenUpCrossDeaBelowZero = False
        difHasBeenUpCrossDeaAboveZero = False
        difHasBeenDownCrossDeaBelowZero = False
        difHasBeenDownCrossDeaAboveZero = False
        highPrice = 0
        for index in dfData.index:
            if difHasBeenUpCrossDeaBelowZero == False:
                difHasBeenUpCrossDeaBelowZero = self.isDifUpCrossDeaBelowZero(dfGenData,index)
            if difHasBeenUpCrossDeaAboveZero == False:
                difHasBeenUpCrossDeaAboveZero = self.isDifUpCrossDeaAboveZero(dfGenData,index)
            if difHasBeenDownCrossDeaBelowZero == False:
                difHasBeenDownCrossDeaBelowZero = self.isDifDownCrossDeaBelowZero(dfGenData,index)
            if difHasBeenDownCrossDeaAboveZero == False:
                difHasBeenDownCrossDeaAboveZero = self.isDifDownCrossDeaAboveZero(dfGenData,index)

            if difHasBeenUpCrossDeaBelowZero:
                difHasBeenDownCrossDeaBelowZero = False
                difHasBeenDownCrossDeaAboveZero = False
                if (self.isIndicatorUp(dfGenData, MA_20, index) 
                    and self.isIndicatorUp(dfGenData, MA_10, index)
                    and dfData.at[index,'dif'] >= dfData.at[index,'dea']):
                    bullMarket = TRUE
                    #print('##########  date:%s set bull market TRUE case1#######################'% dfData.at[index,'date'])
                elif(self.isIndicatorUp(dfGenData, 'dif', index)
                     and dfData.at[index,'dif'] >= dfData.at[index,'dea']):
                    bullMarket = TRUE
                    #print('##########  date:%s set bull market TRUE case0#######################'% dfData.at[index,'date'])
                else:
                    bullMarket = UNDEFINED
                    #print('##########  date:%s set bull market UNDEFINED case2#######################'% dfData.at[index,'date'])
                
            if difHasBeenUpCrossDeaAboveZero and dfData.at[index,'dif'] >= dfData.at[index,'dea']:
                difHasBeenDownCrossDeaBelowZero = False
                difHasBeenDownCrossDeaAboveZero = False
                if (self.isIndicatorUp(dfGenData, MA_20, index) 
                    and self.isIndicatorUp(dfGenData, MA_10, index) 
                    and dfData.at[index,'dif'] >= dfData.at[index,'dea']):
                    bullMarket = TRUE
                    #print('##########  date:%s set bull market TRUE case3#######################'% dfData.at[index,'date'])
                else:
                    bullMarket = UNDEFINED
                    #print('##########  date:%s set bull market UNDEFINED case4#######################'% dfData.at[index,'date'])                    

            if (difHasBeenDownCrossDeaBelowZero
                or difHasBeenDownCrossDeaAboveZero
                or dfData.at[index,'dif'] < dfData.at[index,'dea']):
                difHasBeenUpCrossDeaBelowZero =False
                difHasBeenUpCrossDeaAboveZero = False
                if dfData.at[index,'dif'] < dfData.at[index,'dea']:
                    bullMarket = FALSE
                    #print('##########  date:%s set bull market FALSE case5#######################'% dfData.at[index,'date'])
                elif self.isIndicatorUp(dfGenData, 'dif', index):
                    bullMarket = UNDEFINED
                    #print('##########  date:%s set bull market UNDEFINED case6#######################'% dfData.at[index,'date'])
                else:
                    bullMarket = FALSE
                    #print('##########  date:%s set bull market FALSE case7#######################'% dfData.at[index,'date'])

            if bullMarket == TRUE:
                if self.isStatusOnHaveStock():
                    if(dfData.at[index,'close'] < highPrice):
                        self.stopLose(dfData, index, LOSE_RATE_MONTH,highPrice)
                        print('[Function:%s line:%s date:%s stock:%s] bull market, status on have stock and price is decreasing, check whether need to stop lose#######################'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
                    else:
                        highPrice = dfData.at[index,'close']
                        print('[Function:%s line:%s date:%s stock:%s] bull market, status on have stock and price is increading, don\'t do any exchange#######################'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
                else:
                    if self.isIndicatorUp(dfGenData, 'kdj_j', index):
                        highPrice = dfData.at[index,'close']
                        self.doAction(BUY,dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_MONTH_DIC[2])
                        print('[Function:%s line:%s date:%s stock:%s] bull market, status on have no stock, kdj_j has been up, buy the stock'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
                    elif(dfData.at[index,'close'] > highPrice):
                        if(highPrice != 0):
                            highPrice = dfData.at[index,'close']
                            self.doAction(BUY,highPrice,dfData.at[index,'date'],BUY_REASON_MONTH_DIC[1])
                            print('[Function:%s line:%s date:%s stock:%s] bull market, status on have no stock, price up cross high price, buy the stock'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
                        else:
                            self.doAction(BUY,dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_MONTH_DIC[1])
                            highPrice = dfData.at[index,'close']
                            print('[Function:%s line:%s date:%s stock:%s] bull market, status on have no stock, high price equal to zero, buy the stock'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
                    else:
                        print('[Function:%s line:%s date:%s stock:%s] bull market, status on have no stock, current have no high price, wait for it'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
            else:
                if self.maWillBeUp(dfGenData, index, MA_5, 5):
                    if self.isStatusOnHaveStock():
                        if(dfData.at[index,'close'] < highPrice):
                            self.stopLose(dfData, index, LOSE_RATE_MONTH,highPrice)
                            print('[Function:%s line:%s date:%s stock:%s] ma_5 will be up, on status of having stock, need to check whether need stop lose!'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
                        else:
                            highPrice = dfData.at[index,'close']
                        print('[Function:%s line:%s date:%s stock:%s] ma_5 will be up, should have no stock before!'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
                    else:
                        highPrice = dfData.at[index,'close']
                        self.doAction(BUY, dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_MONTH_DIC[0])
                        print('[Function:%s line:%s date:%s stock:%s] ma_5 will be up, status on no stock, buy it!'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
                else:
                    if self.isStatusOnHaveStock():
                        if dfGenData.at[index,'close'] < dfGenData.at[index-1,'close']:
                            self.stopLose(dfData, index,LOSE_RATE_MONTH, highPrice)
                        else:
                            highPrice = dfData.at[index,'close']
                            print('[Function:%s line:%s date:%s stock:%s] Not bull market, status on have stock and price is increasing, don\'t do any exchange'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))
                    else:
                        print('[Function:%s line:%s date:%s stock:%s] Not bull market, status on have cash, don\'t do any exchange'% (self.exchangeInLongTerm.__name__, sys._getframe().f_lineno,dfData.at[index,'date'],self.code))

    def exchangeInShortTerm(self,initCapital=100000):
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