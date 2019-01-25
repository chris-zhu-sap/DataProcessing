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

SELL_REASON_DIC = {
    0:'dif > 0, bull_top < high price, kdj_j > 90',
    1:'top deviation come',
    2:'dif > 0, dea > 0, close < ma_5,high(index) > bull_top or high(index-1) > bull_top, bull market stopped',
    3:'top deviation come',
    4:'stop gaining more',
    5:'stop lose more',
    }

BUY_REASON_DIC = {
    0:'dif up cross dea below zero',
    1:'dif big than 0 and kdj_j < 50',
    2:'kdj_j(index) > 10 and kdj_j(index) < 30 and kdj_j(index-1) < 10 and kdj_j(index-2) < 10',
    3:'bottom deviation come'
    }

GAIN_LOSE_RATE_LIST_MONTH = [2.5,-1]

MaxIndicatorDateList = ['2001-06-29','2007-10-31','2015-06-30']
pMaxIndicatorDate = pd.Series(MaxIndicatorDateList)

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
        self.getDateListOfMaxPriceInBullMarket()
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
    
    def getDateListOfMaxPriceInBullMarket(self):
        pass
        
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
            
    def exchange(self,initCapital=100000):
        pass          
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
        
                       
#     def getWeekIndex(self,date):
#         df = self.dfWeekGenData[self.dfWeekGenData['date'] <= date]       
#         return df.index[-1]
# 
#     def getMonthIndex(self,date):
#         df = self.dfMonthGenData[self.dfMonthGenData['date'] <= date]       
#         return df.index[-1]
    
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
            
    def isDifUpBelowZero(self,df,index):
        if df.at[index,'dif'] < 0 and self.isIndicatorUp(df, 'dif', index):
            return True
        return False
            
    def getLastMaxPriceDate(self,curDate):
        ps = self.psMaxPrice[self.psMaxPrice < curDate]
        psLength = len(ps)
        if(psLength > 0):
            ps = ps.reset_index(drop = True)
            lastIndex = psLength -1
            return ps[lastIndex]
        
        ps = pMaxIndicatorDate[pMaxIndicatorDate < curDate]
        psLength = len(ps)
        if(psLength > 0):
            ps = ps.reset_index(drop = True)
            lastIndex = psLength -1
            return ps[lastIndex]
        
        return None
    
    def isDevivation(self,df,date,indicator):
        df = self.dfMonthSignalData[self.dfMonthSignalData['aDate'] == date]
        df = df[df['signal'] == indicator]
        if len(df) > 0 :
            return True
        else:
            return False
        
    def stopGainAndLose(self,dfData,index,gainLossRateList):
        gainRate = gainLossRateList[0]
        lossRate = gainLossRateList[1]
        length = len(self.exchangeDf)
        if length > 0:
            indexLast = length - 1
            # action = BUY
            actionType = self.exchangeDf.at[indexLast,'action']
            if actionType == ACTION_STRING[1]:
                buyPrice = self.exchangeDf.at[indexLast,'price']
                currHighPrice = self.dfMonthGenData.at[index,'high']
                possibleRate = (currHighPrice - buyPrice)/buyPrice
                if possibleRate > gainRate:
                    price = buyPrice*(1+gainRate)
                    self.doAction(SELL, price,dfData.at[index,'date'],SELL_REASON_DIC[4])
                    return GAIN
                
                currLowPrice = self.dfMonthGenData.at[index,'low']   
                possibleRate = (currLowPrice - buyPrice)/buyPrice   
                if possibleRate < lossRate:
                    price = buyPrice*(1+lossRate)
                    self.doAction(SELL, price,dfData.at[index,'date'],SELL_REASON_DIC[5])
                    return LOSS
                
        return UNDEFINED
                
    def isDifCrossDeaBelowZero(self,index):
        if self.dfMonthGenData.at[index,'dif'] < 0 and self.dfMonthGenData.at[index,'dea'] < 0 and self.dfMonthGenData.at[index,'dif'] > self.dfMonthGenData.at[index,'dea']:
            return True
        
        return False 
    
    def isDifCrossDeaAboveZero(self,index):
        if self.dfMonthGenData.at[index-1,'dif'] <= self.dfMonthGenData.at[index-1,'dea'] and self.dfMonthGenData.at[index,'dif'] > self.dfMonthGenData.at[index,'dea'] and self.dfMonthGenData.at[index,'dif'] > 0:
            return True
        
        return False            
            
class CyclicalStockExchangeStrategy(ExchangeStrategy):
    def exchange(self):
        # after two year of max price in bull market
        self.exchangeInLongTerm()
   
    def exchangeInLongTerm(self,initCapital=100000):
        # exchange after one year of max price and dif up
        self.capital = initCapital*self.position
        self.cash = self.capital
        dfData = self.dfMonthFilterData
        bullMarket = False
        difHasBeenCrossDeaBelowZero = False
        difHasBeenCrossDeaAboveZero = False
        difUpBelowZero = False
        for index in dfData.index:
            currentDate = dfData.at[index,'date']
            maxPriceDate = self.getLastMaxPriceDate(currentDate)

            if maxPriceDate is not None:
                dateNextYear = sd.getDateNextYear(maxPriceDate)
                if currentDate > dateNextYear or bullMarket == True:
                    if difHasBeenCrossDeaBelowZero == False:
                        difHasBeenCrossDeaBelowZero = self.isDifCrossDeaBelowZero(index)
                    if difHasBeenCrossDeaAboveZero == False:
                        difHasBeenCrossDeaBelowZero = self.isDifCrossDeaBelowZero(index)
                    if difUpBelowZero == False:
                        difUpBelowZero = self.isDifUpBelowZero(self.dfMonthGenData,index)
                    isMonthTopDeviation = self.isDevivation(self.dfMonthSignalData,dfData.at[index,'date'],'dif_top')
                    isMonthBottomDeviation = self.isDevivation(self.dfMonthSignalData,dfData.at[index,'date'],'dif_bottom')
                    if difHasBeenCrossDeaBelowZero == True or difHasBeenCrossDeaBelowZero == True or difUpBelowZero == True:
                        if(len(self.exchangeDf) == 0):
                            self.doAction(BUY, dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_DIC[0])
                            bullMarket = True
                        elif(self.dfMonthGenData.at[index,'dif'] < 0 and self.dfMonthGenData.at[index,'kdj_j'] > 90 and self.dfMonthGenData.at[index,'boll_top'] < self.dfMonthGenData.at[index,'high']):
                            self.doAction(SELL, dfData.at[index,'close'],dfData.at[index,'date'],SELL_REASON_DIC[0])
                        elif(self.dfMonthGenData.at[index,'kdj_j'] < 50 and self.dfMonthGenData.at[index,'dif'] > 0):
                            self.doAction(BUY, dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_DIC[1])
                            bullMarket = True
                        elif(self.dfMonthGenData.at[index,'kdj_j'] < 30 
                             and self.dfMonthGenData.at[index,'kdj_j'] > 10 
                             and self.dfMonthGenData.at[index-1,'kdj_j'] < 10 
                             and self.dfMonthGenData.at[index-2,'kdj_j'] < 10):
                            self.doAction(BUY, dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_DIC[2])
                            bullMarket = True
                        elif(self.dfMonthGenData.at[index,'dif'] > 0
                             and self.dfMonthGenData.at[index,'dea'] > 0
                             and (self.dfMonthGenData.at[index,'high'] >  self.dfMonthGenData.at[index,'boll_top'] or self.dfMonthGenData.at[index-1,'high'] >  self.dfMonthGenData.at[index,'boll_top'])
                             and self.dfMonthGenData.at[index,'close'] < self.dfMonthGenData.at[index,MA_5] ):
                            self.doAction(SELL, dfData.at[index,'close'],dfData.at[index,'date'],SELL_REASON_DIC[2])
                            bullMarket = False
                            difHasBeenCrossDeaBelowZero = False
                            difHasBeenCrossDeaBelowZero = False
                            difUpBelowZero = False
                        else:
                            print('##########  date:%s 情况不明， 不操作 #######################'% dfData.at[index,'date'])
                    elif isMonthTopDeviation:
                        self.doAction(SELL, dfData.at[index,'close'],dfData.at[index,'date'],SELL_REASON_DIC[1])
                        bullMarket = False
                        difHasBeenCrossDeaBelowZero = False
                        difHasBeenCrossDeaBelowZero = False
                        difUpBelowZero = False
                    elif isMonthBottomDeviation:
                        self.doAction(BUY, dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_DIC[3])
                        bullMarket = True
                    else:
                        print('##########  date:%s 底部没有金叉，不操作 #######################'% dfData.at[index,'date'])
                        
                    if self.stopGainAndLose(dfData, index, GAIN_LOSE_RATE_LIST_MONTH) == GAIN:
                        bullMarket = False
                        difHasBeenCrossDeaBelowZero = False
                        difHasBeenCrossDeaBelowZero = False
                        difUpBelowZero = False
                        
                # 熊市不到一年，不操作
                else:
                    print('##########  date:%s 熊市不到一年，不操作 #######################'% dfData.at[index,'date'])
                    
            else:
                print('Max price Date can\'t be found')
#                     if(MA_30 in dfData.columns):
#                         isMa5Up = self.isIndicatorUp(self.dfMonthGenData,MA_5,index)
#                         isMa10Up = self.isIndicatorUp(self.dfMonthGenData,MA_10,index)
#                         isMa20Up = self.isIndicatorUp(self.dfMonthGenData,MA_20,index)
#                         isMa30Up = self.isIndicatorUp(self.dfMonthGenData,MA_30,index)
#                         isMonthTopDeviation = self.isDevivation(self.dfMonthSignalData,dfData.at[index,'date'],'dif_top')
#                         isMonthBottomDeviation = self.isDevivation(self.dfMonthSignalData,dfData.at[index,'date'],'dif_bottom')
#                         # 牛市主升段， 不操作
#                         if (isMa5Up == TRUE and isMa10Up == TRUE and isMa20Up == TRUE and isMa30Up == TRUE):
#                             bullMarket = True
#                             print('##########  date:%s 牛市主升段， 不操作 #######################'% dfData.at[index,'date'])
#                         # 三十月均线向上，低位买入
#                         elif(isMa30Up == TRUE):
#                             if index > 0:
#                                 if (self.dfMonthGenData.at[index-1,'kdj_j'] < 30) and (self.dfMonthGenData.at[index-1,'kdj_j'] < self.dfMonthGenData.at[index,'kdj_j']):
#                                     self.doAction(BUY, dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_DIC['ma30up_jl30'])
#                                     print('##########  date:%s isMa30Up， 操作 #######################'% dfData.at[index,'date'])
#                                 else:
#                                     print('##########  date:%s isMa30Up， 不操作 #######################'% dfData.at[index,'date'])
#                             else:
#                                 print('##########  date:%s isMa30Up， index < 1  #######################'% dfData.at[index,'date'])
#                                     
#                         elif(isMa30Up == TRUE and dfData.at[index,'kdj_j'] > 100):
#                             self.doAction(SELL, dfData.at[index,'close'],dfData.at[index,'date'],SELL_REASON_DIC['ma30up_jm90'])
#                         elif(isMa20Up == TRUE and dfData.at[index,'kdj_j'] < 30):
#                             self.doAction(BUY, dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_DIC['ma20up_jl30'])
#                         elif(isMa20Up == TRUE and dfData.at[index,'kdj_j'] > 100):
#                             self.doAction(SELL, dfData.at[index,'close'],dfData.at[index,'date'],SELL_REASON_DIC['ma20up_jm90'])
#                         elif(isMa10Up == TRUE and dfData.at[index,'kdj_j'] < 20):
#                             self.doAction(BUY, dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_DIC['ma10up_jl20'])
#                         elif(isMa10Up == TRUE and dfData.at[index,'kdj_j'] > 90):
#                             self.doAction(SELL, dfData.at[index,'close'],dfData.at[index,'date'],SELL_REASON_DIC['ma10up_jm80'])
#                         elif(isMa5Up == TRUE and dfData.at[index,'kdj_j'] < 20):
#                             self.doAction(BUY, dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_DIC['ma5up_jl20'])
#                         elif(isMa5Up == TRUE and dfData.at[index,'kdj_j'] > 90):
#                             self.doAction(BUY, dfData.at[index,'close'],dfData.at[index,'date'],SELL_REASON_DIC['ma5up_jm80'])
#                         elif(isMonthTopDeviation):
#                             self.doAction(SELL, dfData.at[index,'close'],dfData.at[index,'date'],SELL_REASON_DIC['top_deviation'])
#                         elif(isMonthBottomDeviation and (isMa5Up == TRUE or isMa10Up == TRUE or isMa20Up == TRUE or isMa30Up == TRUE)):
#                             self.doAction(BUY, dfData.at[index,'close'],dfData.at[index,'date'],BUY_REASON_DIC['bottom_deviation'])
#                         elif(dfData.at[index,'kdj_j'] < 0 and isMa5Up == FALSE and isMa10Up == FALSE and isMa20Up == FALSE and isMa30Up == FALSE):
#                             print('##########  date:%s 均线向下， 不操作 #######################'% dfData.at[index,'date'])
#                         else:
#                             print('##########  date:%s 情况不明， 不操作 #######################'% dfData.at[index,'date'])
# 
#                         #止盈，止损
#                         if bullMarket == False:
#                             self.stopGainAndLose(dfData,index,GAIN_LOSE_RATE_LIST_MONTH)
#                          
#                     # 次新股， 暂时不处理
#                     else:
#                         print('##########  date:%s 次新股， 暂时不处理 #######################'% dfData.at[index,'date'])
#                 elif bullMarket == True:
#                     if dfData.at[index,MA_5] > dfData.at[index,'close']:
#                         bullMarket = False
#                         self.doAction(BUY, dfData.at[index,MA_5],dfData.at[index,'date'],SELL_REASON_DIC['bull_market_stopped'])
#                     else:
#                         print('##########  date:%s 牛市继续中，不操作 #######################'% dfData.at[index,'date'])
                    

    
    def exchangeInMiddleTerm(self,initCapital=100000):
        pass
    
    def exchangeInShortTerm(self,initCapital=100000):
        pass
    
    def getDateListOfMaxPriceInBullMarket(self):
        df = self.dfMonthGenData[self.dfMonthGenData['dif'] > 0]
        df = df[df['dif_div_close'] > 0.1 ]
        startIndex = df.index[0]
        endIndex = df.index[0]-1
        self.dfDateOfMaxPriceInBullMarket = pd.DataFrame()
        indexList = []
        for index in df.index:
            if(index == (endIndex+1)):
                endIndex = index
            else:
                subDf = pd.Series(self.dfMonthGenData['high'][startIndex:(endIndex+1)])
                indexList.append(subDf.idxmax())
                startIndex = index
                endIndex = index
         
        # add last case for date       
        subDf = pd.Series(self.dfMonthGenData['high'][startIndex:(endIndex+1)])
        indexList.append(subDf.idxmax())
                
        self.psMaxPrice = self.dfMonthGenData.loc[indexList,'date']
        
#         print(self.psMaxPrice)
    
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
    sdate = sd.SingletonDate.GetInstance('2019-01-23')
    startDate = '2002-01-16'
    endDate = '2019-01-18'
    print('####################### Begin to test ExchangeStrategy ############################')
    stockExchangeStrategy = Context(CyclicalStockExchangeStrategy('000738',startDate=startDate,endDate=endDate))
    stockExchangeStrategy.doExchange()
    print('####################### End of testing ExchangeStrategy ############################')