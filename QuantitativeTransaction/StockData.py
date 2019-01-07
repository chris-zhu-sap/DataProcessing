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

class SingletonBasicStockInfo():
    instance=None
    mutex=threading.Lock()
    def _init__(self):
        pass
    @staticmethod
    def GetInstance(fileWithPath):
        if(SingletonBasicStockInfo.instance==None):
            SingletonBasicStockInfo.mutex.acquire()
            if(SingletonBasicStockInfo.instance==None):
                SingletonBasicStockInfo.instance=SingletonBasicStockInfo()
                if os.path.exists(fileWithPath):
                    SingletonBasicStockInfo.instance.basicInfoDf = pd.read_csv(fileWithPath,encoding='utf-8',dtype={'code':str})
                    SingletonBasicStockInfo.instance.basicInfoDf.set_index('code',inplace=True)
                else:
                    SingletonBasicStockInfo.instance.basicInfoDf = ts.get_stock_basics()
                    SingletonBasicStockInfo.instance.basicInfoDf.to_csv(fileWithPath,encoding='utf-8',index ='code')
            SingletonBasicStockInfo.mutex.release()
           
        return SingletonBasicStockInfo.instance

def getChinaStockList(url, filename, date=None): 
    filePath = getDataFilePath(date)
    fileWithPath = filePath + filename

    if(os.path.exists(fileWithPath) == False):
        urllib.request.urlretrieve(url, filename)
        if (os.path.exists(filePath) == False):
            os.makedirs(filePath)

        shutil.move(filename,fileWithPath)

    if os.path.exists(fileWithPath):
        df = pd.read_excel(fileWithPath, dtype = {'成分券代码Constituent Code':object},encoding="utf-8")
        stockCode = df.iloc[:,4]
        return stockCode.values.tolist()
    else:
        print('[Function: %s line:%s] %s is not exists!' %(getChinaStockList.__name__, sys._getframe().f_lineno, fileWithPath))
        sys.exit()

def getNameAndCode(url):
    m = re.match(r'.*(\d{6})(.*)', url)
    return m.group(1), m.group(1) + m.group(2)

def isToday(date):
    today = time.strftime('%Y-%m-%d')
    if(date is None or date != today):
        return False;
    
    return True;

def getDataFilePath(date=None):
    if date is None:
        dirNameByDate = time.strftime('%Y-%m-%d')
    else:
        dirNameByDate = date
        
    delemeter = getDelimeter()
    
    return os.getcwd() + delemeter + dirNameByDate + delemeter

def getDfFromBasicList(date=None):
    fileWithPath = getDataFilePath(date) + "stock_basic_list.csv"
    filePath = getDataFilePath(date)
    if (os.path.exists(filePath) == False):
        os.makedirs(filePath)     
    instance = SingletonBasicStockInfo.GetInstance(fileWithPath)
        
    return instance.basicInfoDf

def getStockNameByCode(stockCode,dataDate=None):
    dfBasic = getDfFromBasicList(dataDate)
    return dfBasic.ix[stockCode]['name']

def downloadStockDataAsCSV(codeList=None,date=None):
    df  = getDfFromBasicList()
        
    if(codeList is None):
        codeList = df.index
        print("[Function: %s] All stock in China will be download!" % downloadStockDataAsCSV.__name__)
    else:
        print("[Function: %s] Stock in the code list will be download!" % (downloadStockDataAsCSV.__name__))

    for code in codeList:
        try:
            dateTemp = df.ix[code]['timeToMarket']
            name = df.ix[code]['name']

            dateStr = str(dateTemp)
            dateToMarket = dateStr[:4] + "-" + dateStr[4:6] + "-" + dateStr[6:]

            myStock = StockData(code,name,date)
            myStock.getKDataAsCSV(DAY, startDate=dateToMarket)
            myStock.getKDataAsCSV(WEEK, startDate=dateToMarket)
            myStock.getKDataAsCSV(MONTH, startDate=dateToMarket)
        except Exception as e:
            print("[Function: %s line:%s stock:%s Error:%s] Error happen when download the stock" % (downloadStockDataAsCSV.__name__, sys._getframe().f_lineno, code, e))
            sys.exit()
            
def updateStockDataForList(codeList=None,date=None,k=None,updateDir=False):
    if codeList is None or not len(codeList):
        print("[Function:%s line:%s] The parameter codeList is None or empty!" % (updateStockData.__name__,sys._getframe().f_lineno))
        sys.exit()
        
    if date is None:
        print("[Function:%s line:%s] The parameter date is empty!" % (updateStockData.__name__,sys._getframe().f_lineno))
        sys.exit()

    df  = getDfFromBasicList(date)

    for code in codeList:
        try:
            if code in df.index:
                updateStockData(code, date, k)
            else:
                print("[Function:%s line:%s] The parameter code is not exists now!" % (updateStockData.__name__,sys._getframe().f_lineno))
                sys.exit()
        except Exception as e:
            print("[Function:%s line:%s stock:%s Error:%s] Error happen when update data for the stock" % (updateStockDataForList.__name__,sys._getframe().f_lineno,code, e))
            sys.exit()
            
    if(updateDir):
        today = time.strftime('%Y-%m-%d')
        if(date is None or today !=  date):
            delemeter = getDelimeter()
            oldDataPath = os.getcwd() + delemeter + date
            newDataPath = os.getcwd() + delemeter + today
            if(os.path.exists(oldDataPath)):
                os.rename(oldDataPath, newDataPath)
            else:
                print("[Function:%s line:%s]  oldDataPath: %s is not exists!" % (updateStockDataForList.__name__,sys._getframe().f_lineno,oldDataPath))
                sys.exit()
        
        
def updateStockData(code,date=None,k=None):
    myStock = StockData(code,date=date)
    if(k is None):
        myStock.updateAllKData()
    else:
        myStock.updateKData(k)
        
def getNextDate(dateStr):
    if(dateStr is not None and dateStr != ''):
        newDateStr = ''
        m1 = re.match(r'(\d+)/(\d+)/(\d+)',dateStr)
        m2 = re.match(r'(\d+)-(\d+)-(\d+)',dateStr)
        if(m1 is None and m2 is None):
            print("The format of date is unexpected!") 
            sys.exit()
        else:
            if(m1 is None):
                newDateStr = dateStr
            else:
                newDateStr = m1.group(3) + '-' + m1.group(1) + '-'  + m1.group(2)
        theDate = datetime.datetime.strptime(newDateStr, '%Y-%m-%d')
        nextDate = theDate+datetime.timedelta(days=1)
        return datetime.datetime.strftime(nextDate, "%Y-%m-%d")
    else:
        print("[Function: %s]: The parameter dateStr is empty!" % getNextDate.__name__)
        sys.exit()

def getDelimeter():
    if 'Windows' in platform.system():
        return "\\"
    else:
        return "/"    

class StockData(object):
    def __init__(self,stockCode,stockName=None,date=None):     
        self.stockCode = stockCode
        self.lenUpdated = 0
        self.setDirNameByDate(date)
        self.setDataPath()
        self.createDataDir()
        self.setDataFilePath()
        self.setStockName(stockName,date)
    
    def setStockName(self,stockName=None,date=None):
        if stockName is None:
            self.stockName = getStockNameByCode(self.stockCode,date)
        else:
            self.stockName = stockName
            
    def getStockName(self):
        return self.stockName
        
    def mergeData(self,dfData,period):
        df = pd.DataFrame()
        if(len(dfData) > 0):
            if(period == DAY and os.path.exists(self.dayKDataFilePath) == True):
                df = pd.read_csv(self.dayKDataFilePath)
            elif(period == WEEK and os.path.exists(self.weekKDataFilePath) == True):
                df = pd.read_csv(self.weekKDataFilePath)
            elif(period == MONTH and os.path.exists(self.monthKDataFilePath) == True):
                df = pd.read_csv(self.monthKDataFilePath)
            elif(period == HOUR and os.path.exists(self.hourKDataFilePath) == True):
                df = pd.read_csv(self.hourKDataFilePath)
            else:
                print("[Function: %s line:%d]: The period:%s is not support now!" % (self.mergeData.__name__,sys._getframe().f_lineno, period))
                sys.exit()
        else:
            print("[Function: %s line:%d]: parameter dfData is empty!" % (self.mergeData.__name__,sys._getframe().f_lineno))
            sys.exit()
         
        return pd.concat([df, dfData])
        
    def getDataLenUpdated(self):
        return self.lenUpdated

    def setDirNameByDate(self,date=None):
        if(date is None):
            self.dirNameByDate = time.strftime('%Y-%m-%d')
        else:
            self.dirNameByDate = date

    def setDataFilePath(self):
        self.dayKDataFilePath = self.dataPath + getDelimeter() + self.stockCode + "_k_day.csv"
        self.weekKDataFilePath = self.dataPath + getDelimeter() + self.stockCode + "_k_week.csv"
        self.monthKDataFilePath = self.dataPath + getDelimeter() + self.stockCode + "_k_month.csv"
        self.hourKDataFilePath = self.dataPath + getDelimeter() + self.stockCode + "_k_hour.csv"
            
    def setDataPath(self,indexDir=None):
#         dirNameByDate = time.strftime('%Y%m%d')
        if not indexDir:
            indexDir = self.dirNameByDate
        delemeter = getDelimeter()
        self.dataPath = os.getcwd() + delemeter + indexDir + delemeter + self.stockCode
        self.basicStockInfoFilePath = os.getcwd() + delemeter + indexDir + delemeter
        
    def createDataDir(self):
        isExists = os.path.exists(self.dataPath)
        if not isExists:
            os.makedirs(self.dataPath)

    def getKDataAsCSV(self,k=DAY,startDate=''): 
        if(k==DAY):
            if(os.path.exists(self.dayKDataFilePath) == False):
                df = ts.get_k_data(self.stockCode, start=startDate, ktype=k)
                # delete last column 'code'
                df.drop('code', axis=1,inplace=True)
                df.to_csv(self.dayKDataFilePath,index=0,float_format=FLOAT_FORMAT)
                print("[Function: %s]: file:%s has been download successfully!" % (self.getKDataAsCSV.__name__,self.dayKDataFilePath))
            else:
                print("[Function: %s]: file:%s has been download before, no need to do it again!" % (self.getKDataAsCSV.__name__,self.dayKDataFilePath))
        elif(k==WEEK):
            if(os.path.exists(self.weekKDataFilePath) == False):
                df = ts.get_k_data(self.stockCode, start=startDate, ktype=k)
                # delete last column 'code'
                df.drop('code', axis=1,inplace=True)
                df.to_csv(self.weekKDataFilePath,index=0,float_format=FLOAT_FORMAT)
                print("[Function: %s]: file:%s has been download successfully!" % (self.getKDataAsCSV.__name__,self.weekKDataFilePath))
            else:
                print("[Function: %s]: file:%s has been download before, no need to do it again!" % (self.getKDataAsCSV.__name__,self.weekKDataFilePath))
        elif(k==MONTH):
            if(os.path.exists(self.monthKDataFilePath) == False):
                df = ts.get_k_data(self.stockCode, start=startDate, ktype=k)
                # delete last column 'code'
                df.drop('code', axis=1,inplace=True)
                df.to_csv(self.monthKDataFilePath,index=0,float_format=FLOAT_FORMAT)
                print("[Function: %s]: file:%s has been download successfully!" % (self.getKDataAsCSV.__name__,self.monthKDataFilePath))
            else:
                print("[Function: %s]: file:%s has been download before, no need to do it again!" % (self.getKDataAsCSV.__name__,self.monthKDataFilePath))
        elif(k==HOUR):
            if(os.path.exists(self.hourKDataFilePath) == False):
                df = ts.get_k_data(self.stockCode, start=startDate, ktype=k)
                # delete last column 'code'
                df.drop('code', axis=1,inplace=True)
                df.to_csv(self.hourKDataFilePath,index=0,float_format=FLOAT_FORMAT)
                print("[Function: %s]: file::%s has been download successfully!" % (self.getKDataAsCSV.__name__,self.hourKDataFilePath))
            else:
                print("[Function: %s]: file:%s has been download before, no need to do it again!" % (self.getKDataAsCSV.__name__,self.hourKDataFilePath))
        else:
            print("[Function: %s line:%d]: parameter for k:%s is not support now!" % (self.getKDataAsCSV.__name__,sys._getframe().f_lineno,k))
            sys.exit()

    def updateAllKData(self):
        self.updateKData(DAY)
        self.updateKData(WEEK)
        self.updateKData(MONTH)
#         self.updateKData(HOUR)
                
    def updateKData(self,k=None):
        if(k is not None):
            if(k==DAY):
                self.doUpdate(self.dayKDataFilePath,k)
            elif(k== WEEK):
                self.doUpdate(self.weekKDataFilePath,k)
            elif(k==MONTH):
                self.doUpdate(self.monthKDataFilePath,k)
            elif(HOUR):
                self.doUpdate(self.hourKDataFilePath,k)
            else:
                print("[Function: %s] parameter k:%s is not support now!" % (self.updateKData.__name__, k))
                sys.exit()
        else:
            print("[Function: %s] start and k should not be empty!" % (self.updateKData.__name__))
            sys.exit()
            
    def doUpdate(self,filePath,k):
        if(filePath is None or k is None):
            print("[Function:%s line:%d]: Parameter should not be None!" % (self.doUpdate.__name__,sys._getframe().f_lineno))
            sys.exit()
        
        logStr = 'update_' + k + '_k_data'
        
        if(os.path.exists(filePath) == True):
            df = pd.read_csv(filePath)
            lastDateStr = ''
            if(not df.empty):
                lastDateStr = df['date'].values[-1]
                df.drop(df.index[[-1]],inplace=True)
                dfLatest = ts.get_k_data(self.stockCode, start=lastDateStr, ktype=k)
                self.lenUpdated = len(dfLatest)
                # delete last column 'code'
                dfLatest.drop('code', axis=1,inplace=True)
                if(not dfLatest.empty):
                    newDf = pd.concat([df,dfLatest])
                    newDf.to_csv(filePath,index=0,float_format=FLOAT_FORMAT)
                    print("[Function: %s]: %s: Data of %s has been updated successfully " % (self.doUpdate.__name__, logStr, str(self.stockCode)))
                else:
                    print("[Function: %s]: %s: No record need to be updated for stock %s !" % (self.doUpdate.__name__, logStr, str(self.stockCode)))
            else:
                print("[Function:%s line:%d]: No record can be found in the data file!" % (self.doUpdate.__name__,sys._getframe().f_lineno,filePath))
                sys.exit()
    
        else:
            print("[Function: %s line:%d]: %s: file %s is not exists!" % (self.doUpdate.__name__, sys._getframe().f_lineno, logStr, filePath))
            sys.exit()

     