import StockData as sd
import DataProcess as dp

# dataDate = '2019-01-07'
# sdate = sd.SingletonDate.GetInstance('2019-01-09')
sdate = sd.SingletonDate.GetInstance()
# zz500StockListFileUrl  = 'http://www.csindex.com.cn/uploads/file/autofile/cons/000905cons.xls'
# hs300StockListFileUrl  = 'http://www.csindex.com.cn/uploads/file/autofile/cons/000300cons.xls'
# indexCode, filename= sd.getNameAndCode(hs300StockListFileUrl)
# hs300CodeList = sd.getChinaStockList(hs300StockListFileUrl, filename)
# print('The length of hs300CodeList is: %d!' % (len(hs300CodeList)))
#   
# indexCode, filename= sd.getNameAndCode(zz500StockListFileUrl)
# zz500CodeList = sd.getChinaStockList(zz500StockListFileUrl, filename)
# print('The length of zz500CodeList is: %d' % (len(zz500CodeList)))
#  
myCodeList = ['600030','600036','600061','600893','600498','300033','600547','300383','002716','600109','002353','300059']
# myCodeList = ['300674']
print('The length of myCodeList is: %d' % (len(myCodeList)))
#   
# codeList = hs300CodeList + zz500CodeList + myCodeList
# codeList = list(set(codeList))
codeList = list(set(myCodeList))
print('The length of codeList is: %d' % len(codeList))


########################################################################
# sd.downloadStockDataAsCSV(codeList)
# hs300CodeList = sd.getChinaStockList(hs300StockListFileUrl, filename)
# sd.updateStockDataForList(hs300CodeList,updateDir=True)
# sd.updateStockDataForList(codeList)
# wanke = sd.StockData('000002')
# wanke.updateAllKData()
# wanke.updateKData(sd.DAY)
# wanke.updateKData(sd.WEEK)
# wanke.updateKData(sd.MONTH)
# wanke.getKDataAsCSV('D', startDate='1991-01-29')
# wanke.getKDataAsCSV('W', startDate='1991-01-29')
# wanke.getKDataAsCSV('M', startDate='1991-01-29')
# print("finish get HS300 data")


######################## test for  DataProcess #########################
# wanke = dp.DataProcess('000002',dataDate,'D')
# wanke.makeGenData()
# wanke.saveAsGeneratedData()
# codeList = ['600030','600036','600061','600893','600498','300033','600547','300383','002716','600109','002353','300059']
print("######################## Begin of test for DataProcess!#########################")
sd.downloadStockDataAsCSV(codeList)
sd.updateStockDataForList(codeList)
dp.generateMoreDataForAllStocks(codeList)
dp.updateGeneratedDataForAllStocks(codeList)
dp.getCurrentTradeReport(codeList)
print("######################## End of test for DataProcess!#########################")