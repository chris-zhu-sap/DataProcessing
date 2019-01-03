'''
Created on Dec 12, 2017

@author: Administrator
'''

import numpy as np
import pandas as pd
from datetime import datetime
from sklearn import cluster, covariance, manifold
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection

class StockPlotHs300(object):
    def __init__(self,csvFile):
        self.df =  pd.read_csv(csvFile, usecols=['日期','今开','今收','最高','最低','成交量'],encoding = 'utf-8')
        self.df = self.df[self.df['今开'] != '--']
        
        
if __name__ == '__main__':
    StockPloter = StockPlotHs300('000300_2017_12_12_day.csv')
    print(StockPloter.df)