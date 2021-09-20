
## Import some necessary modules first
import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import requests
import json
import datetime
import time


## Retrieve data from online APIs
## retrieve data from online APIs of nasdaq into json

price_data = requests.get('https://data.nasdaq.com/api/v3/datasets/BCHAIN/MKPRU?start_date=2012-01-01&end_date=2021-03-31&api_key=xS4DStdnRCmiCXY6L3W2')
price_data = price_data.json()
trade_volume_data = requests.get('https://data.nasdaq.com/api/v3/datasets/BCHAIN/TRVOU.json?start_date=2012-01-01&end_date=2021-03-31&api_key=xS4DStdnRCmiCXY6L3W2')
trade_volume_data = trade_volume_data.json()

## Import a CSV file into a Pandas DataFrame
## import the csv which is download from kaggele
bitstamp=pd.read_csv('../../Downloads/bitstampUSD_1-min_data_2012-01-01_to_2021-03-31.csv')
print(bitstamp)

## Use dictionary function
## to see the keys of this dataset
print(price_data.keys())
## to see the data structure in details
print(price_data['dataset'])


## Define a custom function to create reusable code
## define the function named preprocessing to extract data into a DataFrame and translate datetime
def preprocessing(rawdata):
    data=rawdata['dataset']['data']
    data=pd.DataFrame(data,columns=['date','value'])
    data['date']=data['date'].apply(pd.to_datetime,format='%Y-%m-%d')
    return(data)

## Use the function on the datasets
price=preprocessing(price_data)
print(price)
trade_volume=preprocessing(trade_volume_data)
print(trade_volume)


## Merge two DataFrames
data=pd.concat([price,trade_volume['value']],axis=1)

## Rename columns
data.columns=['date','price(USD)','trade_volume(UTC)']
print(data)


## Visualisation

## The first chart:
## Plot them and show the tendency
## Set the figsize,lable,subplot and legend
plt.figure(figsize=(7,5))
plt.subplot(211)
plt.plot(data['date'],data['price(USD)'],'b',label="price(USD)")
plt.grid(True)
plt.axis('tight')
plt.xlabel("Time")
plt.ylabel('Values of price(USD)')
plt.title("The price and trade volume data from 2012 to 2021")
plt.legend(loc=0)
plt.subplot(212)
plt.plot(data['date'],data['trade_volume(UTC)'],'g',label="trade volume(UTC)")
plt.ylabel("Values of trade volume(UTC)")
plt.legend(loc=0)
plt.show()

## Insight 1

## It can be seen that Bitcoin has seen large price fluctuations and changes in trading volume in around 2018 and 2021. Based on these observations, further study the data changes after 2017
## Extract the data since 2017
since2017 = data[(data['date'] >=pd.to_datetime('20170601')) & (data['date'] <= pd.to_datetime('20210601'))]
## Set the date as the index
since2017 =since2017.set_index('date',drop=True)
print(since2017)

## The second chart:
## Plot the price and trade volume since 2017 in one graph and see the comparison
## Set the figsize,lable,subplot and legend
fig,ax1 = plt.subplots(figsize=(12,8))
plt.plot(since2017['price(USD)'],'r',label="price(USD)")
plt.grid(True)
plt.axis('tight')
plt.xlabel("Time")
plt.ylabel('Values of price(USD)')
plt.title("bitcon data since 2017-06")
plt.legend(loc=0)
ax2=ax1.twinx()
plt.plot(since2017['trade_volume(UTC)'],'b',label="trade volume(UTC)")
plt.ylabel("Values of trade volume(UTC)")
plt.legend(loc=1)
plt.show()

## Using Group and Numpy function
## View monthly trading volume
data['date'] = data['date'].map(lambda x: 100*x.year + x.month)
trade=since2017.drop(['price(USD)'],axis=1)

## Summarize data group by month
month_trade=trade.groupby("date").sum()
## Find the month with the top 10 highest trade volume
month_trade.sort_values('trade_volume(UTC)',axis=0,ascending=False,inplace=False,kind='quicksort',na_position='last').head(10)
print(month_trade)

## Loop
## Find the number of days greater than the average trading volume by looping
avg=sum(month_trade['trade_volume(UTC)'])/len(month_trade['trade_volume(UTC)'])
number=0
for i in range(0,len(month_trade['trade_volume(UTC)'])):
    if month_trade['trade_volume(UTC)'][i]>avg:
        number=number+1
print(number)

## It can be seen that the highest trade volume occurs between 2017 and 2018, and between 2020 and 2021. So this monthly trading volume value also confirms that the third insight is correct
## In order to verify the accuracy of this speculation, the relevant data of Bitcoin was downloaded from another website.
## Compare the data from kaggle with the data from Nasdaq
## First,drop turn the unixtime into datetime
bitstamp['Timestamp'] = pd.to_datetime(bitstamp['Timestamp'],unit='s')
## Count number of missing value in each columnﬁ
missing_value = bitstamp.isnull().sum()
print (missing_value)
## Drop missing values
bitstamp= bitstamp.dropna()
print(bitstamp.head())
print(bitstamp.shape)
print(bitstamp.info())

## Extract the data since 2017
another_since2017 = bitstamp[(bitstamp['Timestamp'] >=pd.to_datetime('20170601')) & (bitstamp['Timestamp'] <= pd.to_datetime('20210601'))]
print(another_since2017)

## The third chart:
## Plot the Bitcoin price from the first dataset and the second dataset to make a comparison
fig,ax1 = plt.subplots(figsize=(20,8))
plt.plot(another_since2017['Timestamp'],another_since2017['Weighted_Price'],color='yellow',lw=1,label="2nd price")
plt.grid(True)
plt.axis('tight')
plt.xlabel("Time")
plt.ylabel('Values of price(USD)')
plt.title("the comparison of two datasets")
plt.legend(loc=0)
ax2=ax1.twinx()
plt.plot(since2017['price(USD)'],'r*',lw=0.5,label='1st price')
plt.ylabel("Values of trade volume(UTC)")
plt.legend(loc=1)
plt.show()


## It can be seen that the curve and its tendency are similar enough.
## Since the data set records the opening and closing data in detail, the fluctuations of each hour can be analyzed
## Set the new index
another_since2017 =another_since2017.set_index('Timestamp',drop=True)

## Calculate the hourly change value since 2017 (that is, the price rise and fall)
ChangeValue=another_since2017['Open']-another_since2017['Close']
ChangeValue.columns=['value']
print(ChangeValue)


## Sort the hourly fluctuations
print(ChangeValue.sort_values())

## The above values show that February 22, 2021 and February 23, 2021 accounted for the largest price increase and price decline respectively.
## Extract the data of these two days
UnstableDay = bitstamp[(bitstamp['Timestamp'] >=pd.to_datetime('20210222')) & (bitstamp['Timestamp'] <= pd.to_datetime('20210223'))]
print(UnstableDay)

## The forth chart:
## Plot the price change
plt.figure(figsize=(7,5))
plt.plot(UnstableDay['Timestamp'],UnstableDay['Close'])


## Sum the fallen time
print(sum(ChangeValue<0))
## Sum the rise time
print(sum(ChangeValue>0))


## The End
