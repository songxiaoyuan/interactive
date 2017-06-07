import pandas as pd
def calema(emaperiod,icfilepath):

	stock_data = pd.read_csv(icfilepath, parse_dates=[1])
	ma_list = [5, 20, 60]	
	stock_data['MA_' + str(emaperiod)] = pd.rolling_mean(stock_data['theo'], emaperiod)	
	stock_data['EMA_' + str(emaperiod)] = pd.ewma(stock_data['theo'], span=emaperiod)
	stock_data.to_csv(icfilepath, index=False)

if __name__=='__main__': 
	# filepath='D:\\data\\zdata\\'
	# date='0419'
	# icfilepath=filepath+'2017'+date+'_ic.csv'
	# iffilepath=filepath+'2017'+date+'_if.txt'
	# ihfilepath=filepath+'2017'+date+'_ih.csv'
	#calema(200,icfilepath)
	# calema(200,ihfilepath)
	data = [1,2,3,4,5,6,7,8,9,10]
	data = pd.Series(data)
	ema = pd.ewma(data, span=5)
	print ema
	ma = pd.rolling_mean(data, 5)
	print ma
