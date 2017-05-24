# coding: utf-8
import pandas as pd
import csv
import os


def calEma(path,emaperiod):
	stock_data = pd.read_csv(path)
	csvHeader = ["putLatPrice","putMiddPoint","putWeightActive","callLatPrice","callMiddPoint","callWeightActive"]
	for volum in csvHeader:
		stock_data['EMA_' + volum] = pd.ewma(stock_data[volum], span=emaperiod)
	stock_data.to_csv(path, index=False)

# 此函数主要就是根据现在的文件，然后计算，最后进行返回给想要的数据。putAsk ,putBid,theo1,the2,the3
def writeTheOrderData(path):
	csvFile = file("./put_call_data.csv","rb")
	head = 0
	csvWriteFile = file(path,"w")
	writer = csv.writer(csvWriteFile)
	index =1
	for line in csv.reader(csvFile):
		# import pdb
		# pdb.set_trace()
		if head==0:
			head+=1
			continue
		for i in range(0,len(line)):
			line[i] = float(line[i])
		# putBid = line[0]
		# putAsk=line[1]
		putLatPrice = line[2]
		# putLatPrice = line[7]*(line[10]/line[13])
		putMiddPoint = line[8]*(line[11]/line[14])
		putWeightActive = line[9]*(line[12]/line[15])
		tmp = [index,putLatPrice,putMiddPoint,putWeightActive]
		index +=1
		for i in range(0,len(tmp)):
			tmp[i] = round(tmp[i],2)
		writer.writerow(tmp)
	csvFile.close()
	csvWriteFile.close()


def caculateTheLine(line):
	# 根据传入的数据，进行计算，返回想要的ｂｉｄ,ask ,lastPrice,MiddPoint,WeightActive
	lastPrice = float(line[4])
	bid = float(line[22])
	bidNum = float(line[23])
	ask = float(line[24])
	askNum = float(line[25])
	MiddPoint = (bid+ask)/2
	# 这个主要是用来保留２位小数。
	WeightActive =(ask*bidNum+bid*askNum)/(askNum+bidNum)
	tmp = [bid,ask,lastPrice,MiddPoint,WeightActive]
	return tmp

def caculateTheLineDaoshu(line):
	# 根据传入的数据，进行计算，返回想要的ｂｉｄ,ask ,lastPrice,MiddPoint,WeightActive
	lastPrice = 1/float(line[4])
	bid = float(line[22])
	bidNum = float(line[23])
	ask = float(line[24])
	askNum = float(line[25])
	MiddPoint = 2/(bid+ask)
	# 这个主要是用来保留２位小数。
	WeightActive = (askNum+bidNum)/(ask*bidNum+bid*askNum)
	tmp = [bid,ask,lastPrice,MiddPoint,WeightActive]
	return tmp


def getCsvFileData(putPath,callPath):
	# 将ｃａｌｌ和ｐｕｔ的两个数据读入到内存中，然后等待处理可以先写入ｃｓｖ中，然后在读。
	csvHeader = ["putBid","putAsk","putLatPrice","putMiddPoint","putWeightActive","callBid","callAsk","callLatPrice","callMiddPoint","callWeightActive"]
	csvFile = file("./put_call_data.csv","w")
	writer = csv.writer(csvFile)
	writer.writerow(csvHeader)
	# 开始读取文件，然后将读取的数据插入到新的ｃｓｖ文件中。
	insertData = []
	# fileput = file('./cleanData_20170522_m1709P2750.csv','rb')
	fileput = file(putPath,'rb')
	readerput = csv.reader(fileput)
	# filecall = file('./cleanData_20170522_m1709C2750.csv','rb')
	filecall = file(callPath,'rb')
	readercall = csv.reader(filecall)
	for line in readerput:
		tmp = caculateTheLine(line)
		insertData.append(tmp)
	i=0
	for line in readercall:
		tmp = caculateTheLineDaoshu(line)
		put = insertData[i]
		insertData[i] = put+tmp
		i+=1
	for line in insertData:
		writer.writerow(line)
	csvFile.close()
	fileput.close()
	filecall.close()

	# 开始处理ＥＭＡ，生产相应的ＭＥＡ数据。
	calEma("./put_call_data.csv",200)



if __name__ == '__main__':
	# calEma()
	tmpSet = set()
	for root,dirs,files in os.walk("./clean"):
		for name in files:
			tmp= os.path.join(root,name)
			if 'P' in tmp:
				tmpSet.add(tmp)
				# cleanCsv(tmp)
	for item in tmpSet:
		putPath = item
		callPath = item.replace('P','C')
		print callPath
		getCsvFileData(putPath,callPath)
		getDataPath = "./getData/"+putPath.split('/')[2][5:]
		writeTheOrderData(getDataPath)
	# callPath="clean_20170515_m1709C2700.csv"
	# getDataPath = "./getData"+callPath[5:]
	# print getDataPath
