# coding: utf-8
import pandas as pd
import csv


def calEma(path,emaperiod):
	stock_data = pd.read_csv(path)
	csvHeader = ["putLatPrice","putMiddPoint","putWeightActive","callLatPrice","callMiddPoint","callWeightActive"]
	for volum in csvHeader:
		stock_data['EMA_' + volum] = pd.ewma(stock_data[volum], span=emaperiod)
	stock_data.to_csv(path, index=False)

# 此函数主要就是根据现在的文件，然后计算，最后进行返回给想要的数据。putAsk ,putBid,theo1,the2,the3
def writeTheOrderData():
	csvFile = file("./put_call_data.csv","rb")
	head = 0
	csvWriteFile = file("./getData.csv","w")
	writer = csv.writer(csvWriteFile)
	for line in csv.reader(csvFile):
		# import pdb
		# pdb.set_trace()
		if head==0:
			head+=1
			continue
		for i in range(0,len(line)):
			line[i] = float(line[i])
		putBid = line[0]
		putAsk=line[1]
		putLatPrice = line[7]*(line[10]/line[13])
		putMiddPoint = line[8]*(line[11]/line[14])
		putWeightActive = line[9]*(line[12]/line[15])
		tmp = [putBid,putAsk,putLatPrice,putMiddPoint,putWeightActive]
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
	WeightActive = round((ask*bidNum+bid*askNum)/(askNum+bidNum),2)
	tmp = [bid,ask,lastPrice,MiddPoint,WeightActive]
	return tmp

def caculateTheLineDaoshu(line):
	# 根据传入的数据，进行计算，返回想要的ｂｉｄ,ask ,lastPrice,MiddPoint,WeightActive
	lastPrice = round(1/float(line[4]),2)
	bid = float(line[22])
	bidNum = float(line[23])
	ask = float(line[24])
	askNum = float(line[25])
	MiddPoint = round(2/(bid+ask),2)
	# 这个主要是用来保留２位小数。
	WeightActive = round((askNum+bidNum)/(ask*bidNum+bid*askNum),2)
	tmp = [bid,ask,lastPrice,MiddPoint,WeightActive]
	return tmp


def getCsvFileData():
	# 将ｃａｌｌ和ｐｕｔ的两个数据读入到内存中，然后等待处理可以先写入ｃｓｖ中，然后在读。
	csvHeader = ["putBid","putAsk","putLatPrice","putMiddPoint","putWeightActive","callBid","callAsk","callLatPrice","callMiddPoint","callWeightActive"]
	csvFile = file("./put_call_data.csv","w")
	writer = csv.writer(csvFile)
	writer.writerow(csvHeader)
	# 开始读取文件，然后将读取的数据插入到新的ｃｓｖ文件中。
	insertData = []
	fileput = file('./cleanData_20170522_m1709P2750.csv','rb')
	readerput = csv.reader(fileput)
	filecall = file('./cleanData_20170522_m1709C2750.csv','rb')
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


if __name__ == '__main__':
	# calEma()
	# getCsvFileData()
	# calEma("./put_call_data.csv",200)
	writeTheOrderData()
