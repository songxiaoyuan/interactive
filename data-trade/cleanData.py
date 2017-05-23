# coding: utf-8
import csv
import os

# 根据毫秒的交易时间来去除数据，也就是说每一秒有２条数据
def cleanCsv(path):
	csvFile = file(path,'rb')
	reader = csv.reader(csvFile)
	writeToCsvData = []
	amBegin = 9*3600
	amEnd = 11*3600+30*60
	pmBegin = 13*3600+30*60
	pmEnd = 15*3600
	amRestBegin = 10*3600+15*60
	amRestEnd = 10*3600+30*60
	time =amBegin
	num=0
	preLine = [0]*44
	nowTime = 0
	for line in reader:
		# 获取到时间和ｔｉｃｋ的值
		timeLine = line[20].split(":")
		# tick = line[21]
		nowTime = int(timeLine[0])*3600+int(timeLine[1])*60+int(timeLine[2])
		# print nowTime
		# print time
		# import pdb
		# pdb.set_trace()
		if nowTime>pmEnd or time > pmEnd:
			# 表示过了这一天的交易时间，直接停止处理数据。
			break
		if nowTime==time:
			preLine = line
			if num ==0:
				# 如果ｎｕｍ是０，表示这个是此秒的第一个数据，直接插入就好了。
				writeToCsvData.append(line[:])
				num+=1
			elif num==1:
				# 表示此时是这个秒的第二个数据了，插入之后，开始进行下一秒的清洗。
				writeToCsvData.append(line[:])
				num = 0
				# 注意，每次跟ｔｉｍｅ的时候一定要注意间歇时间，千万不能出现非交易时间的ｔｉｍｅ
				if (time >=amBegin  and time<amRestBegin) or (time>=amRestEnd and time <amEnd) or (time>=pmBegin and time <pmEnd):
					# 表示是正常的交易时间，ｔｉｍｅ可以直接＋１
					time +=1
				elif time==amRestBegin:
					# 上午的休息时间到了
					time = amRestEnd
				elif time ==amEnd:
					# 上午已经结束了
					time = pmBegin
		elif nowTime > time:
			# 表示读取的时间比要处理的时间晚，说明此段时间中有空闲，应该插入空白的数据就好了。插入的数据是前一秒的数据。
			for i in range(time,nowTime):
				preLine[20] = convertNumToTime(i)
				if num ==0:
					# 如果此时ｎｕｍ为０，表示当前时间的数据一条都没有，那么直接插入两条数据，进入下一秒。
					writeToCsvData.append(preLine[:])
					writeToCsvData.append(preLine[:])
				elif num ==1:
					# 如果此时ｎｕｍ为１，表示当前秒的数据已经出现过１次了，那么直接在插入１次，然后进入下一秒。
					writeToCsvData.append(preLine[:])
				num = 0
			# 表示空白的时间已经插入了，那么开始插入现在读取的时间。
			# 现在读取的时间的此秒肯定是第一个，所以ｎｕｍ肯定是０，直接插入就好了，而且要修改ｔｉｍｅ值为当前读取的时间。
			time = nowTime
			writeToCsvData.append(line[:])
			num =1
			preLine = line
		elif nowTime < time:
			# 此时表示时间已经跟新了，这个是重复的数据，需要删除，不做处理。
			continue
	# 最后的读取时间，看看是不是１５：００，不是的话，后面还要添加数据
	for i in range(time,pmEnd+1):
		preLine[20] = convertNumToTime(i)
		if num ==0:
			# 如果此时ｎｕｍ为０，表示当前时间的数据一条都没有，那么直接插入两条数据，进入下一秒。
			writeToCsvData.append(preLine[:])
			writeToCsvData.append(preLine[:])
		elif num ==1:
			# 如果此时ｎｕｍ为１，表示当前秒的数据已经出现过１次了，那么直接在插入１次，然后进入下一秒。
			writeToCsvData.append(preLine[:])
		num = 0
	# 清理最后的不是交易时间额数据，为了保证安全，必须重新清晰一次，因为有的时候，时间会有非空闲数据。
	cleanData = []
	for line in writeToCsvData:
		timeLine = line[20].split(":")
		nowTime = int(timeLine[0])*3600+int(timeLine[1])*60+int(timeLine[2])
		# 只是获取到交易时间的数据。
		if (nowTime>=amBegin and nowTime<amRestBegin) or (nowTime>=amRestEnd and nowTime<amEnd) or(nowTime>=pmBegin and nowTime<=pmEnd):
			cleanData.append(line)

	csvFile.close()
	cleanDataPath = path.split('/')
	cleanPath = "./clean/"+"clean_"+cleanDataPath[2]
	print cleanPath
	writeToCsv(cleanData,cleanPath)

# 将一个二维数组放入到ｃｓｖ文件中。如果第一个数据是０　的话，自动找到第一个不是０　的，进行填充。
def writeToCsv(data,path):
	index =0
	insertLine =[]
	for line in data:
		index +=1
		if len(str(line[0]))>2:
			insertLine=line
	for i in range(0,index):
		data[i]=insertLine
	csvFile = file(path,'w')
	writer = csv.writer(csvFile)
	for line in data:
		writer.writerow(line)
	csvFile.close()

def convertNumToTime(num):
	hour = num/3600
	num = num - hour*3600
	minute = (num)/60
	sec = num%60
	ret = str(hour)+':'+str(minute)+':'+str(sec)
	return ret


if __name__ == '__main__':
	print "clean the data"
	# cleanCsv('./20170522_m1709P2750.csv')
	for root,dirs,files in os.walk("./option"):
		for name in files:
			tmp= os.path.join(root,name)
			cleanCsv(tmp)
