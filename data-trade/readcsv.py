# coding: utf-8
#读取csv文件，读取近月合约，填充tick数据，对其合约
import csv
from FindFilelist import FindFilelist
def mistick(pre_time,pre_tick,now_time,now_tick):
	if pre_time==now_time and pre_tick==now_tick:
		return -10
		pass
	pre_timelist=pre_time.split(':')
	now_timelist=now_time.split(':')
	p_time=int(pre_timelist[0])*3600+int(pre_timelist[1])*60+int(pre_timelist[2])
	n_time=int(now_timelist[0])*3600+int(now_timelist[1])*60+int(now_timelist[2])
	tick=int(now_tick)-int(pre_tick)
	sec=n_time-p_time
	minticknum=sec*2-1+tick/500

	mor_time=11*3600+29*60+59
	after_time=13*3600+0*60+0
	morsec=mor_time-p_time
	afersec=after_time-n_time
	moafertick=(900-int(pre_tick))/500-(100-int(now_tick))/500
	moraferticknum=(morsec+afersec)*2-1+moafertick

	if now_time=='13:00:00' and pre_timelist[0]=='11':
		return moraferticknum
	else:
		return minticknum

def readcsv(path,month):
	csvfile = file(path, 'rb')
	reader = csv.reader(csvfile)
	pretime=''
	pretick=''
	prewriteline=''
	endtime=15*3600
	for line in reader:
		#line=line.split(',')
		writefilepath=''
		pathlist=((path.split('\\'))[2]).split('.')
		writeline=''
		timelist=line[20].split(':')
		phour=timelist[0]
		pmin=timelist[1]
		psec=timelist[2]
		nowtime=int(phour)*3600+int(pmin)*60+int(psec)
		if nowtime-endtime>0:
			break
			pass
		if line[1]==('IC'+month) or line[1]==('IF'+month) or line[1]==('IH'+month):
			theo=(float(line[22])*float(line[25])+float(line[23])*float(line[24]))/(float(line[23])+float(line[25]))
			if prewriteline=='':
				writeline=line[1]+', '+line[20]+', '+line[21]+', '+line[22]+', '+line[23]+', '+line[24]+', '+line[25]+','+str(theo)+"\n"
				pretime=line[20]
				pretick=line[21]
				prewriteline=writeline
			else:
				tick=mistick(pretime,pretick,line[20],line[21])
				if tick==-10:
					continue
					pass
				pretime=line[20]
				pretick=line[21]
				for x in xrange(0,tick):
					writeline=writeline+prewriteline
				writeline=writeline+line[1]+', '+line[20]+', '+line[21]+', '+line[22]+', '+line[23]+', '+line[24]+', '+line[25]+','+str(theo)+"\n"
				prewriteline=line[1]+', '+line[20]+', '+line[21]+', '+line[22]+', '+line[23]+', '+line[24]+', '+line[25]+','+str(theo)+"\n"
			writefilepath="D:\\data\\zdata\\"+pathlist[0]+".txt"
			f=open(writefilepath,'a')
			f.write(writeline)
			f.close()
		#print line

	csvfile.close()
#def calema():
	

if __name__=='__main__': 
	#filename_ic="D:\\data\\20170419_ic.csv"
	#filename_if="D:\\data\\20170419_if.csv"
	#filename_ih="D:\\data\\20170419_ih.csv"
	# for x in xrange(0,1):
	# 	myday=myday+100*x
	# 	evemonth(myday)
	# 	pass
	# 	
	filepath="D:\\data"
	filelist=FindFilelist(1,filepath)
	flist=filelist.printInstanceInfo(1,filepath)
	month='1705'
	for fl in flist:
		csvpath=filepath+'\\'+fl
		readcsv(csvpath,month)
