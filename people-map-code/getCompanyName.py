#!/usr/bin/env python
# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import redis
import MySQLdb
import MySQLdb.cursors as cursors
import json
import uuid
import pymongo
import logging
conn=pymongo.MongoClient('192.168.1.34',27017)
qqdb=conn.qqperson1

#MYSQL 
# MYSQL_HOST = "10.25.113.169"
# MYSQL_HOST = "101.201.102.233"
# MYSQL_PORT = 3306
# MYSQL_DB =  'publicCompanyDataBase'
# MYSQL_CONN = MySQLdb.connect(host=MYSQL_HOST,user='root',passwd='fstxlab@2017!',charset= "UTF8",cursorclass=cursors.SSCursor)
# MYSQL_CONN.select_db('publicCompanyDataBase')
# MYSQL_CONN.set_character_set('utf8')
# MYSQL_CUR = MYSQL_CONN.cursor()

remove = ["有限","责任","公司","股份","集团","会计师","事务所"]

companySame =file("companySame1.txt","a+")

def read_data_from_mysql():
	sqlContent = 'select Company_code,Company_abbreviation,Company_name from publiccompany2'
	MYSQL_CUR.execute(sqlContent)
	#count =0
	file = open("./company_name.txt", 'w+')
	for row in MYSQL_CUR:
		company_code = row[0]
		company_abbreviation = row[1].strip().replace(" ",'')
		company_name = row[2].strip().replace(" ",'')
		#count +=1
		#print count
		file.write(company_code+' ' + company_abbreviation+' '+company_name+'\n')

def loadCompanyCodeNameDict():
	file = open('./company_name.txt','r')
	ret =dict()
	for line in file:
		line = line.split(' ')
		companycode = line[0].strip()
		companyname = line[2].strip()
		ret[companycode] = companyname
	return ret

def isSameCompany(companyname1old,companyname2old):
	companyname1 =companyname1old.decode()
	companyname2 = companyname2old.decode()
	for tmp in remove:
		tmp = tmp.decode()
		companyname1=companyname1.replace(tmp,"")
		companyname2=companyname2.replace(tmp,"")
	if companyname1==companyname2 or companyname1 in companyname2 or companyname2 in companyname1:
		return True
	if len(companyname1) > len(companyname2):
		return isSameCompany(companyname2,companyname1)
	# companyname1 is small than companyname2
	# print companyname1
	# print companyname2
	count =0
	for char in companyname1:
		# print char
		if char in companyname2:
			# print char
			count+=1
	num = float(count)/len(companyname1)
	# print count
	# print num
	# print len(companyname1)
	# if num >0.85:
	# 	# print num
	# 	companySame.write(companyname1old+' '+companyname2old + ' ' + str(num) + '\n')
	if num>0.85:
		return True
	return False


def get_company_person():
	# 主要记录的是每一个公司的工作的员工。值是相应的公司员工的集合。
	code_name = loadCompanyCodeNameDict()
	print "start get total company"
	companyDict = dict()
	for line in qqdb.listedCompanyEmployment.find():
		companyname = line['companycode']
		if companyname in companyDict:
			companyDict[companyname].add(line['id'])
		else:
			companyDict[companyname] =set()
			companyDict[companyname].add(line['id'])

	for item in companyDict:
		obj = {}
		if item in code_name:
			obj["companyname"] = code_name[item]
		else:
			obj["companyname"] = item
		obj["set"] = list(companyDict[item])
		qqdb.companyperson.insert(obj)


def create_total_company():
	# companyDict = set(['鞍钢股份有限公司','恒生证券有限公司','凤凰传奇影业有限公司','北京泽华化工工程有限公司','鞍钢股份'])
	companyDict =set()
	# for item in qqdb.companyperson.find(timeout=False):
	# 	companyname = item['companyname']
	# 	print companyname
	# 	companyDict.add(companyname)
	retDict = []
	for item in companyDict:
		find =False
		print "second: " +item
		for comSet in retDict:
			for comSetItem in comSet:
				same = isSameCompany(item,comSetItem)
				if same==True:
					comSet.add(item)
					find = True
					break
			# if find:
			# 	break
		if find ==False:
			tmpSet =set()
			tmpSet.add(item)
			retDict.append(tmpSet)
	for item in retDict:
		for com in item:
			com = com.strip()
			companySame.write(com +' ')
		companySame.write('\n')

def mongo_company_quchong():
	companySet =set()
	for item in qqdb.companyperson.find(timeout=False):
		companyname = item['companyname']
		companySet.add(companyname)
		# find = False
		# # print companyname
		# for com in companySet:
		# 	same = isSameCompany(com,companyname)
		# 	if same ==True:
		# 		find =True
		# 		print "this is same "+ companyname+' : '+com
		# 		break
		# if find ==False:
		# 	companySet.add(companyname)

	print len(companySet)



def clearCompany():
	fileNew = file("companySameNew.txt","w")
	for line in companySame:
		# line = line.strip()
		tmp = line.split(' ')
		# print tmp
		if len(tmp)>=2 and "鞍钢股份有限公司" in line:
			line = line.replace("鞍钢股份有限公司",'')
			# line = line.strip()
		fileNew.write(line)

def clearCompanyNew():
	fileNew = file("companySameNew.txt","r")
	fileNew2 = file("companySameNew2.txt","w")
	for line in fileNew:
		line = line.strip()
		if len(line) ==0:
			continue
		fileNew2.write(line+'\n')



if __name__ == '__main__':
	# read_data_from_mysql()
	# MYSQL_CUR.close()
	# MYSQL_CONN.close()
	# get_company_person()
	# tag = isSameCompany("万科Ａ","万科股份有限责任公司")
	# print tag
	# a = "万科股份有限责任公司".decode()
	# b = "无限".decode()
	# c= a.replace(b,"")
	# print c
	# create_total_company()
	# companyname1 = "鞍钢股份有限公司"
	# companyname2 = "深业有色金属有限公司"
	# tmp = isSameCompany(companyname1,companyname2)
	# print tmp
	# a = set()
	# a.add(1)
	# print a
	# b = []
	# b.append(a)
	# print b
	# for item in b:
	# 	tmp = item
	# 	tmp.add(2)
	# print b
	# clearCompany()
	# clearCompanyNew()
	mongo_company_quchong()








	companySame.close()

