# coding=utf-8
import os,sys
reload(sys)
sys.setdefaultencoding('utf-8')
import pymongo
import csv
conn=pymongo.MongoClient('192.168.1.34',27017)
# db=conn.qqperson
db=conn.hexunperson


def moveListedCompanyToCsv():
	csvfile = file('listedcompanyemployment.csv', 'wb')
	writer = csv.writer(csvfile)
	writer.writerow(['id', 'job', 'companycode','jobbegin','jobend','leavereason'])
	for item in db.listedcompanyemployment.find():
		data = []
		data.append(item['id'])
		data.append(item['job'])
		data.append(item['companycode'])
		data.append(item['jobbegin'])
		data.append(item['jobend'])
		data.append(item['leavereason'])
		writer.writerow(data)
	csvfile.close()

def moveEmploymentToCsv():
	csvfile = file('employment.csv', 'wb')
	writer = csv.writer(csvfile)
	writer.writerow(['id', 'job', 'companyname','term','description','paidornot'])
	for item in db.employment.find():
		data = []
		data.append(item['id'])
		data.append(item['job'])
		data.append(item['companyname'])
		data.append(item['term'])
		data.append(item['description'])
		data.append(item['paidornot'])
		writer.writerow(data)
	csvfile.close()


def moveShareholdinginfoToCsv():
	csvfile = file('Shareholdinginfo.csv', 'wb')
	writer = csv.writer(csvfile)
	writer.writerow(['id','companyname','holdingnum','changereason','deadline'])
	for item in db.shareholdinginfo.find():
		data = []
		data.append(item['id'])
		data.append(item['companyname'])
		data.append(item['holdingnum'])
		data.append(item['changereason'])
		data.append(item['deadline'])
		writer.writerow(data)
	csvfile.close()

def movePeopleBasicinfoToCsv():
	csvfile = file('PeopleBasicinfo.csv', 'wb')
	writer = csv.writer(csvfile)
	writer.writerow(['id','name','gender','birthday','nationality','educationbackground','introduction'])
	for item in db.peoplebasicinfo.find():
		# print item
		data = []
		data.append(item['id'])
		data.append(item['name'])
		data.append(item['gender'])
		data.append(item['birthday'])
		data.append(item['nationality'])
		data.append(item['educationbackground'])
		data.append(item['introduction'])
		writer.writerow(data)
	csvfile.close()


def moveHexunPersonBasicInfoToCsv():
	csvfile = file('HexunPersonBasicInfo.csv', 'wb')
	writer = csv.writer(csvfile)
	writer.writerow(['姓名','性别','出生日期','民族','国籍','籍贯','最高学历','所属行业','职务','毕业院校'
		,'绰号','逝世日期','所学专业','英文名','政治面貌','工作单位','爱好'])
	for item in db.person.find():
		data = []
		try:
			data.append(item[u'姓名'])
		except Exception as e:
			data.append('')
		try:
			data.append(item[u'性别'])
		except Exception as e:
			data.append('')
		try:
			data.append(item[u'出生日期'])
		except Exception as e:
			try:
				data.append(item[u'出生年月'])
			except Exception as e:
				data.append('')
		try:
			data.append(item[u'民族'])
		except Exception as e:
			data.append('')
		try:
			data.append(item[u'国籍'])
		except Exception as e:
			data.append('')
		try:
			data.append(item[u'最高学历'])
		except Exception as e:
			data.append('')
		try:
			data.append(item[u'所属行业'])
		except Exception as e:
			try:
				data.append(item[u'所在行业'])
			except Exception as e:
				data.append('')
		try:
			data.append(item[u'职务'])
		except Exception as e:
			data.append('')
		try:
			data.append(item[u'毕业院校'])
		except Exception as e:
			data.append('')
		try:
			data.append(item[u'绰号'])
		except Exception as e:
			data.append('')
		try:
			data.append(item[u'逝世日期'])
		except Exception as e:
			data.append('')
		try:
			data.append(item[u'所学专业'])
		except Exception as e:
			data.append('')
		try:
			data.append(item[u'英文名'])
		except Exception as e:
			data.append('')
		try:
			data.append(item[u'政治面貌'])
		except Exception as e:
			data.append('')
		try:
			data.append(item[u'工作单位'])
		except Exception as e:
			data.append('')
		try:
			data.append(item[u'爱好'])
		except Exception as e:
			data.append('')

		writer.writerow(data)
	csvfile.close()


def moveHexunEducationToCsv():
 	csvfile = file('HexunEducation.csv', 'wb')
	writer = csv.writer(csvfile)
	writer.writerow(['姓名','时间','学校','专业','学历','描述'])
	for item in db.person.find():
		try:
			name = item[u'姓名']
		except Exception as e:
			continue
		
		try:
			data = item[u'教育经历']
			for line in data:
				tmp = [name] + line
				writer.writerow(tmp)
		except Exception as e:
			pass
	csvfile.close()

def moveHexunEmploymentToCsv():
 	csvfile = file('HexunEmployment.csv', 'wb')
	writer = csv.writer(csvfile)
	writer.writerow(['姓名','时间','公司名称','职务','描述'])
	for item in db.person.find():
		try:
			name = item[u'姓名']
		except Exception as e:
			continue
		
		try:
			data = item[u'工作经历']
			for line in data:
				tmp = [name] + line
				writer.writerow(tmp)
		except Exception as e:
			pass
	csvfile.close()



if __name__ == '__main__':
	# moveListedCompanyToCsv()
	# moveEmploymentToCsv()
	# moveShareholdinginfoToCsv()
	# movePeopleBasicinfoToCsv()
	# moveHexunPersonBasicInfoToCsv()
	# moveHexunEducationToCsv()
	moveHexunEmploymentToCsv()