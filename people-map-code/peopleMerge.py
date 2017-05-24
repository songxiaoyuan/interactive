# coding=utf-8
import os,sys
reload(sys)
sys.setdefaultencoding('utf-8')
import pymongo
import csv
from pyltp import SentenceSplitter
from pyltp import Segmentor
from pyltp import Postagger
from pyltp import SementicRoleLabeller
from pyltp import NamedEntityRecognizer
from pyltp import Parser
import re
import uuid
conn=pymongo.MongoClient('192.168.1.34',27017)
qqdb=conn.qqperson1
hexundb=conn.hexunperson


# 初始化所有的模型
segmentor = Segmentor()
# segmentor.load('../../../test/ltp_data/cws.model')
segmentor.load_with_lexicon('../ltp/cws.model','../dict/meeting.txt')
postagger = Postagger()
postagger.load('../ltp/pos.model')
recognizer = NamedEntityRecognizer()
recognizer.load('../ltp/ner.model')
parser = Parser()
parser.load('../ltp/parser.model')
labeller = SementicRoleLabeller()
labeller.load('../ltp/srl')


def getOrgFromIntroduction(introduction):
	ret = set()
	content = introduction.encode('utf-8')
	# print content
	contents = re.split(r"。|！|？|；|\n",content)
	for sentence in contents:
		# print sentence
		sentence = sentence.replace(' ','')
		# print len(sentence)
		if len(sentence) <10:
			continue
		words = segmentor.segment(sentence)  #分词
		postags = postagger.postag(words)  #词性标注
		netags = recognizer.recognize(words, postags)  #实体标注
		count = 0
		oragnizeIndex = []
		tmp = []
		for tag in netags:
		    if tag == 'S-Ni':
		        oragnizeIndex.append([count])
		    elif tag == 'B-Ni':
		        tmp.append(count)
		    elif tag == 'I-Ni':
		        tmp.append(count)
		    elif tag == 'E-Ni':
		        tmp.append(count)
		        oragnizeIndex.append(tmp)
		        tmp = []
		    else:
		        pass
		    count += 1

		for eachIndex in oragnizeIndex:
		    og = ''
		    for eachOragnize in eachIndex:
		        og += words[int(eachOragnize)]
		    ret.add(og)
	return ret


def personMerge():
	# 首先通过和讯人物的名字，在腾讯证券的数据中查找有相应名字的人
	count = 0
	for item in hexundb.person.find():
		try:
			name = item[u'姓名']
			hasFindSame = False
			if name != "nbsp;":
				find = qqdb.peopleBasicInfo.find({"name":name})
				for line in find:
					same = isSamePerson(item,line)
					if same:
						# print "this is the same person"
						hasFindSame = True
						mergeHexunAndQQPerson(item,line)
				if hasFindSame ==False:
					# count +=1
					# print count
					print "this has not the same person"
					moveHexunToQQ(item)

		except Exception as e:
			continue

def mergeHexunAndQQPerson(hexunObj,qqObj):
	# 首先是更新ｂａｓｉｃ基本信息，然后是插入新的employment education信息。
	personid = qqObj['id']

	if len(qqObj['birthday'].replace('-','')) ==0:
		try:
		    qqObj['birthday']=hexunObj[u'出生日期']
		except:
			try:
				qqObj['birthday']=hexunObj[u'出生年月']
			except Exception as e:
				qqObj['birthday']=''
	if len(qqObj['educationbackground'].replace('-','')) ==0:
		try:
		    qqObj['educationbackground']=hexunObj[u'最高学历']
		except:
		    qqObj['educationbackground']=''

	if len(qqObj['nationality'].replace('-','')) ==0:
		try:
		    qqObj['nationality']=hexunObj[u'国籍']
		except:
		    qqObj['nationality']=''
	try:
	    qqObj['introduction'] +=hexunObj['description']
	except:
	    pass
	try:
	    qqObj['hobby']=hexunObj[u'爱好']
	except:
	    qqObj['hobby']=''
	try:
	    qqObj['industry']=hexunObj[u'所属行业']
	except:
		try:
			qqObj['industry']=hexunObj[u'所在行业']
		except Exception as e:
			qqObj['industry']=''

	# 插入到ｍｏｎｇｏｄｂ数据库中
	qqdb.peopleBasicInfo.update({"id":qqObj['id']},qqObj)

	insertHexunEmploymentToQQ(hexunObj,personid)
	insertHexunEducationToQQ(hexunObj,personid)

def insertHexunEmploymentToQQ(hexunObj,personid):
	try:
	    employments=hexunObj[u'工作经历']
	    for line in employments:
        	employment = {}
            employment['id']=personid
            try:
                employment['companyname']=line[1]
            except:
                employment['companyname']=''
            try:
                employment['job']= line[2]
            except:
                employment['job']=''
            try:
                employment['term']=line[0]
            except:
                employment['term']=''
            try:
                employment['paidornot']=''
            except:
                employment['paidornot']=''
            try:
                employment['description']= line[3]
            except:
                employment['description']=''
            # for item in employment:
            #     print item,employment[item]
            # 插入数据到mongo，每一条插入一次。
            qqdb.employmentInfo.insert(employment)
	except:
		try:
			print hexunObj[u"工作单位"]
		except Exception as e:
			pass
		# print "this has no 工作经历 info "

def insertHexunEducationToQQ(hexunObj,personid):
	try:
	    educations = hexunObj[u'教育经历']
	    for line in educations:
        	education = {}
            try:
                education['id']= personid
            except:
                education['id']=''
            try:
                education['time']= line[0]
            except:
                education['time']=''
            try:
                education['school']= line[1]
            except:
                education['school']=''
            try:
                education['professional']=line[2]
            except:
                education['professional']=''
            try:
                education['educationbackground']= line[3]
            except:
                education['educationbackground']=''

            # for item in education:
            #     print item,education[item]
            qqdb.educationInfo.insert(education)
	except:
		try:
			print hexunObj[u"毕业院校"]
		except Exception as e:
			pass
		# print "this has no 教育经历 info "


def moveHexunToQQ(hexunObj):
	personid = str(uuid.uuid1()).replace('-','')
	# 因为现在是一个新的数据，就是腾讯没有数据，所以要将现在的数据，重新插入。
	peoplebasicinfo={}
	try:
	    peoplebasicinfo['name']=hexunObj[u'姓名']
	except:
	    peoplebasicinfo['name']=''
	try:
	    peoplebasicinfo['gender']=hexunObj[u'性别']
	except:
	    peoplebasicinfo['gender']=''
	try:
	    peoplebasicinfo['birthday']=hexunObj[u'出生日期']
	except:
		try:
			peoplebasicinfo['birthday']=hexunObj[u'出生年月']
		except Exception as e:
			peoplebasicinfo['birthday']=''
	try:
	    peoplebasicinfo['educationbackground']=hexunObj[u'最高学历']
	except:
	    peoplebasicinfo['educationbackground']=''
	try:
	    peoplebasicinfo['nationality']=hexunObj[u'国籍']
	except:
	    peoplebasicinfo['nationality']=''
	try:
	    peoplebasicinfo['introduction']=hexunObj['description']
	except:
	    peoplebasicinfo['introduction']=''
	try:
	    peoplebasicinfo['hobby']=hexunObj[u'爱好']
	except:
	    peoplebasicinfo['hobby']=''
	try:
	    peoplebasicinfo['industry']=hexunObj[u'所属行业']
	except:
		try:
			peoplebasicinfo['industry']=hexunObj[u'所在行业']
		except Exception as e:
			peoplebasicinfo['industry']=''
	try:
	    peoplebasicinfo['id']= personid
	except:
	    peoplebasicinfo['id']=''

	# 插入到ｍｏｎｇｏｄｂ数据库中
	qqdb.peopleBasicInfo.insert(peoplebasicinfo)

	insertHexunEmploymentToQQ(hexunObj,personid)
	insertHexunEducationToQQ(hexunObj,personid)

		
def isSamePerson(hexunObj,qqObj):
	# 判断两个网站的人物是不是一个，因为是通过姓名找的，所以两者姓名肯定是一样的。然后判断性别，出生年份。
	# 最后通过工作经经历来判断,主要找工作过的公司和简历中的公司的词语的相似度。
	try:
		gender = hexunObj[u"性别"]
		gender2 = qqObj['gender']
		if gender != gender2 and len(gender2.replace('-','')) != 0:
			return False
	except Exception as e:
		pass
	try:
		try:
			birthday = hexunObj[u"出生日期"][0:4]
		except Exception as e:
			try:
				birthday = hexunObj[u"出生年月"][0:4]
			except Exception as e:
				pass
		birthday2 = qqObj['birthday'][0:4]
		if birthday != birthday2 and len(birthday2.replace('-','')) != 0:
			return False
	except Exception as e:
		pass
	try:
		work = hexunObj[u'工作经历']
	except Exception as e:
		# print "catch the segmentor!!!!!"
		work = []
	companys = set()
	for line in work:
		companys.add(line[1])
	hexunset = getOrgFromIntroduction(hexunObj['description'])
	# hexunset =companys
	hexunset = hexunset | companys
	find = qqdb.employmentInfo.find({"id":qqObj['id']})
	companys2 = set()
	for line in find:
		companys2.add(line['companyname'])
	qqset = getOrgFromIntroduction(qqObj['introduction'])
	# qqset = companys2
	qqset = qqset | companys2
	# print "hexun : ",','.join(hexunset)
	# print "qq : ",','.join(qqset)
	hexunset = ','.join(hexunset).encode('utf-8')
	qqset = ','.join(qqset).encode('utf-8')
	hexunWords = set(segmentor.segment(hexunset))
	qqWords =set(segmentor.segment(qqset))
	jiaoji = hexunWords & qqWords
	bingji = hexunWords | qqWords
	# print "hexun : ",','.join(hexunWords)
	# print "qq : ",','.join(qqWords)
	if len(bingji) ==0:
		return False
	num = float(len(jiaoji))/len(bingji)
	# print "the num is  : ", float(len(jiaoji))/len(bingji)
	# 阈值现在设定为０．１５
	if num>=0.15:
		# print "hexun : ",hexunset
		# print "qq: ",qqset
		# print num
		return True
	else:
		if len(qqWords)*len(hexunWords) ==0:
			return False
		num2 = float(len(jiaoji))/len(qqWords)
		num3 = float(len(jiaoji))/len(hexunWords)
		if num2 >=0.6 or num3 >=0.6:
			return True
		# print "hexun : ",hexunset
		# print "qq: ",qqset
		# print num
		# print num2
		# print num3
		return False


if __name__ == '__main__':
	# personMerge()
	# hexunset = getOrgFromIntroduction(" 丛龙云，2003年出任万达期货有限公司总经理。丛龙云宗长一直热心于宗族事业，先后多次拜谒丛氏大宗祠，为家族事业的发展做出贡献。在第二届世界丛氏恳亲大会上，丛龙云宗长被推选为世界丛氏宗亲联合会副会长。                         ")
	# hexunWords = segmentor.segment("桂洲畜产品企业(集团)公司,格兰仕,顺德桂洲羽绒厂")
	# for item in hexunWords:
	# 	print item
	# tmp  = set(hexunset)
	# for item in tmp:
	# 	print item
	# a = [1,2]
	# b =a[0:4]
	# print b
	find = qqdb.peopleBasicInfo.find()
	for line in find:
		print line["introduction"]