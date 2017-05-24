# coding=utf-8
import os,sys
reload(sys)
sys.setdefaultencoding('utf-8')
import urllib,urllib2
from lxml import etree
import pymongo
conn=pymongo.MongoClient('192.168.1.34',27017)
db=conn.qqpersonBackup
db2=conn.qqperson1
import uuid
import json


def get_html(url):
    req_header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        }
    req = urllib2.Request(url, None, req_header)
    resp = urllib2.urlopen(req, None)
    html = resp.read()
    return html

def get_urls():
    f=open("person_urls.txt",'w+')
    urlSet = set()
    for line in range(1,1000):
        url="http://search.10jqka.com.cn/yike/index-page-ajax/?p=%d&filterTag=18"%line
        html=urllib.urlopen(url).read()
        decoded = json.loads(html)
        tmp = decoded["list"]
        for item in tmp:
            # print item["URL"]
            personurl = 'http://search.10jqka.com.cn'+item["URL"]
            urlSet.add(personurl)
            print personurl
    for item in urlSet:
        f.write(item+'\n')
    f.close()


def get_person_html():
    count=0
    name = 0
    for line in open("person_urls.txt"):
        line = line.strip()
        name +=1
        if not os.path.exists("../../wencai/%d.html"%name):
            f = open("../../wencai/%d.html" % name, 'w+')
            try:
                content = get_html(line)
                f.write(content)
                f.close()
                count+=1
                print count,line
            except:
                print "the html is not Done !!!!!"


def person_html_parser():
    # 解析的是具体的一个人物页面的信息，找出三个表格，插入到ｍｏｎｇｏ数据库中。
    urlUUID = loadUrlIDDict()
    hasDone = set()
    for root,dirs,files in os.walk("../../person_html"):
        for file in files:
            print file,'start to exit!!!!!!'
            tmp = file.split("_")
            # companyId = tmp[0]
            personid = tmp[1].split('.')[0]
            if personid not in hasDone:
                hasDone.add(personid)
            else:
                continue
            # url = "http://stock.finance.qq.com/corp1/person.php?zqdm=%s&pid=%s"%(companyId,personid)
            peoplebasicinfo={}
            file_path=os.path.join(root,file)
            content=open(file_path).read()
            # print content
            try:
                selector=etree.HTML(content)
            except:
                continue
            try:
                peoplebasicinfo['name']=selector.xpath('.//table[@class="list"][1]/tr[3]/td/text()')[0]
            except:
                peoplebasicinfo['name']=''
            try:
                peoplebasicinfo['gender']=selector.xpath('.//table[@class="list"][1]/tr[3]/td/text()')[1]
            except:
                peoplebasicinfo['gender']=''
            try:
                peoplebasicinfo['birthday']=selector.xpath('.//table[@class="list"][1]/tr[3]/td/text()')[2]
            except:
                peoplebasicinfo['birthday']=''
            try:
                peoplebasicinfo['educationbackground']=selector.xpath('.//table[@class="list"][1]/tr[3]/td/text()')[3]
            except:
                peoplebasicinfo['educationbackground']=''
            try:
                peoplebasicinfo['nationality']=selector.xpath('.//table[@class="list"][1]/tr[3]/td/text()')[4]
            except:
                peoplebasicinfo['nationality']=''
            try:
                peoplebasicinfo['introduction']=selector.xpath('.//table[@class="list"][1]/tr[4]/td/text()')[0].replace('\n','').replace('\t','')
            except:
                peoplebasicinfo['introduction']=''
            try:
                peoplebasicinfo['id']= urlUUID[personid]
            except:
                peoplebasicinfo['id']=''

            # 插入到ｍｏｎｇｏｄｂ数据库中
            db.peopleBasicInfo.insert(peoplebasicinfo)

            try:
                tr_list=selector.xpath('.//table[@class="list"][2]/tr')
                for tr in tr_list[2:]:
                    if len(tr.xpath("./td/text()"))>1:
                        shareholdinginfo = {}
                        try:
                            shareholdinginfo['id']= urlUUID[personid]
                        except:
                            shareholdinginfo['id']=''
                        try:
                            shareholdinginfo['companyname']= tr.xpath("./td[1]/text()")[0]
                        except:
                            shareholdinginfo['companyname']=''
                        try:
                            shareholdinginfo['holdingnum']= tr.xpath("./td[2]/span/text()")[0] + tr.xpath("./td[2]/text()")[0]
                        except:
                            shareholdinginfo['holdingnum']=''
                        try:
                            shareholdinginfo['changereason']=tr.xpath("./td[3]/text()")[0]
                        except:
                            shareholdinginfo['changereason']=''
                        try:
                            shareholdinginfo['deadline']= tr.xpath("./td[4]/text()")[0]
                        except:
                            shareholdinginfo['deadline']=''
                        # for item in shareholdinginfo:
                        #     print item,shareholdinginfo[item]
                        # 插入数据到mongo，每一条插入一次。
                        db.shareHoldingInfo.insert(shareholdinginfo)
            except:
                print "this has no sharholding info "

            try:
                tr_list2 = selector.xpath('.//table[@class="list"][3]/tr')
                for tr in tr_list2[2:]:
                    if len(tr.xpath("./td/text()")) > 1:
                        employment = {}
                        try:
                            employment['id']= urlUUID[personid]
                        except:
                            employment['id']=''
                        try:
                            employment['companyname']= tr.xpath("./td[1]/text()")[0]
                        except:
                            employment['companyname']=''
                        try:
                            employment['job']= tr.xpath("./td[2]/text()")[0]
                        except:
                            employment['job']=''
                        try:
                            employment['term']=tr.xpath("./td[3]/text()")[0]
                        except:
                            employment['term']=''
                        try:
                            employment['paidornot']= tr.xpath("./td[4]/text()")[0]
                        except:
                            employment['paidornot']=''
                        try:
                            # 这个应该是和讯人物一起更新的一个信息。
                            # employment['description']= tr.xpath("./td[]/text()")[0]
                            employment['description']= ''
                        except:
                            employment['description']=''

                        # for item in employment:
                        #     print item,employment[item]
                        db.employmentInfo.insert(employment)
            except:
                print "this has no employment info "
            
def person_listed_company_info():
    # 主要是提取腾讯证券每一个公司的基本信息那一页的信息。每一个人物在本公司的职务
    urlUUID = loadUrlIDDict()
    for root,dirs,files in os.walk("../../HTML"):
        for file in files:
            print file,'start to exit!!!!!!'
            companycode = file.split('.')[0]
            content=open(os.path.join(root,file)).read()
            selector=etree.HTML(content)
            tds = selector.xpath(".//td[@class='nobor_l']/a")
            for td in tds:
                listedcompanyemployment = {}
                url = td.xpath("./@href")[0]
                personid = url[-8:]
                parentTr = td.xpath("./../..")[0]
                try:
                    listedcompanyemployment['companycode']=companycode
                except:
                    listedcompanyemployment['companycode']=''
                try:
                    listedcompanyemployment['job']=parentTr.xpath('./td[2]/text()')[0]
                except:
                    listedcompanyemployment['job']=''
                try:
                    listedcompanyemployment['jobbegin']=parentTr.xpath('./td[3]/text()')[0]
                except:
                    listedcompanyemployment['jobbegin']=''
                try:
                    listedcompanyemployment['jobend']=parentTr.xpath('./td[4]/text()')[0]
                except:
                    listedcompanyemployment['jobend']=''
                try:
                    listedcompanyemployment['leavereason']=parentTr.xpath('./td[5]/text()')[0]
                except:
                    listedcompanyemployment['leavereason']=''
                try:
                    listedcompanyemployment['id']= urlUUID[personid]
                except:
                    listedcompanyemployment['id']=''

                # 插入到ｍｏｎｇｏｄｂ数据库中
                db.listedCompanyEmployment.insert(listedcompanyemployment)
                # for item in listedcompanyemployment:
                #     print item,listedcompanyemployment[item]
                # return

def backUp():
    cols = db.collection_names()
    for name in cols:
        if "system" not in name:
            print name
            for item in db[name].find():
                # print item['id']
                db2[name].insert(item)


if __name__=="__main__":
    # html_spider()
    # person_extract()
    # get_person_html()
    # person_html_parser()
    # person_listed_company_info()
    # backUp()
    # get_urls()
    get_person_html()
