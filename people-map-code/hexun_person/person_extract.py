# coding=utf-8
import os,sys
reload(sys)
sys.setdefaultencoding('utf-8')
import urllib,urllib2
from lxml import etree
import pymongo
conn=pymongo.MongoClient('192.168.1.34',27017)
db=conn.hexunperson


def get_urls():
    urls = []
    for i in range(1,186):
        tmp = "http://renwu.hexun.com/ggList.aspx?oder=sj&t=2&page=%d"%i
        urls.append(tmp)
    return urls

def get_html(url):
    req_header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        }
    req = urllib2.Request(url, None, req_header)
    resp = urllib2.urlopen(req, None)
    html = resp.read()
    return html


def get_people_urls():
    urls = get_urls()
    person_list=[]
    for url in urls:
        print url
        html = get_html(url).decode("gb18030")
        selector = etree.HTML(html)
        lis = selector.xpath("//div[@class='slistBox']/ul/li")
        print len(lis)
        for li in lis:
            people = li.xpath('./a/@href')[0]
            person_list.append(people)

    person_list = list(set(person_list))
    f = open('person_url.txt','w+')
    for people in person_list:
        # print people
        f.write(people+'\n')

def printObj(obj):
    for item in obj:
        if item =="工作经历" or item =="教育经历":
            print item + " : "
            for item1 in obj[item]:
                print " ".join(item1)
        else:
            print item + ' : ' + obj[item]
    print len(obj.keys())
    print '*'*50

def get_person_info():
    for root,dirs,files in os.walk("../../hexun_person_html"):
        for file in files:
            print file
            content=open(os.path.join(root,file)).read().decode('utf-8')
            selector=etree.HTML(content)  
            person = get_person_obj(selector)
            # printObj(person)
            # return
            db.person.insert(person) 

def textHtml():
    f = open('tmp.html','r').read()
    selector = etree.HTML(f)
    lis = selector.xpath("//div[@class='setBase']/div[@class='right']/ul/li")
    print len(lis)
    for li in lis:
        tag = li.xpath("./strong/text()")[0]
        print tag
    return   

def get_people_htmls():
    f = open('person_url.txt','r')
    for line in f:
        url = line.strip()
        person = url.split('/')[-1]
        print url
        if not os.path.exists("../../hexun_person_html/%s"%person):
            html = get_html(url).decode("gb18030")
            fhtml=open("../../hexun_person_html/%s"%person,'w+')
            fhtml.write(html)
            fhtml.close() 
    f.close()

def get_total_tags():
    tags = set()
    for root,dirs,files in os.walk("../../hexun_person_html"):
        for file in files:
            # print file
            content=open(os.path.join(root,file)).read().decode('utf-8')
            selector=etree.HTML(content)     
            lis = selector.xpath("//div[@class='setBase']/div[@class='right']/ul/li")
            for li in lis:
                tag = li.xpath("./strong/text()")
                if len(tag) >0:
                    tag = tag[0]
                    tag = tag.replace('：',"").replace(' ','').replace('　',"")
                    print tag
                    tags.add(tag)
    tagf = open('tag.txt','w+')
    for tag in tags:
        tagf.write(tag+'\n')
    tagf.close()

def get_person_obj(selector):
    data={}
    lis = selector.xpath("//div[@class='setBase']/div[@class='right']/ul/li")
    if len(lis)<1:
        return data
    for li in lis[:-1]:
        tag = li.xpath("./strong/text()")[0].replace('：',"").replace(' ','').replace('　',"")
        tmpStr = li.xpath('./text()')
        if len(tmpStr) !=0 and len(tmpStr[0].replace("nbsp;",'').strip()) !=0:
            data[tag] = li.xpath('./text()')[0]
        else:
            # print tag
            if len(li.xpath('./a/text()'))>0:
                data[tag] = li.xpath('./a/text()')[0]
        # print tag,data[tag]
    li = lis[-1]
    if len(li.xpath('./p/text()'))>0:
        data['description'] = li.xpath('./p/text()')[0].replace('简介：','')
    conts = selector.xpath("//div[@class='contBox']")
    for div in conts:
        tag = div.xpath('./h3/div/text()')[0]
        # 这两项内容是一个数组，记录着每一个ｐ的信息，每一个ｐ的信息，又是一个数组，记录着时间，内容，职务，描述等信息。
        if tag =="工作经历" or tag =="教育经历":
            tmpArray = []
            ps = div.xpath("./div[@class='cont']/p")
            for p in ps:
                array1 = p.xpath("./span/text()")[0].split(' ')
                if len(p.xpath("./text()"))>0:
                    # 此部分主要是用来添加描述信息，因为有的ｐ是有描述信息的
                    array1 = array1+p.xpath("./text()")
                array1clear = []
                for item in array1:
                    if len(item.strip())>0:
                        array1clear.append(item.strip())
                tmpArray.append(array1clear)

            data[tag] =tmpArray
            # print tag
            # for item in data[tag]:
            #     print item
                # for item1 in item:
                #     print item1


    return data



if __name__=="__main__":
    # testTXT()
    get_person_info()
    # get_total_tags()
    # get_people_htmls()

    
