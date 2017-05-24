#!/usr/bin/env python
# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import uuid
import pymongo
import logging
from py2neo import Graph, Node, Relationship
conn=pymongo.MongoClient('192.168.1.34',27017)
qqdb=conn.qqperson1

graph = Graph("http://127.0.0.1:7474", username="neo4j", password="111111")

graph.delete_all()


def makeNodeAndRelation(obj):
	persons = obj['set']
	company = obj['companyname']
	print "this is out "+company
	node_company = graph.find_one("Company","name",company)
	if node_company==None:
		node_company = Node("Company")
		node_company["name"] = company
		graph.create(node_company)
	for person in persons:
		node_person = graph.find_one("Person","id",person)
		if node_person==None:
			findBasicInfo = qqdb.peopleBasicInfo.find({"id":person})
			for item in findBasicInfo:
				node_person = Node("Person")
				for tag in item:
					# print tag,item[tag]
					if tag =="_id":
						continue
					node_person[tag] = item[tag]
				graph.create(node_person)
				person_work_company = Relationship(node_person,"WORK_IN",node_company)
				person_work_company['id'] = node_person["id"]+node_company["name"]
				graph.create(person_work_company)
		else:
			# 说明已经有了这个人的节点，所以现在需要判断有没有对应的变，如果没有的话创建，有的话就不需要重新创建
			relation = graph.find("WORK_IN","id",node_person["id"]+node_company["name"])
			if relation==None:
				relation = Relationship(node_person,"WORK_IN",node_company)
				relation['id'] = node_person["id"]+node_company["name"]
				graph.create(person_work_company)


def makeNodeAndRelation2(obj):
	persons = obj['set']
	company = obj['companyname']
	node_company = "create (%s:Company {name:'%s' } )"%(company,company)
	# graph.run(node_company)
	# graph.create(node_company)
	for person in persons:
		node_person = "(%s:Person {name:'%s' } )"%(person,person)
		node_company = node_company + ',\n' +node_person 
		# graph.run(node_person)
		relation = "(%s)-[:WORK_IN]->(%s)"%(person,company)
		node_company = node_company + ',\n'+relation
		# graph.run(relation)
	print node_company
	graph.run(node_company)

def makeNodeAndRelation3(obj):
	persons = obj['set']
	company = obj['companyname']
	node_company = "create (%s:Company {name:'%s' } )"%(company,company)
	graph.run(node_company)
	# graph.create(node_company)
	for person in persons:
		node_person = "create (%s:Person {name:'%s' } )"%(person,person)
		graph.run(node_person)
		relation = "match (m:Person {name:'%s'}),(n:Company {name:'%s'}) create (m)-[:WORK_IN]->(n)"%(person,company)
		graph.run(relation)


def insertQQToNeo4j():
	count =0
	for item in qqdb.companyperson.find(timeout=False):
		makeNodeAndRelation(item)
		count +=1
		print count


if __name__ == '__main__':

	insertQQToNeo4j()