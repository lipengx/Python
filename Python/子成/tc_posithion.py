#!/usr/bin/python
#-*-coding:UTF8-*-
import sys,os
import cx_Oracle
import psycopg2
import urllib2
import codecs 
import csv
import time
reload(sys)            
sys.setdefaultencoding("utf-8")

def connect_oracle():
	dsn = cx_Oracle.makedsn("10.28.192.101","1521","fundsale")
	conn = cx_Oracle.connect("fundsale","oracle",dsn)
	print "Oracle connect is seccussedfully "
	return conn
def connect_postgre():    
    try:
        conn = psycopg2.connect(database="PASUserDB",user="postgres",password="yHLsoft100",host="121.40.163.105", port="5432")
        print "postgre数据库连接成功".decode("utf8")
        return conn
    except Exception, e:
        print "postgre数据库连接失败".decode("utf8")
		
def dict_cursor(cursor,code):
	print "changing to dict !~"
	try:
		ross = cursor.fetchall()
		if len(ross) > 0:		
			index = cursor.description
			result = []
			for res in ross:
				out = {}
				for i in range(len(index)-1):
					out[index[i][0]] = res[i]
				insert_data_postgre(out)	
		else:
			print code,"不存在于数据库中=========================================".decode("utf8")


	except Exception, e:
		print "sorry",e
def searcher_oracle_cursor_operation(quary):
	try:
		conn = connect_oracle()
		cursor = conn.cursor()
		cursor.execute(quary)
		results_array = cursor.fetchall()
		return results_array
		conn.close()
	except Exception, e:
		print e


def searcher_oracle(quary):
	try:
		conn = connect_oracle()
		cursor = conn.cursor()
		cursor.execute(quary)
		dict_cursor(cursor)
		conn.close()
	except Exception, e:
		print e

def drop_postgre_all_date(tradeacco,fundcode):
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "DELETE FROM public.tc_positions where tradeacco = '%s' and fundcode = '%s'"%(tradeacco,fundcode)
	cursor.execute(quary)
	conn.commit()  
	print fundcode,"删除成功".decode("utf8")
	cursor.close ()  
	conn.close () 
	

def searcher_postgre(tradeacco,fundcode):
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "select * from public.tc_positions where tradeacco = '%s' and fundcode = '%s'"%(tradeacco,fundcode)
	cursor.execute(quary)
	print "public.tc_positions ",quary
	result_dict = {}
	for row in cursor:
		out = {}
		for i,col in enumerate(cursor.description):
			out[col[0]] = row[i]
		result_dict["tradeacco"] = out["tradeacco"]
		result_dict["fundcode"] = out["fundcode"]
	return result_dict


def operation_result(dataTime):
	try:
		if dataTime == "Null" or dataTime == '' or dataTime == '0':
			dataTime = GetNowTime()
		quary = "select * from his_staticshare where lastdate = '%s'"%(str(dataTime))
		print quary
		conn = connect_oracle()
		cursor = conn.cursor()
		cursor.execute(quary)
		for row in cursor:
			out = {}
			for i,col in enumerate(cursor.description):
				out[col[0]] = row[i]
			print len(out)
			check_dict = searcher_postgre(str(out["TRADEACCO"]),str(out["FUNDCODE"]))
			print len(check_dict.values())
			if len(check_dict.values()) > 0:
				drop_postgre_all_date(str(out["TRADEACCO"]),str(out["FUNDCODE"]))
			else:
				print "不存在".decode("utf8")
			insert_data_postgre(out)
	except Exception, e:
		print "get result_array is wrong ",e


def operation_exchange():
	try:
		# results_postgre = searcher_postgre()
		# if len(results_postgre) > 0:
		# 	drop_postgre_all_date()
		# else:
		dataTime = raw_input('plase enter the dataTime sting :')
		print dataTime
		results_array = operation_result(dataTime)
	except Exception, e:
		print "result_array is wrong ",e				




def change_dict_tuple(dict):
	tuple_items = dict.items()
	tupe_keys = []
	tup_value = []
	tup_total = []
	for k,v in tuple_items:
		tupe_keys.append(k)
		tup_value.append(v)	
	print len(tupe_keys)
	print "\n"
	print len(tup_value)
	tup_total.append(tupe_keys)
	tup_total.append(tup_value)
	return tup_total

def dict_opration_keysTostr(dict):
	tuple_keys = []
	for key in dict:
		tuple_keys.append('%s'%key)
	string = ','.join(tuple_keys)
	return string
def dict_opration_valeusTostr(dict):

	tuple_values = []
	for value in dict:
		if value == "None":
			value = ''
		if value == 'Null':
			value = ''    
		tuple_values.append("'%s'"%value) 
	string = ','.join(tuple_values)
	return string
def insert_data_postgre(dict):
	try:


		print "==========================***开始插入***================".decode("utf8")
	
		conn = connect_postgre()
		cursor = conn.cursor()
		param = {
		"custtype":dict["CUSTTYPE"],
		"fundacco":dict["FUNDACCO"],
  		"tradeacco":dict["TRADEACCO"],
  		"fundcode":dict["FUNDCODE"],
  		"sharetype":dict["SHARETYPE"],
  		"totalshare":dict["TOTALSHARE"],
  		"frozenshare":dict["FROZENSHARE"],
  		"requestshare":dict["REQUESTSHARE"],
  		"dividendmethod":dict["DIVIDENDMETHOD"],
  		"lastdate":dict["LASTDATE"],
  		"income":dict["INCOME"],
		"frozenincome":dict["FROZENSHARE"]
  		}
		tup_total = change_dict_tuple(param)  
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1])
		quary = 'insert into public.tc_positions(%s) VALUES(%s)'%(key_string,values_string)
		print quary
		cursor.execute(quary)
		conn.commit()
		print "=============================***插入成功***=========================".decode("utf8");
		cursor.close()
		conn.close() 
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")



def GetNowTime():
	#return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
	time_today = time.strftime("%Y%m%d ",time.localtime(time.time()))
	time_str = str(time_today)
	print time_str
	return time_str.strip()	
if __name__ == '__main__':
	operation_exchange()




			







			



