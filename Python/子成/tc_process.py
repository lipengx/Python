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
        conn = psycopg2.connect(database="zhongjianku",user="postgres",password="yHLsoft100",host="121.40.163.105", port="5432")
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

def drop_postgre_all_date():
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "DELETE FROM public.tc_process"
	cursor.execute(quary)
	conn.commit()  
	print "删除成功".decode("utf8")
	cursor.close ()  
	conn.close () 
	

def searcher_postgre(code):
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "select * from public.tc_transactions  "
	print "public.tc_transactions ",quary
	cursor.execute(quary)
	results_array = cursor.fetchall()
	return results_array


def operation_result(dataTime):
	try:
		if dataTime == "Null" or dataTime == ' ' or dataTime == '0':
			dataTime = GetNowTime()
		print dataTime
		quary_date = "select * from tc_process where date= '%s'"%(str(dataTime))

		# quary_date = "select * from his_process where his_process.batchdate= '%s'"%(str(dataTime))
		# his_process.batchdate = his_process.current_date &&his_process. processtype ='A' && his_process.processflag='2'

		
		conn = connect_oracle()
		cursor = conn.cursor()		
		cursor.execute(quary_date)
		for row in cursor:
			out = {}
			for i,col in enumerate(cursor.description):
				out[col[0]] = row[i]	
			if out["PROCESSTYPE"] == 'A' or out["PROCESSTYPE"] == 'T' or out["PROCESSTYPE"] == 'C' or out["PROCESSTYPE"] == 'F' and out["PROCESSFLAG"] == '2':
					update_data_postgre(out)
			else:
				print "代销系统指令未处理完".decode("utf8");
		cursor.close()
		conn.close()	
	except Exception, e:
		print "get result_array is wrong ",e


def operation_exchange():
	try:
		dataTime = raw_input('password: ')
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
		if value != '':
			value = value.decode("GBK")	  
		tuple_values.append("'%s'"%value) 
	string = ','.join(tuple_values)
	return string

def update_data_postgre(dict):
	try:


		print "==========================***开始更新***================".decode("utf8")

		quary = "UPDATE public.tc_process SET status ='%s',type ='%s',date = '%s' where BATCHDATE = '%s'"%(str(dict["PROCESSTYPE"]).decode("GBK"),str(dict["PROCESSFLAG"]).decode("GBK"),str(dict["BATCHDATE"]).decode("GBK"),str(dict["BATCHDATE"]).decode("GBK"))
		print quary
		cursor.execute(quary)
		conn.commit()
		print "=============================***更新成功***=========================".decode("utf8");
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




			







			



