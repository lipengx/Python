#!/usr/bin/python
#-*-coding:UTF8-*-
import time
import sys,os
import cx_Oracle
import psycopg2
import urllib2
import codecs 
import csv
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
# def dict_cursor(cursor):
# 	print "转化字典，插入".decode("utf8")
# 	for row in cursor:
# 		print "转化插入中".decode("utf8")
# 		out = {}
# 		for i,col in enumerate(cursor.description):
# 			out[col[0]] = row[i]
# 		print len(out)
# 		insert_data_postgre(out)
		
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
	quary = "DELETE FROM public_fund.tc_confirmations"
	cursor.execute(quary)
	conn.commit()  
	print "删除成功".decode("utf8")
	cursor.close ()  
	conn.close () 

def searcher_postgre():
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "select * from public_fund.tc_confirmations"
	cursor.execute(quary)
	results_array = cursor.fetchall()
	return results_array

def operation_result(dataTime):
	try:
		if dataTime == "Null" or dataTime == '' or dataTime == '0':
			dataTime = GetNowTime()

		quary_date = "select * from his_process where his_process.batchdate= '%s'"%(str(dataTime))
		# his_process.batchdate = his_process.current_date &&his_process. processtype ='A' && his_process.processflag='2'

		
		conn = connect_oracle()
		cursor = conn.cursor()		
		cursor.execute(quary_date)
		for row in cursor:
			out = {}
			for i,col in enumerate(cursor.description):
				out[col[0]] = row[i]	
			if out["PROCESSTYPE"] == 'C' and out["PROCESSFLAG"] == '2':
				quary = "select * from his_tradeconfirm where confirmdate ='%s'"%(str(dataTime))
				cursor.execute(quary)
				for row in cursor:
					out = {}
					for i,col in enumerate(cursor.description):
						out[col[0]] = row[i]
					insert_data_postgre(out)

			else:
				print "没有准备好".decode("utf8");
		cursor.close()
		conn.close()			
	except Exception, e:
		print "get result_array is wrong ",e	

def operation_exchange():
	try:
		# results_postgre = searcher_postgre()
		# if len(results_postgre) > 0:
		# 	drop_postgre_all_date()
		# else:	
		dataTime = raw_input('password: ')
		results_array = operation_result(dataTime)
	except Exception, e:
		print "result_array is wrong ",e	

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
		if value == 'None' or value == None:
			value = ''
		if value == 'Null':
			value = ''
		if type(value) == 'str' or type(value) == "<type 'str'>" or type(value) == type("aaaa"):
			value =value.decode("gbk")   
		tuple_values.append("'%s'"%value) 
	string = ','.join(tuple_values)
	return string
def insert_data_postgre(dict):
	print "==========================***开始插入***================".decode("utf8")
	try:
		conn = connect_postgre()
		cursor = conn.cursor()
		param = {
	# "id":" ",	
	"tacode":dict["TACODE"],
	"confirmno":dict["CONFIRMNO"],
	"confirmdate":dict["CONFIRMDATE"],
	"confirmflag":dict["CONFIRMFLAG"],
	"confirmcode":dict["CONFIRMFLAG"],
	"confirmmsg":dict["CONFIRMMSG"],
	"confirmbala":dict["CONFIRMBALA"],
	"confirmshare" :dict["CONFIRMSHARE"],
	"nav":dict["NAV"],
	"discount":dict["DISCOUNT"],
	"fare":dict["FARE"],
	"income":dict["INCOME"],
	"hisrequestno":dict["HISREQUESTNO"],
	"hisbatchdate":dict["HISBATCHDATE"],
	"hisbusinflag":dict["HISBUSINFLAG"],
	"hisadviserno":dict["HISADVISERNO"],
	"hisproductcode":dict["HISPRODUCTCODE"],
	"businflag":dict["BUSINFLAG"],
	"requestno":dict["REQUESTNO"],
	"requestdate":dict["REQUESTDATE"],
	"requesttime":dict["REQUESTTIME"],
	"machinedate":dict["MACHINEDATE"],
	"fundacco":dict["FUNDACCO"],
	"custno":dict["CUSTNO"],
	"tradeacco":dict["TRADEACCO"],
	"custname":dict["CUSTNAME"],
	"certtype":dict["CERTTYPE"],
	"certno":dict["CERTNO"],
	"fundcode":dict["FUNDCODE"],
	"sharetype":dict["SHARETYPE"],
	"requestbala":dict["REQUESTBALA"],
	"moneytype":dict["MONEYTYPE"],
	"requestshare":dict["REQUESTSHARE"],
	"exceedflag":dict["EXCEEDFLAG"],
	"otherfundcode":dict["OTHERFUNDCODE"],
	"otherconfirmshare":dict["OTHERCONFIRMSHARE"],
	"othersharetype":dict["OTHERSHARETYPE"],
	"othernav":dict["OTHERNAV"],
	"dividendmethod":dict["DIVIDENDMETHOD"],
	# "requestshare":" ",
 #    "sharetype":" ",
 #  	"process_flag":" ",
 #    "confirmdateex":" ",
 #    "machinedateex":" ",
 #    "requestdateex":" "
	# "account_id":""
  		}
		tup_total = change_dict_tuple(param)  
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1])
		quary = 'insert into public.tc_confirmations(%s) VALUES(%s)'%(key_string,values_string)
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




			







			



