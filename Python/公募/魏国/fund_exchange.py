#!/usr/bin/python
#-*-coding:UTF8-*-
#净值
import sys,os
import cx_Oracle
import psycopg2
import urllib2
import codecs 
import csv
import string
reload(sys)            
sys.setdefaultencoding("utf-8")

def connect_oracle():
	dsn = cx_Oracle.makedsn("10.28.192.101","1521","fundsale")
	conn = cx_Oracle.connect("fundsale","oracle",dsn)
	print "Oracle connect is seccussedfully "
	return conn

def connect_postgre():    
    try:
        conn = psycopg2.connect(database="fund_public",user="postgres",password="yHLsoft100",host="121.40.163.105", port="5432")
        print "postgre数据库连接成功".decode("utf8")
        return conn
    except Exception, e:
        print "postgre数据库连接失败".decode("utf8")

def change_dict(cursor):
	try:
		for row in cursor:
			out = {}
			for i,col in enumerate(cursor.description):		
				out[col[0]] = row[i]
			insert_data_postgre(out)
	except Exception, e:
		print e,"没有数据".decode("utf8")

			
def get_array(postgres_codes):
	codes = []
	for code in postgres_codes:
		codes.append(string.atoi(str(code[0])))
	return codes

def dict_max_cursor(cursor):
	out = {}
	for row in cursor:
		for i,col in enumerate(cursor.description):
			out[col[0]] = row[i]
		print " ******* getted  the   max **** "
	return out

def searchr_postgre():
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "select 基金代码 from public_fund.fund_exchange"
	cursor.execute(quary)
	results_array = cursor.fetchall()
	return results_array


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
		change_dict(cursor)
		conn.close()
	except Exception, e:
		print e
def drop_postgre_all_date():
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "DELETE FROM public_fund.fund_exchange"
	cursor.execute(quary)
	conn.commit()  
	print "删除成功".decode("utf8")
	cursor.close ()  
	conn.close () 
	operation_exchange()

def searcher_postgre():
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "select * from public_fund.fund_exchange"
	cursor.execute(quary)
	results_array = cursor.fetchall()
	return results_array
	
def operation_get_code():
	try:
		quary = "select c_fundcode from tchangelimit"
		results = searcher_oracle_cursor_operation(quary)
		return results
		# results = ["070028","070008","070026","070013","070012","460006","460106","460005","460003","460010"]
		# return results	
	except Exception, e:
		print "result_array is wrong ",e


def operation_date_update():
	try:
		results_postgre = searcher_postgre()
		if len(results_postgre) > 0:
			drop_postgre_all_date()
		else:	
		# results_array = operation_get_code()
		# for code in results_array:	
			quary = "select  * from %s "%("tchangelimit")	
			print quary
			searcher_oracle(quary)
		print "\n"
		
	except Exception, e:
		print "result_array is wrong ",e					
	writer_csv()


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
		tuple_keys.append("%s"%key)
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

	string =','.join(tuple_values)
	return string

def dict_update_cursor(cursor):
	for row in cursor:
		out = {}
		for i,col in enumerate(cursor.description):
			out[col[0]] = row[i]

		print len(out)
		insert_data_postgre(out)	
	
def insert_data_postgre(dict):
	print "==========================***开始插入***================".decode("utf8")

	try:
		conn = connect_postgre()
		cursor = conn.cursor()
		param = {
		"id":'Null',
		"基金代码":"Null",
		"元基金":str(dict["C_FUNDCODE"]),
		"目标基金":str(dict["C_OTHERCODE"]),	
		"元基金份额类别":str(dict["C_SHARETYPE"]),
		"目标基金份额类别":str(dict["C_OTHERSHARE"]),				
		"起始日期":str(dict["VC_BEGINREQUESTDATE"]),
		"结束日期":str(dict["VC_ENDREQUESTDATE"])
  		}

		print "==========================",dict["C_OTHERCODE"]
		tup_total = change_dict_tuple(param) 

		key_string = dict_opration_keysTostr(tup_total[0])

		values_string = dict_opration_valeusTostr(tup_total[1])

		quary = 'insert into public_fund.fund_exchange(%s) VALUES(%s)'%(key_string,values_string)
		print quary
		cursor.execute(quary)
		conn.commit()

		print "insert  ok   on rpad..."
		cursor.close()
		conn.close() 
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")


	
def writer_csv():
	print "=====begine ***** writer====="
	conn = connect_postgre()
	cursor = conn.cursor()

	quary_searcher = "select * from public_fund.fund_exchange"
	cursor.execute(quary_searcher)
	body = cursor.fetchall()
	header = []
	print "*********************插入成功，开始写入**********************".decode("utf8")
	for i,col in enumerate(cursor.description):
				
		header.append(str(col[0]))
	print "header=====",len(header)
	
	str_name = '基金转换.csv'   
	fobj = open(str_name.decode("utf8"),'wb')  
	fobj.write(codecs.BOM_UTF8)  
	writer = csv.writer(fobj)  
	writer.writerow(header)  
	writer.writerows(body)
	print "==============***写入成功***=============".decode("utf-8")
	fobj.close() 
	cursor.close()
	conn.close() 	 

if __name__ == '__main__':
	operation_date_update()




			



