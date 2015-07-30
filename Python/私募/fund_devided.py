#!/usr/bin/python
#-*-coding:UTF8-*-

import sys,os
import cx_Oracle
import psycopg2
import urllib2
import codecs 
import csv
import string
import time
reload(sys)            
sys.setdefaultencoding("utf-8")

def connect_oracle():

	dsn = cx_Oracle.makedsn("127.0.0.1","1521","test")
	conn = cx_Oracle.connect("oracle","oracle",dsn)
	print "Oracle connect is seccussedfully "
	return conn

def connect_postgre():    
    try:
        conn = psycopg2.connect(database="fund_public",user="postgres",password="yHLsoft100",host="121.40.163.105", port="5432")
        print "postgre数据库连接成功".decode("utf8")
        return conn
    except Exception, e:
        print "postgre数据库连接失败".decode("utf8")


def check_add(code):
	print "========================........checking adding .........================================"
	quary_oracle = "select * from %s where INNER_CODE = '%s' AND PROVIDE_DATE = (select MAX(PROVIDE_DATE) from %s where INNER_CODE = '%s')"%("PGENIUS.TRUST_PROFIT",str(code),"PGENIUS.TRUST_PROFIT",str(code))	
	print quary_oracle
	conn_oracle = connect_oracle()
	cursor_oracle = conn_oracle.cursor()
	cursor_oracle.execute(quary_oracle)
	dict_oracle = dict_max_cursor(cursor_oracle)
	print "=====================================oracle->postgres========================"
	quary_postgre = "select * from %s where 发放日期 = '%s' AND 发放日期 = (select MAX(发放日期) from %s where 发放日期 = '%s') "%("private_fund.devided",str(code),"private_fund.devided",str(code))
	conn_postgre = connect_postgre()
	cursor_postgre = conn_postgre.cursor()
	cursor_postgre.execute(quary_postgre)
	dict_postgre = dict_max_cursor(cursor_postgre)
	print "============================********checking_time*******================= ?"
	a = str(dict_postgre["发放日期"]).strip()
	a = time.strptime(a,"%Y-%m-%d %H:%M:%S")
	b = time.strptime(dict_oracle["PROVIDE_DATE"],"%Y-%m-%d %H:%M:%S")
	print "sss*********************************** compare to time *********************************************"
	if a == b:
		print "没有增量,不存在增量".decode("utf8")
	else:	
		# quary = "select * from PGENIUS.TRUST_PROFIT where INNER_CODE ='%s' and PROVIDE_DATE > to_date('%s','YYYY-MM-DD HH24:MI:SS') and PROVIDE_DATE <= to_date('%s','YYYY-MM-DD HH24:MI:SS')"%(str(code),dict_postgre["发放日期"],str(dict_oracle["PROVIDE_DATE"]))
		quary = "select * from PGENIUS.TRUST_PROFIT where INNER_CODE ='%s' and PROVIDE_DATE > '%s' and PROVIDE_DATE <= '%s'"%(str(code),dict_postgre["发放日期"],str(dict_oracle["PROVIDE_DATE"]))
		insert_update_data_postgre(quary)
	print "增量操作成功".decode("utf8")
	conn_oracle.close()
	conn_postgre.close()


def check_exist(cursor,code):
	postgres_codes = searchr_postgre()
	print "*************************************code ===************************ ",code[0]
	print "lenght's a",len(postgres_codes)
	codes = get_array(postgres_codes)
	codes = set(codes)
	if string.atoi(str(code)) in codes:
		print "test_ok",code
		print "已经存在 ".decode("utf8")
		check_add(code)
	else:	
		print "test_no",code
		print "不存在".decode("utf8")
		ross = cursor.fetchall()
		if len(ross) > 0:
			index = cursor.description
			result = []
			for res in ross:
				out = {}
				print "aaaa"
				for i in range(len(index)-1):
					out[index[i][0]] = res[i]
				insert_data_postgre(out)
				
		else:
			print "数据不存在".decode("utf8")	

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
	quary = "select 基金代码 from private_fund.devided"
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


def searcher_oracle(quary,code):
	try:
		conn = connect_oracle()
		cursor = conn.cursor()
		cursor.execute(quary)
		check_exist(cursor,code)
		conn.close()
	except Exception, e:
		print e
	
def operation_get_code():
	try:
		quary = "select 私募id from private_fund.fund_info"

		conn = connect_postgre()
		cursor = conn.cursor()
		cursor.execute(quary)
		results = cursor.fetchall()

		print "查找成功".decode("utf8"),len(results)
		return results
	except Exception, e:
		print "result_array is wrong ",e


def operation_date_update():
	try:
		results_array = operation_get_code()
		for fund_code in results_array:
			print "fund_code ============== ",fund_code[0]
			quary = "select * from %s where INNER_CODE ='%s' "%("PGENIUS.TRUST_PROFIT",str(fund_code[0]))		
			searcher_oracle(quary,fund_code[0])
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
	print "len(tupe_keys)",len(tupe_keys)
	print "\n"
	print "len(tup_value)",len(tup_value)
	tup_total.append(tupe_keys)
	tup_total.append(tup_value)
	return tup_total

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
	
def insert_data_postgre(dict):
	print "==========================***开始插入***================".decode("utf8")

	try:
		conn = connect_postgre()
		cursor = conn.cursor()
		time_now = GetNowTime()
		param = {
		"id":'Null',
		"基金代码":str(dict["INNER_CODE"]),
		"单位净值分红额":str(dict["DIV_MONEY"]),
		"发放日期":str(dict["PROVIDE_DATE"]),
		"登记日期":str(dict["REGI_DATE"]),
		"更改日期":str(time_now)
  		}
		tup_total = change_dict_tuple(param) 
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1])
		quary = 'insert into private_fund.devided(%s) VALUES(%s)'%(key_string,values_string)
		print quary
		cursor.execute(quary)
		conn.commit()
		print "======*******数据已经成功插入********=========".decode("utf8")
		cursor.close()
		conn.close()
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")


	
def writer_csv():
	print "=====begine ***** writer====="

	conn = connect_postgre()
	cursor = conn.cursor()
	time_str = compare_time()
	quary_searcher = "select * from private_fund.devided where 修改日期 >= '%s'"%time_str
	cursor.execute(quary_searcher)
	print "*******************************************"
	body = cursor.fetchall()
	
	header = []

	for i,col in enumerate(cursor.description):	
		header.append(str(col[0]))
	print "header=====",len(header)  		
	
	str_name ='分红.csv'   
	fobj = open(str_name.decode("utf8"),'wb') 
	fobj.write(codecs.BOM_UTF8)
	writer = csv.writer(fobj)
	writer.writerow(header)  
	writer.writerows(body)
	print "==============***写入成功***=============".decode("utf-8")
	fobj.close()  
	cursor.close()
	conn.close() 

def insert_update_data_postgre(quary):
	print "增量监测中".decode("utf8")
	conn_oracle = connect_oracle()
	cursor_oracle = conn_oracle.cursor()
	cursor_oracle.execute(quary)
	dict_cursor(cursor_oracle)


def dict_cursor(cursor):
	print "change ............."
	try:
		ross = cursor.fetchall()
		if len(ross) > 0:		
			index = cursor.description
			result = []
			for res in ross:
				out = {}
				for i in range(len(index)-1):
					out[index[i][0]] = res[i]
				print "out ==************===**************************************=====",len(out)
				insert_data_postgre(out)	
	except Exception, e:
		 print e
def GetNowTime():
	return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))

def compare_time():

	# return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
	time_today =  time.strftime("%Y-%m-%d ",time.localtime(time.time()))
	time_str = str(time_today)+"00:00:00"
	print time_str
	return time_str
if __name__ == '__main__':
	operation_date_update()



	




			



