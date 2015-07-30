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
def check_add(code):
	quary_oracle = "select * from %s where FUNDCODE = '%s' AND HTRISKDATE = (select MAX(HTRISKDATE) from %s where FUNDCODE = '%s')"%("tfundinfoextra",str(code[0]),"tfundinfoextra",str(code[0]))	
	print "===========================checking================================"
	
	print quary_oracle
	conn_oracle = connect_oracle()
	cursor_oracle = conn_oracle.cursor()
	cursor_oracle.execute(quary_oracle)
	dict_oracle = dict_max_cursor(cursor_oracle)

	print "=====================================oracle->postgres========================"

	quary_postgre = "select * from %s where 基金代码 = '%s' AND 评级时间 = (select MAX(评级时间) from %s where 基金代码 = '%s') "%("public_fund.wg_fundinfoextra",str(code[0]),"public_fund.wg_fundinfoextra",str(code[0]))
	conn_postgre = connect_postgre()
	cursor_postgre = conn_postgre.cursor()
	cursor_postgre.execute(quary_postgre)
	dict_postgre = dict_max_cursor(cursor_postgre)

	print "============================**********checking_time*******================= ?"

	# quary = "select * from TFUNDMARKET where VC_NAVDATE >%s AND VC_NAVDATE <= %s"%(dict_postgre["时间"],dict_oracle["VC_NAVDATE"]) 
	quary = "select * from tfundinfoextra where fundcode='%s' and HTRISKDATE between '%s' and '%s' "%(str(code[0]),dict_postgre["评级时间"],dict_oracle["HTRISKDATE"])
	insert_update_data_postgre(quary)
	
	conn_oracle.close()
	conn_postgre.close()


def check_exist(cursor,code):
	postgres_codes = searchr_postgre()
	print "**********************code ===************************",code[0]
	codes = get_array(postgres_codes)
	codes = set(codes)
	if str(code[0]) in codes:
		print code[0],"已经存在 ".decode("utf8")	
		check_add(code)
	else:	
		print code[0],"不存在".decode("utf8")
		
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
			print "**************************no   data!!********************8"
def get_array(postgres_codes):
	codes = []
	for code in postgres_codes:
		# codes.append(string.atoi(str(code[0])))
		codes.append(str(code[0]).strip())		
		# print type(str(code[0]))
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
	quary = "select 基金代码 from public_fund.wg_fundinfoextra"
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
		quary = "select VC_FUNDCODE from %s WHERE C_TYPE != '8'"%"TFUNDINFO"
		results = searcher_oracle_cursor_operation(quary)
		return results
	except Exception, e:
		print "result_array is wrong ",e


def operation_date_update():
	try:
		results_array = operation_get_code()
		for code in results_array:
			print code[0]
			quary = "select  * from %s where FUNDCODE = '%s'"%("tfundinfoextra",str(code[0]))	
			print quary
			searcher_oracle(quary,code)
			print "\n"
		writer_csv()
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
		tuple_keys.append("%s"%key)
	string = ','.join(tuple_keys)
	return string
def dict_opration_valeusTostr(dict):

	tuple_values = []
	for value in dict:
		if value == "None":
			value = ' '
		if value == 'Null':
			value = ' ' 

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
		time_now = GetNowTime()
		param = {
		"ID":"Null",
		"基金代码":str(dict["FUNDCODE"]),
		"评级机构":"海通风险",
		"风险等级":str(dict["HTRISKLEVEL"]),
		"评级时间":str(dict["HTRISKDATE"]),
		"修改时间":str(time_now)
  		}

		tup_total = change_dict_tuple(param)  
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1])
		quary = 'insert into public_fund.wg_fundinfoextra (%s) VALUES(%s)'%(key_string,values_string)
		print quary
		cursor.execute(quary)
		conn.commit()
		print "插入成功".decode("utf8")
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
	quary_searcher = "select * from public_fund.wg_fundinfoextra  修改时间 >= '%s'"%time_str
	cursor.execute(quary_searcher)
	body = cursor.fetchall()
	header = []
	for i,col in enumerate(cursor.description):	
		header.append(str(col[0]))
	print "header=====",len(header)

	str_name = '基金评级.csv'   
	fobj = open(str_name.decode("utf8"),'wb')
	fobj.write(codecs.BOM_UTF8)  
	writer = csv.writer(fobj)  
	writer.writerow(header)  
	writer.writerows(body)
	print "==============***写入成功***=============".decode("utf-8")
	fobj.close()  
def insert_update_data_postgre(quary):
	print "============*************update************===="
	conn_oracle = connect_oracle()
	cursor_oracle = conn_oracle.cursor()
	cursor_oracle.execute(quary)
	print "qweqewqe"
	dict_update_cursor(cursor_oracle)

def GetNowTime():
	return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))	
def compare_time():
	#return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
	time_today = time.strftime("%Y-%m-%d ",time.localtime(time.time()))
	time_str = str(time_today)+"00:00:00"
	print time_str
	return time_str	
if __name__ == '__main__':
	operation_date_update()




			



