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
        conn = psycopg2.connect(database="fund_public",user="postgres",password="yHLsoft100",host="121.40.163.105", port="5432")
        print "postgre数据库连接成功".decode("utf8")
        return conn
    except Exception, e:
        print "postgre数据库连接失败".decode("utf8")


def check_add(code):
	quary_oracle = "select * from tlimit_parallel t1, tlimitscheme t2 where t1.vc_schemeno=t2.vc_schemeno and t1.vc_fundcode='%s' AND VC_LASTDATE = (select MAX(VC_LASTDATE) from tlimit_parallel t1, tlimitscheme t2 where vc_fundcode = '%s')"%(str(code[0]),str(code[0]))	
	print "===========================checking================================"
	
	print quary_oracle
	conn_oracle = connect_oracle()
	cursor_oracle = conn_oracle.cursor()
	cursor_oracle.execute(quary_oracle)
	dict_oracle = dict_max_cursor(cursor_oracle)
	print "=====================================oracle->postgres========================"
	quary_postgre = "select * from %s where 基金代码 = '%s' AND 日期 = (select MAX(日期) from %s where 基金代码 = '%s') "%("public_fund.fund_limit",str(code[0]),"public_fund.fund_limit",str(code[0]))
	conn_postgre = connect_postgre()
	cursor_postgre = conn_postgre.cursor()
	cursor_postgre.execute(quary_postgre)
	dict_postgre = dict_max_cursor(cursor_postgre)
	print "============================**********checking_time*******================= ?"
	a = str(dict_postgre["日期"]).strip()
	a = a +" "+"00:00:00"
	a = time.strptime(a,"%Y-%m-%d %H:%M:%S")
	b = time.strptime(dict_oracle["VC_LASTDATE"],"%Y-%m-%d %H:%M:%S")
	print "sss*********************************** compare to time *********************************************"
	if a == b:
		print "没有增量,不存在增量".decode("utf8")
	else:	
		quary = "select * from tchangelimit where C_>FUNDCODE ='%s' and VC_LASTDATE > '%s' and VC_LASTDATE <='%s' "%(str(code[0]),dict_postgre["日期"],dict_oracle["VC_LASTDATE"])
		insert_update_data_postgre(quary)
	print "增量查找处理成功".decode("utf8")	
	conn_oracle.close()
	conn_postgre.close()


def check_exist(cursor,code):
	postgres_codes = searchr_postgre()
	print "*************************************code ===************************ ",code[0]
	codes = get_array(postgres_codes)
	codes = set(codes)
	print codes
	if str(code[0])in codes:
		print code[0],"已经存在 ".decode("utf8")	
		check_add(code)
	else:	
		for row in cursor:
			print "转化插入中".decode("utf8")
			out = {}
			for i,col in enumerate(cursor.description):
				out[col[0]] = row[i]
			print len(out)
			insert_data_postgre(out)
			
		
def get_array(postgres_codes):
	codes = []
	for code in postgres_codes:
		codes.append(str(code[0]).strip())
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
	quary = "select 基金代码 from public_fund.fund_limit"
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
		# # return results
		# results = ["070028","070008","070026","070013","070012","460006","460106","460005","460003","460010"]
		return results			
	except Exception, e:
		print "result_array is wrong ",e


def operation_date_update():
	try:
		results_array = operation_get_code()
		for code in results_array:
			print code[0]
			quary = "select * from tlimit_parallel t1, tlimitscheme t2 where t1.vc_schemeno=t2.vc_schemeno and t1.vc_fundcode='%s' and t1.VC_LASTDATE = (select MAX(VC_LASTDATE) from tlimit_parallel t1 where t1.vc_fundcode = '%s')"%(str(code[0]),str(code[0]))	
			print quary
			searcher_oracle(quary,code)
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
	print "=======================***开始插入***================".decode("utf8")
	try:
		conns = connect_oracle()
		cursors = conns.cursor()
		quarys = "select * from tlimit_parallel where VC_FUNDCODE ='%s'"%(str(dict["VC_FUNDCODE"]))
		cursors.execute(quarys)
		print "quarys",quarys
		for row in cursors:
			outs = {}
			for i,col in enumerate(cursors.description):
				outs[col[0]] = row[i]		
		print "outs[] ==========",outs["VC_LASTDATE"],type(outs["VC_LASTDATE"])
		conn = connect_postgre()
		cursor = conn.cursor()
		time_now = GetNowTime()
		param = {
		"id":"Null",
		"基金代码":str(dict["VC_FUNDCODE"]),
		"最小额":str(dict["EN_MINVALUE"]),
		"最小追加额":str(dict["EN_SECONDMIN"]),	
		# "元基金份额类别":str(dict["C_SHARETYPE"]),
		"最大额":str(dict["EN_MAXVALUE"]),				
		"业务类型":str(dict["C_BUSINFLAG"]),
		"日期":time.strftime("%Y-%m-%d",time.strptime(outs["VC_LASTDATE"],"%Y%m%d%H%M%S")),
		"委托方式":str(dict["C_TRUST"]),
		"更改日期":str(time_now)
  		}
  		print "****VC_LASTDATE ===",param["日期"]
		tup_total = change_dict_tuple(param)  
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1])

		quary = 'insert into public_fund.fund_limit (%s) VALUES(%s)'%(key_string,values_string)
		print quary
		cursor.execute(quary)
		conn.commit()
		print "============***********插入成功 insert seccussedfully !~****************========".decode("utf8")
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

	# quary_searcher = "select * from public_fund.fund_limit where 修改日期 >= '%s'"%time_str
	quary_searcher = "select * from public_fund.fund_limit where 更改日期 >= '%s'"%time_str

	cursor.execute(quary_searcher)
	body = cursor.fetchall()
	header = []
	print "*********************插入成功，开始写入**********************".decode("utf8")
	for i,col in enumerate(cursor.description):
				
		header.append(str(col[0]))
	print "header=====",len(header)
	str_name = '基金限额.csv'   
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
	print "增量处理中....".decode("utf8")
	conn_oracle = connect_oracle()
	cursor_oracle = conn_oracle.cursor()
	cursor_oracle.execute(quary)
	dict_update_cursor(cursor_oracle)

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




			



