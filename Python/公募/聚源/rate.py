#!/usr/bin/python
#-*-coding:UTF8-*-
import sys,os
import cx_Oracle
import psycopg2
import urllib2
import codecs 
import csv
import pymssql
reload(sys)            
sys.setdefaultencoding("utf-8")
def connect_sql_server():
  try:
    conn = pymssql.connect(host ="10.0.185.138",database ="JYDB",user="js_dev",password="js_dev",charset = "utf8")
    print "sql_server 数据库连接成功".decode("utf8")
    return conn
  except Exception, e:
	print "sql_server 数据库连接失败".decode("utf8"),e
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


def dict_cursor(cursor,code):
	print "change ......."	
	for row in cursor:
		out = {}
		for i,col in enumerate(cursor.description):
			out[col[0]] = row[i]
		print len(out)
		insert_data_postgre(out,code)

	
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

		dict_cursor(cursor,code)

		conn.close()
	except Exception, e:
		print e
def drop_postgre_all_date():
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "DELETE FROM public_fund.rate"
	cursor.execute(quary)
	conn.commit()  
	print "删除成功".decode("utf8")
	cursor.close ()  
	conn.close () 
	operation_exchange()

def searcher_postgre():
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "select * from public_fund.rate"
	cursor.execute(quary)
	results_array = cursor.fetchall()
	return results_array


def operation_get_code():
	try:
		# quary = "select VC_FUNDCODE from %s WHERE C_TYPE != '8'"%"TFUNDINFO"
		# results = searcher_oracle_cursor_operation(quary)
		# return results
		results = ["070028","070008","070026","070013","070012","460006","460106","460005","460003","460010"]
		return results		
	except Exception, e:
		print "result_array is wrong ",e


def operation_exchange():
	try:
		results_postgre = searcher_postgre()
		if len(results_postgre) > 0:
			drop_postgre_all_date()
		else:	
			results_array = operation_get_code()
			for code in results_array:
				print code
				quary = "select * from %s where VC_FUNDCODE = '%s'"%("tfundtype",str(code))	
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
	print "111111"
	return tup_total

def dict_opration_keysTostr(dict):
	tuple_keys = []
	for key in dict:
		tuple_keys.append('%s'%key)
	string = ','.join(tuple_keys)
	print "2222222"
	return string
def dict_opration_valeusTostr(dict_value,param):
	
	tuple_values = []
	for value in dict_value:
		if value == "None":
			value = ''
		if value == 'Null':
			value = '' 
		if value == str(param["费率类别"]):
			value = value
		else:
			value = value.encode("latin1").decode("gbk") 	
		tuple_values.append("'%s'"%value) 
	print "33333"
	string = ','.join(tuple_values)
	return string
def insert_data_postgre(out,code):
	print "==========================***开始插入***================".decode("utf8")
	try:
		conn = connect_postgre()
		cursor = conn.cursor()
		print str(out["C_SHARETYPE"])
		param = {
		"id": "Null",
		"基金代码":code,
  		"是否执行":"Null",
  		"执行时间": "Null",
  		"取消时间": "Null",
  		"费率类别" : str(out["C_SHARETYPE"]),
  		"收费模式": "Null",
  		"费率区间划分" :"Null",

  		"期限划分单位" :"Null",
  		"申购金额区间起始":"Null",
		"申购金额区间截止": "Null",

  		"费率描述":"Null",
  		"区间描述" :"Null",
  		"费率": "Null",
  		"发布日期": "Null"
  		}
		quary_sql = "select * from dbo.MF_ChargeRate where InnerCode = (select InnerCode from dbo.MF_FundArchives where SecurityCode = '%s')"%(code)
		conn_sql = connect_sql_server()
		cursor_sql = conn_sql.cursor()
		cursor_sql.execute(quary_sql)
		dict_temp ={}
		for row_s in cursor_sql:
			out_sql = {}
			for i,col in enumerate(cursor_sql.description):		
				out_sql[col[0]] = row_s[i]
			param["是否执行"] = str(out_sql["IfExecuted"])
  			param["执行时间"] = str(out_sql["ExcuteDate"])
  			param["取消时间"] = str(out_sql["CancelDate"])
  			param["费率类别"] = str(out_sql["ChargeRateType"])
  			param["收费模式"] = str(out_sql["ChargePattern"])
  			# param["费率区间划分"] = str(out_sql["IntervalDescription"])
  			param["期限划分单位"] = str(out_sql["TermMarkUnit"])
  			param["申购金额区间起始"] = str(out_sql["BeginOfApplySumInterval"])
			param["申购金额区间截止"] = str(out_sql["EndOfApplySumInterval"])
  			param["费率描述"] = str(out_sql["ChargeRateDesciption"])
  			param["区间描述"] = str(out_sql["IntervalDescription"])
  			param["费率"] = "Null"
  			param["发布日期"] = str(out_sql["XGRQ"])
  			dict_temp["id"] = str(out_sql["ID"])
		quary_sql_f = "select * from dbo.MF_ChargeRate_SE where id = '%s'"%(dict_temp["id"])
		print quary_sql_f
		cursor_sql.execute(quary_sql_f)
		print cursor_sql.description
		for row_s_f in cursor_sql:
			out_sql_f = {}
			for i,col in enumerate(cursor_sql.description):		
				out_sql_f[col[0]] = row_s_f[i]
			param["费率区间划分"] = str(out_sql_f["TypeCode"])

  		cursor_sql.close()
  		conn_sql.close()	

		tup_total = change_dict_tuple(param) 
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1],param)
		print "okokokokoko"
		quary = 'insert into public_fund.rate(%s) VALUES(%s)'%(key_string,values_string)
		print quary
		cursor.execute(quary)
		conn.commit()
		print "=============================***插入成功***=========================".decode("utf8");
		cursor.close()
		conn.close() 
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")

def writer_csv():
	print "=====begine ***** writer====="
	conn = connect_postgre()
	cursor = conn.cursor()
	quary_searcher = "select * from public_fund.rate"
	cursor.execute(quary_searcher)
	body = cursor.fetchall()
	header = []
	print "*******************************************"
	for i,col in enumerate(cursor.description):	
		header.append(str(col[0]))
	print "header=====",len(header)

	str_name = '费率.csv'   
	fobj = open(str_name.decode("utf8"),'wb') 
	fobj.write(codecs.BOM_UTF8)  
	writer = csv.writer(fobj)  
	writer.writerow(header)  
	writer.writerows(body)
	print "==============***写入成功***=============".decode("utf-8")
	fobj.close()  

if __name__ == '__main__':
	operation_exchange()




			







			



