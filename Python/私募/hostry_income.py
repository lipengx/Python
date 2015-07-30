#!/usr/bin/python
#-*-coding:UTF8-*-
import sys,os
import cx_Oracle
import psycopg2
import urllib2
import codecs 
import csv
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

def operation_get_code():
	try:
		quary = "select 私募id from private_fund.fund_info"
		conn = connect_postgre()
		cursor = conn.cursor()
		cursor.execute(quary)
		results = cursor.fetchall()

		print "查找成功".decode("utf8")
		return results
	except Exception, e:
		print "result_array is wrong ",e

def dict_cursor(cursor):
	print "change_to _ dict !~~~"
	ross = cursor.fetchall()
	print "result's  lenght  is ",len(ross)
	if len(ross) > 0:

		index = cursor.description
		result = []
		for res in ross:
			out = {}
			print "check ing  insert"
			for i in range(len(index)-1):
				out[index[i][0]] = res[i]
			result.append(out)

			insert_data_postgre(out)	
	else:
		print "数据为空".decode("utf8")		
		

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
	quary = "DELETE FROM private_fund.history_income"
	cursor.execute(quary)
	conn.commit()  
	print "删除成功".decode("utf8")
	cursor.close ()  
	conn.close () 
	operation_exchange()

def searcher_postgre():
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "select * from private_fund.history_income"
	cursor.execute(quary)
	results_array = cursor.fetchall()
	return results_array

def operation_exchange():
	try:
		results_postgre = searcher_postgre()
		if len(results_postgre) > 0:
			drop_postgre_all_date()
		else:	
			results_array = operation_get_code()
			for code in results_array:
				print code[0]
				quary = "select * from %s where INNER_CODE = '%s' and ENDDATE = (select max(ENDDATE) from PGENIUS.TRUST_INCM_GR where INNER_CODE = '%s' )"%("PGENIUS.TRUST_INCM_GR",str(code[0]),str(code[0]))	
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
	print "==========================***开始插入***================".decode("utf8")
	try:
		conn = connect_postgre()
		cursor = conn.cursor()
		print "============***&&&&&&================*******",type(dict["ANNU_GR_FAC"]),dict["MONTH_GR"]/100.0

		param = {
		"产品id": str(dict["INNER_CODE"]),
		"最新单位净值": str(dict["UNT_NET"]),
  		"日回报率":"Null",
  		"本周回报率": "Null",
  		"本月回报率": "Null",
  		"一周回报率": "Null",
  		"一个月回报率" : str(dict["MONTH_GR"]/100.0),
  		"三个月回报率": str(dict["MONTH_3_GR"]/100.0),
  		"六个月回报率" :str(dict["HALF_ANNU_GR"]/100.0),
  		"今年以来回报率" : str(dict["NET_GR"]/100.0),
  		"一年回报率": str(dict["ANNU_GR"]/100.0),
		"二年回报率": str(dict["ANNU_2_GR"]/100.0),
  		"三年回报率": str(dict["ANNU_3_GR"]/100.0),
  		"设立以来回报率" :"Null",
  		"设立以来年后回报率":str(dict["BUILD_ANNU_GR"]/100.0),
  		"时间":str(dict["ENDDATE"])
  		}
		tup_total = change_dict_tuple(param)  
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1])
		quary = 'insert into private_fund.history_income(%s) VALUES(%s)'%(key_string,values_string)
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
	quary_searcher = "select * from private_fund.history_income"
	cursor.execute(quary_searcher)
	body = cursor.fetchall()
	header = []
	print "*******************************************"
	for i,col in enumerate(cursor.description):	
		header.append(str(col[0]))
	str_name = '基金回报率.csv'   
	fobj = open(str_name.decode("utf8"),'wb') 
	fobj.write(codecs.BOM_UTF8)  
	writer = csv.writer(fobj)  
	writer.writerow(header)  
	writer.writerows(body)
	print "==============***写入成功***=============".decode("utf-8")
	fobj.close()  

if __name__ == '__main__':
	operation_exchange()




			







			



