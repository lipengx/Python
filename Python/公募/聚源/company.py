#!/usr/bin/python
#-*-coding:utf-8-*-
import os
import cx_Oracle
import psycopg2
import urllib2
import codecs 
import csv
import pymssql
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def connect_sql_server():
  try:
    conn = pymssql.connect(host ="10.0.185.138",database ="JYDB",user="js_dev",password="js_dev",charset = "utf8")
    print "sql_server 数据库连接成功".decode("utf8")
    return conn
  except Exception, e:
	print "sql_server 数据库连接失败".decode("utf8"),e

def connect_oracle():

	dsn = cx_Oracle.makedsn("192.168.6.133","1521","oradb")
	conn = cx_Oracle.connect("fundsale","oracle",dsn)
	print "Oracle connect is seccussedfully "
	return conn

def searcher_oracle(quary):
  try:
	conn = connect_oracle()
	cursor = conn.cursor()
	cursor.execute(quary)
	for row in cursor:
		out = {}
		for i,col in enumerate(cursor.description):
			out[col[0]] = row[i]
		print len(out)
   		print "================= **** searching .....****========="
	conn.close()
  except Exception, e:
    print e


def searcher_sql_server(quary):
  try:
	conn = connect_sql_server()
	cursor = conn.cursor()
	cursor.execute(quary)
	print "seaching  sql_server db !"
	dict_cursor(cursor)
	conn.close()
  except Exception, e:
    print e
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
def drop_postgre_all_date():
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "DELETE FROM public_fund.company"
	cursor.execute(quary)
	conn.commit()  
	print "删除成功".decode("utf8")
	cursor.close ()  
	conn.close () 
	operation_exchange()

def searcher_postgre():
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "select * from public_fund.company"
	cursor.execute(quary)
	results_array = cursor.fetchall()
	return results_array


def operation_get_code():
  try:
    quary = "select VC_FUNDCODE from %s WHERE C_TYPE != '8'"%"TFUNDINFO"
    results = searcher_oracle_cursor_operation(quary)
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
				InnerCode_quary = "select InvestAdvisorCode from dbo.MF_FundArchives where SecurityCode ='%s'"%(str(code[0]))
				conn = connect_sql_server()
				cursor = conn.cursor()
				cursor.execute(InnerCode_quary)

				result_code = cursor.fetchall()

				print "result_code ==== ",result_code
				quary = "select * from %s where InvestAdvisorCode ='%s'"%("dbo.MF_InvestAdvisorOutline",str(result_code[0][0]))
				print quary
				searcher_sql_server(quary)


				InnerCode_quary_a = "select TrusteeCode from dbo.MF_FundArchives where SecurityCode ='%s'"%(str(code[0]))

				cursor.execute(InnerCode_quary_a)

				result_code_a = cursor.fetchall()

				print "result_code ==== ",result_code_a
				quary_a = "select * from %s where TrusteeCode ='%s'"%("dbo.MF_TrusteeOutline",str(result_code_a[0][0]))
				print quary_a
				searcher_sql_server(quary_a)

				print "\n"
			
	except Exception, e:
		print "result_array is wrong ",e        
	writer_csv()
def connect_postgre():    
    try:
        conn = psycopg2.connect(database="fund_public",user="postgres",password="yHLsoft100",host="121.40.163.105", port="5432")
        print "postgre数据库连接成功".decode("utf8")
        return conn
    except Exception, e:
        print "postgre数据库连接失败".decode("utf8")


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
	print "keys is ok "
	return string
def dict_opration_valeusTostr(dict,param):

	tuple_values = []
	for value in dict:
		if value == "None":
			value = ''
		if value == 'Null':
			value = '' 
		if value == None:
			value = ''	
		if value == param["公司类型"]:
			print value
		 	# value = value.decode("utf8")
		else: 					
			value = value.encode("latin1").decode("gbk")
		  
		tuple_values.append("'%s'"%value) 

	string =','.join(tuple_values)
	print "value is  ok ....."
	return string
def dict_cursor(cursor):
	print "changing to dict !~"
	try:
		for row in cursor:
			out = {}
			keys = []
			print "testing............................................."
			for i,col in enumerate(cursor.description):
				out[col[0]] = row[i]
		
			for x in out.keys():
				
				x = x.decode("utf8")
				keys.append(x)

			if "InvestAdvisorCode" in keys:
				
				insert_manager_data_postgre(out)
			if "TrusteeCode" in keys:
				insert_truster_data_postgre(out)	
			print "gogogogogoogogogogo"
			
	except Exception, e:
		print "sorry",e


def insert_manager_data_postgre(dict):
  print "==========================***开始插入基——金管理人***================".decode("utf8")
  try:
    conn = connect_postgre()
    cursor = conn.cursor()
    print "***************************===********************",str(dict["GeneralManager"])
    param = {
  		"公司id":str(dict["ID"]),
  		"公司类型":"管理人",
 		"公司简称":str(dict["InvestAdvisorAbbrName"]),
  		"公司全称":str(dict["InvestAdvisorName"]),
  		"公司名称编号":str(dict["InvestAdvisorCode"]),
  		"公司简介":"Null",
  		"组织形式":str(dict["OrganizationForm"]),
  		"注册资本（百万）":str(dict["RegCapital"]),
  		"成立日期":str(dict["EstablishmentDate"]),
  		"董事长":"Null",
  		"总经理":str(dict["GeneralManager"]),
  		"法人代表":str(dict["LegalRepr"]),
  		"办公地址":str(dict["OfficeAddr"]),
  		"注册地址":str(dict["RegAddr"]),
  		"联系地址":str(dict["ContactAddr"]),
 		"公司电话":str(dict["Tel"]),
  		"客服电话":str(dict["ServiceLine"]),
  		"邮编":str(dict["ZipCode"]),
  		"邮箱":str(dict["Email"]),
  		"公司网址":str(dict["WebSite"]),
  		"所属地区":str(dict["Region"]),
  		"背景介绍":str(dict["Background"]),
  		"更新日期":str(dict["XGRQ"])
      }
    tup_total = change_dict_tuple(param)  
    key_string = dict_opration_keysTostr(tup_total[0])
    values_string = dict_opration_valeusTostr(tup_total[1],param)
    print '.........................................................................'
    quary = 'insert into public_fund.company(%s) VALUES(%s)'%(key_string,values_string)
    print quary
    cursor.execute(quary)  
    conn.commit()
    print "插入管理人成功".decode("utf8")
    cursor.close()
    conn.close() 
  except Exception, e:
    print "error is ",e
    print "sorry,插入管理人失败".decode("utf8") 
def insert_truster_data_postgre(dict):
  print "==========================***开始插入基金托管人***================".decode("utf8")
  try:
    conn = connect_postgre()
    cursor = conn.cursor()
    param = {
		"公司id":str(dict["ID"]),
  		"公司类型":"托管人",
 		"公司简称":"Null",
  		"公司全称":str(dict["TrusteeName"]),
  		"公司名称编号":str(dict["TrusteeCode"]),
  		"公司简介":"Null",
  		"组织形式":"Null",
  		"注册资本（百万）":str(dict["RegCapital"]),
  		"成立日期":str(dict["EstablishmentDate"]),
  		"董事长":"Null",
  		"总经理":"Null",
  		"法人代表":str(dict["LegalRepr"]),
  		"办公地址":str(dict["OfficeAddr"]),
  		"注册地址":str(dict["RegAddr"]),
  		"联系地址":str(dict["ContactAddr"]),
 		"公司电话":"Null",
  		"客服电话":"Null",
  		"邮编":str(dict["ZipCode"]),
  		"邮箱":str(dict["Email"]),
  		"公司网址":str(dict["WebSite"]),
  		"所属地区":"Null",
  		"背景介绍":str(dict["Background"]),
  		"更新日期":str(dict["XGRQ"])
      }
    tup_total = change_dict_tuple(param)  
    key_string = dict_opration_keysTostr(tup_total[0])
    values_string = dict_opration_valeusTostr(tup_total[1],param)
    quary = 'insert into public_fund.company(%s) VALUES(%s)'%(key_string,values_string)
    print quary
    cursor.execute(quary)
    conn.commit()
    print "插入托管人成功 。。。。。。。。。。。".decode("utf8")
    conn.close() 
  except Exception, e:
    print "error is ",e
    print "sorry,插入托管人失败".decode("utf8") 
def writer_csv():
	conn = connect_postgre()
	cursor = conn.cursor()
	print "=====begine ***** writer====="
	print "=============================***插入成功***=========================".decode("utf8");
	quary_searcher = "select * from public_fund.company"
	cursor.execute(quary_searcher)
	body = cursor.fetchall()   
	header = []
	print "*******************************************"
	for i,col in enumerate(cursor.description):	
		header.append(str(col[0]))
	print "header=====",len(header)  			
	str_name = '公司.csv'  
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
	operation_exchange()




      







      



