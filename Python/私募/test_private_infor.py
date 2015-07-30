#!/usr/bin/python
#-*-coding:UTF8-*-

import sys,os
import cx_Oracle
import psycopg2
import urllib2
import codecs 
import csv
import pymssql  
import time
reload(sys)            
sys.setdefaultencoding("utf-8")
def drop_postgre_all_date():
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "DELETE FROM private_fund.fund_info"
	cursor.execute(quary)
	conn.commit()  
	print "删除成功".decode("utf8")
	cursor.close ()  
	conn.close () 
	operation_exchange()
	
def search_postgre_db():
	try:
		conn = connect_postgre()
		cursor = conn.cursor()
		quary = "select * from private_fund.fund_info"
		cursor.execute(quary)
		results_array = cursor.fetchall()
		return results_array
	except Exception, e:
		print e


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
		return cursor
		conn.close()
	except Exception, e:
		print e
	
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

   		value = value.decode("gbk") 
		tuple_values.append("'%s'"%value) 

	string =','.join(tuple_values)
	return string
def insert_data_postgre(dict):
	print "==========================***开始插入***================".decode("utf8")
	try:
		conn = connect_postgre()
		cursor = conn.cursor()
		param = {
		"机构名称":str(dict["ORGNAME"]),
  		"机构介绍":"Null",
  		"机构类型":"Null",
  		"投资顾问名称":"Null",
  		"投资经理名称":str(dict["TRUST_MNG"]),
 		"投资经理介绍" :"Null",
  		"私募id":str(dict["INNER_CODE"]),
  		"嘉实产品类型" :"Null",
  		"嘉实收益类型":"Null",
  		"风险等级" :"Null",
  		"产品状态":"Null",
  		"产品名称" :str(dict["TRUST_NAME"]),
  		"产品简称" :str(dict["TRUST_SNAME"]),
  		"法律载体":"Null",
		"保管银行" :"Null",
		"投资风格":"Null",
		"投资目标":"Null",
		"投资理念":"Null",
		"投资范围":str(dict["INVEST_REMARK"]),
		"投资策略":"Null",
  		"是否结构化":str(dict["IS_STRU"]),
  		"封闭期说明":str(dict["CLOSE_REMARK"]),
  		"提交赎回申请时间":"Null",
  		"提交申购申请时间":"Null",
  		"赎回限制说明":"Null",
  		"赎回资金到帐说明":"Null",
		"初始资金募集账户":"Null",
		"初始资金募集账户开户行":"Null",
		"初始资金募集账户账号":"Null",
		"初始资金募集大额支付号":"Null",
		"开放资金募集账户":"Null",
		"开放资金募集账户开户行":"Null",
		"开放资金募集账户账号":"Null",
		"开放资金募集大额支付号":"Null",
  		"归集方":"Null",
  		"资金归集账户名称":"Null",
  		"资金归集账户开户行":"Null",
  		"资金归集账户账号":"Null",
  		"归集路径":"Null",	
  		"资金归集大额支付号":"Null",
  		"预计发行规模":str(dict["PLAN_ISS_SIZE"]),
  		"单位面值":str(dict["BOOK_VAL"]),
  		"产品资料":"Null",
  		"认购起点":str(dict["MIN_CAP"]),
  		"最小追加额" :str(dict["ADD_CAP"]),
  		"递增金额":str(dict["MIN_CAP_REMARK"]),
  		"币种":str(dict["CURNCY"]),
  		"上线日期":"Null",
  		"打款开始日期":"Null",
  		"预计打款结束日期":"Null",
  		"成立日期":str(dict["BUILD_DATE"]),
  		"认购费":"Null",
  		"固定管理费":"Null",
  		"固定投资顾问费":"Null",
 		"浮动业绩报酬":"Null",
  		"托管费" :"Null",
  		"销售服务费":"Null",
  		"赎回费":"Null",
  		"费用说明":str(dict["FEE_REMARK"]),
  		"公司前端收入比例":"Null",
  		"公司后端收入比例" :"Null",
  		"公司前端支付佣金比例":"Null",
  		"公司后端支付佣金比例":"Null",
  		"资产类别":"Null",
  		"ta代码":"Null"
		}
  		print "param [bizhong]  ====",param["币种"],param["机构名称"]
		if param["币种"] == '1':
			param["币种"] = u"人民币".encode("gbk")
		elif param["币种"] == '2':
			param["币种"] = u"港币".encode("gbk")
		elif param["币种"] == '3':
			param["币种"] = u"美元".encode("gbk")
		elif param["币种"] == '5':
			param["币种"] = u"英镑".encode("gbk")			
		tup_total = change_dict_tuple(param) 
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1])
		print len(tup_total[0]),"+++",len(tup_total[1])
		quary ="insert into private_fund.fund_info(%s) VALUES(%s)"%(key_string,values_string)
		print quary
		print "=============================*************=========================".decode("utf8");
		cursor.execute(quary)
		conn.commit()
		print "=============================***插入成功***=========================".decode("utf8");
		cursor.close()
		conn.close() 
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")


def reader_csv():
	pass

def writer_csv():
	print "=====begine ***** writer====="

	conn = connect_postgre()
	cursor = conn.cursor()
	quary_searcher = "select * from private_fund.fund_info"
	cursor.execute(quary_searcher)
	print "*******************************************"
	body = cursor.fetchall()
	
	header = []

	for i,col in enumerate(cursor.description):	
		header.append(str(col[0]))
	print "header=====",len(header)  		
	
	str_name ='私募.csv'   
	fobj = open(str_name.decode("utf8"),'wb') 
	fobj.write(codecs.BOM_UTF8)
	writer = csv.writer(fobj)
	writer.writerow(header)  
	writer.writerows(body)
	print "==============***写入成功***=============".decode("utf-8")
	fobj.close()  
	cursor.close()
	conn.close() 






def searcher_oracle_infor(fund_name):
	try:
		quary = "select * from PGENIUS.trust_profile where trust_name like'%%%s%%'"%(fund_name)
		cursor = searcher_oracle(quary)
		# for result in cursor:
		# 	print result
		# ross = cursor.fetchall()
		# print "=====???????????????==????????????????????????????======",len(ross)
		# if len(ross) > 0:	
		# 	index = cursor.description
		# 	for res in ross:
		# 		out = {}
		# 		for i in range(len(index)-1):
		# 			out[index[i][0]] = res[i]
		for row in cursor:
			out = {}
			for i,col in enumerate(cursor.description):
				out[col[0]] = row[i]				
			
			insert_data_postgre(out)		
	except Exception, e:
		print e
	 


def search_private_fund_info():
	try:
		results = search_postgre_db()
		if len(results) > 0:
			drop_postgre_all_date()
		else:	
			a = str("淡水泉").decode("utf8").encode("gbk")
			b = str("和聚").decode("utf8").encode("gbk")
			c = str("展博").decode("utf8").encode("gbk")
			d = str("鼎锋").decode("utf8").encode("gbk")
			e = str("鼎萨").decode("utf8").encode("gbk")
			f = str("理成").decode("utf8").encode("gbk")
			h = str("中国龙").decode("utf8").encode("gbk")
			searcher_array = [a,b,c,d,e,h]
			for value in searcher_array:
		
				searcher_oracle_infor(value)

			print ".............private_fund_info search ok ..... ........."
	except Exception, e:
		print e
	writer_csv()


if __name__ == '__main__':
	search_private_fund_info()




			



