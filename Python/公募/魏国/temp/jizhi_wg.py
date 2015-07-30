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

	dsn = cx_Oracle.makedsn("192.168.6.133","1521","oradb")
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
	quary_oracle = "select * from %s where VC_FUNDCODE = '%s' AND VC_NAVDATE = (select MAX(VC_NAVDATE) from %s where VC_FUNDCODE = '%s')"%("TFUNDMARKET",str(code),"TFUNDMARKET",str(code))	
	print " ===========================*checking*================================ "
	print quary_oracle
	conn_oracle = connect_oracle()
	cursor_oracle = conn_oracle.cursor()
	cursor_oracle.execute(quary_oracle)
	dict_oracle = dict_max_cursor(cursor_oracle)

	print "=====================================oracle->postgres========================"

	quary_postgre = "select * from %s where 产品id = '%s' AND 时间 = (select MAX(时间) from %s where 产品id = '%s') "%("public_fund.wg_fund_jinzhi",str(code),"public_fund.wg_fund_jinzhi",str(code))
	conn_postgre = connect_postgre()
	cursor_postgre = conn_postgre.cursor()
	cursor_postgre.execute(quary_postgre)
	dict_postgre = dict_max_cursor(cursor_postgre)


	print "============================********checking_time*******================= "
	a = str(dict_postgre["时间"]).strip()
	# a = time.strptime(a,"%Y-%m-%d %H:%M:%S")
	a = time.strptime(a,"%Y%m%d")
	print "aaaaaaaaaaaaaaaaaaaaaaaaa",a
	b = time.strptime(dict_oracle["VC_NAVDATE"],"%Y%m%d")
	print "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",b
	print "******** compare to time *******"
	if a == b:
		print "没有增量,不存在增量".decode("utf8")
	else:
		print "有增量,增量查找中".decode("utf8")
		# quary = "select * from TFUNDMARKET where vc_fundcode ='%s' and vc_navdate > '%s' and vc_navdate <= to_date('%s','YYYY-MM-DD HH24:MI:SS')"%(str(code),a,str(dict_oracle["VC_NAVDATE"]))
		quary = "select * from TFUNDMARKET where vc_fundcode ='%s' and vc_navdate > '%s' and vc_navdate <= '%s'"%(str(code),str(dict_postgre["时间"]).strip(),str(dict_oracle["VC_NAVDATE"]))

		print quary
		insert_update_data_postgre(quary)
	print "增量操作成功".decode("utf8")
	conn_oracle.close()
	conn_postgre.close()


def check_exist(cursor,code):
	postgres_codes = searchr_postgre()
	print "**********************code ===************************",code
	codes = get_array(postgres_codes)
	codes = set(codes)
	print str(code)
	if str(code) in codes:
		print code,"已经存在".decode("utf8")	
		check_add(code)
	else:	
		print code,"不存在".decode("utf8")
		for row in cursor:
			print "增量生成中".decode("utf8")
			out = {}
			for i,col in enumerate(cursor.description):
				out[col[0]] = row[i]
			print len(out)

			insert_data_postgre(out)
		else:
			print "没有数据".decode("utf8")
		
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
	quary = "select 产品id from public_fund.wg_fund_jinzhi"
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
		# results = ["078001","078002","461012","0H0401","0H0402","0H0403","0H0404"]
		# return results
		quary = "select VC_FUNDCODE from %s WHERE C_TYPE != '8'"%"TFUNDINFO"
		results = searcher_oracle_cursor_operation(quary)
		return results
	except Exception, e:
		print "result_array is wrong ",e


def operation_date_update():
	try:
		results_array = operation_get_code()
		for code in results_array:
			print code
			quary = "select  * from %s where VC_FUNDCODE = '%s'"%("TFUNDMARKET",code[0])	
			print quary
			searcher_oracle(quary,code[0])
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
			value = ''
		if value == 'Null':
			value = ''
		tuple_values.append("'%s'"%value) 
	string =','.join(tuple_values)

	return string

def dict_update_cursor(cursor):
	try:
		print ".....++++++++++++........"
		for row in cursor:
			print "增量生成中".decode("utf8")
			out = {}
			for i,col in enumerate(cursor.description):
				out[col[0]] = row[i]
			print len(out)

			insert_data_postgre(out)	
	except Exception, e:
		print e,"没有数据".decode("utf8")
	# try:
	# 	ross = cursor.fetchall()
	# 	print ross
	# 	if len(ross) > 0:		
	# 		index = cursor.description
	# 		for res in ross:
	# 			out = {}
	# 			for i in range(len(index)-1):
	# 				out[index[i][0]] = res[i]
	# 			print "out ==************===**************************************=====",len(out)
	# 			insert_data_postgre(out)
	# 	else:
	# 		print "没有数据".decode("utf8")			
	# except Exception, e:
	# 	 print e


	
def insert_data_postgre(dict):
	print "==========================***开始插入***================".decode("utf8")
	print "lenght of dict",dict.keys()
	try:
		conn = connect_postgre()
		cursor = conn.cursor()
		time_now = GetNowTime()
		print dict.keys()
		param = {
		"产品id":str(dict["VC_FUNDCODE"]),
		"单位净值":str(dict["EN_NAV"]),
		"单位累计净值":str(dict["EN_TOTALNAV"]),
		"时间":str(dict["VC_NAVDATE"]),
		"基金收益":str(dict["EN_FUNDINCOME"]),
		"基金收益率":str(dict["EN_FUNDINCOMERATIO"]),
		"万分基金单位收益率":str(dict["EN_FUNDINCOMEUNIT"]),
		"修改时间":str(time_now)
  		}
		tup_total = change_dict_tuple(param)  
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1])
		quary = 'insert into public_fund.wg_fund_jinzhi(%s) VALUES(%s)'%(key_string,values_string)
		print quary
		cursor.execute(quary)
		conn.commit()

		print "======================插入成功".decode("utf8")

		cursor.close()
		conn.close() 
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")


	
def writer_csv():
	conn = connect_postgre()
	cursor = conn.cursor()
	time_str = compare_time()
	quary_searcher = "select * from public_fund.wg_fund_jinzhi where 修改时间 >= '%s'"%time_str
	cursor.execute(quary_searcher)
	body = cursor.fetchall()
	header_array = []
	print "*******************************************"
	for i,col in enumerate(cursor.description):
				
		header_array.append(str(col[0]))
	print "header=====",len(header_array)

	str_name = '卫国_基金净值.csv'   
	fobj = open(str_name.decode("utf8"),'wb') 
	fobj.write(codecs.BOM_UTF8)  
	writer = csv.writer(fobj)  
	writer.writerow(header_array)  
	writer.writerows(body)
	print "==============***写入成功***=============".decode("utf-8")

	fobj.close()  
	cursor.close()
	conn.close() 
def insert_update_data_postgre(quary):
	print "增量增加中...".decode("utf8")
	conn_oracle = connect_oracle()
	cursor_oracle = conn_oracle.cursor()
	cursor_oracle.execute(quary)
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
