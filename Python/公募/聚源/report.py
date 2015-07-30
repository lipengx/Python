#!/usr/bin/python
#-*-coding:UTF8-*-
import sys,os
import cx_Oracle
import psycopg2
import urllib2
import codecs 
import csv
import string
import pymssql
import time
reload(sys)            
sys.setdefaultencoding("utf8")
def searcher_sql_server(quary,innner_code,code):
  try:
	conn = connect_sql_server()
	cursor = conn.cursor()
	print "seaching  sql_server db !"
	cursor.execute(quary)
	check_exist(cursor,code)
	print "!!"
	conn.close()
  except Exception,e:
    print e
def check_exist(cursor,code):
	postgres_codes = searchr_postgre()
	print "*************************************code ===************************ ",code[0]
	print "lenght's a",len(postgres_codes)

	codes = get_array(postgres_codes)
	codes = set(codes)
	print codes
	if string.atoi(str(code)) in codes:
		print code,"已经存在 ".decode("utf8")	
		check_add(code)
	else:	
		print code,"不存在".decode("utf8")
		ross = cursor.fetchall()
		if len(ross) > 0:
			index = cursor.description
			result = []
			for res in ross:
				out = {}
				print "aaaa"
				for i in range(len(index)-1):
					out[index[i][0]] = res[i]
				insert_data_postgre(out,code)
				
				
		else:
			print code,"数据不存在".decode("utf8")	
			
			

def dict_cursor(cursor):
	result = cursor.fetchall()
	print "result lenght is  ",len(result)
	for row in cursor:
		out = {}
		for i,col in enumerate(cursor.description):
			out[col[0]] = row[i]
		print len(out)

		insert_data_postgre(out,code)

def connect_sql_server():
  try:
    conn = pymssql.connect(host ="10.0.185.138",database ="JYDB",user="js_dev",password="js_dev")
    print "sql_server 数据库连接成功".decode("utf8")
    return conn
  except Exception, e:
	print "sql_server 数据库连接失败".decode("utf8"),e
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
	quary_sql_server = "select * from %s where InnerCode = '%s' AND InfoPublDate = (select MAX(InfoPublDate) from %s where InnerCode = '%s')"%("dbo.MF_Announcement",str(code),"dbo.MF_Announcement",str(code))	
	print "===========================checking================================"
	
	print quary_sql_server
	conn_sql_server = connect_sql_server()
	cursor_sql_server = conn_sql_server.cursor()
	cursor_sql_server.execute(quary_sql_server)
	dict_sql_servere = dict_max_cursor(cursor_sql_server)

	print "=====================================oracle->postgres========================"

	quary_postgre = "select * from %s where 基金代码 = '%s' AND 公告日期 = (select MAX(公告日期) from %s where 基金代码 = '%s') "%("public_fund.report",str(code),"public_fund.report",str(code))
	conn_postgre = connect_postgre()
	cursor_postgre = conn_postgre.cursor()
	cursor_postgre.execute(quary_postgre)
	dict_postgre = dict_max_cursor(cursor_postgre)
	print "==============**************** checking_time ***************** ================= ?"
	a = dict_postgre["公告日期"]).strip()
	a = time.strptime(a,"%Y-%m-%d %H:%M:%S")
	b = time.strptime(str(dict_oracle["InfoPublDate"]),"%Y-%m-%d %H:%M:%S")
	if a == b:
		print "没有增量,不存在增量".decode("utf8")
	else:	
	# quary = "select * from TFUNDMARKET where VC_NAVDATE >%s AND VC_NAVDATE <= %s"%(dict_postgre["时间"],dict_oracle["VC_NAVDATE"]) 
		# quary = "select * from dbo.MF_Announcement where INNER_CODE ='%s' and InfoPublDate > to_date('%s','YYYY-MM-DD HH24:MI:SS') and InfoPublDate <= to_date('%s','YYYY-MM-DD HH24:MI:SS')"%(str(code),str(dict_postgre["公告日期"]),str(dict_oracle["InfoPublDate"]))
		quary = "select * from dbo.MF_Announcement where INNER_CODE ='%s' and InfoPublDate '%s' and InfoPublDate <= '%s'"%(str(code),str(dict_postgre["公告日期"]),str(dict_oracle["InfoPublDate"]))
		insert_update_data_postgre(quary,code)
	print "增量操作成功".decode（"utf8"）
	conn_oracle.close()
	conn_postgre.close()


def get_array(postgres_codes):
	codes = []
	print "code is === ",len(postgres_codes)
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
	quary = "select 基金代码 from public_fund.report"
	cursor.execute(quary)
	results_array = cursor.fetchall()
	print "本地数据库查询完毕".decode("utf8")
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
	
def operation_get_code():
	try:
		quary = "select VC_FUNDCODE from %s WHERE C_TYPE != '8'"%"TFUNDINFO"
		results = searcher_oracle_cursor_operation(quary)
		print "result lenght is == ",len(results)
		return results
	except Exception, e:
		print "searcher_oracle_cursor is wrong ",e


def operation_date_update():
	try:
		results_array = operation_get_code()
		print "results_array is",len(results_array)
		for code in results_array:
			print code
			InnerCode_quary = "select InnerCode from dbo.MF_FundArchives where SecurityCode ='%s'"%(code[0])
			conn = connect_sql_server()
			cursor = conn.cursor()
			cursor.execute(InnerCode_quary)
			result_code = cursor.fetchall()
			cursor.close()
			conn.close()
			print "result_code",result_code
			if len(result_code) > 0:
				quary = "select  * from dbo.MF_Announcement where InnerCode = '%s'"%(str(result_code[0][0]))	
				print quary
				searcher_sql_server(quary,result_code[0][0],code[0])
				print "\n"
			else:
				print "不存在与sql_server 数据库中".decode("utf8")	

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
def dict_opration_valeusTostr(dict,param):

	tuple_values = []
	for value in dict:
			
		if value == "None":
			value = ''
		if value == 'Null':
			value = '' 
			value = value.encode("latin1")	
		tuple_values.append("'%s'"%value) 

	string =','.join(tuple_values)
	return string
def dict_update_cursor(cursor,code):
	for row in cursor:
		out = {}
		for i,col in enumerate(cursor.description):
			out[col[0]] = row[i]
		print len(out)
		insert_data_postgre(out,code)	

		
	
def insert_data_postgre(dict,code):
	print "==========================***开始插入***================".decode("utf8")
	try:
		conn = connect_postgre()
		cursor = conn.cursor()
		time_now = GetNowTime()
		param = {
		"公告":str(dict["ID"]),
		"基金代码":code,
		"公告标题":str(dict["InfoTitle"]),
		"公告小标题":str(dict["SubTitle"]),
		"公告内容":"./document/"+str(dict["ID"])+".txt",
		"公告文件号":str(dict["SerialNumber"]),
		"公告类别":str(dict["Category"]),
		"公告日期":str(dict["InfoPublDate"]),
		"修改日期":str(time_now)
		}

		tup_total = change_dict_tuple(param)
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1],param)
		print "changing  ok .. ..it's just ok !`".decode("utf8")
		quary = "insert into public_fund.report (%s) values(%s)"%(key_string,values_string)		
		#quary = "insert into public_fund.devided values('quary','quary','qq','weqw','wqeqwewq')"
		print quary,"***********************************"	
		cursor.execute(quary)
		conn.commit()
		print "=====插入成功！====！loading ".decode("utf8")
		txt_writer(str(dict["Content"]).encode("latin1"),str(dict["ID"]))
		print "生成txt 文件".decode("utf8")
		cursor.close()
		conn.close() 
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")


	
def writer_csv():
	print "=====begine ***** writer====="
	conn = connect_postgre()
	cursor = conn.cursor()
	quary_searcher = "select * from public_fund.report"
	cursor.execute(quary_searcher)
	body = cursor.fetchall()
	index = cursor.description
	header = []
	print "*******************************************"
	for i,col in enumerate(cursor.description):
				
		header.append(str(col[0]))
	print "header=====",len(header)
	str_name = '公告.csv'   
	fobj = open(str_name.decode("utf8"),'wb')  
	fobj.write(codecs.BOM_UTF8)  
	writer = csv.writer(fobj)  
	writer.writerow(header)  
	writer.writerows(body)
	print "==============***写入成功***=============".decode("utf-8")
	fobj.close()  
def insert_update_data_postgre(quary,code):
	print "增量中。。。。。。。。。。。。。。。。。。。。。".decode("utf8")
	conn_oracle = connect_oracle()
	cursor_oracle = conn_oracle.cursor()
	cursor_oracle.execute(quary)
	dict_update_cursor(cursor_oracle,code)

def txt_writer(txt,ID_code):
	print "创建开始写入".decode("UTF8")
	# txt_name = "./temp/"+ fundacode + "&&"+ time +".txt"
	txt_name = "./document/" + ID_code +".txt"
	fl=open(txt_name, 'wb')	
	txt = txt.decode("gbk").encode("utf8")
	fl.write(txt) 
	fl.close()
	print "okokokokokokokokokoko"
	# f2 = open(txt_name)
	# a = f2.read()
	# print a
	# f2.close()

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




			



