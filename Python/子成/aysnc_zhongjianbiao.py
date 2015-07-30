#!/usr/bin/python
#-*-coding:UTF8-*-
import sys,os
import cx_Oracle
import psycopg2
import urllib2
import codecs 
import csv
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
        conn = psycopg2.connect(database="PASUserDB",user="postgres",password="yHLsoft100",host="121.40.163.105", port="5432")
        print "postgre数据库连接成功".decode("utf8")
        return conn
    except Exception, e:
        print "postgre数据库连接失败".decode("utf8")

def connect_postgre_zhongjianku():    
    try:
        conn = psycopg2.connect(database="zhongjianku",user="postgres",password="yHLsoft100",host="121.40.163.105", port="5432")
        print "postgre数据库连接成功".decode("utf8")
        return conn
    except Exception, e:
        print "postgre数据库连接失败".decode("utf8")		
def dict_cursor(cursor,code):
	print "changing to dict !~"
	try:
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
			print code,"不存在于数据库中=========================================".decode("utf8")


	except Exception, e:
		print "sorry",e
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

def operation_result(dataTime):
	try:
		if dataTime == "Null" or dataTime == '' or dataTime == '0':
			dataTime = GetNowTime()

		quary_check = "select status from tc_process where processtype='P' and date = '%s'"%(str(dataTime))
		print quary_check
		conn = connect_postgre_zhongjianku()
		cursor = conn.cursor()	
		cursor.execute(quary_check)
		results = cursor.fetchall()

		print "result======================================================================================",results		

		if len(results) > 0:
			print "已经完成".decode("utf8")
		else:	

			quary_date = "select * from his_process where BATCHDATE= '%s' and PROCESSFLAG = '2' and processtype in ('A','T','C','F')"%(str(dataTime))
			print quary_date
			conn = connect_oracle()
			cursor = conn.cursor()		
			cursor.execute(quary_date)
			print "============================+++++++++++++++++++++++======================".decode("utf8")
			results = cursor.fetchall()
			check_bool = 0
			if len(results) == 4:

				check_bool = 1

			if check_bool == 1:
				print "开始更新中间表".decode("utf8")
				 #开户应答
				print "开户应答开户应答开户应答开户应答 开始更新中间表".decode("utf8") 
				quary_open ="select  hisrequestno,hisbatchdate,hisbusinflag,custno,tradeacco,acceptstatus,acceptcode,acceptmsg,confirmflag  from his_accoreq where HISBATCHDATE = '%s'"%(str(dataTime))
				cursor.execute(quary_open)
				for row in cursor:
					out = {}
					for i,col in enumerate(cursor.description):
						out[col[0]] = row[i]
					update_open_data_postgre(out)
				#交易应答
				print "交易应答交易应答交易应答交易应答交易应答交易应答交易应答 开始更新中间表".decode("utf8") 

				quary_tr = "select hisrequestno,hisbatchdate,accepttime,acceptstatus,acceptcode,acceptmsg,businflag,requestno,requesttime,confirmflag from his_tradereq where hisbatchdate =%s"%(str(dataTime))
				print quary_tr
				cursor.execute(quary_tr)
				for row in cursor:
					out = {}
					for i,col in enumerate(cursor.description):
						out[col[0]] = row[i]
					update_tr_data_postgre(out)
				#交易确认
				print "交易确认交易确认交易确认交易确认交易确认交易确认交易确认交易确认交易确认 开始更新中间表".decode("utf8") 

				quary_rate = "select * from  his_tradeconfirm where confirmdate = '%s'"%(str(dataTime))
				cursor.execute(quary_rate)
				for row in cursor:
					out = {}
					for i,col in enumerate(cursor.description):
						out[col[0]] = row[i]
					update_rate_data_postgre(out)	

				#postion 交易c持仓
				print "交易c持仓 交易c持仓交易c持仓交易c持仓交易c持仓交易c持仓交易c持仓".decode("utf8")
				quary = "select * from his_staticshare where lastdate = '%s'"%(str(dataTime))
				print quary
				conn = connect_oracle()
				cursor = conn.cursor()
				cursor.execute(quary)
				for row in cursor:
					out = {}
					for i,col in enumerate(cursor.description):
						out[col[0]] = row[i]
			
					# check_dict = searcher_postgre(str(out["TRADEACCO"]),str(out["FUNDCODE"]))
					# print len(check_dict.values())
					# if len(check_dict.values()) > 0:
					# 	drop_postgre_all_date(str(out["TRADEACCO"]),str(out["FUNDCODE"]))
					# else:
						# 	print "不存在".decode("utf8")
					insert_data_position_postgre(out)

				#更新tc_process
				update_data_postgre(str(dataTime))
		
			else:
				print "代销系统指令未处理完".decode("utf8")
			cursor.close()
			conn.close()	
	except Exception, e:
		print " sorry , wrong ",e


def operation_exchange():
	try:
		dataTime = raw_input('dataTime: ')
		results_array = operation_result(dataTime)
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
		tuple_keys.append('%s'%key)
	string = ','.join(tuple_keys)
	return string
def dict_opration_valeusTostr(dict):

	tuple_values = []
	a = None
	b = "aaaa"
	for value in dict:
		# if value == "None":
		# 	value = ''
		# if value == "Null":
		# 	value = ''
		if type(a) == type(value):

			tuple_values.append("0")
		else:
			if type(value) == type(b):
				value = value.decode("GBK")
			tuple_values.append("'%s'"%value) 
		# if value != ' ' and value != 'None' and value != 'Null':

			
		# 	value = value.decode("utf8")	  
		# if value == "Null" :
		# 	tuple_values.append("%s"%value)
		# else:	
		# 	tuple_values.append("'%s'"%value) 
	string = ','.join(tuple_values)
	return string
def update_rate_data_postgre(dict):
	print "==========================***开始插入***================".decode("utf8")
	try:
		conn = connect_postgre_zhongjianku()
		cursor = conn.cursor()
	# 	param = {
	# "tacode":str(dict["TACODE"]),
	# "confirmno":str(dict["CONFIRMNO"]),
	# "confirmdate":str(dict["CONFIRMDATE"]),
	# "confirmflag":str(dict["CONFIRMFLAG"]),
	# "confirmcode":str(dict["CONFIRMFLAG"]),
	# "confirmmsg":str(dict["CONFIRMMSG"]),
	# "confirmbala":str(dict["CONFIRMBALA"]),
	# "confirmshare" :str(dict["CONFIRMSHARE"]),
	# "nav":str(dict["NAV"]),
	# "discount":str(dict["DISCOUNT"]),
	# "fare":str(dict["FARE"]),
	# "income":str(dict["INCOME"]),
	# "hisrequestno":str(dict["HISREQUESTNO"]),
	# "hisbatchdate":str(dict["HISBATCHDATE"]),
	# "hisbusinflag":str(dict["HISBUSINFLAG"]),
	# "hisadviserno":str(dict["HISADVISERNO"]),
	# "hisproductcode":str(dict["HISPRODUCTCODE"]),
	# "businflag":str(dict["BUSINFLAG"]),
	# "requestno":str(dict["REQUESTNO"]),
	# "requestdate":str(dict["REQUESTDATE"]),
	# "requesttime":str(dict["REQUESTTIME"]),
	# "machinedate":str(dict["MACHINEDATE"]),
	# "fundacco":str(dict["FUNDACCO"]),
	# "custno":str(dict["CUSTNO"]),
	# "tradeacco":str(dict["TRADEACCO"]),
	# "custname":str(dict["CUSTNAME"]),
	# "certtype":str(dict["CERTTYPE"]),
	# "certno":str(dict["CERTNO"]),
	# "fundcode":str(dict["FUNDCODE"]),
	# "sharetype":str(dict["SHARETYPE"]),
	# "requestbala":str(dict["REQUESTBALA"]),
	# "moneytype":str(dict["MONEYTYPE"]),
	# "requestshare":str(dict["REQUESTSHARE"]),
	# "exceedflag":str(dict["EXCEEDFLAG"]),
	# "otherfundcode":str(dict["OTHERFUNDCODE"]),
	# "otherconfirmshare":str(dict["OTHERCONFIRMSHARE"]),
	# "othersharetype":str(dict["OTHERSHARETYPE"]),
	# "othernav":str(dict["OTHERNAV"]),
	# "dividendmethod":str(dict["DIVIDENDMETHOD"])
 #  		}
 		param = {
	"tacode":dict["TACODE"],
	"confirmno":dict["CONFIRMNO"],
	"confirmdate":dict["CONFIRMDATE"],
	"confirmflag":dict["CONFIRMFLAG"],
	"confirmcode":dict["CONFIRMFLAG"],
	"confirmmsg":dict["CONFIRMMSG"],
	"confirmbala":dict["CONFIRMBALA"],
	"confirmshare":dict["CONFIRMSHARE"],
	"nav":dict["NAV"],
	"discount":dict["DISCOUNT"],
	"fare":dict["FARE"],
	"income":dict["INCOME"],
	"hisrequestno":dict["HISREQUESTNO"],
	"hisbatchdate":dict["HISBATCHDATE"],
	"hisbusinflag":dict["HISBUSINFLAG"],
	"hisadviserno":dict["HISADVISERNO"],
	"hisproductcode":dict["HISPRODUCTCODE"],
	"businflag":dict["BUSINFLAG"],
	"requestno":dict["REQUESTNO"],
	"requestdate":dict["REQUESTDATE"],
	"requesttime":dict["REQUESTTIME"],
	"machinedate":dict["MACHINEDATE"],
	"fundacco":dict["FUNDACCO"],
	"custno":dict["CUSTNO"],
	"tradeacco":dict["TRADEACCO"],
	"custname":dict["CUSTNAME"],
	"certtype":dict["CERTTYPE"],
	"certno":dict["CERTNO"],
	"fundcode":dict["FUNDCODE"],
	"sharetype":dict["SHARETYPE"],
	"requestbala":dict["REQUESTBALA"],
	"moneytype":dict["MONEYTYPE"],
	"requestshare":dict["REQUESTSHARE"],
	"exceedflag":dict["EXCEEDFLAG"],
	"otherfundcode":dict["OTHERFUNDCODE"],
	"otherconfirmshare":dict["OTHERCONFIRMSHARE"],
	"othersharetype":dict["OTHERSHARETYPE"],
	"othernav":dict["OTHERNAV"],
	"dividendmethod":dict["DIVIDENDMETHOD"]
  		}
		tup_total = change_dict_tuple(param)  
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1])
		quary = 'insert into public.tc_confirmations(%s) VALUES(%s)'%(key_string,values_string)
		print quary
		cursor.execute(quary)
		conn.commit()
		print "=============================***插入成功***=========================".decode("utf8");


		cursor.close()
		conn.close() 
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")
def update_tr_data_postgre(dict):
	try:


		print "==========================***交易应答   开始更新***================".decode("utf8")
	
		conn = connect_postgre_zhongjianku()
		cursor = conn.cursor()
		# quary = "UPDATE public.tc_transactions SET hisbatchdate = '%s',res_accepttime = '%s',res_acceptstatus = '%s',res_acceptcode = '%s',res_acceptmsg = '%s',hisbusinflag = '%s',res_requestno = '%s',res_requesttime = '%s',res_confirmflag = '%s' where hisrequestno = '%s'"%(str(dict["HISBATCHDATE"]).decode("GBK"),str(dict["ACCEPTTIME"]).decode("GBK"),str(dict["ACCEPTSTATUS"]).decode("GBK"),str(dict["ACCEPTCODE"]).decode("GBK"),str(dict["ACCEPTMSG"]).decode("GBK"),str(dict["BUSINFLAG"]).decode("GBK"),str(dict["REQUESTNO"]).decode("GBK"),str(dict["REQUESTTIME"]).decode("GBK"),str(dict["CONFIRMFLAG"]).decode("GBK"),str(dict["HISREQUESTNO"]).decode("GBK"))
		quary = "UPDATE public.tc_transactions SET hisbatchdate = '%s',res_accepttime = '%s',res_acceptstatus = '%s',res_acceptcode = '%s',res_acceptmsg = '%s',res_requestno = '%s',res_requesttime = '%s',res_confirmflag = '%s' where hisrequestno = '%s'"%(str(dict["HISBATCHDATE"]).decode("GBK"),str(dict["ACCEPTTIME"]).decode("GBK"),str(dict["ACCEPTSTATUS"]).decode("GBK"),str(dict["ACCEPTCODE"]).decode("GBK"),str(dict["ACCEPTMSG"]).decode("GBK"),str(dict["REQUESTNO"]).decode("GBK"),str(dict["REQUESTTIME"]).decode("GBK"),str(dict["CONFIRMFLAG"]).decode("GBK"),str(dict["HISREQUESTNO"]).decode("GBK"))
		
		print quary
		cursor.execute(quary)
		conn.commit()
		print "=============================*** 交易应答   更新成功***=========================".decode("utf8");
		cursor.close()
		conn.close() 
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")
def update_open_data_postgre(dict):
	print "==========================***开始更新 开户开户 开户 开会***================".decode("utf8")
	try:
		conn = connect_postgre_zhongjianku()
		cursor = conn.cursor()
		print "===============",dict["TRADEACCO"]
		quary = "UPDATE public.tc_open_transactions SET hisbatchdate = '%s',hisbusinflag = '%s',custno = '%s',tradeacco = '%s',acceptstatus = '%s',acceptcode = '%s',acceptmsg = '%s',confirmflag  = '%s' where hisrequestno = '%s'"%(str(dict["HISBATCHDATE"]).decode("GBK"),str(dict["HISBUSINFLAG"]).decode("GBK"),str(dict["CUSTNO"]).decode("GBK"),str(dict["TRADEACCO"]).decode("GBK"),str(dict["ACCEPTSTATUS"]).decode("GBK"),str(dict["ACCEPTCODE"]).decode("GBK"),str(dict["ACCEPTMSG"]).decode("GBK"),str(dict["CONFIRMFLAG"]).decode("GBK"))
		print quary

		cursor.execute(quary)
		conn.commit()
		print "=============================***开户开户 开户 开会  更新成功***=========================".decode("utf8");
		cursor.close()
		conn.close()  
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")
def update_data_postgre(dataTime):
	try:
		print "==========================***开始。。。。。。流程。。。确认***================".decode("utf8")
		conn = connect_postgre_zhongjianku()
		cursor = conn.cursor()
		quary =  "insert into public.tc_process(status, processtype, date) values('0','P',%s)"%(dataTime)
		print quary
		cursor.execute(quary)
		conn.commit()
		print "=============================***交易。。。。。。流程。。。。。。。。。流程。。。kookokookok**=========================".decode("utf8");
		cursor.close()
		conn.close() 
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")



def insert_data_position_postgre(dict):
	try:
		print "==========================***开始插入***================".decode("utf8")
		conn = connect_postgre_zhongjianku()
		cursor = conn.cursor()
		param = {
		"custtype":dict["CUSTTYPE"],
		"fundacco":dict["FUNDACCO"],
  		"tradeacco":dict["TRADEACCO"],
  		"fundcode":dict["FUNDCODE"],
  		"sharetype":dict["SHARETYPE"],
  		"totalshare":dict["TOTALSHARE"],
  		"frozenshare":dict["FROZENSHARE"],
  		"requestshare":dict["REQUESTSHARE"],
  		"dividendmethod":dict["DIVIDENDMETHOD"],
  		"lastdate":dict["LASTDATE"],
  		"income":dict["INCOME"],
		"frozenincome":dict["FROZENSHARE"]
  		}
		tup_total = change_dict_tuple(param)  
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1])
		quary = 'insert into public.tc_positions(%s) VALUES(%s)'%(key_string,values_string)
		print quary
		cursor.execute(quary)
		conn.commit()
		print "=============================***插入成功***=========================".decode("utf8");
		cursor.close()
		conn.close() 
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")		
def GetNowTime():
	#return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
	time_today = time.strftime("%Y%m%d ",time.localtime(time.time()))
	time_str = str(time_today)
	print time_str
	return time_str.strip()	
if __name__ == '__main__':
	operation_exchange()




			







			



