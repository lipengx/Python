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


		quary_date = "select status from tc_process where date= '%s' and processtype = '%s'"%(dataTime,'R')
		print quary_date
		# his_process.batchdate = his_process.current_date &&his_process. processtype ='A' && his_process.processflag='2'
		conn = connect_postgre_zhongjianku()
		cursor = conn.cursor()		
		cursor.execute(quary_date)
		result = cursor.fetchall()
		print "result======================================================================================",result		
		if str(result[0][0]) == '0':
			print "开始判断更新".decode("utf8")

				
			#更新开户信息
			print "更新开户开户开户开户开户开户开户开户开户开户信息".decode("utf8")
			quary_open = "select hisrequestno,hisbatchdate,hisbusinflag,hismachinedate,hisadviserno,hisproductcode,custno,tradeacco,custtype, custname,nationality,certtype, certno,certexpire,orgno,registno,industry,businesscope,bankno,bankaccono,bankacconame,bankname,bankprovince,bankcity,riskendure,sex,birthday,mobileno,telno,faxno,email,regaddress,address,postcode,provincecode,cityno,vocation,education,income,marriage,invest,szacco,shacco,instreprname,instreprcerttype,instreprcertno,instreprcertexpire,contactname,contactcerttype,contactcertno,contactcertexpire,contactphone from tc_open_transactions where hisbatchdate = '%s'"%(str(dataTime))
			cursor.execute(quary_open)
			for row in cursor:
				out = {}
				for i,col in enumerate(cursor.description):
					out[col[0]] = row[i]
				update_open_data_postgre(out,str(dataTime))
			#更新交易
			print "更新交易交易交易交易交易交易交易交易信息".decode("utf8")

			quary_tr = "select hisrequestno, hisbatchdate,hisbusinflag,hismachinedate,hisadviserno,hisproductcode,custno,tradeacco,custname,certtype,certno,bankno,bankaccono,bankacconame,fundcode,sharetype,requestbala,moneytype,requestshare,exceedflag,otherfundcode,othersharetype,dividendmethod from tc_transactions  where hisbatchdate = '%s'"%(str(dataTime))
			cursor.execute(quary_tr)
			for row in cursor:
				out = {}
				for i,col in enumerate(cursor.description):
					out[col[0]] = row[i]
				update_tr_data_postgre(out,str(dataTime))
			print "更新rateraterateraterateraterateraterateraterate信息".decode("utf8")

			quary_rate = "select * from  public.tc_fares where reqdate = '%s'"%(str(dataTime))
			conn_z = connect_postgre_zhongjianku()
			cursor =conn_z.cursor()
			cursor.execute(quary_rate)
			for row in cursor:
				out = {}
				for i,col in enumerate(cursor.description):
					out[col[0]] = row[i]
				update_rate_data_postgre(out)	

			# cursor_z.close()
			# conn_z.close()		
			#更新his_process
			print "更新his_processhis_processhis_processhis_processhis_processhis_processhis_process信息".decode("utf8")

			update_data_postgre(str(dataTime))	
		elif result[0][0] == '1':
			print "交易指令已经同步至中间库".decode("utf8")
		else:
			print "交易指令尚未同步至中间库".decode("utf8")

		cursor.close()
		conn.close()	
	except Exception, e:
		print "get result_array is wrong ",e


def operation_exchange():
	try:
		dataTime = raw_input('date_time: ')
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
	str_temp = "text"
	for value in dict:
		print type(value)

		# if value == None:
		#  	value = 'Null'
		# if value == 'Null':
		# 	value = ' '  


		if type(str_temp) == type(value):
			value = value.decode("utf8")	
			tuple_values.append("'%s'"%value) 
		elif type(value) == type(None):
			tuple_values.append("0")
		else:
			tuple_values.append("%s"%value)
		# tuple_values.append("'%s'"%value) 
	string = ','.join(tuple_values)
	return string
def update_rate_data_postgre(dict):
	try:


		print "==========================***卫国代销系统费用更新开始***================".decode("utf8")
	
		conn = connect_oracle()
		cursor = conn.cursor()
		param = {
		"TRADEACCO":dict["tradeacco"],
		"REQUESTNO":dict["requestno"],
		"FUNDACCO":dict["fundacco"],
		"CONTRACTID":dict["contractid"],
		"SOLUTIONID":dict["solutionid"],
		"ACCAMT":dict["accamt"],	
		"DISCOUNT":dict["discount"],
		"STARTDATE":dict["startdate"],
		"ENDDATE":dict["enddate"],	
		"REQDATE":dict["reqdate"],
		"RESERVE":dict["reserve"]
		}
		tup_total = change_dict_tuple(param)  
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1])
		quary = 'insert into his_fare(%s) VALUES(%s)'%(key_string,values_string)

		print quary
		cursor.execute(quary)
		conn.commit()
		print "=============================***卫国代销系统费用更新成功***=========================".decode("utf8");
		cursor.close()
		conn.close() 
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")	
def update_tr_data_postgre(dict,dataTime):
	try:
		print "==========================***交易 交易 * 交易 交易 ****================".decode("utf8")
		conn = connect_oracle()
		cursor = conn.cursor()
		param = {

	"hisrequestno":dict["hisrequestno"],
	"hisbatchdate":dict["hisbatchdate"],
	"hisbusinflag":dict["hisbusinflag"],
	"hismachinedate":dict["hismachinedate"],
	"hisadviserno":dict["hisadviserno"],
	"hisproductcode":dict["hisproductcode"],
	"custno":dict["custno"],
	"tradeacco":dict["tradeacco"],
	"custname":dict["custname"],
	"certtype":dict["certtype"],
	"certno":dict["certno"],
	"bankno":dict["bankno"],
	"bankaccono":dict["bankaccono"],
	"bankacconame":dict["bankacconame"],
	"fundcode":dict["fundcode"],
	"sharetype":dict["sharetype"],
	"requestbala":dict["requestbala"],
	"moneytype":dict["moneytype"],
	"requestshare":dict["requestshare"],
	"otherfundcode":dict["otherfundcode"],
	"othersharetype":dict["othersharetype"],
	"acceptstatus":'3',
	# "accepttime":str(dataTime)
		}
		tup_total = change_dict_tuple(param)  
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1])
		# quary = "insert into  his_tradereq(hisrequestno, hisbatchdate,hisbusinflag,hismachinedate,hisadviserno,hisproductcode,custno,tradeacco,custname,certtype,certno,bankno,bankaccono,bankacconame,fundcode,sharetype,requestbala,moneytype,requestshare,otherfundcode,othersharetype,accepttime,acceptstatus) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(
			# dict["hisrequestno"],dict["hisbatchdate"],dict["hisbusinflag"],dict["hismachinedate"],dict["hisadviserno"],dict["hisproductcode"],dict["custno"],dict["tradeacco"],dict["custname"],dict["certtype"],dict["certno"],dict["bankno"],dict["bankaccono"],dict["bankacconame"],dict["fundcode"],dict["sharetype"],dict["requestbala"],dict["moneytype"],dict["requestshare"],dict["otherfundcode"],dict["othersharetype"],'3')
		# quary = "insert into  his_tradereq(hisrequestno, hisbatchdate,hisbusinflag,hismachinedate,hisadviserno,hisproductcode,custno,tradeacco,custname,certtype,certno,bankno,bankaccono,bankacconame,fundcode,sharetype,requestbala,moneytype,requestshare,exceedflag,otherfundcode,othersharetype,dividendmethod,accepttime,acceptstatus) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(dict["hisrequestno"],dict["hisbatchdate"],dict["hisbusinflag"],dict["hismachinedate"],dict["hisadviserno"],dict["hisproductcode"],dict["custno"],dict["tradeacco"],dict["custname"],dict["certtype"],dict["certno"],dict["bankno"],dict["bankaccono"],dict["bankacconame"],dict["fundcode"],dict["sharetype"],dict["requestbala"],dict["moneytype"],dict["requestshare"],dict["exceedflag"],dict["otherfundcode"],dict["othersharetype"],dict["dividendmethod"],str(dataTime),'3')
		quary = "insert into  his_tradereq(%s) values(%s)"%(key_string,values_string)
		#quary = "UPDATE public.tc_transactions SET hisrequestno ='%s',res_acceptstatus ='%s',res_acceptcode = '%s',res_acceptmsg = '%s',res_confirmflag = '%s'where hisrequestno = '%s'"%(str(dict["HISREQUESTNO"]).decode("GBK"),str(dict["ACCEPTSTATUS"]).decode("GBK"),str(dict["ACCEPTCODE"]).decode("GBK"),str(dict["ACCEPTMSG"]).decode("GBK"),str(dict["CONFIRMFLAG"]).decode("GBK"),str(dict["HISREQUESTNO"]).decode("GBK"))
		# quary_1 = "insert into his_process(batchdate, processtype, processflag) values(%s,'A','0')"%(dataTime)
		print quary
		cursor.execute(quary)

		conn.commit()
		print "==========================***交易 交易 * 交易 交易 okokokokokokok****================".decode("utf8")
		quary = "UPDATE his_tradereq SET accepttime =  to_date ('%s', 'YYYYMMDD') where hisrequestno = '%s'"%(str(dataTime),dict["hisrequestno"])
		# quary = "UPDATE his_accoreq SET accepttime = '%s' where hisrequestno = '%s'"%(str(dataTime),dict["hisrequestno"])
		print quary
		cursor.execute(quary)
		conn.commit()
		cursor.close()
		conn.close() 
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")	
def update_open_data_postgre(dict,dataTime):
	try:
		print "=============================***开始插入 。。。。。。。开户。。。。。~开户。。。。。。~开户***=========================".decode("utf8");
		conn = connect_oracle()
		cursor = conn.cursor()
		# param = {
		# "hisrequestno":str(dict["hisrequestno"]),
		# "hisbatchdate":str(dict["hisbatchdate"]),
		# "hisbusinflag":str(dict["hisbusinflag"]),
		# "hismachinedate":str(dict["hismachinedate"]),
		# "hisadviserno":str(dict["hisadviserno"]),
		# "hisproductcode":str(dict["hisproductcode"]),
		# "custno":str(dict["custno"]),
		# "tradeacco":str(dict["tradeacco"]),
		# "custtype":str(dict["custtype"]),
		# "custname":str(dict["custname"]),
		# "nationality":str(dict["nationality"]),
		# "certtype":str(dict["certtype"]),
		# "certno":str(dict["certno"]),
		# "certexpire":str(dict["certexpire"]),
		# "orgno":str(dict["orgno"]),
		# "registno":str(dict["registno"]),
		# "industry":str(dict["industry"]),
		# "businesscope":str(dict["businesscope"]),
		# "bankno":str(dict["bankno"]),
		# "bankaccono":str(dict["bankaccono"]),
		# "bankacconame":str(dict["bankacconame"]),
		# "bankname":str(dict["bankname"]),
		# "bankprovince":str(dict["bankprovince"]),
		# "bankcity":str(dict["bankcity"]),
		# "riskendure":str(dict["riskendure"]),
		# "sex":str(dict["sex"]),
		# "birthday":str(dict["birthday"]),
		# "mobileno":str(dict["mobileno"]),
		# "telno":str(dict["telno"]),
		# "faxno":str(dict["faxno"]),
		# "email":str(dict["email"]),
		# "regaddress":str(dict["regaddress"]),
		# "address":str(dict["address"]),
		# "postcode":str(dict["postcode"]),
		# "provincecode":str(dict["provincecode"]),
		# "cityno":str(dict["cityno"]),
		# "vocation":str(dict["vocation"]),
		# "education":str(dict["education"]),
		# "income":str(dict["income"]),
		# "marriage":str(dict["marriage"]),
		# "invest":str(dict["invest"]),
		# "szacco":str(dict["szacco"]),
		# "shacco":str(dict["shacco"]),
		# "instreprname":str(dict["instreprname"]),
		# "instreprcerttype":str(dict["instreprcerttype"]),
		# "instreprcertno":str(dict["instreprcertno"]),
		# "instreprcertexpire":str(dict["instreprcertexpire"]),
		# "contactname":str(dict["contactname"]),
		# "contactcerttype":str(dict["contactcerttype"]),
		# "contactcertno":str(dict["contactcertno"]),
		# "contactcertexpire":str(dict["contactcertexpire"]),
		# "contactphone":str(dict["contactphone"]),
		# "accepttime":str(dataTime),
		# "acceptstatus":"3"
		# }
		param = {
		"hisrequestno":dict["hisrequestno"],
		"hisbatchdate":dict["hisbatchdate"],
		"hisbusinflag":dict["hisbusinflag"],
		"hismachinedate":dict["hismachinedate"],
		"hisadviserno":dict["hisadviserno"],
		"hisproductcode":dict["hisproductcode"],
		"custno":dict["custno"],
		"tradeacco":dict["tradeacco"],
		"custtype":dict["custtype"],
		"custname":dict["custname"],
		"nationality":dict["nationality"],
		"certtype":dict["certtype"],
		"certno":dict["certno"],
		"certexpire":dict["certexpire"],
		"orgno":dict["orgno"],
		"registno":dict["registno"],
		"industry":dict["industry"],
		"businesscope":dict["businesscope"],
		"bankno":dict["bankno"],
		"bankaccono":dict["bankaccono"],
		"bankacconame":dict["bankacconame"],
		"bankname":str(dict["bankname"]),
		"bankprovince":dict["bankprovince"],
		"bankcity":dict["bankcity"],
		"riskendure":dict["riskendure"],
		"sex":dict["sex"],
		"birthday":dict["birthday"],
		"mobileno":dict["mobileno"],
		"telno":dict["telno"],
		"faxno":dict["faxno"],
		"email":dict["email"],
		"regaddress":dict["regaddress"],
		"address":dict["address"],
		"postcode":dict["postcode"],
		"provincecode":dict["provincecode"],
		"cityno":dict["cityno"],
		"vocation":dict["vocation"],
		"education":dict["education"],
		"income":dict["income"],
		"marriage":dict["marriage"],
		"invest":dict["invest"],
		"szacco":dict["szacco"],
		"shacco":dict["shacco"],
		"instreprname":dict["instreprname"],
		"instreprcerttype":dict["instreprcerttype"],
		"instreprcertno":dict["instreprcertno"],
		"instreprcertexpire":dict["instreprcertexpire"],
		"contactname":dict["contactname"],
		"contactcerttype":dict["contactcerttype"],
		"contactcertno":dict["contactcertno"],
		"contactcertexpire":dict["contactcertexpire"],
		"contactphone":dict["contactphone"],
		"acceptstatus":"3"
		}

		print "tstttttttttttttttttttttttttttt",dict["hisrequestno"]
		tup_total = change_dict_tuple(param)  
		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1])
		# quary = "insert into his_accoreq(%s) VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(key_string,dict["hisrequestno"],dict["hisbatchdate"],dict["hisbusinflag"],dict["hismachinedate"],dict["hisadviserno"],dict["hisproductcode"],dict["custno"],dict["tradeacco"],dict["custtype"],dict["custname"],dict["nationality"],dict["certtype"],dict["certno"],dict["certexpire"],dict["orgno"],dict["registno"],dict["industry"],dict["businesscope"],dict["bankno"],dict["bankaccono"],dict["bankacconame"],dict["bankname"],dict["bankprovince"],dict["bankcity"],dict["riskendure"],dict["sex"],dict["birthday"],dict["mobileno"],dict["telno"],dict["faxno"],dict["email"],dict["regaddress"],dict["address"],dict["postcode"],dict["provincecode"],dict["cityno"],dict["vocation"],dict["education"],dict["income"],dict["marriage"],dict["invest"],dict["szacco"],dict["shacco"],dict["instreprname"],dict["instreprcerttype"],dict["instreprcertno"],dict["instreprcertexpire"],dict["contactname"],dict["contactcerttype"],dict["contactcertno"],dict["contactcertexpire"],dict["contactphone"],str(dataTime),"3")

		quary = "insert into his_accoreq(%s) VALUES(%s)"%(key_string,values_string)
		print quary
		cursor.execute(quary)
		print key_string
		conn.commit()
		quary = "UPDATE his_accoreq SET accepttime =  to_date ('%s', 'YYYYMMDD') where hisrequestno = '%s'"%(str(dataTime),dict["hisrequestno"])
		# quary = "UPDATE his_accoreq SET accepttime = '%s' where hisrequestno = '%s'"%(str(dataTime),dict["hisrequestno"])
		print quary
		cursor.execute(quary)
		conn.commit()

		print "=============================***开户~开户~开户 okokokokokokoko***=========================".decode("utf8");
		cursor.close()
		conn.close() 
	except Exception, e:
		print "error is ",e
		print "sorry,插入失败".decode("utf8")
def update_data_postgre(dataTime):
	try:


		print "==========================***开始更新his_process卫国代销his_process系统***================".decode("utf8")
	
		conn = connect_oracle()
		cursor = conn.cursor()

		# tup_total = change_dict_tuple(param)  
		# key_string = dict_opration_keysTostr(tup_total[0])
		# values_string = dict_opration_valeusTostr(tup_total[1])
		# quary = 'insert into public.tc_transactions(%s) VALUES(%s)'%(key_string,values_string)

		#quary = "UPDATE public.tc_transactions SET hisrequestno ='%s',res_acceptstatus ='%s',res_acceptcode = '%s',res_acceptmsg = '%s',res_confirmflag = '%s'where hisrequestno = '%s'"%(str(dict["HISREQUESTNO"]).decode("GBK"),str(dict["ACCEPTSTATUS"]).decode("GBK"),str(dict["ACCEPTCODE"]).decode("GBK"),str(dict["ACCEPTMSG"]).decode("GBK"),str(dict["CONFIRMFLAG"]).decode("GBK"),str(dict["HISREQUESTNO"]).decode("GBK"))
		quary_1 = "insert into his_process(batchdate, processtype, processflag) values(%s,'A','0')"%(dataTime)
		quary_2 = "insert into his_process(batchdate, processtype, processflag) values(%s,'T','0')"%(dataTime)
		quary_3 =  "insert into his_process(batchdate, processtype, processflag) values(%s,'F','0')"%(dataTime)
		cursor.execute(quary_1)
		cursor.execute(quary_2)
		cursor.execute(quary_3)

		conn.commit()
		print "=============================***开始更新his_process卫国代销his_process系统成功***=========================".decode("utf8");
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




			







			



