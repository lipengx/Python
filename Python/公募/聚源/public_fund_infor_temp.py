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

def searcher_sql_server(quary,code):
  try:
	conn = connect_sql_server()
	cursor = conn.cursor()
	cursor.execute(quary)
	print "seaching  sql_server db !"
	dict_cursor(cursor,code)
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
	quary = "DELETE FROM public_fund.fund_infor"
	cursor.execute(quary)
	conn.commit()  
	print "删除成功".decode("utf8")
	cursor.close ()  
	conn.close () 
	operation_exchange()

def searcher_postgre():
	conn = connect_postgre()
	cursor = conn.cursor()
	quary = "select * from public_fund.fund_infor"
	cursor.execute(quary)
	results_array = cursor.fetchall()
	return results_array


def operation_get_code():
  try:
    quary = "select VC_FUNDCODE from %s WHERE C_TYPE != '8'"%"TFUNDINFO"
    results = searcher_oracle_cursor_operation(quary)
    # results = ["070028","070008","070026","070013","070012","460006","460106","460005","460003","460010"]
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
			print "begining ...............",len(results_array)
			for code in results_array:
				print 'code is =============================================',code[0]
				quary = "select * from dbo.MF_FundArchives where SecurityCode = '%s'"%(code[0])
				print quary
				searcher_sql_server(quary,code[0])		
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
	print "len(tupe_keys)",len(tupe_keys)
	print "\n"
	print "len(tup_value)",len(tup_value)
	tup_total.append(tupe_keys)
	tup_total.append(tup_value)
	return tup_total

def dict_opration_keysTostr(dict):
	tuple_keys = []
	for key in dict:
		tuple_keys.append("%s"%key)
	string = ','.join(tuple_keys)
	return string
def dict_opration_valeusTostr(param,dict):
	tuple_values = []
	values = []
	for value in param:
		if value == 'None':
			value = ''
		if value == 'Null':
			value = '' 
		if value == None:
			value = ''	

		tuple_values.append("%s"%value) 
	print "value is ok ......."

 	for a_value in tuple_values:
		
		if  a_value == dict["除权日期"] or  a_value == dict["基金简称"] or a_value == dict["权益登记日期"] or a_value == dict["分红日"] or a_value== dict["基金性质"] or a_value == dict["基金状态"] or a_value == '':
			a_value = a_value.decode("gbk")
		else :
			
			a_value = a_value.encode("latin1").decode("gbk") 
		values.append("'%s'"%a_value)
	print "string",values
	string =','.join(values)
	return string	
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
				
				insert_data_postgre(out,code)	
		else:
			print code,"不存在于巨灵数据库中==========45454545454545454545454545454545454==========================================================================".decode("utf8")
			out = {"InnerCode":"999999999999999999999"}
			insert_data_postgre_null(out,code)

	except Exception, e:
		print "sorry",e

def insert_data_postgre(dict,code):
	print "==========================***开始插入***================".decode("utf8")
	try:
		conn = connect_postgre()
		cursor = conn.cursor()
		param = {}
		print len(dict)
		if len(dict.values()) > 0:	
			param = {
			"基金代码id":code,
			"前端申购代码":str(dict["ApplyingCodeFront"]),
			"后端申购代码":str(dict["ApplyingCodeBack"]),
			"基金交易代码id":str(dict["SecurityCode"]),
			"基金经理":str(dict["Manager"]),
			"基金管理人":str(dict["InvestAdvisorCode"]),
			"基金托管人":str(dict["TrusteeCode"]),
			"注册登记机构":str(dict["RegInstCode"]),
			"保本担保机构":str(dict["Warrantor"]),

     		# "基金简称" : ""str(dict[""])"",
      		# "基金全称": str(dict[""]),
      		# "基金性质": str(dict[""]),

			# "基金简称" :out["jc_fund"]
			# "基金全称": out["vc_fundname"],
			# "基金性质": out["c_sharetype"],
			"基金简称" :"Null",
			"基金全称": "Null",
			"基金性质":str(dict["FundNature"]),
			"基金类别": str(dict["FundType"]),
			"基金投资类别" : str(dict["InvestmentType"]),
			"基金投资风格": str(dict["InvestStyle"]),
			"注册登记结构": "Null",
			"基金投资方向": str(dict["InvestOrientation"]),
			"基金投资目标": str(dict["InvestTarget"]),
			"基金投资范围": str(dict["InvestField"]),
			"风险收益特征代码": str(dict["RiskReturnCode"]),
			"风险收益特征": str(dict["RiskReturncharacter"]),
			"收益分配原则": str(dict["ProfitDistributionRule"]),
			"基金简介":str(dict["BriefIntro"]),
			"基金规模" : str(dict["FoundedSize"]),
       		# "基金状态": str(dict["c_state"]),
       		"基金状态":"Null",
			"基金转换状态":"Null",
			"定期定额状态":"Null",
			"转托管状态":"Null",
			"赎回到帐天数": str(dict["DeliveryDays"]),
			"成立日期":str(dict["EstablishmentDate"]),
			"上市日期": str(dict["ListedDate"]),

			"基金募集开始日期":"Null",
			"基金募集结束日期":"Null",

			"续存期年限": str(dict["Duration"]),
			"存续期起始日": str(dict["StartDate"]),
			"存续期截至日": str(dict["ExpireDate"]),

			# "分红日": str(dict[""]),
			# "分红方式": str(dict[""]),
			# "权益登记日期": str(dict[""]),
			# "除权日期" : str(dict[""]),
			# "结算币种": str(dict[""]),
			"分红日":"Null",
			"分红方式":"Null",
			"权益登记日期":"Null",
			"除权日期" :"Null",
			"结算币种":"Null",

			"货币基金结转日" : str(dict["CarryOverDate"]),
			"货币基金结转说明": str(dict["CarryOverDateRemark"]),
			"货币基金收益分配方式":str(dict["CarryOverType"]),
			"资产类别":"Null",
			"tacode":"Null"
			
			}
		else:
			
			conn = connect_postgre()
			cursor = conn.cursor()
			param = {

			"基金代码id":code ,
			"前端申购代码":"Null",
			"后端申购代码":"Null",
			"基金交易代码id":"Null",
			"基金经理": "Null",
			"基金管理人":"Null",
			"基金托管人" :"Null",
			"注册登记机构":"Null",
			"保本担保机构" :"Null",

     		# "基金简称" : ""str(dict[""])"",
      		# "基金全称": str(dict[""]),
      		# "基金性质": str(dict[""]),

			# "基金简称" :out["jc_fund"]
			# "基金全称": out["vc_fundname"],
			# "基金性质": out["c_sharetype"],
			"基金简称":"Null",
			"基金全称":"Null",
			"基金性质":"Null",
			"基金类别":"Null",
			"基金投资类别" :"Null",
			"基金投资风格":"Null",
			"注册登记结构": "Null",
			"基金投资方向":"Null",
			"基金投资目标":"Null",
			"基金投资范围":"Null",
			"风险收益特征代码":"Null",
			"风险收益特征":"Null",
			"收益分配原则":"Null",
			"基金简介" :"Null",
			"基金规模" :"Null",
       	 # "基金状态": str(dict["c_state"]),
        	"基金状态": "Null",
			"基金转换状态":"Null",
			"定期定额状态" :"Null",
			"转托管状态" : "Null",
			"赎回到帐天数":"Null",
			"成立日期" :"Null",
			"上市日期":"Null",

			"基金募集开始日期":"Null",
			"基金募集结束日期":"Null",

			"续存期年限": "Null",
			"存续期起始日":"Null",
			"存续期截至日":"Null",

			# "分红日": str(dict[""]),
			# "分红方式": str(dict[""]),
			# "权益登记日期": str(dict[""]),
			# "除权日期" : str(dict[""]),
			# "结算币种": str(dict[""]),
			"分红日":"Null",
			"分红方式":"Null",
			"权益登记日期":"Null",
			"除权日期" :"Null",
			"结算币种":"Null",

			"货币基金结转日" : "Null",
			"货币基金结转说明":"Null",
			"货币基金收益分配方式":"Null",
			"资产类别":"Null",
			"tacode":"Null"
			}	
		print "adding ................................................."
		quary_o = "select * from TFUNDMARKET where VC_FUNDCODE ='%s'"%str(dict["SecurityCode"])
		quary_o_1 = "select * from TFUNDINFO where VC_FUNDCODE = '%s'"%(code)
		print quary_o
		conn_o = connect_oracle()
		cursor_o = conn_o.cursor()
		cursor_o.execute(quary_o)
		out = {}
		for row in cursor_o:
			
			for i,col in enumerate(cursor_o.description):
				out[col[0]] = row[i]

		print "out=====",len(out)
		cursor_o.execute(quary_o_1)

		cur_out = {}

		for row_1 in cursor_o:
			for i,col in enumerate(cursor_o.description):
				cur_out[col[0]] = row_1[i]	
		print quary_o_1				
		if len(out) >0:
			if "C_STATE" in out.keys():
				param["基金状态"] = out["C_STATE"]
			else:
				param["基金状态"] = "Null"
			# print "============************=========",out["c_state"]
			param["基金简称"] = out["VC_FUNDNAME"]
			# param["基金性质"] = out["C_SHARETYPE"]
			# param["基金状态"] = out["C_STATE"]
			param["结算币种"] = cur_out["C_MONEYTYPE"]
			param["tacode"] = cur_out["VC_TACODE"]
			connect_sql = connect_sql_server()
			cursor_sql = connect_sql.cursor()
			quary_s2 = "select MAX(InfoPublDate) from dbo.MF_Dividend where InnerCode = '%s'"%(str(dict["InnerCode"]))
			cursor_sql.execute(quary_s2)
			result_get = cursor_sql.fetchall()

			print "result_get================",result_get[0][0]
			if result_get[0][0] != 'None' and result_get[0][0] != None:			
				quary_s = "select * from dbo.MF_Dividend where InnerCode = '%s' AND infoPublDate = '%s'"%(str(dict["InnerCode"]),str(result_get[0][0]))
				print "quary_s ==== is",quary_s
				cursor_sql.execute(quary_s)
		
				out_s = {}
				for row in cursor_sql:
			
					for i,col in enumerate(cursor_sql.description):
						out_s[col[0]] = row[i]
				print "out_s =====",len(out_s) 
			
					
				param["分红日"] = str(out_s["InfoPublDate"])
				param["分红方式"] = "Null"
				param["权益登记日期"] = str(out_s["ReDate"])
				param["除权日期"] = str(out_s["ExRightDate"])
				
			else:
				print "devided 不存在 InnerCode",str(dict["InnerCode"])
		else:
			print "没有增量".decode("utf8")




		InnerCode_quary = "select * from dbo.SecuMain where InnerCode = '%s'"%(str(dict["InnerCode"]))
		conns_sql = connect_sql_server()
		cursors_sql = conns_sql.cursor()
		cursors_sql.execute(InnerCode_quary)
		print "*******************",InnerCode_quary
		for row in cursors_sql:
			out_sql = {}
			for i,col in enumerate(cursors_sql.description):		
				out_sql[col[0]] = row[i]	
			param["基金全称"] = out_sql["ChiName"]
			# param["基金简称"] = out_sql["ChiNameAbbr"]

		cursors_sql.close()
		conns_sql.close()	
		print "changing .............................................to  str"	
		tup_total = change_dict_tuple(param) 

		key_string = dict_opration_keysTostr(tup_total[0])
		values_string = dict_opration_valeusTostr(tup_total[1],param)
		print "==============================******************==================="
		quary = "insert into public_fund.fund_infor (%s) values(%s)"%(key_string,values_string)
		print quary
		cursor.execute(quary)
		conn.commit()
		print "=============================***插入成功***=========================".decode("utf8");
		cursor.close()
		conn.close() 
		cursor_o.close()
		conn_o.close()


	except Exception, e:
		print "error is ",e
		print "sorry,插入失败**************************************************************************************************************************************************************88".decode("utf8")
 
def insert_data_postgre_null(dict_temp,code):
	print "&&&&&&&&&&&&&&&&&&&&&&&&&*************************&&&&&&&&&&&&&&&&&&&&&&&&&&"
	print "插入不存在".decode("utf8")
	conn = connect_postgre()
	cursor = conn.cursor()
 	param = {
		"基金代码id":code,
		"前端申购代码":"Null",
		"后端申购代码":"Null",
		"基金交易代码id":"Null",
		"基金经理": "Null",
		"基金管理人":"Null",
		"基金托管人" :"Null",
		"注册登记机构":"Null",
		"保本担保机构" :"Null",
		"基金简称" :"Null",
		"基金全称":"Null",
		"基金性质":"Null",
		"基金类别":"Null",
		"基金投资类别" :"Null",
		"基金投资风格":"Null",
		"注册登记结构": "Null",
		"基金投资方向":"Null",
		"基金投资目标":"Null",
		"基金投资范围":"Null",
		"风险收益特征代码":"Null",
		"风险收益特征":"Null",
		"收益分配原则":"Null",
		"基金简介" :"Null",
		"基金规模" :"Null",
       	 # "基金状态": str(dict["c_state"]),
        "基金状态": "Null",
		"基金转换状态":"Null",
		"定期定额状态" :"Null",
		"转托管状态" : "Null",
		"赎回到帐天数":"Null",
		"成立日期" :"Null",
		"上市日期":"Null",

		"基金募集开始日期":"Null",
		"基金募集结束日期":"Null",

		"续存期年限": "Null",
		"存续期起始日":"Null",
		"存续期截至日":"Null",
		"分红日":"Null",
		"分红方式":"Null",
		"权益登记日期":"Null",
		"除权日期" :"Null",
		"结算币种":"Null",

		"货币基金结转日" : "Null",
		"货币基金结转说明":"Null",
		"货币基金收益分配方式":"Null",
		"资产类别":"Null",
		"tacode":"Null"
		}
	print "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
 	quary_o = "select * from TFUNDMARKET where VC_FUNDCODE ='%s'"%str(code)
	quary_o_1 = "select * from TFUNDINFO where VC_FUNDCODE = '%s'"%str(code)
	print quary_o
	conn_o = connect_oracle()
	cursor_o = conn_o.cursor()
	cursor_o.execute(quary_o)
	out = {}
	for row in cursor_o:
			
		for i,col in enumerate(cursor_o.description):
			out[col[0]] = row[i]


	cursor_o.execute(quary_o_1)

	cur_out = {}

	for row_1 in cursor_o:
		for i,col in enumerate(cursor_o.description):
			cur_out[col[0]] = row_1[i]	

	if len(out) >0:
		# print "============************=========",out["c_state"]
		param["基金简称"] = out["VC_FUNDNAME"]
		# param["基金性质"] = out["C_SHARETYP"]
		param["基金状态"] = out["C_STATE"]
		param["结算币种"] = cur_out["C_MONEYTYPE"]
		param["tacode"] = cur_out["VC_TACODE"]
	else:
		print "没有数据".decode("utf8")	
# #从主函数中取出名称简称
# 	InnerCode_quary = "select * from dbo.SecuMain where InnerCode = '%s'"%(str(dict["InnerCode"]))
# 	conns_sql = connect_sql_server()
# 	cursors_sql = conns_sql.cursor()
# 	cursors_sql.execute(InnerCode_quary)
# 	print "*******************",InnerCode_quary
# 	for row in cursors_sql:
# 		out_sql = {}
# 		for i,col in enumerate(cursors_sql.description):		
# 			out_sql[col[0]] = row[i]	
# 		param["基金全称"] = out_sql["ChiName"]
# 		param["基金简称"] = out_sql["ChiNameAbbr"]

	print "changing .............................................to  str"	
	# cursors_sql.close()
	# conns_sql.close()	
	tup_total = change_dict_tuple(param) 
	key_string = dict_opration_keysTostr(tup_total[0])
	values_string = dict_opration_valeusTostr(tup_total[1],param)
	print "==============================******************==================="
	quary = "insert into public_fund.fund_infor (%s) values(%s)"%(key_string,values_string)
	print quary
	cursor.execute(quary)
	conn.commit()
	print "=============================**Null ..........*插入成功***=========================".decode("utf8");
	cursor.close()
	conn.close() 


def writer_csv():
	print "=====begine ***** writer====="

	conn = connect_postgre()
	cursor = conn.cursor()
	quary_searcher = "select * from public_fund.fund_infor"
	cursor.execute(quary_searcher)
	body = cursor.fetchall()
	
	header = []
	print "*******************************************"
	for i,col in enumerate(cursor.description):	
		header.append(str(col[0]))
	print "header=====",len(header)  		
	
	str_name ='公募.csv'   
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




      







      



