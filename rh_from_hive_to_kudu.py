#!/usr/bin/python
# -*- coding: utf-8 -*-
#author:
#date:2018-08-22
#**
# ////////////////////////////////////////////////////////////////////
# //                          _ooOoo_                               //
# //                         o8888888o                              //
# //                         88" . "88                              //
# //                         (| ^_^ |)                              //
# //                         O\  =  /O                              //
# //                      ____/`---'\____                           //
# //                    .'  \\|     |//  `.                         //
# //                   /  \\|||  :  |||//  \                        //
# //                  /  _||||| -:- |||||-  \                       //
# //                  |   | \\\  -  /// |   |                       //
# //                  | \_|  ''\---/''  |   |                       //
# //                  \  .-\__  `-`  ___/-. /                       //
# //                ___`. .'  /--.--\  `. . ___                     //
# //              ."" '<  `.___\_<|>_/___.'  >'"".                  //
# //            | | :  `- \`.;`\ _ /`;.`/ - ` : | |                 //
# //            \  \ `-.   \_ __\ /__ _/   .-` /  /                 //
# //      ========`-.____`-.___\_____/___.-`____.-'========         //
# //                           `=---='                              //
# //      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^        //
# //         佛祖保佑    			再无Bug				            //
# ////////////////////////////////////////////////////////////////////
# User:=
# Date:2018/8/20 0018
# 从离线hive库到kudu
#/
import re
import kudu
from kudu.client import Partitioning
import kudu.client

import json 
import sys
import os
import time
import datetime
import rh_hive_conn
import rh_kudu_conn
import rh_impala_conn

import realtime_data_handler
import rh_ods_rt_conn

#cd /data/etlscript/CMRH_ODS/SCRIPT
class  HiveOper():
	"""docstring for  """
	def __init__(self, ip, port, user, password, db, tbl):
		self.ip = ip
		self.port = int(port)
		self.authMechanism = 'PLAIN'
		self.user = user
		self.password = password
		self.db = db
		self.tbl = tbl 
		self.get_hive_table_sql = "describe %s.%s " %(self.db ,self.tbl)
		self.hive_client = rh_hive_conn.HiveClient(self.ip , self.port ,self.authMechanism ,self.user ,self.password ,self.db)

	def execute_hive_command(self,method_name,sql):
		print self.hive_client

		if  method_name == 'execute_hive_sql_no_return':
			try:
				self.hive_client.execute_hive_sql_no_return(sql)
			except:
				print "执行 execute_hive_sql_no_return 命令失败！"

		elif method_name == 'get_hive_all_result':
			try:
				print sql
				print self.ip , self.port,self.user,self.password,self.get_hive_table_sql
				return self.hive_client.get_hive_all_result(sql)
			except:
				print "执行 get_hive_all_result 命令失败！"			

		elif method_name == 'get_hive_one_result':
			try:
				return self.hive_client.get_hive_one_result(sql)
			except:
				print "执行 get_hive_one_result 命令失败！"		

	def get_hive_table_structure(self):
		hive_table_structure = self.execute_hive_command('get_hive_all_result', self.get_hive_table_sql)
		return hive_table_structure
		

class KuduOper():
	def __init__(self, host, port, tbl):
		self.host = host 
		self.port = int(port)
		self.tbl = tbl 
		self.kudu_client = rh_kudu_conn.KuduClient(host = self.host ,port = self.port,table_name = self.tbl)

	def get_kudu_table_structure(self):
		kudu_table_structure = self.kudu_client.get_kudu_table_structure()
		return kudu_table_structure

	def del_kudu_table(self):
		return self.kudu_client.del_kudu_table(self.tbl)

class CheckHiveKuduTbl():
	"""docstring for CheckHiveKuduTbl"""
	def __init__(self, hive_table_instance, kudu_table_structure):
		self.hive_table_instance = hive_table_instance
		self.kudu_table_structure = kudu_table_structure

#primary key转化：hive 类型映射到kudu 类型
class HiveColToKuduCol():
	def __init__(self,srcTable, hive_table_structure):
		self.srcTable = srcTable
		self.hive_table_structure = hive_table_structure

	def change_column_data_type(self, col_date_tuple):
		kudu_data_tuple = ()

		if col_date_tuple[1].find('varchar') != -1 or col_date_tuple[1].find('date') != -1 or col_date_tuple[1].find('timestamp') != -1 or col_date_tuple[1].find('string') != -1:
			kudu_data_tuple = (col_date_tuple[0], 'kudu.string')

		elif col_date_tuple[1].find('int') != -1:
			kudu_data_tuple = (col_date_tuple[0], 'kudu.int32')

		elif col_date_tuple[1].find('float') != -1:
			kudu_data_tuple = (col_date_tuple[0], 'kudu.float')

		elif col_date_tuple[1].find('double') != -1:
			kudu_data_tuple = (col_date_tuple[0], 'kudu.double')

		elif col_date_tuple[1].find('decimal') != -1:
			kudu_data_tuple = (col_date_tuple[0], 'kudu.decimal')			

		elif col_date_tuple[1].find('bool') != -1:
			kudu_data_tuple = (col_date_tuple[0], 'kudu.bool')	

		elif col_date_tuple[1].find('binary') != -1:
			kudu_data_tuple = (col_date_tuple[0], 'kudu.binary')	

		return kudu_data_tuple

	def change_hive_col_to_kudu_primary_key(self,unchanged_key_list):
		
		key_list = []
		unfind_key_list = []

		for pk in unchanged_key_list:
			pk_status = 0

			for hive_col in self.hive_table_structure:
				if pk == hive_col[0] and len(hive_col[0]) > 0 and len(pk) > 0 :
					pk_status = 1
					key_tuple = self.change_column_data_type((hive_col[0], hive_col[1]))
					key_list.append(key_tuple)

			if pk_status == 0 :
				print  "=========提交的kudu  %s 不在hive表里================"%(hive_col[0])
				unfind_key_list.append(pk)


		if len(unfind_key_list) == 0 :
			print '=================================== unfind_key_list ==================================='
			print  unfind_key_list
			return key_list

	def change_hive_col_to_kudu_key(self, key_tuple_list):
		kudu_key_tuple_list = []
		for key_tuple in key_tuple_list:
			changed_key_tuple = self.change_column_data_type(key_tuple)
			kudu_key_tuple_list.append(changed_key_tuple)

		return kudu_key_tuple_list

	def change_oracleT_to_kuduT(self,oracle_t, oracle_t_reg):
		oracle_t = str(oracle_t) 
		
		r1 = re.compile(oracle_t_reg)
		time_group1 = r1.search(oracle_t)

		#把oracle时间中的空格替换成T
		oracle_to_kudu = re.compile(r'\s')
		kudu_time = re.sub(oracle_to_kudu, 'T', time_group1.group())

		oracle_to_kudu = re.compile(r'\w+\.')
		time_group2 = oracle_to_kudu.search(kudu_time)

		oracle_to_kudu = re.compile(r'\.\w+')
		time_group3 = oracle_to_kudu.search(kudu_time)

		return kudu_time


if __name__ == "__main__":

	#多个partitioning name 字段需要用,隔开
	#srcTable = {'ip':'xx.xx.xx.xx','port':'10001','db':'impala_kudu_cmrh','tbl':'bas_bancass_yinbao_2','impala_port':21050,'impala_db':'impala_kudu_cmrh','impala_tbl':'kudu_bancass_yinbao_2','user':'linkx001','password':'Pass@word1','primary_key':'policy_no,product_code,prem_actural_date,etl_time','partitioning_name':'etl_time','num_buckets':'8'}
	
	#kudu_table=srcTable['db']+'_'+srcTable['tbl']
	#destTable = {'ip':['xx.xx.xx.xx','xx.xx.xx.xx','xx.xx.xx.xx'],'tbl':kudu_table,'port':7051}
	#print destTable['ip']
	#print type(destTable['ip'])
	
	with open('rl_from_hive_to_kudu.txt') as f:

		for i  in f.read().split('\n'):
			srcTable = json.loads( i[i.find('\"srcTable\":')+11:i.find('\"destTable\":')-1], strict=False)
			print srcTable
			print 

			kudu_table=srcTable['tbl']
			print kudu_table

			print i[i.find('\"destTable\":') + 12 : len(i)-1 ]
			destTable = json.loads( i[i.find('\"destTable\":') + 12 : len(i)-1], strict=False )
			destTable['ip'] = destTable['ip'][1:-1].replace('"', '\'')
			print destTable['ip']
			print 

			destTable['ip'] = destTable['ip'].split(',')
			print destTable['ip']
			print 

			#得到kudu的表结构
			kudu_instance = KuduOper(destTable['ip'], destTable['port'], destTable['tbl'])
			kudu_table_structure = kudu_instance.get_kudu_table_structure()
			print "=================kudu_table_structure==================="
			print kudu_table_structure

			#删除KUDU表
			#is_delete_or_not = kudu_instance.del_kudu_table()
			#print "=================is_delete_or_not==================="
			#print is_delete_or_not

			#得到hive的表结构
			hive_instance = HiveOper(srcTable['ip'],srcTable['port'], srcTable['user'],srcTable['password'], srcTable['db'], srcTable['tbl'])
			hive_table_structure = hive_instance.get_hive_table_structure()
			print "=================hive_table_structure==================="
			print hive_table_structure


			#将指定的hive 主键 转化为kudu类型的主键
			primary_key_list = srcTable['primary_key'].split(',')
			print "=================primary_key_list==================="
			print primary_key_list
			print 


			hive_to_kudu_instance = HiveColToKuduCol(srcTable, hive_table_structure)
			kudu_primary_key_data = hive_to_kudu_instance.change_hive_col_to_kudu_primary_key(primary_key_list)
			print "=================kudu_primary_key_data==================="
			print kudu_primary_key_data


			#将hive的字段转化为kudu类型的字段
			key_list = []
			hive_table_structure_tmp = []

			for hive_tbl in hive_table_structure:


				is_primary = False
				key_list.append((hive_tbl[0], hive_tbl[1]))


				for pk_data in kudu_primary_key_data:

					if hive_tbl[0] == pk_data[0]:
						is_primary = True


				if is_primary == False:
					hive_table_structure_tmp.append((hive_tbl[0], hive_tbl[1],0))
				else:
					hive_table_structure_tmp.append((hive_tbl[0], hive_tbl[1],1))


			hive_table_structure = hive_table_structure_tmp
			print hive_table_structure


			kudu_key_tuple_list = hive_to_kudu_instance.change_hive_col_to_kudu_key(key_list)
			kudu_primary_key_data_list = []


			hive_concat_str = "SELECT CONCAT_WS(',' ,"
			move_hive_data_sql = 'insert overwrite table %s.%s select '%(srcTable['db'],srcTable['tbl']+'_tmp')


			for pk_data in kudu_primary_key_data:

				for kk_data in kudu_key_tuple_list:

					try:
						if pk_data[0] == kk_data[0] and len(pk_data[0]) >0 and len(kk_data[0]) >0 :

							if pk_data[1].find('decimal') != -1 :


								kk_data = (kk_data[0], 'kudu.string')
								kudu_primary_key_data_list.append(kk_data)

							else:
								kudu_primary_key_data_list.append(kk_data)

					except:
						pass


			for pk_data in kudu_primary_key_data:
				kudu_key_tuple_list.remove(pk_data)


			print "===============kudu_primary_key_data_list=================="
			print kudu_primary_key_data_list


			print "=================kudu_key_tuple_list==================="
			print  kudu_key_tuple_list


			partitioning_name = srcTable['partitioning_name']
			num_buckets = srcTable['num_buckets']
			table_name = destTable['tbl']


			#如果目的kudu数据库还未建表，则先建表；如果已建表，则对比两个表字段是否一致，如果不一直则备份、删除原表、新建与hive一样表
			kudu_client = rh_kudu_conn.KuduClient(destTable['ip'],destTable['port'],table_name)
			if  len(kudu_table_structure) == 0 and len(hive_table_structure) > 0:
				kudu_client.create_kudu_table(kudu_primary_key_data_list, kudu_key_tuple_list,partitioning_name,num_buckets)
				print "=============建表成功======================"
			else:

				unfind_kudu_col = []

				for kudu_col in kudu_table_structure:
					find = 0
					for hive_col in hive_table_structure:

						if kudu_col[0] == hive_col[0]:
							find = 1


					if find == 0:
						unfind_kudu_col.append(kudu_col)

				print "==============unfind_kudu_col========================"
				print unfind_kudu_col
				print  '\n'


				unfind_hive_col = []

				for hive_col in hive_table_structure:
					find = 0

					for kudu_col in kudu_table_structure:
						if kudu_col[0] == hive_col[0]:
							find = 1


					if find == 0:
						unfind_hive_col.append(hive_col)
				print "==============unfind_hive_col========================"
				print unfind_hive_col
				print  '\n'

			#if   len(unfind_kudu_col) > 0 or len(unfind_hive_col) > 0 :
			#首先把旧的kudu表备份出来，然后删掉原表，再新建新表kudu
			#print kudu_client.delete_kudu_table(table_name)
			#删除
			#直接从hive同步数据 到kudu表
			#创建一张hive_kudu的过渡表

			print srcTable['ip'],srcTable['impala_port'], srcTable['user'],srcTable['password']
			impala_client = rh_impala_conn.ImpalaConn(srcTable['ip'],srcTable['impala_port'], srcTable['user'],srcTable['password'])
			from_hive_to_kudu_impala_sql='drop table if exists %s.%s'%(srcTable['impala_db'],srcTable['impala_tbl'])
			impala_client.execute_sql_no_return(from_hive_to_kudu_impala_sql)

			from_hive_to_kudu_impala_sql = 'CREATE EXTERNAL TABLE %s.%s\
			STORED AS KUDU \
			TBLPROPERTIES (\
  			\'kudu.table_name\' = \'%s\');'%(srcTable['impala_db'],srcTable['impala_tbl'],kudu_table)
  			print from_hive_to_kudu_impala_sql
  			try:
  				impala_client.execute_sql_no_return(from_hive_to_kudu_impala_sql)
				impala_client.close()
			except:
  				pass


			hive_kudu_tmp_table_sql = 'drop table if exists %s.%s '%(srcTable['db'],srcTable['tbl']+'_tmp')
			res = hive_instance.execute_hive_command('execute_hive_sql_no_return' ,hive_kudu_tmp_table_sql)
			print hive_kudu_tmp_table_sql
			#print res 
			print "======================================================"	

			hive_kudu_tmp_table_sql =' create table %s.%s('%(srcTable['db'],srcTable['tbl']+'_tmp')
			print len(hive_table_structure)
			hts_lth = len(hive_table_structure) - 1 


			for col_idx in range(0,len(hive_table_structure)):

				if hive_table_structure[col_idx][0] == None :
					continue

				hive_kudu_tmp_table_sql = hive_kudu_tmp_table_sql +'`'+ hive_table_structure[col_idx][0] + '` string '
				hive_concat_str = hive_concat_str + hive_table_structure[col_idx][0] 


				if hive_table_structure[col_idx][1].find('varchar') != -1 or hive_table_structure[col_idx][1].find('string')!= -1 :
					move_hive_data_sql = move_hive_data_sql +'CONCAT(\'\\"%s\\":\\"\', IF(%s IS NOT NULL,%s,\"NULL\"),\'"\')'%(hive_table_structure[col_idx][0]
																												  ,hive_table_structure[col_idx][0]
																												  ,hive_table_structure[col_idx][0]
																												  )
				elif hive_table_structure[col_idx][1].find('time') !=-1 or hive_table_structure[col_idx][1].find('date') !=-1:
					move_hive_data_sql = move_hive_data_sql +'CONCAT(\'\\"%s\\":\\"\',IF(%s IS NOT NULL,concat(substring(cast(%s as string),1,10),\'T\',substring(cast(%s as string),12)), \"NULL\"),\'"\')'%(hive_table_structure[col_idx][0]
																																	  ,hive_table_structure[col_idx][0]
																										  							  ,hive_table_structure[col_idx][0]
																										  							  ,hive_table_structure[col_idx][0]
																																	  )

				elif hive_table_structure[col_idx][1].find('decimal') != -1 or hive_table_structure[col_idx][1].find('int') != -1 or hive_table_structure[col_idx][1].find('double') != -1:
					if hive_table_structure[col_idx][2] == 0:
						move_hive_data_sql = move_hive_data_sql +'CONCAT(\'\\"%s\\":\',IF(%s IS NOT NULL,CAST(%s AS STRING),\'0\'))'%(hive_table_structure[col_idx][0]
																													 ,hive_table_structure[col_idx][0]
																													 ,hive_table_structure[col_idx][0]
																													 )
					elif hive_table_structure[col_idx][2] == 1:
						move_hive_data_sql = move_hive_data_sql +'CONCAT(\'\\"%s\\":"\',IF(%s IS NOT NULL,CAST(%s AS STRING),\"NULL\"),\'"\')'%(hive_table_structure[col_idx][0]
																													 ,hive_table_structure[col_idx][0]
																													 ,hive_table_structure[col_idx][0]
																													 )


				move_hive_data_sql += '\n'
				hive_kudu_tmp_table_sql += '\n'
				if col_idx < hts_lth :
					move_hive_data_sql += ','
					hive_kudu_tmp_table_sql += ','
					hive_concat_str += ','


			hive_kudu_tmp_table_sql = hive_kudu_tmp_table_sql + ')'
	
	
			res = hive_instance.execute_hive_command('execute_hive_sql_no_return' ,hive_kudu_tmp_table_sql)
			print '正在execute_hive_sql_no_return'
			print hive_kudu_tmp_table_sql
			#print res
			print "======================================================"

			move_hive_data_sql = move_hive_data_sql + ' from %s.%s'%(srcTable['db'],srcTable['tbl'])

			res = hive_instance.execute_hive_command('execute_hive_sql_no_return' ,move_hive_data_sql)

			print '正在execute_hive_sql_no_return'
			print move_hive_data_sql
			#print res
			print "======================================================"

			#将hive里的时间数据处理成为KUDU可以执行的数据
			data_conn_instance = rh_ods_rt_conn.DataConn(
			 src_type = 'hive'
			,src_ip   = '100.69.216.40'
			,src_port = '10001'
			,src_authMechanism = 'PLAIN'
			,src_user = 'dengs001'
			,src_password = 'Pass@word1'
			,src_db = srcTable['db']                       #ODS_CMRH_SLIS  dbmgr
			,src_table_name = srcTable['tbl']   #ODS_SLIS_BAS_RN_PAID_INFO test_conn_for_ogg
			#kudu 表配置
			,dest_type = 'kudu'
			,dest_ip = destTable['ip']
			,dest_port = destTable['port']
			,dest_table_name=kudu_table      #'ods_slis_bas_rn_paid_info'
			,dest_table_primary_key=srcTable['primary_key']
			)

			print kudu_table

			hive_concat_str += ') FROM %s.%s '%(srcTable['db'], srcTable['tbl']+'_tmp')
			print  "===============483 hive_concat_str======================"
			print hive_concat_str
			concated_data = hive_instance.execute_hive_command('get_hive_all_result'
		                                     		   ,hive_concat_str)

			#print '==============concated_data==================='
			#print concated_data[0]
			print '正在execute_hive_sql_no_return'
			print move_hive_data_sql
			print "======================================================"

			kudu_client.new_batch_upsert_into_kudu(1000,concated_data)


