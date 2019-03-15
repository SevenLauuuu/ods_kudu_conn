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
# User:ytliu
# Date:2018/8/20 0018
#/

import kudu
from kudu.client import Partitioning
import kudu.client

import pytz

import json 
import sys
import os
import time
import datetime 


reload(sys)
sys.setdefaultencoding('utf8')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  
##########################################################初始化开始#######################################################################################
#cd /data/etlscript/CMRH_ODS/SCRIPT
class KuduClient:
	def __init__(self,host,port,table_name):
		self.host_list = host
		self.host = host
		self.port = int(port)
		self.table_name = table_name
		self.client = kudu.connect(host = self.host, port = self.port)
		self.builder = kudu.schema_builder()
		
	def get_kudu_client(self):
		print self.client
		return self.client
		
	def get_kudu_schema_builder(self):
		
		print self.builder
		print "========get_kudu_schema_builder SUCCESS========"
		return self.builder

	def get_kudu_table(self):
		self.table = self.client.table(self.table_name)
		return self.table

	def get_kudu_schema(self,primary_key_list, key_list):
		print '==================primary_key_list===================='
		print primary_key_list
		print '=================key_list====================='
		print key_list

		primary_keys = []
		self.columns = []

		for pk in primary_key_list:
			#self.builder.add_column()
			primary_keys.append(pk[0])

		self.builder.set_primary_keys(primary_keys)
		#添加kudu primary key 字段,kudu的指定名后回去检查kudu表里是否有这个字段，

		for column in primary_key_list:
			if  column[1].find('int8') != -1 :
				self.builder.add_column(column[0],kudu.int8, False)
			elif column[1].find('int16') != -1:
				self.builder.add_column(column[0], kudu.int16,False)
			elif column[1].find('int32') != -1:
				self.builder.add_column(column[0], kudu.int32, False)
			elif column[1].find('int64') != -1:
				self.builder.add_column(column[0],kudu.int64,False)
			elif column[1].find('double') != -1:
				self.builder.add_column(column[0],kudu.double,False)
			elif column[1].find('float') != -1:
				self.builder.add_column(column[0],kudu.float,False)			
			elif column[1].find('binary') != -1:
				self.builder.add_column(column[0],kudu.binary,False)
			elif column[1].find('string') != -1:
				self.builder.add_column(column[0],kudu.string,False)	
			elif column[1].find('unixtime_micros') != -1:
				self.builder.add_column(column[0],kudu.unixtime_micros,False)
			elif column[1].find('number') != -1:
				self.builder.add_column(column[0],kudu.unixtime_micros,False)
			print column
			print '\n'

		print self.builder 

		for column in key_list:
			#添加kudu 非primary key 字段
			print "=============key_list==============="
			print column,len(column)
			if len(column) == 0:
				continue

			if column[1].find('int') != -1:
				self.builder.add_column(column[0], type_ = kudu.int32, nullable=True, compression='lz4' )
			
			elif column[1].find('varchar') != -1:
				self.builder.add_column(column[0], type_ = kudu.string, nullable=True, compression='lz4' )
			
			elif column[1].find('decimal') != -1:
				self.builder.add_column(column[0], type_ = kudu.double, nullable=True, compression='lz4' )
			
			elif column[1].find('date') != -1:
				self.builder.add_column(column[0], type_ = kudu.unixtime_micros, nullable=True, compression='lz4' )
			
			elif column[1].find('float') != -1:
				self.builder.add_column(column[0], type_ = kudu.float, nullable=True, compression='lz4' )
			
			elif column[1].find('double') != -1:
				self.builder.add_column(column[0], type_ = kudu.double, nullable=True, compression='lz4' )
			
			elif column[1].find('binary') != -1:
				self.builder.add_column(column[0], type_ = kudu.binary, nullable=True, compression='lz4' )
			
			elif column[1].find('bool') != -1:
				self.builder.add_column(column[0], type_ = kudu.bool, nullable=True, compression='lz4' )
			
			elif column[1].find('string') != -1 or column[1].find('timestamp') != -1 or column[1].find('date') != -1 :
				self.builder.add_column(column[0], type_ = kudu.string, nullable=True, compression='lz4' )
			elif column[1].find('number') != -1:
				self.builder.add_column(column[0], type_ = kudu.float, nullable=True, compression='lz4' )

		print self.builder

		self.schema = self.builder.build()

	def get_kudu_partitioning(self,column_names,num_buckets):
		self.get_kudu_schema_builder()
		self.partitioning = Partitioning().add_hash_partitions(column_names=[column_names], num_buckets=int(num_buckets))
		print "get_kudu_partitioning done"

	def create_kudu_table(self, primary_key_list, key_list,column_names,num_buckets):
		self.get_kudu_client()
		self.get_kudu_schema(primary_key_list,key_list)
		self.get_kudu_partitioning(column_names, num_buckets)
		self.client.create_table(self.table_name,self.schema,self.partitioning)

	def delete_kudu_table(self):
		self.get_kudu_client()
		
		if self.client.table_exists(self.table_name):
			self.client.delete_table(self.table_name)

		return client.table_exists(self.table_name)

	def scan_kudu_table(self, filter):
		self.get_kudu_client()

		is_table_exists = self.client.table_exists(self.table_name)

		if is_table_exists == False : 
			print  "===%s do not exist !!!==="%(self.table_name)
			return  None

	def get_kudu_table_structure(self):
		self.get_kudu_client()
		is_table_exists = self.client.table_exists(self.table_name)
		if is_table_exists == False : 
			print  "===%s do not exist !!!==="%(self.table_name)
			return []

		self.table = self.client.table(self.table_name)
		col_tuple_list = []
		
		for i in range(0, self.table.num_columns):
			col_tuple_list.append((self.table[i].name, self.table[i].spec.type.name))

		return col_tuple_list
		
	def exchange_hive_to_kudu(self,hive_table,primary_key,hive_table_structure):
		self.kudu_structure_json = '{\'self.table_name\':\'%s\',\'table_structure\':{\'primary_key\': '%(hive_table)
		print self.kudu_structure_json
		print '\n'
		print hive_table_structure

		for pk in primary_key:
			print pk

		for table_structure in hive_table_structure :
			print table_structure[1].find('varchar')
			print table_structure[1].find('string')

			if table_structure[1].find('varchar') != -1 or table_structure[1].find('string') != -1 :
				print self.kudu_structure_json


	def get_kudu_session(self):
		self.session = self.client.new_session(flush_mode='manual', timeout_ms = 3000)
		print self.session
		return self.session
		
	def get_kudu_scanner(self):
		self.scanner = self.table.scanner()
		self.scanner.open()

	def get_kudu_all_results(self):
		self.get_kudu_client()
		self.get_kudu_session()		
		all_results = self.scanner.read_all_tuples()
		print all_results

	def get_datetime_str_to_tuple(self,data_json_str):
		print data_json_str
		print int(data_json_str[0:4]) ,int(data_json_str[5:7]) ,int(data_json_str[8:10]) ,int(data_json_str[11:13])
		print int(data_json_str[14:16]),int(data_json_str[17:19])

		data_json = datetime.datetime(int(data_json_str[0:4])
									 ,int(data_json_str[5:7])
									 ,int(data_json_str[8:10])
									 ,int(data_json_str[11:13])
									 ,int(data_json_str[14:16])
									 ,int(data_json_str[17:19])
									 ,int(data_json_str[20:22])
									 )
		print data_json
		return data_json.replace(tzinfo=pytz.utc)

	def new_insert_into_kudu(self, data_json):
		self.get_kudu_client()
		self.get_kudu_session()
		self.get_kudu_table()

		insert_op = self.table.new_insert(data_json)
		
		try:
			self.session.apply(insert_op)
			self.session.flush()
			print "~~~~~~~~~~~insert SUCCESS~~~~~~~~~~~~~"
		except:
			pass

	def new_update_into_kudu(self, data_json):
		self.get_kudu_client()
		self.get_kudu_session()
		self.get_kudu_table()

		insert_op = self.table.new_update(data_json)
		
		try:
			self.session.apply(insert_op)
			self.session.flush()
		except kudu.KuduBadStatus as e:
			print(self.session.get_pending_errors())
			pass

	def get_kudu_decimal_col(self, table):
		kudu_dec_lst = list()

		kudu_col_list = self.get_kudu_table_structure()
		print "get_kudu_table_structure"
		print kudu_col_list

		for kd_col_idx in range(0, len(kudu_col_list)):
			if kudu_col_list[kd_col_idx][1] == 'double' or kudu_col_list[kd_col_idx][1] == 'float' or kudu_col_list[kd_col_idx][1] == 'int':
				kudu_dec_lst.append(kudu_col_list[kd_col_idx][0])

		return kudu_dec_lst

	def new_upsert_into_kudu(self, data_json, primary_key_list,kudu_decimal_list,etl_time):
		self.get_kudu_client()
		self.get_kudu_session()
		self.get_kudu_table()

		data_json_str = ''
		if isinstance(data_json, (dict)) == True:

			for k in data_json:

				print  k.lower(), type(data_json[k])

				if isinstance(data_json[k],(str,unicode)) == True:
					print '======str,unicode======='
					print k, data_json[k].lower()

					data_json_str = data_json_str + '"%s":"%s",'%(k.lower(),data_json[k].lower())

				elif isinstance(data_json[k], (int,float)) == True:
					print '=======int,float======'
					print  k.lower() ,data_json[k]
					tmp = k.lower()
					if tmp in primary_key_list:
						data_json_str = data_json_str + '"%s":"%s",'%(k.lower(),data_json[k])
					else:
						print "======kkkkkkkkkkk============"
						data_json_str = data_json_str + '"%s":%d,'%(k.lower(),data_json[k])

				elif data_json[k] == None:
					print '=======None======'
					print  k, data_json[k]

					if k.lower() not in kudu_decimal_list:
						data_json_str = data_json_str + '"%s":"NULL",'%(k.lower())
					else:
						data_json_str = data_json_str + '"%s":0,'%(k.lower())
				else:
					print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
					print  k, data_json[k]



		data_json_str = data_json_str + '"etl_time":"%s"'%(etl_time)
		data_json_str = '{'+data_json_str+'}'
		print "=======================303"
		print data_json_str
		#try:
		cdata_json = json.loads(data_json_str, strict=False)
		
		print "=======================cdata_json=======================cdata_json"
		print cdata_json

		#except:
		#	pass

		insert_op = self.table.new_upsert(cdata_json)
		
		try:
			self.session.apply(insert_op)
			self.session.flush()
		except kudu.KuduBadStatus as e:
			print(self.session.get_pending_errors())
			pass

	def alter_table_name(self,table_o, table_n):
		table = self.client.table(table_o)
		alterer = client.new_table_alterer(table)
		table = alterer.rename(table_n).alter()


	def new_del_into_kudu(self, data_json):
		self.get_kudu_client()
		self.get_kudu_session()
		self.get_kudu_table()

		print "del info ing  ==========="
		print data_json
		insert_op = self.table.new_delete(data_json)
		
		try:
			self.session.apply(insert_op)
			self.session.flush()
		except kudu.KuduBadStatus as e:
			print(self.session.get_pending_errors())
			pass

	def new_batch_del_into_kudu(self,batch_num, data_json_list):
		self.get_kudu_client()
		self.get_kudu_session()
		self.get_kudu_table()

		print "==================len(data_json_list)===================="
		print batch_num,len(data_json_list)

		for data_json in data_json_list:
			data_index = 0
			if data_index < batch_num - 1:
				try:
					insert_op = self.table.new_delete(data_json)
					self.session.apply(insert_op)
					data_index +=1
				except:
					pass
			elif data_index == batch_num - 1:
				try:
					insert_op = self.table.new_delete(data_json)
					self.session.apply(insert_op)
					self.session.flush()
					data_index = 0
				except:
					pass			


	def new_batch_upsert_into_kudu(self,batch_num, data_json_list):
		self.get_kudu_client()
		self.get_kudu_session()
		self.get_kudu_table()

		idx_tmp = 0
		cdata_json=''


		for data_json_idx in range(0, len(data_json_list)):
			cdata='{'+data_json_list[data_json_idx][0]+'}'

			try:
				cdata_json = json.loads(cdata,strict=False)
			except:
				print "====================================================="
				print cdata

			try:
				insert_op = self.table.new_upsert(cdata_json)
				self.session.apply(insert_op)
			except :
				print cdata_json
				break

			if data_json_idx == 10 or data_json_idx == 100:
				print cdata_json
				
			if idx_tmp == batch_num or data_json_idx == len(data_json_list)-1 :
				try:
					self.session.flush()
					print idx_tmp
					print '==========idx_tmp==============='
				except kudu.KuduBadStatus as e:
					print(self.session.get_pending_errors())
					pass
				
				idx_tmp = 0

			idx_tmp += 1 
			

	def new_batch_update_into_kudu(self,batch_num, data_json_list):
		self.get_kudu_client()
		self.get_kudu_session()
		self.get_kudu_table()

		idx_tmp = 0
		cdata_json=''


		for data_json_idx in range(0, len(data_json_list)):
			cdata='{'+data_json_list[data_json_idx][0]+'}'

			try:
				cdata_json = json.loads(cdata,strict=False)
			except:
				print "====================================================="
				print cdata

			try:
				insert_op = self.table.new_update(cdata_json)
				self.session.apply(insert_op)
			except :
				print cdata_json
				break

			if data_json_idx == 10 or data_json_idx == 100:
				print cdata_json
				
			if idx_tmp == batch_num or data_json_idx == len(data_json_list)-1 :
				try:
					self.session.flush()
					print idx_tmp
					print '==========idx_tmp==============='
				except kudu.KuduBadStatus as e:
					print(self.session.get_pending_errors())
					pass
				
				idx_tmp = 0

			idx_tmp += 1 

	def new_batch_insert_into_kudu(self,batch_num, data_json_list):
		self.get_kudu_client()
		self.get_kudu_session()
		self.get_kudu_table()

		idx_tmp = 0
		cdata_json=''


		for data_json_idx in range(0, len(data_json_list)):
			cdata='{'+data_json_list[data_json_idx][0]+'}'

			try:
				cdata_json = json.loads(cdata,strict=False)
			except:
				print "====================================================="
				print cdata

			try:
				insert_op = self.table.new_insert(cdata_json)
				self.session.apply(insert_op)
			except :
				print cdata_json
				break

			if data_json_idx == 10 or data_json_idx == 100:
				print cdata_json
				
			if idx_tmp == batch_num or data_json_idx == len(data_json_list)-1 :
				try:
					self.session.flush()
					print idx_tmp
					print '==========idx_tmp==============='
				except kudu.KuduBadStatus as e:
					print(self.session.get_pending_errors())
					pass
				
				idx_tmp = 0

			idx_tmp += 1 

	def del_kudu_table(self, kudu_table_name):
		self.get_kudu_client()
		self.get_kudu_session()
		self.get_kudu_table()

		is_table_exists = self.client.table_exists(kudu_table_name)

		if is_table_exists == True :
			self.client.delete_table(kudu_table_name)
			return  False if self.client.table_exists(kudu_table_name) else True



if __name__ == "__main__":
	ip_port_list = ["cnsz92vl00745"]

	#data='{"table_name":"example12","table_structure":{"primary_key": [{"name":"uodaret","type": "kudu.int64","nullable": "False"}],"columns": [{"name": "ts_val","type": "kudu.int64","nullable": "False","compression": "lz4"},{"name": "ts","type": "kudu.int64","nullable": "False","compression": "lz4"}],"partitioning": {"add_hash_partitions": {"column_names": "uodaret","num_buckets": "3"}}}}'
	data = '{"table_name":"TEST_CONN_FOR_OGG","table_structure":{"primary_key": [{"name":"TESTER","type": "kudu.string","nullable": "False"}],"columns": [{"name": "SOURCE_DATABASE","type": "kudu.string","nullable": "False","compression": "lz4"},{"name": "TARGET_DATABASE","type": "kudu.string","nullable": "False","compression": "lz4"},{"name": "TEST_TIME","type": "kudu.unixtime_micros","nullable": "False","compression": "lz4"},{"name": "DESCRIPTION","type": "kudu.string","nullable": "True","compression": "lz4"}],"partitioning": {"add_hash_partitions": {"column_names": "TEST_TIME1","num_buckets": "3"}}}}'

	#create table DBMGR.TEST_CONN_FOR_OGG(TESTER string , SOURCE_DATABASE string ,TARGET_DATABASE string ,TEST_TIME TIMESTAMP, DESCRIPTION string)
	#create table DBMGR.TEST_CONN_FOR_OGG(TESTER string , SOURCE_DATABASE string ,TARGET_DATABASE string ,TEST_TIME TIMESTAMP, DESCRIPTION string)
	table_structure_json = json.loads(data)

	table_name = table_structure_json['table_name']
	table_schema = table_structure_json['table_structure']
	primary_key_list = table_structure_json['table_structure']['primary_key']
	key_list = table_structure_json['table_structure']['columns']
	table_partitioning = table_structure_json['table_structure']['partitioning']
	column_names=table_structure_json['table_structure']['partitioning']['add_hash_partitions']['column_names']
	num_buckets=table_structure_json['table_structure']['partitioning']['add_hash_partitions']['num_buckets']
	
	#create table 
	#kudu_client = KuduClient(host = "100.69.216.37",port=7051,table_name = table_name)
	#kudu_client.create_kudu_table(primary_key_list, key_list,column_names,num_buckets)
	
	
	#kafka_client.get_kudu_schema(table_schema)
	#kafka_client.get_kudu_partitioning(table_name,table_partitioning)
	#kafka_client.get_kudu_table_structure('python-example')
	
	#client.delete_table('python-example1111')
	#client = kafka_client.get_kudu_client()
	#builder = kafka_client.get_kudu_schema_builder()
	
	
	#builder.add_column('key').type(kudu.int64).nullable(False).primary_key()
	#builder.add_column('ts_val', type_=kudu.unixtime_micros, nullable=False, compression='lz4')

	#schema = builder.build()
	#partitioning = Partitioning().add_hash_partitions(column_names=['key'], num_buckets=3)

	#client.create_table('python-example1111', schema, partitioning)
	
	#all_tables=client.list_tables()
	#print all_tables
	
