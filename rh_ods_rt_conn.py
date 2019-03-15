#!/usr/bin/python
# -*- coding: utf-8 -*-
#author:ytliu 
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

import json
import re

import rh_hive_conn
import rh_mysql_conn
import rh_oracle_conn
import rh_kudu_conn
import rh_kafka_conn

#cd /data/etlscript/CMRH_ODS/SCRIPT

class  DataConn():
	"""docstring for  HandlerInfo"""
	def __init__(self,**kwargs):
		self.src_conn_args  = dict()
		self.dest_conn_args  = dict()
		self.handler_src_instance = None
		self.handler_dest_instance = None
		self.handler_src_instance_list = list()
		self.handler_dest_instance_list = list()


		self.kudu_client_table = {}
		print kwargs

		for key in kwargs:

			if key.find('src') != -1 :
				self.src_conn_args[key] = kwargs[key]

			elif key.find('dest') != -1 :
				self.dest_conn_args[key] = kwargs[key]

		print "=================src_conn_args======================="
		print self.src_conn_args

		print "=================dest_conn_args======================="
		print self.dest_conn_args

		self.all_supported_method = [
									  'handle_kafka_info'
									 ,'handle_data_into_kudu'
									 ,'handle_kudu_data_into_kudu'
									 ,'get_hive_all_result'
									 ]


	def get_src_instance(self):

		if self.src_conn_args['src_type'] ==  'kafka':

			try:
				print self.src_conn_args['src_topic_name'] ,self.src_conn_args['src_bootstrap_servers'],self.src_conn_args['src_consumer_timeout_ms']
			except:
				print "src 参数错误！"

			print  "==================get_src_instance kafka====================="

			for src_topic_name_idx in range(0, len(self.src_conn_args['src_topic_name'])):
				print self.src_conn_args['src_topic_name'][src_topic_name_idx]

				try:
					self.handler_src_instance = rh_kafka_conn.KFKConsumer(
												     self.src_conn_args['src_topic_name']
												     #self.src_conn_args['src_topic_name'][src_topic_name_idx]
												    ,self.src_conn_args['src_bootstrap_servers']									    
												    ,self.src_conn_args['src_consumer_timeout_ms']
													)
					
					self.handler_src_instance_list.append((self.src_conn_args['src_topic_name'][src_topic_name_idx], self.handler_src_instance))

					print self.handler_src_instance_list
					print "初始化 kafkaConsumer 成功"
				except:
					print "初始化 kafkaConsumer 失败"

		elif self.src_conn_args['src_type'] ==  'hive':
			try:
				print self.src_conn_args['src_ip'],self.src_conn_args['src_port'],self.src_conn_args['src_user'],self.src_conn_args['src_password'],self.src_conn_args['src_db'],self.src_conn_args['src_table_name']
			except:
				print "src 参数错误！"


			try:
				self.handler_src_instance = rh_hive_conn.HiveClient(self.src_conn_args['src_ip']
																   ,self.src_conn_args['src_port']
																   ,self.src_conn_args['src_authMechanism']
																   ,self.src_conn_args['src_user']
																   ,self.src_conn_args['src_password']
																   ,self.src_conn_args['src_db']
																   )
				print "初始化 HiveClient 成功"
			except:
				print "初始化 HiveClient 失败"


	def get_dest_instance(self, table):

		if self.dest_conn_args['dest_type'] == 'kudu':

			try:
				print self.dest_conn_args['dest_ip'],self.dest_conn_args['dest_port'],self.dest_conn_args['dest_table_name']
			except:
				print "dest 参数错误！"


			try:
				self.handler_dest_instance = self.kudu_client_table[table]
			except:
				try:
					self.handler_dest_instance = rh_kudu_conn.KuduClient(
																	host = self.dest_conn_args['dest_ip']
																  , port = int(self.dest_conn_args['dest_port']) 
																  ,table_name = table)
					print "初始化kudu client 成功"
				except:
					print "初始化kudu client 失败"

			return self.handler_dest_instance

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

	def decode_kafka_data(self,data_json_list):
		data_json_before_list = []
		data_json_after_list = []
		oracle_t_reg = r'\d{4}(-\d{2}){2} (\d{2}:){2}\d{2}.\d{2}'

		print "==============data_json_list==============="
		print data_json_list

		for data_json in data_json_list:
			data_json_before = {}
			data_json_after =  {}
			print data_json
			try:
				for key in data_json['after']:
					k = key.decode('gbk').lower()
					if k.find('time') != -1:
						kudu_time = self.change_oracleT_to_kuduT(data_json['after'][key], oracle_t_reg)
						data_json_after[k] = kudu_time
					else:
						data_json_after[k] = (data_json['after'][key]).decode('gbk')
			except:
				print "==================无after字段====================="
				pass

			print "============data_json_after==========="
			print data_json_after
			data_json_after_list.append(data_json_after)

			try:
				for key in data_json['before']:
					k = key.decode('gbk').lower()
					if k.find('time') != -1:
						kudu_time = self.change_oracleT_to_kuduT(data_json['before'][key], oracle_t_reg)
						data_json_before[k] = kudu_time
					else:
						data_json_before[k] = (data_json['before'][key]).decode('gbk')
			except:
				print "==================无before字段====================="
				pass

			print "============data_json_before==========="
			print data_json_before
			data_json_before_list.append(data_json_before)

		return data_json_before_list, data_json_after_list

	def del_kudu_primary_data(self,primary_key_list,data_json_before_list):
		to_be_del_json_str = '{'

		for pk_idx in range(0, len(primary_key_list)):
			print primary_key_list[pk_idx], data_json_before_list

			if pk_idx < len(primary_key_list) -1:
				to_be_del_json_str = to_be_del_json_str +'"'+ primary_key_list[pk_idx] +'":"'+ data_json_before_list[primary_key_list[pk_idx]] +'",'

			elif pk_idx == len(primary_key_list) -1:
				to_be_del_json_str = to_be_del_json_str +'"'+ primary_key_list[pk_idx] +'":"'+  data_json_before_list[primary_key_list[pk_idx]] +'"'
			
			print to_be_del_json_str
		to_be_del_json_str += '}'
		to_be_del_json=json.loads(to_be_del_json_str)

		print to_be_del_json
		print "===============即将被删掉 ================="
		print to_be_del_json
		self.kudu_client_instance.new_del_into_kudu(to_be_del_json)
		print '\n'			

	def get_method(self, **kwargs):

		if len(kwargs['direction']) == 0 or kwargs['method'] == 0 :
			print "get method 参数错误"
			return ""

		if kwargs['method'] not in self.all_supported_method:
			print  "暂不支持 %s 方法"%(kwargs['method'])


		if kwargs['direction'] == 'src':
			self.get_src_instance()
			
			if   kwargs['method'] == 'handle_kafka_info':
				print "!!!!!!!!!!!!!!!handle_kafka_info!!!!!!!!!!!!!!!!!!"
				return self.handler_src_instance.handle_kafka_info()

			elif kwargs['method'] == 'get_hive_all_result':
				print " ================正在使用 method get_hive_all_result======================="

				print kwargs['sql'] 
				return self.handler_src_instance.get_hive_all_result(kwargs['sql'])

		elif kwargs['direction'] == 'dest':

			if   kwargs['method'] == 'handle_kudu_data_into_kudu':
				print "==========================================="
				print kwargs['data_json_list']
				data_json_before_list, data_json_after_list = self.decode_kafka_data(kwargs['data_json_list'])
	
				print data_json_before_list, data_json_after_list
				for data_index in range(0, len(kwargs['data_json_list'])):
					table_name = kwargs['data_json_list'][data_index]['table'].split('.')[1].lower()
					print table_name

					self.kudu_client_instance = self.get_dest_instance(table_name)

					if   kwargs['data_json_list'][data_index]['op_type'] == 'I':
						print "=================I================"
						print data_json_after_list[data_index]
						self.kudu_client_instance.new_insert_into_kudu(data_json_after_list[data_index])

					elif kwargs['data_json_list'][data_index]['op_type'] == 'U':
						self.kudu_client_instance.new_update_into_kudu(data_json_after_list[data_index])

					elif kwargs['data_json_list'][data_index]['op_type'] == 'D':
						print "===========================del info ==============================="
						print 'test_time',data_json_before_list[data_index]['test_time']
						print 'test_time',data_json_before_list[data_index]
						primary_key_list = self.dest_conn_args['dest_table_primary_key'].split(',')
						print data_json_before_list[data_index]

						#删除只要拿出kudu的主键去进行删除
						self.del_kudu_primary_data(primary_key_list,data_json_before_list[data_index])
						print '\n'


if __name__ == "__main__":
	data_conn_instance = DataConn(
		 src_type = 'hive'
		,src_ip   = 'xxx.xx.xxx.xx'
		,src_port = '10001'
		,src_authMechanism = 'PLAIN'
		,src_user = 'dengs001'
		,src_password = 'Pass@word1'
		,src_db = 'ods_cmrh_slis'
		,src_table_name = 'ods_slis_bas_rn_paid_info_tmp'
		,src_hive_concat_str ='show databases '
		,dest_type = 'kudu'
		,dest_ip = 'xxx.xx.xxx.xx'
		,dest_port = '7051'
		,dest_table_name='rn_paid_info_kafkatest'
		,dest_table_primary_key='test_time,tester'
		)

	data_conn_instance.get_method(direction='src', method = 'get_hive_all_result')
