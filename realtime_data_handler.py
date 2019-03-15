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
# User:
# Date:2018/8/20 0018
#/
from kafka import KafkaConsumer                                                                                                                                                                                                             
import json
import re
import time
import rh_hive_conn
import rh_mysql_conn
import rh_oracle_conn

import rh_kudu_conn
import rh_ods_rt_conn
import rh_kafka_conn


def change_oracleT_to_kuduT(oracle_t, oracle_t_reg):
	print "======================"
	print oracle_t, oracle_t_reg
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
	print "kudu_time"+kudu_time
	return kudu_time

def decode_kafka_data(data_json_list):
	data_json_before_list = []
	data_json_after_list = []

	for data_json in data_json_list:
		data_json_before = {}
		data_json_after =  {}
		print "==============decode_kafka_data data_json['after']==============="
		#print data_json['after']
		print "==============data_json['after'] ==============="
		try:
			for key in data_json['after']:
				
				k = key.decode('gbk').lower()
				
				if k.find('time') != -1 or ( k.find('date') != -1 and k[len(k)-4:len(k)].lower() == 'date'):
					oracle_t_reg = ''
					if len(data_json['after'][key]) == 19:
						oracle_t_reg = r'\d{4}(-\d{2}){2} (\d{2}:){2}\d{2}'
					elif len(data_json['after'][key]) == 21:
						oracle_t_reg = r'\d{4}(-\d{2}){2} (\d{2}:){2}\d{2}.\d{2}'

					print data_json['after'][key]
					kudu_time = change_oracleT_to_kuduT(data_json['after'][key], oracle_t_reg)
					data_json_after[k] = kudu_time
				else:
					if data_json['after'][key] == None :
						data_json_after[k] = u'NULL'
					else:
						print "======================93="
						print type(data_json['after'][key])
						if isinstance(data_json['after'][key], (str)) == True:
							data_json_after[k] = (data_json['after'][key]).decode('gbk')
						else:
							data_json_after[k] = data_json['after'][key]

		except:
			print "======================================="
			pass
		print "============data_json_after==========="
		print data_json_after
		data_json_after_list.append(data_json_after)

		try:
			for key in data_json['before']:
				k = key.decode('gbk').lower()
				if k.find('time') != -1 or k.find('date') != -1:

					oracle_t_reg = ''
					if len(data_json['before'][key]) == 19:
						oracle_t_reg = r'\d{4}(-\d{2}){2} (\d{2}:){2}\d{2}'

					elif len(data_json['before'][key]) == 21:
						oracle_t_reg = r'\d{4}(-\d{2}){2} (\d{2}:){2}\d{2}.\d{2}'

					kudu_time = change_oracleT_to_kuduT(data_json['before'][key], oracle_t_reg)
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

def split_kafka_json_str(json_str):
	splited_json_list =[]
	try:
		msg_split_list = json_str.split('}{')
		msg_len = len(json_str.split('}{'))

		for msg_index in  range(0, msg_len):
			if msg_index == 0:
				splited_json_list.append(json.loads(msg_split_list[msg_index] + '}'))
				
			elif msg_index > 0 and msg_index < len(msg_split_list) - 1 :
				splited_json_list.append(json.loads('{' + msg_split_list[msg_index] + '}'))
				
			elif msg_index == len(msg_split_list) - 1:
				splited_json_list.append(json.loads('{' + msg_split_list[msg_index]))
	except:
		msg_split_list = json_str
		msg_len = 1

		splited_json_list.append(json.loads(msg_split_list))

	print "========================splited_json_list================================="
	print splited_json_list
	print "========================splited_json_list================================="
	return splited_json_list

def get_kudu_client(kudu_client_instance_list,table):
	kudu_client_instance = None

	for kd_ist_idx in range(0, len(kudu_client_instance_list)):

		if kudu_client_instance_list[kd_ist_idx][0] == table.lower():
			kudu_client_instance = kudu_client_instance_list[kd_ist_idx][2]
			
	return kudu_client_instance

def get_kudu_primary_key(kudu_table_list, table):
	primary_key_list = list()

	for kd_ist_idx in range(0, len(kudu_table_list)):
		table = table.lower()

		if kudu_table_list[kd_ist_idx][0] == table :
			primary_key_list = kudu_table_list[kd_ist_idx][2].split(',')

	return primary_key_list


#cd /data/etlscript/CMRH_ODS/SCRIPT
if __name__ == "__main__":
	#开发环境
	kudu_table_list = [
	('i_posdata.pos_info','ods_slis_bas_pos_info','pos_no,prod_seq,etl_time'),
	('i_gpsdata.pos_prem_fin_detail','ods_gis_bas_pos_prem_fin_detail','primary_pk,etl_time'),
	('i_gpbdata.policy_grp','ods_gis_bas_policy_grp','grp_policy_no,etl_time'),
	('i_gncdata.gnc_apply_prem','ods_gis_bas_gnc_apply_prem','grp_apply_no,etl_time'),
	('i_gpbdata.policy_sig','ods_gis_bas_policy_sig','sig_policy_no,etl_time'),
	('i_gpbdata.policy_plan_duty_sig','ods_gis_bas_policy_plan_duty_sig','sig_policy_no,etl_time'),
	('i_gncdata.gnc_apply_plan_duty_sig','ods_gis_bas_gnc_apply_plan_duty_sig','product_code,duty_code,sig_apply_no,etl_time'),
	('i_gncdata.gnc_apply_grp','ods_gis_bas_gnc_apply_grp','grp_apply_no,etl_time'),
	('i_findata.fin_business_cost','ods_slis_bas_fin_business_cost','business_cost_no,etl_time'),
	('i_finrule.fin_amt_type_subject','ods_slis_bas_fin_amt_type_subject','policy_no,etl_time'),
	('i_findata.fin_internal_carryover_cost','ods_slis_bas_fin_internal_carryover_cost','carryover_no,etl_time'),
	('i_rendata.rn_paid_info','ods_slis_bas_rn_paid_info','paid_pk,etl_time'),
	('i_pubdata.policy','ods_slis_bas_policy','policy_no,etl_time'),
	('i_nbudata.uw_apply_info','ods_slis_bas_uw_apply_info','apply_no,etl_time'),
	('i_nbudata.uw_apply_product','ods_slis_bas_uw_apply_product','apply_no,prod_seq,etl_time'),
	('i_biadata.nbia_policy','ods_slis_bas_nbia_policy','policyid,etl_time'),
	('i_biadata.nbia_coverage','ods_slis_bas_nbia_coverage','covid,etl_time'),
	('i_biabdata.biab_policy_product','ods_slis_bas_biab_policy_product','prod_id,etl_time'),
	('i_biabdata.biab_policy_custom','ods_slis_bas_biab_policy_custom','custom_id,etl_time'),
	('i_biadata.nbia_customer','ods_slis_bas_nbia_customer','custid,etl_time'),
	('i_pubdata.policy_product','ods_slis_bas_policy_product','policy_no,prod_seq,etl_time'),
	('i_pubdata.policy_product_prem','ods_slis_bas_policy_product_prem','policy_no,prod_seq,etl_time'),
	('i_gpsdata.pos_info','ods_gis_bas_pos_info','pos_no,etl_time'),
	('i_icmsdata.staff_salary_all','ods_icms_bas_staff_salary_all','calc_ym,emp_no,salary_code,calc_type,etl_time'),
	('i_icmsdata.staff_info','ods_icms_bas_staff_info','emp_no,etl_time'),

	]

	kudu_host_list = ['100.69.216.37','100.69.216.38','100.69.216.39']
	kudu_port = 7051
	kudu_client_instance_list = list()

	for kd_table_idx in range(0, len(kudu_table_list)):
		print kudu_table_list[kd_table_idx][0]
		print kudu_table_list[kd_table_idx][1]
		print "=============kudu_table_list[kd_table_idx][1]=================="
		kudu_client = rh_kudu_conn.KuduClient(kudu_host_list,kudu_port,kudu_table_list[kd_table_idx][1])
		print kudu_table_list[kd_table_idx][0],kudu_table_list[kd_table_idx][1]
		kudu_table = kudu_client.get_kudu_table()
		kudu_session = kudu_client.get_kudu_session()
		print kudu_table
		kudu_client_instance_list.append((kudu_table_list[kd_table_idx][0],kudu_table_list[kd_table_idx][1], kudu_client))

	print kudu_client_instance_list

	src_topic_name = 'RN_PAID_INFO'
	src_bootstrap_servers = 'xx.xxx.xx.xx:9092,xx.xxx.xx.xx:9092,xx.xxx.xx.xx:9092,xx.xxx.xx.xx:9092,xx.xxx.xx.xx:9092,xx.xxx.xx.xx:9092,xx.xxx.xx.xx:9092,xx.xxx.xx.xx:9092'
	src_consumer_timeout_ms=3000


	while True:
		consumer = KafkaConsumer(src_topic_name,bootstrap_servers=src_bootstrap_servers,consumer_timeout_ms = src_consumer_timeout_ms)
		print "==================consumer : %s ================="%(consumer)
		
		for msg in consumer:
			topic_name = msg.topic
			partition = msg.partition
			offset = msg.offset
			value = msg.value

			print  "==========value=========="
			print  value 
			json_str = split_kafka_json_str(value)
			print  "==========json_str=========="
			print  json_str
			data_json_before_list, data_json_after_list = decode_kafka_data(json_str)

			print "==========data_json_before_list : %s========="%(data_json_before_list)
			print data_json_before_list
			print "==========data_json_after_list : %s========="%(data_json_after_list)
			print data_json_after_list

			for js_idx in range(0, len(json_str)):
				print json_str[js_idx]
				print json_str[js_idx]['op_type']

				kudu_client_instance = get_kudu_client(kudu_client_instance_list, json_str[js_idx]['table'].lower())
				print "========kudu_client_instance==========="
				print kudu_client_instance
				print "==========data_json_after_list========"
				print data_json_after_list

				if json_str[js_idx]['op_type'] == 'I' or json_str[js_idx]['op_type'] == 'U':

					print "insert ing "
					print "table"
					print json_str[js_idx]['table'].lower()

					primary_key_list = get_kudu_primary_key(kudu_table_list, json_str[js_idx]['table'].lower())
					etl_time = time.strftime("%Y%m%d", time.localtime())

					print "============json_str[js_idx]['table']========="
					print json_str[js_idx]['table'].lower()
					kudu_decimal_list = kudu_client_instance.get_kudu_decimal_col(json_str[js_idx]['table'].lower())
					print "===========kudu_decimal_list================="
					print kudu_decimal_list
					print "===============primary_key_list============="
					print primary_key_list
					
					kudu_client_instance.new_upsert_into_kudu(json_str[js_idx]['after'], primary_key_list,kudu_decimal_list,etl_time)


