
pyspark --jars /home/oicq/guomm/kudu-spark2_2.11-1.6.0.jar # 启动 
sqlContext = pyspark.sql.SQLContext(spark) # 创建sql连接 
df = sqlContext.read.format('org.apache.kudu.spark.kudu').options(**{"kudu.master":"127.0.0.1:7051", "kudu.table":"python-example"}).load() # 读取kudu表
df.write.format('org.apache.kudu.spark.kudu').option('kudu.master', '127.0.0.1:7051').option('kudu.table', 'python-example1').mode('append').save() # 写入kudu表

from impala.dbapi import connect
conn = connect(host='xxx.xx.xxx.40', port=10025, auth_mechanism=self.mechanism, user=self.user, password=self.password)


import kudu
from kudu.client import Partitioning
client = kudu.connect(host=['xxx.xx.xxx.xx','xxx.xx.xxx.xx','xxx.xx.xxx.xx'], port=7051) 
client.delete_table('ods_cmrh_slis_ods_slis_bas_pos_info')
all_tables=client.list_tables()
print all_tables

import kudu
from kudu.client import Partitioning
client = kudu.connect(host=['100.69.xx.xx','100.69.xx.xx','100.69.xxx.xx'], port=7051)
table = client.table('ods_slis_bas_policy_product')
scanner = table.scanner()
scanner.open()
batch = scanner.read_all_tuples()
tuples = batch.as_tuples()
print tuples

table = client.table('TEST-NULL')
alterer = client.new_table_alterer(table)
table = alterer.rename('test-null').alter()


#更新插入数据
import kudu
from kudu.client import Partitioning
client = kudu.connect(host=['xxx.xx.xxx.xx','xxx.xx.xxx.xx','xxx.xx.xxx.xx'], port=7051) 
table=client.table('ods_slis_bas_policy_product')
session = client.new_session()

insert_op=table.new_upsert({u'relation_desc': u'NULL', u'insured_no': u'C00000323225', u'created_user': u'toracle', u'product_code': u'5010', u'effect_date': u'2018-06-12T00:00:00', u'pk_serial': u' ', u'policy_no': u'test153', u'lapse_reason': u'NULL', u'is_primary_plan': u'Y', u'prod_seq': 1, u'units': 1, u'smoke_flag': u'NULL', u'product_level': u'NULL', u'updated_date': u'2018-06-12T04:00:10', u'coverage_period': 12, u'relationship': u'01', u'insured_seq': 1, u'social_insurance': u'NULL', u'cover_period_type': u'5', u'updated_user': u'toracle', u'duty_status': u'1', u'renewal_permit': u'NULL', u'base_sum_ins': 2000.0, u'maturity_date': u'2019-06-11T00:00:00', u'special_term': u'NULL', u'created_date': u'2018-06-11T17:40:53', u'dividend_sum_ins': 0})
insert_op=table.new_upsert(cdata)
{u'relation_desc': u'NULL', u'insured_no': u'C00000323225', u'created_user': u'toracle', u'product_code': u'5010', u'effect_date': u'2018-06-12T00:00:00', u'pk_serial': u'20180000000061954868', u'policy_no': u'test1298', u'lapse_reason': u'NULL', u'is_primary_plan': u'Y', u'prod_seq': u'1', u'units': u'1', u'smoke_flag': u'NULL', u'product_level': u'NULL', u'updated_date': u'2018-06-12T04:00:10', u'coverage_period': u'12', u'relationship': u'01', u'insured_seq': u'1', u'social_insurance': u'NULL', u'cover_period_type': u'5', u'updated_user': u'toracle', u'duty_status': u'1', u'renewal_permit': u'NULL', u'base_sum_ins': u'2000.0', u'maturity_date': u'2019-06-11T00:00:00', u'special_term': u'NULL', u'etl_time': u'20181025', u'created_date': u'2018-06-11T17:40:53', u'dividend_sum_ins': u'0'}
session.apply(insert_op)
session.flush()


#查看所有数据
table=client.table("ods_cmrh_slis_ods_slis_bas_pos_info")
scanner=table.scanner()
scanner.open()
batch=scanner.read_all_tuples()
tuples=batch.as_tuples()
printtuples

#删除表
client.delete_table("ods_cmrh_nods_ods_nods_bas_nbia_coverage")





session=client.new_session()
scanner=table.scanner()
scanner.open()
tuples=scanner.read_all_tuples()
printtuples
builder=kudu.schema_builder()
schema=builder.build()
schema.primary_keys()

importkudu
fromkudu.clientimportPartitioning
client=kudu.connect(host="xxx.xx.xxx.38",port=7051)
client=kudu.connect(host="xxx.xx.xxx.37",port=7051)
client=kudu.connect(host="xxx.xx.xxx.39",port=7051)
client.delete_table("ods_cmrh_nods_ods_nods_bas_nbia_coverage")
client.delete_table("ods_cmrh_slis_ods_slis_bas_rn_paid_info")
table=client.table("ods_slis_bas_rn_paid_info")
#insert_op=table.new_insert({"paid_pk":"20180000000089426296","supervise_no":"NULL"})
session=client.new_session()
session.set_mutation_buffer_space(1024*1024*10)
session.apply(insert_op)
session.flush()

session=client.new_session()
scanner=table.scanner()
scanner.open()
tuples=scanner.read_all_tuples()
printtuples


self.builder_pred=self.table.scan_token_builder()
self.builder_pred=self.table.scan_token_builder()
self.builder_pred.add_predicate(pred)
pre=[table["tester"]=="22222"]
#,table["test_time"]=="2018-09-19T17:44:09.65"]
scanner.add_predicates(pre)
scanner.add_predicate("table["test_time"]=="2018-09-19T15:57:59.75"")
scanner.open()
tuples=scanner.read_all_tuples()
printtuples
		
importkudu
fromkudu.clientimportPartitioning
client=kudu.connect(host="xxx.xx.xxx.38",port=7051)
table=client.table("test_conn_for_ogg")
pre=[table["tester"]=="ljn001"]
pre=[table["test_time"]==datetime("2018-09-19T15:57:59.750000")]
scanner=table.scanner()
scanner.add_predicates(pre)
scanner.open()
tuples=scanner.read_all_tuples()
printtuples
		
		
table=client.table("test_conn_for_ogg")
session=client.new_session()
printtable,session

insert_op=table.new_insert({"tester":"lyt","target_database":"lafak","test_time":"2018-09-07T19:21:06.83","description":"c2dddc2"})
session.apply(insert_op)

insert_op=table.new_insert({u'relation_desc': u'NULL', u'insured_no': u'C00000323225', u'created_user': u'toracle', u'product_code': u'5010', u'effect_date': u'2018-06-12T00:00:00', u'pk_serial': u'20180000000061954868', u'policy_no': u'test164', u'lapse_reason': u'NULL', u'is_primary_plan': u'Y', u'prod_seq': u'1', u'units': 1, u'smoke_flag': u'NULL', u'product_level': u'NULL', u'updated_date': u'2018-06-12T04:00:10', u'coverage_period': 12, u'relationship': u'01', u'insured_seq': 1, u'social_insurance': u'NULL', u'cover_period_type': u'5', u'updated_user': u'toracle', u'duty_status': u'1', u'renewal_permit': u'NULL', u'base_sum_ins': 2000.0, u'maturity_date': u'2019-06-11T00:00:00', u'special_term': u'NULL', u'created_date': u'2018-06-11T17:40:53', u'dividend_sum_ins': 0})
session.apply(insert_op)
session.flush()

insert_op=table.new_update({"tester":"lyt343","target_database":"lafak111","test_time":"2018-09-17T17:29:23.56","description":"c2dddc2111"})
session.apply(insert_op)

insert_op=table.new_update({"test_time":"2018-09-19T15:16:36.36","target_database":"mods1","source_database":"slis1","tester":"lllll"})
session.apply(insert_op)

op=table.new_delete({"tester":"lyt343","test_time":"2018-09-17T17:29:23.56"})
session.apply(op)
session.flush()

op=table.new_delete({"tester":"lyt343"})
session.apply(op)
session.flush()

client.delete_table("date-json")
all_tables=client.list_tables()
printall_tables


#Flushwriteoperations,iffailuresoccur,captureprintthem.
try:
session.flush()
exceptkudu.KuduBadStatusase:
print(session.get_pending_errors())
	
session.flush()

insert_op=table.new_update({"test_time":"2018-09-13T20:08:39.200000","target_database":"lafak","source_database":"slis","tester":"lyt003"})
session.apply(insert_op)
session.flush()

fromimpala.dbapiimportconnect
#需要注意的是这里的auth_mechanism必须有，但database不必须
conn=connect(host="xxx.xx.xxx.40",port=21050,database="default",auth_mechanism="PLAIN")
cur=conn.cursor()

cur.execute("SHOWDATABASES")
print(cur.fetchall())

cur.execute("SHOWTables")
print(cur.fetchall())


importdatetime
table=client.table("test_conn_for_ogg")
op=table.new_update()
op["tester"]="lyt001"
op["test_time"]=datetime.datetime(2018,9,13,14,59,5,360000)
op["source_database"]="nods"
op["target_database"]="slis"
op["description"]="111111111111111222"
session.apply(op)

CREATEEXTERNALTABLEmap_test_conn_for_ogg
STOREDASKUDU
TBLPROPERTIES(
"kudu.table_name"="test_conn_for_ogg"
);
CREATEEXTERNALTABLEmap_test_conn_for_ogg
STOREDASKUDU
TBLPROPERTIES(
"kudu.table_name"="test_conn_for_ogg"
);sys_dw_srv-vf;aADc
#conn=pyhs2.connect(host="xxx.xx.xxx.40",port=10001,authMechanism="PLAIN",user="linkx001",password="Pass@word1",database="ODS_CMRH_ATP")
#cursor=conn.cursor()
#sql="describeODS_CMRH_ATP.ODS_ATP_BAS_EXPENSE_REIMBURSE_APPLY_DTL"
#cursor.execute(sql)
#printcursor.fetchall()

#表的多少列
table.num_columns
#表的列
printtable[0]
#表结构第一列名Column(ts_val,parent=python-example,type=unixtime_micros)
printtable[0].name
printtable[0].spec.type.name


printtable[1].name
printtable[1].spec.type.name

op=table.new_insert()
op["key"]=1
#"%Y-%m-%dT%H:%M:%S.%f
op["ts_val"]="2018-08-27T08:50:01.03"

session.apply(op)
session.flush()

td=table.new_delete("python-example")
scanner=table.scanner()
scanner.add_predicate(table["key"]>10)
scanner.open()

=========================================
importkudu
fromkudu.clientimportPartitioning
client=kudu.connect(host="xxx.xx.xxx.37",port=7051)

table=client.table("test_conn_for_ogg")
scanner=table.scanner()
scanner.open()
tuples=scanner.read_all_tuples()
printtuples
td=table.new_delete(tuples)

=========================================
builder=table.scan_token_builder()
builder.set_fault_tolerant().add_predicate(table["key"]>10)
tokens=builder.build()
fortokenintokens:
scanner=token.into_kudu_scanner()
scanner.open()
tuples=scanner.read_all_tuples()


=========================================
importkudu
fromkudu.clientimportPartitioning
client=kudu.connect(host="xxx.xx.xxx.38",port=7051)
table=client.table("test_conn_for_ogg")

scanner=table.scanner()
scanner.add_predicate(table["key"]>10)
scanner.open()
batch=scanner.read_all_tuples()
tuples=batch.as_tuples()
client=kudu.connect(host="xxx.xx.xxx.38",port=7051)

createtablekudu_mytest(
idintnotnull,
id1intnotnull,
namestring,
primarykey(id1,id)
)partitionbyhash(id)partitions4storedaskuduTBLPROPERTIES(
"kudu.table_name"="KUDU_MYTEST",
"kudu.master_addresses"="xxx.xx.xxx.37:7051,xxx.xx.xxx.38:7051,xxx.xx.xxx.39:7051"
);


importkudu
fromkudu.clientimportPartitioning
client=kudu.connect(host=["xxx.xx.xxx.37","xxx.xx.xxx.38","xxx.xx.xxx.39"],port=7051)
printclient.is_multimaster
client.is_multimaster=True
builder=kudu.schema_builder()
primary_keys=["policy_no","product_code","amt_type","prem_actural_date","etl_time"]
columns=[("policy_no","kudu.string",False),("product_code","kudu.string",False),("amt_type","kudu.string",False),("prem_actural_date","kudu.string",False),("etl_time","kudu.double",False),("agent_no","kudu.string",True),("is_primary_plan","kudu.string",True),("ins_age","kudu.double",True),("prem_period_type","kudu.string",True),("prem_term","kudu.double",True),("branch_code","kudu.string",True),("dept_no","kudu.string",True),("duty_status","kudu.string",True),("business_cost_no","kudu.string",True),("coverage_year","kudu.double",True),("modal_total_prem","kudu.double",True)]
builder=kudu.schema_builder()
forname,typename,nullableincolumns:
builder.add_column(name,typename,nullable=nullable)
builder.set_primary_keys(primary_keys)
schema=builder.build()
partitioning=Partitioning().add_hash_partitions(column_names=["etl_time"],num_buckets=3)
client.create_table("misdata_bancass_detail_yj_temp1",schema,partitioning)

misdata_bancass_detail_yj_temp1;
|--agent_no:string(nullable=true)
|--policy_no:string(nullable=false)
|--product_code:string(nullable=false)
|--is_primary_plan:string(nullable=true)
|--ins_age:double(nullable=true)
|--prem_period_type:string(nullable=true)
|--prem_term:double(nullable=true)
|--branch_code:string(nullable=true)
|--dept_no:string(nullable=true)
|--duty_status:string(nullable=true)
|--prem_actural_date:string(nullable=false)
|--amt_type:string(nullable=false)
|--business_cost_no:string(nullable=true)
|--coverage_year:double(nullable=true)
|--modal_total_prem:double(nullable=true)
builder.add_column("policy_no").type(kudu.string).nullable(False).primary_key()
builder.add_column("product_code").type(kudu.string).nullable(False).primary_key()
builder.add_column("amt_type").type(kudu.string).nullable(False).primary_key()
builder.add_column("prem_actural_date").type(kudu.string).nullable(False).primary_key()

builder.add_column("etl_time").type(kudu.string).nullable(False).primary_key()
builder.set_primary_keys(["policy_no","product_code","amt_type","prem_actural_date","etl_time"])
builder.add_column("policy_no",type_=kudu.string,nullable=False)
builder.add_column("product_code",type_=kudu.string,nullable=False)
builder.add_column("amt_type",type_=kudu.string,nullable=False)
builder.add_column("prem_actural_date",type_=kudu.string,nullable=False)
builder.add_column("etl_time",type_=kudu.string,nullable=False)

builder.add_column("agent_no",type_=kudu.string,nullable=True,compression="lz4")
builder.add_column("is_primary_plan",type_=kudu.string,nullable=True,compression="lz4")
builder.add_column("ins_age",type_=kudu.double,nullable=True,compression="lz4")
builder.add_column("prem_period_type",type_=kudu.string,nullable=True,compression="lz4")
builder.add_column("prem_term",type_=kudu.double,nullable=True,compression="lz4")
builder.add_column("branch_code",type_=kudu.string,nullable=True,compression="lz4")
builder.add_column("dept_no",type_=kudu.string,nullable=True,compression="lz4")
builder.add_column("duty_status",type_=kudu.string,nullable=True,compression="lz4")
builder.add_column("business_cost_no",type_=kudu.string,nullable=True,compression="lz4")
builder.add_column("coverage_year",type_=kudu.double,nullable=True,compression="lz4")
builder.add_column("modal_total_prem",type_=kudu.double,nullable=True,compression="lz4")

schema=builder.build()
partitioning=Partitioning().add_hash_partitions(column_names=["etl_time"],num_buckets=3)

client.create_table("misdata_bancass_detail_yj_temp1",schema,partitioning)
client.create_table("python-example1",schema,partitioning)
client.create_table("python-example2",schema,partitioning)

建立一个impala表，将这个表映射到kudu上。


CREATETABLEmy_first_table
(
idBIGINT,
nameSTRING,
PRIMARYKEY(id)
)
PARTITIONBYHASHPARTITIONS16
STOREDASKUDU;

#查看所有kudu里的表
all_tables=client.list_tables()
printall_tables

#删除kudu里的表
client.delete_table("ods_slis_bas_rn_paid_info")
all_tables=client.list_tables()
printall_tables


importkudu
fromkudu.clientimportPartitioning
fromdatetimeimportdatetime

#ConnecttoKudumasterserver
client=kudu.connect(host="kudu.master",port=7051)

#Defineaschemaforanewtable
builder=kudu.schema_builder()
builder.add_column("key").type(kudu.int64).nullable(False).primary_key()
builder.add_column("ts_val",type_=kudu.unixtime_micros,nullable=False,compression="lz4")
schema=builder.build()

#Definepartitioningschema
partitioning=Partitioning().add_hash_partitions(column_names=["key"],num_buckets=3)

#Createnewtable
client.create_table("python-example",schema,partitioning)

#Openatable
table=client.table("python-example")

#Createanewsessionsothatwecanapplywriteoperations
session=client.new_session()

#Insertarow
op=table.new_insert({"key":111,"ts_val":datetime.utcnow()})
session.apply(op)

#Upsertarow
session=client.new_session()
op=table.new_insert()
op["tester"]="lyt"
op["source_database"]="222222222"
op["target_database"]="lafak"
op["test_time"]="2018-09-0715:23:05.481621000"
op["description"]="11111111111111"
session.apply(op)
session.flush()
		
"after":{"TESTER":"lyt","SOURCE_DATABASE":"222222222","TARGET_DATABASE":"lafak","TEST_TIME":"2018-09-0715:23:05.481621000","DESCRIPTION":"c2dddc2"}
op=table.new_insert({"test_time":"2018-09-13T10:03:49.27","target_database":"slis","description":"11111111111111","source_database":"nods","tester":"lyt001"})
session.apply(op)

#Updatingarow
op=table.new_update({"key":1,"ts_val":("2017-01-01","%Y-%m-%d")})
session.apply(op)

#Deletearow
op=table.new_delete({"test_time":"2018-09-13T10:03:49.27","target_database":"slis","description":"11111111111111","source_database":"nods","tester":"lyt001"})
session.apply(op)

#Flushwriteoperations,iffailuresoccur,captureprintthem.
try:
session.flush()
exceptkudu.KuduBadStatusase:
print(session.get_pending_errors())

#Createascannerandaddapredicate
scanner=table.scanner()
scanner.add_predicate(table["ts_val"]==datetime(2017,1,1))

#OpenScannerandreadalltuples
#Note:Thisdoesn"tscaleforlargescans
s1=scanner.open()
prints1.read_all_tuples()

#!/usr/bin/envpython



#Copyright2014Cloudera,Inc.

#

#LicensedundertheApacheLicense,Version2.0(the"License");

#youmaynotusethisfileexceptincompliancewiththeLicense.

#YoumayobtainacopyoftheLicenseat

#

#http://www.apache.org/licenses/LICENSE-2.0

#

#Unlessrequiredbyapplicablelaworagreedtoinwriting,software

#distributedundertheLicenseisdistributedonan"ASIS"BASIS,

#WITHOUTWARRANTIESORCONDITIONSOFANYKIND,eitherexpressorimplied.

#SeetheLicenseforthespecificlanguagegoverningpermissionsand

#limitationsundertheLicense.



from__future__importdivision



importjson

importfnmatch

importnose

importos

importshutil

importsubprocess

importtempfile

importtime

importunittest

importsignal



importkudu



classKuduBasicsBase(object):

"""Basetestclassthatwillstartaconfigurablenumberofmasterandtablet

servers."""



BASE_PORT=37000

NUM_TABLET_SERVERS=3



@classmethod

defstart_cluster(cls):

local_path=tempfile.mkdtemp(dir=os.getenv("TEST_TMPDIR",None))

bin_path="{0}/build/latest".format(os.getenv("KUDU_HOME"))



os.makedirs("{0}/master/".format(local_path))

os.makedirs("{0}/master/data".format(local_path))

os.makedirs("{0}/master/logs".format(local_path))



path=["{0}/kudu-master".format(bin_path),

"-rpc_server_allow_ephemeral_ports",

"-rpc_bind_addresses=0.0.0.0:0",

"-fs_wal_dir={0}/master/data".format(local_path),

"-fs_data_dirs={0}/master/data".format(local_path),

"-log_dir={0}/master/logs".format(local_path),

"-logtostderr",

"-webserver_port=0",

"-server_dump_info_path={0}/master/config.json".format(local_path)

]



p=subprocess.Popen(path,shell=False)

fid=open("{0}/master/kudu-master.pid".format(local_path),"w+")

fid.write("{0}".format(p.pid))

fid.close()



#Wehavetowaitforthemastertosettlebeforetheconfigfileappears

config_file="{0}/master/config.json".format(local_path)

for_inrange(30):

ifos.path.exists(config_file):

break

time.sleep(1)

else:

raiseException("Couldnotfindkudu-masterconfigfile")



#Iftheserverwasstartedgetthebindportfromtheconfigdump

master_config=json.load(open("{0}/master/config.json".format(local_path),"r"))

#Onemasterboundonlocalhost

master_port=master_config["bound_rpc_addresses"][0]["port"]



forminrange(cls.NUM_TABLET_SERVERS):

os.makedirs("{0}/ts/{1}".format(local_path,m))

os.makedirs("{0}/ts/{1}/logs".format(local_path,m))



path=["{0}/kudu-tserver".format(bin_path),

"-rpc_server_allow_ephemeral_ports",

"-rpc_bind_addresses=0.0.0.0:0",

"-tserver_master_addrs=127.0.0.1:{0}".format(master_port),

"-webserver_port=0",

"-log_dir={0}/master/logs".format(local_path),

"-logtostderr",

"-fs_data_dirs={0}/ts/{1}/data".format(local_path,m),

"-fs_wal_dir={0}/ts/{1}/data".format(local_path,m),

]

p=subprocess.Popen(path,shell=False)

fid=open("{0}/ts/{1}/kudu-tserver.pid".format(local_path,m),"w+")

fid.write("{0}".format(p.pid))

fid.close()



returnlocal_path,master_port



@classmethod

defstop_cluster(cls,path):

forroot,dirnames,filenamesinos.walk("{0}/..".format(path)):

forfilenameinfnmatch.filter(filenames,"*.pid"):

withopen(os.path.join(root,filename))asfid:

a=fid.read()

r=subprocess.Popen(["kill","{0}".format(a)])

r.wait()

os.remove(os.path.join(root,filename))

shutil.rmtree(path,True)



@classmethod

defsetUpClass(cls):

cls.cluster_path,master_port=cls.start_cluster()

time.sleep(1)

cls.client=kudu.Client("127.0.0.1:{0}".format(master_port))



cls.schema=cls.example_schema()



cls.ex_table="example-table"

ifcls.client.table_exists(cls.ex_table):

cls.client.delete_table(cls.ex_table)

cls.client.create_table(cls.ex_table,cls.schema)



@classmethod

deftearDownClass(cls):

cls.stop_cluster(cls.cluster_path)



@classmethod

defexample_schema(cls):

col1=kudu.ColumnSchema.create("key",kudu.INT32)

col2=kudu.ColumnSchema.create("int_val",kudu.INT32)

col3=kudu.ColumnSchema.create("string_val",kudu.STRING)



returnkudu.schema_from_list([col1,col2,col3],1)





classTestSchema(unittest.TestCase):



deftest_column_schema(self):

pass



deftest_create_schema(self):

col1=kudu.ColumnSchema.create("key",kudu.INT32)

col2=kudu.ColumnSchema.create("int_val",kudu.INT32)

col3=kudu.ColumnSchema.create("string_val",kudu.STRING)



cols=[col1,col2,col3]



#Onekeycolumn

schema=kudu.schema_from_list(cols,1)

self.assertEqual(len(schema),3)



#Questionwhetherwewanttogotheoverloadingroute

self.assertTrue(schema.at(0).equals(col1))

self.assertTrue(schema.at(1).equals(col2))

self.assertTrue(schema.at(2).equals(col3))



#Thisisn"tyetveryeasy

#self.assertEqual(schema["key"],col1)

#self.assertEqual(schema["int_val"],col2)

#self.assertEqual(schema["string_val"],col3)



deftest_column_schema_repr(self):

col1=kudu.ColumnSchema.create("key",kudu.INT32)



result=repr(col1)

expected="ColumnSchema(name=key,type=int32,nullable=False)"

self.assertEqual(result,expected)



deftest_column_schema_default_value(self):

pass





classTestTable(KuduBasicsBase,unittest.TestCase):



defsetUp(self):

pass



deftest_table_basics(self):

table=self.client.open_table(self.ex_table)



self.assertEqual(table.name,self.ex_table)

self.assertEqual(table.num_columns,len(self.schema))



deftest_table_exists(self):

self.assertFalse(self.client.table_exists("nonexistent-table"))

self.assertTrue(self.client.table_exists(self.ex_table))



deftest_delete_table(self):

name="peekaboo"

self.client.create_table(name,self.schema)

self.assertTrue(self.client.delete_table(name))

self.assertFalse(self.client.table_exists(name))



#Shouldraiseamoremeaningfulexceptionatsomepoint

val=self.client.delete_table(name)

self.assertFalse(val)



deftest_open_table_nonexistent(self):

self.assertRaises(kudu.KuduException,self.client.open_table,

"__donotexist__")



deftest_insert_nonexistent_field(self):

table=self.client.open_table(self.ex_table)

op=table.insert()

self.assertRaises(KeyError,op.__setitem__,"doesntexist",12)



deftest_insert_rows_and_delete(self):

nrows=100

table=self.client.open_table(self.ex_table)

session=self.client.new_session()

foriinrange(nrows):

op=table.insert()

op["key"]=i

op["int_val"]=i*2

op["string_val"]="hello_%d"%i

session.apply(op)



#Cannotapplythesameinserttwice,doesnotblowupinC++

self.assertRaises(Exception,session.apply,op)



#synchronous

self.assertTrue(session.flush())



#Deletetherowswejustwrote

foriinrange(nrows):

op=table.delete()

op["key"]=i

session.apply(op)

session.flush()

#TODO:verifythetableisnowempty



deftest_capture_kudu_error(self):

pass





classTestScanner(KuduBasicsBase,unittest.TestCase):



@classmethod

defsetUpClass(cls):

super(TestScanner,cls).setUpClass()



cls.nrows=100

table=cls.client.open_table(cls.ex_table)

session=cls.client.new_session()



tuples=[]

foriinrange(cls.nrows):

op=table.insert()

tup=i,i*2,"hello_%d"%i

op["key"]=tup[0]

op["int_val"]=tup[1]

op["string_val"]=tup[2]

session.apply(op)

tuples.append(tup)

session.flush()



cls.table=table

cls.tuples=tuples



@classmethod

deftearDownClass(cls):

pass



defsetUp(self):

pass



deftest_scan_rows_basic(self):

#Let"sscanwithnopredicates

scanner=self.table.scanner().open()



batch=scanner.read_all()

self.assertEqual(len(batch),self.nrows)



result_tuples=batch.as_tuples()

self.assertEqual(result_tuples,self.tuples)



deftest_scan_rows_simple_predicate(self):

scanner=self.table.scanner()

scanner.add_comparison_predicate("key",kudu.GREATER_EQUAL,20)

scanner.add_comparison_predicate("key",kudu.LESS_EQUAL,49)

scanner.open()



batch=scanner.read_all()

tuples=batch.as_tuples()



self.assertEqual(tuples,self.tuples[20:50])



deftest_scan_rows_string_predicate(self):

scanner=self.table.scanner()



scanner.add_comparison_predicate("string_val",kudu.GREATER_EQUAL,"hello_20")

scanner.add_comparison_predicate("string_val",kudu.LESS_EQUAL,"hello_25")

scanner.open()



batch=scanner.read_all()

tuples=batch.as_tuples()



self.assertEqual(tuples,self.tuples[20:26])



deftest_scan_invalid_predicates(self):

scanner=self.table.scanner()

try:

scanner.add_comparison_predicate("foo",kudu.GREATER_EQUAL,"x")

exceptException,e:

self.assertEqual("Notfound:columnnotfound:foo",str(e))



try:

scanner.add_comparison_predicate("string_val",kudu.GREATER_EQUAL,1)

exceptException,e:

self.assertEqual("Invalidargument:non-stringvalue"+

"forstringcolumnstring_val",str(e))



try:

scanner.add_comparison_predicate("string_val",kudu.GREATER_EQUAL,None)

exceptException,e:

self.assertEqual("unabletoconvertpythontype<type"NoneType">",str(e))





if__name__=="__main__":

nose.runmodule(argv=[__file__,"-vvs","-x","--pdb",

"--pdb-failure","-s"],exit=False)
