#!/bin/bash

# 定义日期
if [ $# -ne 0 ]
then
	pdt=$1
else
	pdt=`date -d '-1 day' +%Y-%m-%d`
fi

# 表比较少：将每张表的采集命令直接放在脚本中即可
/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '${pdt}' as dt from t_store where 1=1 and (create_time between '${pdt} 00:00:00' and '${pdt} 23:59:59') or (update_time between '${pdt} 00:00:00' and '${pdt} 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_store \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '${pdt}' as dt from t_brand where 1=1 and (create_time between '${pdt} 00:00:00' and '${pdt} 23:59:59') or (update_time between '${pdt} 00:00:00' and '${pdt} 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_brand \
-m 1

……



# 表比较多：不同的采集方式的表名放入不同的文件中
# 全量采集表：ods_full_tbnames.txt 
while read  full_tbname
do
	/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
	--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
	--username root \
	--password 123456 \
	--query "select * from ${full_tbname} where 1=1 and  \$CONDITIONS" \
	--hcatalog-database yp_ods \
	--hcatalog-table ${full_tbname} \
	-m 1
done < ods_full_tbnames.txt 


# 新增采集表：ods_incr_new_tbnames.txt
while read  tbname
do
	/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
	--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
	--username root \
	--password 123456 \
	--query "select * from ${tbname} where 1=1 and substr(create_time，0,10) = '${pdt}' and  \$CONDITIONS" \
	--hcatalog-database yp_ods \
	--hcatalog-table ${tbname} \
	-m 1
done < ods_incr_new_tbnames.txt 

# 新增及更新采集表：ods_incr_update_tbname.txt
……



















