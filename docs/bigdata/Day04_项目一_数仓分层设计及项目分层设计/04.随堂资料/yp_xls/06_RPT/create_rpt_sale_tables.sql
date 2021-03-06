--门店月销售单量排行
DROP TABLE IF EXISTS yp_rpt.rpt_sale_store_cnt_month;
CREATE TABLE yp_rpt.rpt_sale_store_cnt_month(
   date_time string COMMENT '统计日期,不能用来分组统计',
   year_code string COMMENT '年code',
   year_month string COMMENT '年月',
   
   city_id string COMMENT '城市id',
   city_name string COMMENT '城市name',
   trade_area_id string COMMENT '商圈id',
   trade_area_name string COMMENT '商圈名称',
   store_id string COMMENT '店铺的id',
   store_name string COMMENT '店铺名称',
   
   order_store_cnt BIGINT COMMENT '店铺成交单量',
   miniapp_order_store_cnt BIGINT COMMENT '店铺成交单量',
   android_order_store_cnt BIGINT COMMENT '店铺成交单量',
   ios_order_store_cnt BIGINT COMMENT '店铺成交单量',
   pcweb_order_store_cnt BIGINT COMMENT '店铺成交单量'
)
COMMENT '门店月销售单量排行' 
ROW format delimited fields terminated BY '\t' 
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');


--日销售曲线
DROP TABLE IF EXISTS yp_rpt.rpt_sale_day;
CREATE TABLE yp_rpt.rpt_sale_day(
   date_time string COMMENT '统计日期,不能用来分组统计',
   year_code string COMMENT '年code',
   month_code string COMMENT '月份编码', 
   day_month_num string COMMENT '一月第几天', 
   dim_date_id string COMMENT '日期',

   sale_amt DECIMAL(38,2) COMMENT '销售收入',
   order_cnt BIGINT COMMENT '成交单量'
)
COMMENT '日销售曲线' 
ROW format delimited fields terminated BY '\t' 
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');


--渠道销量占比
DROP TABLE IF EXISTS yp_rpt.rpt_sale_fromtype_ratio;
CREATE TABLE yp_rpt.rpt_sale_fromtype_ratio(
   date_time string COMMENT '统计日期,不能用来分组统计',
   time_type string COMMENT '统计时间维度：year、month、day',
   year_code string COMMENT '年code',
   year_month string COMMENT '年月',
   dim_date_id string COMMENT '日期',
   
   order_cnt BIGINT COMMENT '成交单量',
   miniapp_order_cnt BIGINT COMMENT '小程序成交单量',
   miniapp_order_ratio DECIMAL(5,2) COMMENT '小程序成交量占比',
   android_order_cnt BIGINT COMMENT '安卓APP订单量',
   android_order_ratio DECIMAL(5,2) COMMENT '安卓APP订单量占比',
   ios_order_cnt BIGINT COMMENT '苹果APP订单量',
   ios_order_ratio DECIMAL(5,2) COMMENT '苹果APP订单量占比',
   pcweb_order_cnt BIGINT COMMENT 'PC商城成交单量',
   pcweb_order_ratio DECIMAL(5,2) COMMENT 'PC商城成交单量占比'
)
COMMENT '渠道销量占比' 
ROW format delimited fields terminated BY '\t' 
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');



