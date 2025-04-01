create database dev_offline_electronic_business;
use dev_offline_electronic_business;


-- 商品基础信息表
drop table dev_offline_electronic_business.ods_product_info;
CREATE external TABLE dev_offline_electronic_business.ods_product_info (
    product_no STRING COMMENT '商品编号',
    product_name STRING COMMENT '商品名称',
    store_no STRING COMMENT '所属店铺编号',
    source_system STRING COMMENT '来源系统',
    source_table STRING COMMENT '来源表',
    load_time TIMESTAMP COMMENT '加载时间'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
stored as textfile
location '/origin_data/dev_offline_electronic_business/ods/ods_product_info';

load data inpath '/lx/ods_product_info.csv' into table dev_offline_electronic_business.ods_product_info;

-- 用户访问行为原始表
CREATE external TABLE dev_offline_electronic_business.ods_product_visit (
    log_id BIGINT COMMENT '日志ID',
    store_no STRING COMMENT '店铺编号',
    product_no STRING COMMENT '商品编号',
    user_id STRING COMMENT '用户ID',
    event_time TIMESTAMP COMMENT '行为时间',
    event_type STRING COMMENT '事件类型(pv/uv)',
    stay_duration INT COMMENT '停留时长(秒)',
    source_system STRING COMMENT '来源系统',
    source_table STRING COMMENT '来源表',
    load_time TIMESTAMP COMMENT '加载时间'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
stored as TEXTFILE
location '/origin_data/dev_offline_electronic_business/ods/ods_product_visit';



load data inpath '/lx/ods_product_visit.csv' into table dev_offline_electronic_business.ods_product_visit;




-- 购物车行为原始表
CREATE external TABLE dev_offline_electronic_business.ods_cart_behavior (
    cart_id BIGINT COMMENT '加购记录ID',
    user_id STRING COMMENT '用户ID',
    product_no STRING COMMENT '商品编号',
    store_no STRING COMMENT '店铺编号',
    quantity INT COMMENT '加购数量',
    action_time TIMESTAMP COMMENT '操作时间',
    source_system STRING COMMENT '来源系统',
    source_table STRING COMMENT '来源表',
    load_time TIMESTAMP COMMENT '加载时间'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
stored as TEXTFILE
location '/origin_data/dev_offline_electronic_business/ods/ods_cart_behavior';

load data inpath '/lx/ods_cart_behavior.csv' into table dev_offline_electronic_business.ods_cart_behavior;



-- 订单交易原始表
CREATE external TABLE dev_offline_electronic_business.ods_order_transaction (
    order_id STRING COMMENT '订单号',
    user_id STRING COMMENT '用户ID',
    store_no STRING COMMENT '店铺编号',
    product_no STRING COMMENT '商品编号',
    order_time TIMESTAMP COMMENT '下单时间',
    quantity INT COMMENT '购买数量',
    amount DECIMAL(18,2) COMMENT '订单金额',
    order_status STRING COMMENT '订单状态',
    is_new_buyer BOOLEAN COMMENT '是否新买家',
    source_system STRING COMMENT '来源系统',
    source_table STRING COMMENT '来源表',
    load_time TIMESTAMP COMMENT '加载时间'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
stored as TEXTFILE
location '/origin_data/dev_offline_electronic_business/ods/ods_order_transaction';

load data inpath '/lx/ods_order_transaction.csv' into table dev_offline_electronic_business.ods_order_transaction;




-- 支付行为原始表
CREATE external TABLE dev_offline_electronic_business.ods_payment_record (
    payment_id STRING COMMENT '支付流水号',
    order_id STRING COMMENT '关联订单号',
    payment_time TIMESTAMP COMMENT '支付时间',
    payment_amount DECIMAL(18,2) COMMENT '实付金额',
    payment_channel STRING COMMENT '支付渠道',
    source_system STRING COMMENT '来源系统',
    source_table STRING COMMENT '来源表',
    load_time TIMESTAMP COMMENT '加载时间'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
stored as TEXTFILE
location '/origin_data/dev_offline_electronic_business/ods/ods_payment_record';


load data inpath '/lx/ods_payment_record.csv' into table dev_offline_electronic_business.ods_payment_record;



-- 退款退货原始表
CREATE external TABLE dev_offline_electronic_business.ods_refund_record (
    refund_id STRING COMMENT '退款单号',
    order_id STRING COMMENT '原订单号',
    refund_amount DECIMAL(18,2) COMMENT '退款金额',
    refund_time TIMESTAMP COMMENT '退款时间',
    refund_type STRING COMMENT '退款类型',
    refund_status STRING COMMENT '退款状态',
    source_system STRING COMMENT '来源系统',
    source_table STRING COMMENT '来源表',
    load_time TIMESTAMP COMMENT '加载时间'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored as TEXTFILE
location '/origin_data/dev_offline_electronic_business/ods/ods_refund_record';


drop table dev_offline_electronic_business.ods_tmp_refund_record;
CREATE temporary TABLE dev_offline_electronic_business.ods_tmp_refund_record (
    refund_id STRING COMMENT '退款单号',
    order_id STRING COMMENT '原订单号',
    refund_amount DECIMAL(18,2) COMMENT '退款金额',
    refund_time TIMESTAMP COMMENT '退款时间',
    refund_type STRING COMMENT '退款类型',
    refund_status STRING COMMENT '退款状态',
    source_system STRING COMMENT '来源系统',
    source_table STRING COMMENT '来源表',
    load_time TIMESTAMP COMMENT '加载时间'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
stored as TEXTFILE;

load data inpath '/lx/ods_refund_record.csv' into table ods_tmp_refund_record;

insert into ods_refund_record
select *
from ods_tmp_refund_record;




-- 商品收藏原始表
CREATE external TABLE dev_offline_electronic_business.ods_product_favorites (
    fav_id BIGINT COMMENT '收藏记录ID',
    user_id STRING COMMENT '用户ID',
    product_no STRING COMMENT '商品编号',
    store_no STRING COMMENT '店铺编号',
    fav_time TIMESTAMP COMMENT '收藏时间',
    source_system STRING COMMENT '来源系统',
    source_table STRING COMMENT '来源表',
    load_time TIMESTAMP COMMENT '加载时间'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
stored as TEXTFILE
location '/origin_data/dev_offline_electronic_business/ods/ods_product_favorites';

load data inpath '/lx/ods_product_favorites.csv' into table ods_product_favorites;

-- 商品竞争力原始数据表
CREATE external TABLE dev_offline_electronic_business.ods_product_competitiveness (
    store_no STRING COMMENT '店铺编号',
    product_no STRING COMMENT '商品编号',
    score_source STRING COMMENT '评分来源',
    raw_score DECIMAL(5,2) COMMENT '原始评分',
    calc_time TIMESTAMP COMMENT '计算时间',
    source_system STRING COMMENT '来源系统',
    source_table STRING COMMENT '来源表',
    load_time TIMESTAMP COMMENT '加载时间'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
stored as TEXTFILE
location '/origin_data/dev_offline_electronic_business/ods/ods_product_competitiveness';

load data inpath '/lx/ods_product_competitiveness.csv' into table ods_product_competitiveness;

-- 微详情访问原始表
CREATE external TABLE dev_offline_electronic_business.ods_micro_detail_visit (
    visit_id BIGINT COMMENT '访问记录ID',
    product_no STRING COMMENT '商品编号',
    user_id STRING COMMENT '用户ID',
    visit_time TIMESTAMP COMMENT '访问时间',
    stay_duration INT COMMENT '停留时长(秒)',
    source_system STRING COMMENT '来源系统',
    source_table STRING COMMENT '来源表',
    load_time TIMESTAMP COMMENT '加载时间'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
stored as TEXTFILE
location '/origin_data/dev_offline_electronic_business/ods/ods_micro_detail_visit';


load data inpath '/lx/ods_micro_detail_visit.csv' into table dev_offline_electronic_business.ods_micro_detail_visit;

SET hive.exec.dynamic.partition.mode=nonstrict;

-- 启用动态分区
SET hive.exec.dynamic.partition=true;

-- 设置每个mapper或reducer可以创建的最大动态分区数
SET hive.exec.max.dynamic.partitions.pernode=100;

-- 设置总共可以创建的最大动态分区数
SET hive.exec.max.dynamic.partitions=1000;


drop table ods_flow_rate_source_v1;
CREATE EXTERNAL TABLE dev_offline_electronic_business.ods_flow_rate_source_v1(
    log_id STRING COMMENT '日志ID',
    user_id STRING COMMENT '用户ID',
    session_id STRING COMMENT '会话ID',
    flow_source STRING COMMENT '流量来源渠道',
    page_url STRING COMMENT '访问页面URL',
    entry_time TIMESTAMP COMMENT '进入时间',
    leave_time TIMESTAMP COMMENT '离开时间',
    is_payment TINYINT COMMENT '是否支付(0否1是)',
    device_type STRING COMMENT '设备类型',
    ip_address STRING COMMENT 'IP地址',
    province STRING COMMENT '省份',
    city STRING COMMENT '城市'
) COMMENT '流量来源原始数据表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/origin_data/dev_offline_electronic_business/ods/ods_flow_rate_source_v1';

CREATE temporary TABLE dev_offline_electronic_business.ods_flow_rate_source_v1_tmp(
    log_id STRING COMMENT '日志ID',
    user_id STRING COMMENT '用户ID',
    session_id STRING COMMENT '会话ID',
    flow_source STRING COMMENT '流量来源渠道',
    page_url STRING COMMENT '访问页面URL',
    entry_time TIMESTAMP COMMENT '进入时间',
    leave_time TIMESTAMP COMMENT '离开时间',
    is_payment TINYINT COMMENT '是否支付(0否1是)',
    device_type STRING COMMENT '设备类型',
    ip_address STRING COMMENT 'IP地址',
    province STRING COMMENT '省份',
    city STRING COMMENT '城市',
    dt STRING COMMENT '日期分区'
) COMMENT '流量来源原始数据表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
load data inpath '/lx/ods_flow_rate_source_v1.csv' into table ods_flow_rate_source_v1_tmp;

insert into ods_flow_rate_source_v1 partition (dt)
select *
from ods_flow_rate_source_v1_tmp;


drop table ods_sku_info_v1;
CREATE EXTERNAL TABLE dev_offline_electronic_business.ods_sku_info_v1(
    sku_id STRING COMMENT 'SKU唯一标识',
    product_id STRING COMMENT '关联商品ID',
    sku_code STRING COMMENT 'SKU编码',
    color STRING COMMENT '颜色属性',
    size STRING COMMENT '尺寸属性',
    spec STRING COMMENT '规格属性',
    price DECIMAL(10,2) COMMENT '单价',
    cost DECIMAL(10,2) COMMENT '成本价',
    inventory_quantity INT COMMENT '当前库存量',
    inventory_update_time TIMESTAMP COMMENT '库存更新时间',
    is_valid TINYINT COMMENT '是否有效(0无效1有效)'
) COMMENT 'SKU基础信息表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/origin_data/dev_offline_electronic_business/ods/ods_sku_info_v1';

CREATE temporary TABLE dev_offline_electronic_business.ods_sku_info_v1_tmp(
    sku_id STRING COMMENT 'SKU唯一标识',
    product_id STRING COMMENT '关联商品ID',
    sku_code STRING COMMENT 'SKU编码',
    color STRING COMMENT '颜色属性',
    size STRING COMMENT '尺寸属性',
    spec STRING COMMENT '规格属性',
    price DECIMAL(10,2) COMMENT '单价',
    cost DECIMAL(10,2) COMMENT '成本价',
    inventory_quantity INT COMMENT '当前库存量',
    inventory_update_time TIMESTAMP COMMENT '库存更新时间',
    is_valid TINYINT COMMENT '是否有效(0无效1有效)',
    dt STRING COMMENT '日期分区'
) COMMENT 'SKU基础信息表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


load data inpath '/lx/ods_sku_info_v1.csv' into table ods_sku_info_v1_tmp;

insert into ods_sku_info_v1 partition (dt)
select *
from ods_sku_info_v1_tmp;


CREATE EXTERNAL TABLE dev_offline_electronic_business.ods_search_terms_v1(
    search_id STRING COMMENT '搜索记录ID',
    user_id STRING COMMENT '用户ID',
    search_keyword STRING COMMENT '搜索关键词',
    search_time TIMESTAMP COMMENT '搜索时间',
    click_product_id STRING COMMENT '点击商品ID',
    is_payment TINYINT COMMENT '是否支付(0否1是)',
    device_type STRING COMMENT '设备类型',
    search_source STRING COMMENT '搜索来源(app/web等)'
) COMMENT '用户搜索词原始数据表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/origin_data/dev_offline_electronic_business/ods/ods_search_terms_v1';



CREATE temporary TABLE dev_offline_electronic_business.ods_search_terms_v1_tmp(
    search_id STRING COMMENT '搜索记录ID',
    user_id STRING COMMENT '用户ID',
    search_keyword STRING COMMENT '搜索关键词',
    search_time TIMESTAMP COMMENT '搜索时间',
    click_product_id STRING COMMENT '点击商品ID',
    is_payment TINYINT COMMENT '是否支付(0否1是)',
    device_type STRING COMMENT '设备类型',
    search_source STRING COMMENT '搜索来源(app/web等)',
    dt STRING COMMENT '日期分区'
) COMMENT '用户搜索词原始数据表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

load data inpath '/lx/ods_search_terms_v1.csv' into table ods_search_terms_v1_tmp;

insert into ods_search_terms_v1 partition (dt)
select *
from ods_search_terms_v1_tmp;


drop table ods_product_info_v1;
CREATE EXTERNAL TABLE dev_offline_electronic_business.ods_product_info_v1(
    product_id STRING COMMENT '商品ID',
    shop_id STRING COMMENT '店铺ID',
    product_name STRING COMMENT '商品名称',
    category_id STRING COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    brand_id STRING COMMENT '品牌ID',
    brand_name STRING COMMENT '品牌名称',
    price DECIMAL(10,2) COMMENT '商品价格',
    cost_price DECIMAL(10,2) COMMENT '成本价',
    status TINYINT COMMENT '商品状态(1上架0下架)',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    description string COMMENT '商品描述'
) COMMENT '商品基础信息表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/origin_data/dev_offline_electronic_business/ods/ods_product_info_v1';

CREATE temporary TABLE dev_offline_electronic_business.ods_product_info_v1_tmp(
    product_id STRING COMMENT '商品ID',
    shop_id STRING COMMENT '店铺ID',
    product_name STRING COMMENT '商品名称',
    category_id STRING COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    brand_id STRING COMMENT '品牌ID',
    brand_name STRING COMMENT '品牌名称',
    price DECIMAL(10,2) COMMENT '商品价格',
    cost_price DECIMAL(10,2) COMMENT '成本价',
    status TINYINT COMMENT '商品状态(1上架0下架)',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    description string COMMENT '商品描述',
    dt STRING COMMENT '日期分区'
) COMMENT '商品基础信息表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


load data inpath '/lx/ods_product_info_v1.csv' into table ods_product_info_v1_tmp;

insert into ods_product_info_v1 partition (dt)
select *
from ods_product_info_v1_tmp;


CREATE EXTERNAL TABLE dev_offline_electronic_business.ods_product_behavior_v1(
    log_id STRING COMMENT '日志ID',
    user_id STRING COMMENT '用户ID',
    product_id STRING COMMENT '商品ID',
    behavior_type TINYINT COMMENT '行为类型(1浏览2加购3下单4支付)',
    behavior_time TIMESTAMP COMMENT '行为时间',
    session_id STRING COMMENT '会话ID',
    stay_duration INT COMMENT '停留时长(秒)',
    device_type STRING COMMENT '设备类型',
    ip_address STRING COMMENT 'IP地址'
) COMMENT '商品行为明细表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/origin_data/dev_offline_electronic_business/ods/ods_product_behavior_v1';


CREATE temporary EXTERNAL TABLE dev_offline_electronic_business.ods_product_behavior_v1_tmp(
    log_id STRING COMMENT '日志ID',
    user_id STRING COMMENT '用户ID',
    product_id STRING COMMENT '商品ID',
    behavior_type TINYINT COMMENT '行为类型(1浏览2加购3下单4支付)',
    behavior_time TIMESTAMP COMMENT '行为时间',
    session_id STRING COMMENT '会话ID',
    stay_duration INT COMMENT '停留时长(秒)',
    device_type STRING COMMENT '设备类型',
    ip_address STRING COMMENT 'IP地址',
    dt STRING COMMENT '日期分区'
) COMMENT '商品行为明细表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


load data inpath '/lx/ods_product_behavior_v1.csv' into table ods_product_behavior_v1_tmp;

insert into ods_product_behavior_v1 partition (dt)
select *
from ods_product_behavior_v1_tmp;