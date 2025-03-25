use gmall;
----------------日志-----------------
-- 启动日志表

DROP TABLE IF EXISTS dwd_start_log;
CREATE EXTERNAL TABLE dwd_start_log(
                                       `area_code` STRING COMMENT '地区编码',
                                       `brand` STRING COMMENT '手机品牌',
                                       `channel` STRING COMMENT '渠道',
                                       `is_new` STRING COMMENT '是否首次启动',
                                       `model` STRING COMMENT '手机型号',
                                       `mid_id` STRING COMMENT '设备id',
                                       `os` STRING COMMENT '操作系统',
                                       `user_id` STRING COMMENT '会员id',
                                       `version_code` STRING COMMENT 'app版本号',
                                       `entry` STRING COMMENT 'icon手机图标 notice 通知 install 安装后启动',
                                       `loading_time` BIGINT COMMENT '启动加载时间',
                                       `open_ad_id` STRING COMMENT '广告页ID ',
                                       `open_ad_ms` BIGINT COMMENT '广告总共播放时间',
                                       `open_ad_skip_ms` BIGINT COMMENT '用户跳过广告时点',
                                       `ts` BIGINT COMMENT '时间'
) COMMENT '启动日志表'
    PARTITIONED BY (`dt` STRING) -- 按照时间创建分区
    STORED AS PARQUET -- 采用parquet列式存储
    LOCATION '/warehouse/gmall/dwd/dwd_start_log' -- 指定在HDFS上存储位置
    TBLPROPERTIES('parquet.compression'='lzo');

insert into dwd_start_log partition (dt = '2025-03-03')
select
    get_json_object(line,'$.common.ar') area_code,
    get_json_object(line,'$.common.ba') brand,
    get_json_object(line,'$.common.ch') channel,
    get_json_object(line,'$.common.is_new') is_new,
    get_json_object(line,'$.common.md') model,
    get_json_object(line,'$.common.mid') mid_id,
    get_json_object(line,'$.common.os') os,
    get_json_object(line,'$.common.uid') user_id,
    get_json_object(line,'$.common.vc') version_code,
    get_json_object(line,'$.start.entry') entry,
    get_json_object(line,'$.start.loading_time') loading_time,
    get_json_object(line,'$.start.open_ad_id') open_ad_id,
    get_json_object(line,'$.start.open_ad_ms') open_ad_ms,
    get_json_object(line,'$.start.open_ad_skip_ms') open_ad_skip_ms,
    get_json_object(line,'$.ts') ts
from ods_log;




-- 页面日志表
DROP TABLE IF EXISTS dwd_page_log;
CREATE EXTERNAL TABLE dwd_page_log(
                                      `area_code` STRING COMMENT '地区编码',
                                      `brand` STRING COMMENT '手机品牌',
                                      `channel` STRING COMMENT '渠道',
                                      `is_new` STRING COMMENT '是否首次启动',
                                      `model` STRING COMMENT '手机型号',
                                      `mid_id` STRING COMMENT '设备id',
                                      `os` STRING COMMENT '操作系统',
                                      `user_id` STRING COMMENT '会员id',
                                      `version_code` STRING COMMENT 'app版本号',
                                      `during_time` BIGINT COMMENT '持续时间毫秒',
                                      `page_item` STRING COMMENT '目标id ',
                                      `page_item_type` STRING COMMENT '目标类型',
                                      `last_page_id` STRING COMMENT '上页类型',
                                      `page_id` STRING COMMENT '页面ID ',
                                      `source_type` STRING COMMENT '来源类型',
                                      `ts` bigint
) COMMENT '页面日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_page_log'
    TBLPROPERTIES('parquet.compression'='lzo');


insert into dwd_page_log partition (dt = '2025-03-03')
select
    get_json_object(line,'$.common.ar') area_code,
    get_json_object(line,'$.common.ba') brand,
    get_json_object(line,'$.common.ch') channel,
    get_json_object(line,'$.common.is_new') is_new,
    get_json_object(line,'$.common.md') model,
    get_json_object(line,'$.common.mid') mid_id,
    get_json_object(line,'$.common.os') os,
    get_json_object(line,'$.common.uid') user_id,
    get_json_object(line,'$.common.vc') version_code,
    get_json_object(line,'$.page.during_time') during_time,
    get_json_object(line,'$.page.page_item') page_item,
    get_json_object(line,'$.page.page_item_type') page_item_type,
    get_json_object(line,'$.page.last_page_id') last_page_id,
    get_json_object(line,'$.page.page_id') page_id,
    get_json_object(line,'$.page.source_type') source_type,
    get_json_object(line,'$.ts') ts
from ods_log;

-- 动作日志表
DROP TABLE IF EXISTS dwd_action_log;
CREATE EXTERNAL TABLE dwd_action_log(
                                        `area_code` STRING COMMENT '地区编码',
                                        `brand` STRING COMMENT '手机品牌',
                                        `channel` STRING COMMENT '渠道',
                                        `is_new` STRING COMMENT '是否首次启动',
                                        `model` STRING COMMENT '手机型号',
                                        `mid_id` STRING COMMENT '设备id',
                                        `os` STRING COMMENT '操作系统',
                                        `user_id` STRING COMMENT '会员id',
                                        `version_code` STRING COMMENT 'app版本号',
                                        `during_time` BIGINT COMMENT '持续时间毫秒',
                                        `page_item` STRING COMMENT '目标id ',
                                        `page_item_type` STRING COMMENT '目标类型',
                                        `last_page_id` STRING COMMENT '上页类型',
                                        `page_id` STRING COMMENT '页面id ',
                                        `source_type` STRING COMMENT '来源类型',
                                        `action_id` STRING COMMENT '动作id',
                                        `item` STRING COMMENT '目标id ',
                                        `item_type` STRING COMMENT '目标类型',
                                        `ts` BIGINT COMMENT '时间'
) COMMENT '动作日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_action_log'
    TBLPROPERTIES('parquet.compression'='lzo');

insert into dwd_action_log partition (dt = '2025-03-02')
select
    get_json_object(line,'$.common.ar') area_code,
    get_json_object(line,'$.common.ba') brand,
    get_json_object(line,'$.common.ch') channel,
    get_json_object(line,'$.common.is_new') is_new,
    get_json_object(line,'$.common.md') model,
    get_json_object(line,'$.common.mid') mid_id,
    get_json_object(line,'$.common.os') os,
    get_json_object(line,'$.common.uid') user_id,
    get_json_object(line,'$.common.vc') version_code,
    get_json_object(line,'$.action.during_time') during_time,
    get_json_object(line,'$.action.page_item') page_item,
    get_json_object(line,'$.action.page_item_type') page_item_type,
    get_json_object(line,'$.action.last_page_id') last_page_id,
    get_json_object(line,'$.action.page_id') page_id,
    get_json_object(line,'$.action.source_type') source_type,
    get_json_object(line,'$.action.action_id') action_id,
    get_json_object(line,'$.action.item') item,
    get_json_object(line,'$.action.item_type') item_type,
    get_json_object(line,'$.ts') ts
from ods_log;

-- 曝光日志表
DROP TABLE IF EXISTS dwd_display_log;
CREATE EXTERNAL TABLE dwd_display_log(
                                         `area_code` STRING COMMENT '地区编码',
                                         `brand` STRING COMMENT '手机品牌',
                                         `channel` STRING COMMENT '渠道',
                                         `is_new` STRING COMMENT '是否首次启动',
                                         `model` STRING COMMENT '手机型号',
                                         `mid_id` STRING COMMENT '设备id',
                                         `os` STRING COMMENT '操作系统',
                                         `user_id` STRING COMMENT '会员id',
                                         `version_code` STRING COMMENT 'app版本号',
                                         `during_time` BIGINT COMMENT 'app版本号',
                                         `page_item` STRING COMMENT '目标id ',
                                         `page_item_type` STRING COMMENT '目标类型',
                                         `last_page_id` STRING COMMENT '上页类型',
                                         `page_id` STRING COMMENT '页面ID ',
                                         `source_type` STRING COMMENT '来源类型',
                                         `ts` BIGINT COMMENT 'app版本号',
                                         `display_type` STRING COMMENT '曝光类型',
                                         `item` STRING COMMENT '曝光对象id ',
                                         `item_type` STRING COMMENT 'app版本号',
                                         `order` BIGINT COMMENT '曝光顺序',
                                         `pos_id` BIGINT COMMENT '曝光位置'
) COMMENT '曝光日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_display_log'
    TBLPROPERTIES('parquet.compression'='lzo');
-- 错误日志表

DROP TABLE IF EXISTS dwd_error_log;
CREATE EXTERNAL TABLE dwd_error_log(
                                       `area_code` STRING COMMENT '地区编码',
                                       `brand` STRING COMMENT '手机品牌',
                                       `channel` STRING COMMENT '渠道',
                                       `is_new` STRING COMMENT '是否首次启动',
                                       `model` STRING COMMENT '手机型号',
                                       `mid_id` STRING COMMENT '设备id',
                                       `os` STRING COMMENT '操作系统',
                                       `user_id` STRING COMMENT '会员id',
                                       `version_code` STRING COMMENT 'app版本号',
                                       `page_item` STRING COMMENT '目标id ',
                                       `page_item_type` STRING COMMENT '目标类型',
                                       `last_page_id` STRING COMMENT '上页类型',
                                       `page_id` STRING COMMENT '页面ID ',
                                       `source_type` STRING COMMENT '来源类型',
                                       `entry` STRING COMMENT ' icon手机图标  notice 通知 install 安装后启动',
                                       `loading_time` STRING COMMENT '启动加载时间',
                                       `open_ad_id` STRING COMMENT '广告页ID ',
                                       `open_ad_ms` STRING COMMENT '广告总共播放时间',
                                       `open_ad_skip_ms` STRING COMMENT '用户跳过广告时点',
                                       `actions` STRING COMMENT '动作',
                                       `displays` STRING COMMENT '曝光',
                                       `ts` STRING COMMENT '时间',
                                       `error_code` STRING COMMENT '错误码',
                                       `msg` STRING COMMENT '错误信息'
) COMMENT '错误日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_three/dwd/dwd_error_log'
    TBLPROPERTIES('parquet.compression'='lzo');


