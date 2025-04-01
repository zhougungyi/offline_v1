#!/bin/bash

# MySQL 连接信息
MYSQL_HOST="cdh03"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASSWORD="root"
MYSQL_DATABASE="dev_offline_electronic_business"
MYSQL_TABLE="dws_store_anys_info_1df"

# Hive 表信息
HIVE_TABLE="dws_store_anys_info_1df"

# Sqoop 导出命令
sqoop export \
  --connect "jdbc:mysql://$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DATABASE" \
  --username $MYSQL_USER \
  --password $MYSQL_PASSWORD \
  --table $MYSQL_TABLE \
  --export-dir "/origin_data/dev_offline_electronic_business/dws/dws_store_anys_info_1df" \
  --input-fields-terminated-by '\t' \
  --input-lines-terminated-by '\n'
