DROP TABLE IF EXISTS dim_user_info;
CREATE EXTERNAL TABLE dim_user_info
(
    `id`           string COMMENT '用户 id',
    `name`         string COMMENT '姓名',
    `birthday`     string COMMENT '生日',
    `gender`       string COMMENT '性别',
    `email`        string COMMENT '邮箱',
    `user_level`   string COMMENT '用户等级',
    `create_time`  string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `start_date`   string COMMENT '有效开始日期',
    `end_date`     string COMMENT '有效结束日期'
) COMMENT '用户表'
    STORED AS PARQUET
    LOCATION '/warehouse/gmall_three_three/dim/dim_user_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

insert into dim_user_info
select *
from (select id,
             name,
             birthday,
             gender,
             email,
             user_level,
             create_time,
             operate_time,
             '2025-03-01' start_date,
             '9999-99-99' end_date
      from ods_user_info
      where dt = '2025-03-01'
      union all
      select uh.id,
             uh.name,
             uh.birthday,
             uh.gender,
             uh.email,
             uh.user_level,
             uh.create_time,
             uh.operate_time,
             uh.start_date,
             if(uh.id is not null and uh.end_date = '9999-99-99', date_add(ui.dt, -1), uh.end_date) end_date
      from dim_user_info uh
               left join (select *
                          from ods_user_info
                          where dt = '2025-03-02') ui on uh.id = ui.id) his
order by his.id, start_date;
