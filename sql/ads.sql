use gmall;

set hive.support.concurrency=false;
--1,7,30浏览量,访客量,会话数

drop table ads_user_sum;
create EXTERNAL  table if not exists  ads_user_sum(
                                                       recent_days string,
                                                       pv int,
                                                       uv int,
                                                       sv int
)
    STORED AS PARQUET -- 采用parquet列式存储
    LOCATION '/warehouse/gmall/ads/ads_user_sum' -- 指定在HDFS上存储位置
    TBLPROPERTIES('parquet.compression'='lzo');
insert into ads_user_sum
select recent_days,
       count(*)               pv,
       count(distinct mid_id) uv,
       count(DISTINCT sid)    SV
from (select *,
             concat(mid_id, '-',
                    last_value(if(last_page_id is null, ts, null), true) over (partition by mid_id order by ts)) as sid
      from dwd_page_log lateral view explode(array(1, 7, 30)) tmp as recent_days
      WHERE dt >= date_add('2025-03-03', -recent_days + 1)) t1
GROUP BY recent_days;


--跳出数量跟跳出率
drop table ads_user_jump;
create EXTERNAL  table if not exists  ads_user_jump(
                                                       recent_days string,
                                                       jump_sum int,
                                                       bounce_rate double
)
    STORED AS PARQUET -- 采用parquet列式存储
    LOCATION '/warehouse/gmall/ads/ads_user_jump' -- 指定在HDFS上存储位置
    TBLPROPERTIES('parquet.compression'='lzo');


insert into ads_user_jump
select recent_days,
       sum(if(c = 1, 1, 0)) jump_sum,
       round(sum(if(c = 1, 1, 0)) * 1.0 / count(*), 2) bounce_rate
from (select recent_days,
             sid,
             count(*)               c,
             count(distinct mid_id) sv
      from (select *,
                   concat(mid_id, '-',
                          last_value(if(last_page_id is null, ts, null), true)
                                     over (partition by mid_id order by ts)) as sid
            from dwd_page_log lateral view explode(array(1, 7, 30)) tmp as recent_days
            where dt >= date_add('2025-03-03', -recent_days + 1)) t1
      group by recent_days, sid) t2
group by recent_days;


--统计1,7,30的起止页面访问次数
drop table ads_user_path;
create EXTERNAL  table if not exists  ads_user_path(
                                                             mid_id int,
                                                             sid string,
                                                             `start` string,
                                                             `end` string
)
    STORED AS PARQUET -- 采用parquet列式存储
    LOCATION '/warehouse/gmall/ads/ads_user_path' -- 指定在HDFS上存储位置
    TBLPROPERTIES('parquet.compression'='lzo');

select mid_id,
       sid,
       concat(step, source)     as `start`,
       concat(step + 1, target) as `end`
from (select *,
             page_id                                           as source,
             lead(page_id) over (partition by sid order by ts) as target,
             row_number() over (partition by sid order by ts)  as step
      from (select *,
                   concat(mid_id, '-',
                          last_value(if(last_page_id is null, ts, null), true)
                                     over (partition by mid_id order by ts)) as sid
            from dwd_page_log) t1) t2
group by mid_id, sid, concat(step, source), concat(step + 1, target);





drop table ads_user_continuous;
create EXTERNAL  table if not exists  ads_user_continuous(
                                     user_id int,
                                     dt string,
                                     dt1 string,
                                     dt2 string
)
    STORED AS PARQUET -- 采用parquet列式存储
    LOCATION '/warehouse/gmall/ads/ads_user_continuous' -- 指定在HDFS上存储位置
    TBLPROPERTIES('parquet.compression'='lzo');




--连续三天登录

with a as (select user_id, dt, rank() over (partition by user_id order by dt) as a
           from dwd_page_log
           group by user_id, dt)
insert into ads_user_continuous
select user_id,
       dt,
       dt1,
       dt2
from (select user_id,
             cast(dt as DATE)                                   as dt,
             lag(dt, 1) over (partition by user_id order by dt) as dt1,
             lag(dt, 2) over (partition by user_id order by dt) as dt2
      from a) as b
where datediff(dt, dt1) = 1
  and datediff(dt1, dt2) = 1;













