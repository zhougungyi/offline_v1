use dev_offline_electronic_business;
-- 全量快照 day更新 每天，每个店铺的每个商品的详情指标统计 source -> ods -> dws
drop table dev_offline_electronic_business.dws_store_anys_info_1df;
create external table dev_offline_electronic_business.dws_store_anys_info_1df
(
    store_no                         string COMMENT '店铺编号',
    product_no                       string COMMENT '商品编号',
    product_name                     string COMMENT '商品名称',
    product_num_uv                   string COMMENT '商品访客数量',
    product_num_pv                   string COMMENT '商品浏览量',
    product_stopover_avg_ts          string COMMENT '商品平均停留时长',
    product_info_page_jump_rate      string COMMENT '商品详情跳出率',
    product_trove_num                string COMMENT '商品收藏人数',
    product_buy_more_num_pv          string COMMENT '商品加购件数',
    product_buy_more_num_uv          string COMMENT '商品加购人数',
    product_trove_rate               smallint COMMENT '商品收藏转化率',
    order_buyer_num                  string COMMENT '下单买家数',
    order_item_num                   string COMMENT '下单件数',
    order_amount                     string COMMENT '下单金额',
    order_conversion_rate            string COMMENT '下单转化率',
    payment_buyer_num                string COMMENT '支付买家数',
    payment_item_num                 string COMMENT '支付件数',
    payment_amount                   string COMMENT '支付金额',
    payment_conversion_rate          string COMMENT '支付转化率',
    new_payment_buyer_num            string COMMENT '支付新买家数',
    old_payment_buyer_num            string COMMENT '支付老买家数',
    old_buyer_payment_amount         string COMMENT '老买家支付金额',
    customer_unit_price              string COMMENT '客单价',
    successful_refund_return_amount  string COMMENT '成功退款退货金额',
    annual_payment_amount            string COMMENT '年累计支付金额',
    visitor_avg_value                string COMMENT '访客平均价值',
    competitiveness_score            string COMMENT '竞争力评分',
    product_micro_detail_visitor_num string COMMENT '商品微详情访客数',
    source_system                    string COMMENT '数据来源系统',
    source_table                     string COMMENT '数据来源表',
    load_time                        timestamp COMMENT '数据加载时间'
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    location '/origin_data/dev_offline_electronic_business/dws/dws_store_anys_info_1df';



-- with new_table as (select oot.order_id,
--                           oot.user_id,
--                           oot.store_no,
--                           oot.product_no,
--                           oot.quantity,
--                           oot.amount,
--                           oot.order_status,
--                           oot.is_new_buyer,
--                           oot.order_time,
--                           opv.event_time,
--                           opv.stay_duration,
--                           orr.refund_id,
--                           orr.refund_amount,
--                           orr.refund_status,
--                           ocb.cart_id,
--                           ocb.quantity,
--                           opi.product_name,
--                           opr.payment_amount,
--                           opr.payment_channel,
--                           opc.score_source,
--                           opc.raw_score,
--                           opf.fav_id,
--                           omdv.visit_id
--                    from ods_order_transaction oot
--                             left join ods_product_visit opv on oot.user_id = opv.user_id
--                             left join ods_refund_record orr on oot.order_id = orr.order_id
--                             left join ods_cart_behavior ocb on oot.user_id = ocb.user_id
--                             left join ods_product_info opi on oot.product_no = opi.product_no
--                             left join ods_payment_record opr on oot.order_id = opr.order_id
--                             left join ods_product_competitiveness opc on oot.product_no = opc.product_no
--                             left join ods_product_favorites opf on oot.user_id = opf.user_id
--                             left join ods_micro_detail_visit omdv on omdv.product_no = oot.product_no)
-- select new.store_no,
--        new.product_no,
--        new.product_name,
--        count(distinct new.user_id)                                    as product_num_uv,
--        count(*)                                                       as product_num_pv,
--        avg(new.stay_duration)                                         as product_stopover_avg_ts,
--        sum(if(new.visit_id is not null, 1, 0)) / count(*)             as product_info_page_jump_rate,
--        if(new.fav_id is not null, count(distinct new.user_id), 0)     as product_trove_num,
--        sum(quantity)                                              as product_buy_more_num_pv,
--        count(distinct if(new.cart_id is not null, new.user_id, null)) as product_buy_more_num_uv,
--        sum(if(new.fav_id is not null, 1, 0)) / count(*)             as product_info_page_jump_rate,
--        count(distinct if(order_id is not null ,user_id,null))   as order_buyer_num,
--        sum(if(order_id is not null ,1,0))   as order_item_num,
--        sum(amount) as order_amount,
--         count(distinct user_id) /count(*) as order_conversion_rate,
--        sum(payment_id)
--
-- from new_table as new lateral view explode(array(1, 7, 30)) tmp as recent_days
-- where order_time >= DATE_ADD('2025-03-31', -recent_days + 1)
-- group by recent_days, store_no, product_no, product_name;










WITH
-- 商品基础信息
product_base AS (
    SELECT
        product_no,
        product_name,
        store_no
    FROM ods_product_info
),
-- 流量指标
visit_metrics AS (
    SELECT
        product_no,
        store_no,
        recent_days,
        COUNT(DISTINCT CASE WHEN event_type='uv' THEN user_id END) AS product_num_uv,
        COUNT(CASE WHEN event_type='pv' THEN 1 END) AS product_num_pv,
        ROUND(AVG(stay_duration), 2) AS product_stopover_avg_ts,
        ROUND(SUM(CASE WHEN stay_duration < 10 THEN 1 ELSE 0 END) * 100.0 /
              COUNT(*), 2) AS product_info_page_jump_rate
    FROM ods_product_visit lateral view explode(array(1, 7, 30)) tmp as recent_days
where event_time >= DATE_ADD('2025-03-31', -recent_days + 1)
    GROUP BY product_no, store_no,recent_days
),

-- 行为指标
behavior_metrics AS (
    SELECT
        product_no,
        store_no,
        action_time,
        COUNT(DISTINCT user_id) AS product_trove_num,
        SUM(quantity) AS product_buy_more_num_pv,
        COUNT(DISTINCT user_id) AS product_buy_more_num_uv
    FROM ods_cart_behavior lateral view explode(array(1, 7, 30)) tmp as recent_days
where action_time >= DATE_ADD('2025-03-31', -recent_days + 1)
    GROUP BY product_no, store_no,action_time
),

-- 交易核心指标
transaction_metrics AS (
    SELECT
        t.product_no,
        t.store_no,
        COUNT(DISTINCT t.user_id) AS order_buyer_num,
        SUM(t.quantity) AS order_item_num,
        SUM(t.amount) AS order_amount,
        COUNT(DISTINCT t.user_id) AS payment_buyer_num,
        SUM(CASE WHEN p.payment_id IS NOT NULL THEN t.quantity ELSE 0 END) AS payment_item_num,
        SUM(COALESCE(p.payment_amount, 0)) AS payment_amount,
        SUM(CASE WHEN t.is_new_buyer AND p.payment_id IS NOT NULL THEN 1 ELSE 0 END) AS new_payment_buyer_num,
        SUM(CASE WHEN NOT t.is_new_buyer AND p.payment_id IS NOT NULL THEN 1 ELSE 0 END) AS old_payment_buyer_num,
        SUM(CASE WHEN NOT t.is_new_buyer THEN COALESCE(p.payment_amount, 0) ELSE 0 END) AS old_buyer_payment_amount
    FROM ods_order_transaction t
    LEFT JOIN ods_payment_record p ON t.order_id = p.order_id
    GROUP BY t.product_no, t.store_no
),

-- 售后指标
refund_metrics AS (
    SELECT
        t.product_no,
        t.store_no,
        SUM(r.refund_amount) AS successful_refund_return_amount
    FROM ods_order_transaction t
    JOIN ods_refund_record r ON t.order_id = r.order_id
    WHERE r.refund_status = '已完成'
    GROUP BY t.product_no, t.store_no
),

-- 微详情指标
micro_visit_metrics AS (
    SELECT
        product_no,
        COUNT(DISTINCT user_id) AS product_micro_detail_visitor_num
    FROM ods_micro_detail_visit
    GROUP BY product_no
),

-- 竞争力评分
competitiveness AS (
    SELECT
        product_no,
        store_no,
        AVG(raw_score) AS competitiveness_score
    FROM ods_product_competitiveness
    GROUP BY product_no, store_no
)

-- 最终整合
INSERT into TABLE dws_store_anys_info_1df
SELECT
    p.store_no,
    p.product_no,
    p.product_name,
    -- 流量指标
    COALESCE(v.product_num_uv, 0),
    COALESCE(v.product_num_pv, 0),
    COALESCE(v.product_stopover_avg_ts, 0),
    COALESCE(v.product_info_page_jump_rate, 0),
    -- 行为指标
    COALESCE(f.product_trove_num, 0),
    COALESCE(b.product_buy_more_num_pv, 0),
    COALESCE(b.product_buy_more_num_uv, 0),
    CASE
        WHEN COALESCE(v.product_num_uv, 0) = 0 THEN 0
        ELSE ROUND(COALESCE(f.product_trove_num, 0) * 100.0 / v.product_num_uv, 2)
    END AS product_trove_rate,
    -- 交易指标
    COALESCE(t.order_buyer_num, 0),
    COALESCE(t.order_item_num, 0),
    COALESCE(t.order_amount, 0),
    CASE
        WHEN COALESCE(v.product_num_uv, 0) = 0 THEN 0
        ELSE ROUND(COALESCE(t.order_buyer_num, 0) * 100.0 / v.product_num_uv, 2)
    END AS order_conversion_rate,
    COALESCE(t.payment_buyer_num, 0),
    COALESCE(t.payment_item_num, 0),
    COALESCE(t.payment_amount, 0),
    CASE
        WHEN COALESCE(t.order_buyer_num, 0) = 0 THEN 0
        ELSE ROUND(COALESCE(t.payment_buyer_num, 0) * 100.0 / t.order_buyer_num, 2)
    END AS payment_conversion_rate,
    -- 客户分析
    COALESCE(t.new_payment_buyer_num, 0),
    COALESCE(t.old_payment_buyer_num, 0),
    COALESCE(t.old_buyer_payment_amount, 0),
    CASE
        WHEN COALESCE(t.payment_buyer_num, 0) = 0 THEN 0
        ELSE ROUND(COALESCE(t.payment_amount, 0) / t.payment_buyer_num, 2)
    END AS customer_unit_price,
    -- 售后指标
    COALESCE(r.successful_refund_return_amount, 0),
    -- 综合指标
    COALESCE(t.payment_amount, 0) AS annual_payment_amount, -- 简化计算，实际应按年累计
    CASE
        WHEN COALESCE(v.product_num_uv, 0) = 0 THEN 0
        ELSE ROUND(COALESCE(t.payment_amount, 0) / v.product_num_uv, 2)
    END AS visitor_avg_value,
    COALESCE(c.competitiveness_score, 0),
    COALESCE(m.product_micro_detail_visitor_num, 0),
    -- 元数据
    'ODS' AS source_system,
    'ods_*' AS source_table,
    CURRENT_TIMESTAMP() AS load_time
FROM product_base p
LEFT JOIN visit_metrics v ON p.product_no = v.product_no AND p.store_no = v.store_no
LEFT JOIN behavior_metrics b ON p.product_no = b.product_no AND p.store_no = b.store_no
LEFT JOIN (
    SELECT user_id, product_no, store_no, COUNT(*) AS product_trove_num
    FROM ods_product_favorites
    GROUP BY user_id, product_no, store_no
) f ON p.product_no = f.product_no AND p.store_no = f.store_no
LEFT JOIN transaction_metrics t ON p.product_no = t.product_no AND p.store_no = t.store_no
LEFT JOIN refund_metrics r ON p.product_no = r.product_no AND p.store_no = r.store_no
LEFT JOIN micro_visit_metrics m ON p.product_no = m.product_no
LEFT JOIN competitiveness c ON p.product_no = c.product_no AND p.store_no = c.store_no;













--
-- -- 创建临时日期维度表
-- WITH date_dimension AS (
--     SELECT explode(array('1d', '7d', '30d')) AS time_dimension
-- ),
--
-- -- 计算日期范围
-- date_range AS (
--     SELECT
--         current_date() AS current_date,
--         date_sub(current_date(), 1) AS start_date_1d,
--         date_sub(current_date(), 7) AS start_date_7d,
--         date_sub(current_date(), 30) AS start_date_30d
-- ),
--
-- -- 商品基础信息
-- product_base AS (
--     SELECT
--         product_no,
--         product_name,
--         store_no
--     FROM ods_product_info
--     WHERE dt = date_sub(current_date(), 1)  -- 假设有dt分区
-- ),
--
-- -- 访问行为指标（按时间维度）
-- visit_metrics AS (
--     SELECT
--         v.product_no,
--         v.store_no,
--         d.time_dimension,
--         COUNT(DISTINCT CASE WHEN v.event_type='uv' THEN v.user_id END) AS product_num_uv,
--         COUNT(CASE WHEN v.event_type='pv' THEN 1 END) AS product_num_pv,
--         ROUND(AVG(v.stay_duration), 2) AS product_stopover_avg_ts,
--         ROUND(SUM(CASE WHEN v.stay_duration < 10 THEN 1 ELSE 0 END) * 100.0 /
--               NULLIF(COUNT(*), 0), 2) AS product_info_page_jump_rate
--     FROM ods_product_visit v
--     CROSS JOIN date_dimension d
--     CROSS JOIN date_range r
--     WHERE
--         ((d.time_dimension = '1d' AND v.event_time >= r.start_date_1d) OR
--          (d.time_dimension = '7d' AND v.event_time >= r.start_date_7d) OR
--          (d.time_dimension = '30d' AND v.event_time >= r.start_date_30d))
--         AND v.dt >= date_sub(current_date(), 30)  -- 分区裁剪
--     GROUP BY v.product_no, v.store_no, d.time_dimension
-- ),
--
-- -- 购物车行为指标（按时间维度）
-- cart_metrics AS (
--     SELECT
--         c.product_no,
--         c.store_no,
--         d.time_dimension,
--         SUM(c.quantity) AS product_buy_more_num_pv,
--         COUNT(DISTINCT c.user_id) AS product_buy_more_num_uv
--     FROM ods_cart_behavior c
--     CROSS JOIN date_dimension d
--     CROSS JOIN date_range r
--     WHERE
--         ((d.time_dimension = '1d' AND c.action_time >= r.start_date_1d) OR
--          (d.time_dimension = '7d' AND c.action_time >= r.start_date_7d) OR
--          (d.time_dimension = '30d' AND c.action_time >= r.start_date_30d))
--         AND c.dt >= date_sub(current_date(), 30)
--     GROUP BY c.product_no, c.store_no, d.time_dimension
-- ),
--
-- -- 收藏行为指标（按时间维度）
-- favorite_metrics AS (
--     SELECT
--         f.product_no,
--         f.store_no,
--         d.time_dimension,
--         COUNT(DISTINCT f.user_id) AS product_trove_num
--     FROM ods_product_favorites f
--     CROSS JOIN date_dimension d
--     CROSS JOIN date_range r
--     WHERE
--         ((d.time_dimension = '1d' AND f.fav_time >= r.start_date_1d) OR
--          (d.time_dimension = '7d' AND f.fav_time >= r.start_date_7d) OR
--          (d.time_dimension = '30d' AND f.fav_time >= r.start_date_30d))
--         AND f.dt >= date_sub(current_date(), 30)
--     GROUP BY f.product_no, f.store_no, d.time_dimension
-- ),
--
-- -- 订单交易指标（按时间维度）
-- order_metrics AS (
--     SELECT
--         t.product_no,
--         t.store_no,
--         d.time_dimension,
--         COUNT(DISTINCT t.user_id) AS order_buyer_num,
--         SUM(t.quantity) AS order_item_num,
--         SUM(t.amount) AS order_amount
--     FROM ods_order_transaction t
--     CROSS JOIN date_dimension d
--     CROSS JOIN date_range r
--     WHERE
--         ((d.time_dimension = '1d' AND t.order_time >= r.start_date_1d) OR
--          (d.time_dimension = '7d' AND t.order_time >= r.start_date_7d) OR
--          (d.time_dimension = '30d' AND t.order_time >= r.start_date_30d))
--         AND t.dt >= date_sub(current_date(), 30)
--     GROUP BY t.product_no, t.store_no, d.time_dimension
-- ),
--
-- -- 支付指标（按时间维度）
-- payment_metrics AS (
--     SELECT
--         t.product_no,
--         t.store_no,
--         d.time_dimension,
--         COUNT(DISTINCT t.user_id) AS payment_buyer_num,
--         SUM(t.quantity) AS payment_item_num,
--         SUM(p.payment_amount) AS payment_amount,
--         SUM(CASE WHEN t.is_new_buyer THEN 1 ELSE 0 END) AS new_payment_buyer_num,
--         SUM(CASE WHEN NOT t.is_new_buyer THEN 1 ELSE 0 END) AS old_payment_buyer_num,
--         SUM(CASE WHEN NOT t.is_new_buyer THEN p.payment_amount ELSE 0 END) AS old_buyer_payment_amount
--     FROM ods_order_transaction t
--     JOIN ods_payment_record p ON t.order_id = p.order_id
--     CROSS JOIN date_dimension d
--     CROSS JOIN date_range r
--     WHERE
--         ((d.time_dimension = '1d' AND p.payment_time >= r.start_date_1d) OR
--          (d.time_dimension = '7d' AND p.payment_time >= r.start_date_7d) OR
--          (d.time_dimension = '30d' AND p.payment_time >= r.start_date_30d))
--         AND p.dt >= date_sub(current_date(), 30)
--         AND t.dt >= date_sub(current_date(), 30)
--     GROUP BY t.product_no, t.store_no, d.time_dimension
-- ),
--
-- -- 退款指标（按时间维度）
-- refund_metrics AS (
--     SELECT
--         t.product_no,
--         t.store_no,
--         d.time_dimension,
--         SUM(r.refund_amount) AS successful_refund_return_amount
--     FROM ods_order_transaction t
--     JOIN ods_refund_record r ON t.order_id = r.order_id
--     CROSS JOIN date_dimension d
--     CROSS JOIN date_range r2
--     WHERE
--         ((d.time_dimension = '1d' AND r.refund_time >= r2.start_date_1d) OR
--          (d.time_dimension = '7d' AND r.refund_time >= r2.start_date_7d) OR
--          (d.time_dimension = '30d' AND r.refund_time >= r2.start_date_30d))
--         AND r.refund_status = '已完成'
--         AND r.dt >= date_sub(current_date(), 30)
--         AND t.dt >= date_sub(current_date(), 30)
--     GROUP BY t.product_no, t.store_no, d.time_dimension
-- ),
--
-- -- 微详情访问指标（按时间维度）
-- micro_visit_metrics AS (
--     SELECT
--         m.product_no,
--         d.time_dimension,
--         COUNT(DISTINCT m.user_id) AS product_micro_detail_visitor_num
--     FROM ods_micro_detail_visit m
--     CROSS JOIN date_dimension d
--     CROSS JOIN date_range r
--     WHERE
--         ((d.time_dimension = '1d' AND m.visit_time >= r.start_date_1d) OR
--          (d.time_dimension = '7d' AND m.visit_time >= r.start_date_7d) OR
--          (d.time_dimension = '30d' AND m.visit_time >= r.start_date_30d))
--         AND m.dt >= date_sub(current_date(), 30)
--     GROUP BY m.product_no, d.time_dimension
-- ),
--
-- -- 竞争力评分（最新值）
-- competitiveness AS (
--     SELECT
--         c1.product_no,
--         c1.store_no,
--         AVG(c1.raw_score) AS competitiveness_score
--     FROM (
--         SELECT
--             product_no,
--             store_no,
--             raw_score,
--             ROW_NUMBER() OVER (PARTITION BY product_no, store_no ORDER BY calc_time DESC) AS rn
--         FROM ods_product_competitiveness
--         WHERE dt >= date_sub(current_date(), 30)
--     ) c1
--     WHERE c1.rn = 1
--     GROUP BY c1.product_no, c1.store_no
-- )
--
-- -- 最终整合所有指标
-- INSERT OVERWRITE TABLE dws_store_anys_info_1df
-- SELECT
--     p.store_no,
--     p.product_no,
--     p.product_name,
--     d.time_dimension,
--
--     -- 流量指标
--     COALESCE(v.product_num_uv, 0) AS product_num_uv,
--     COALESCE(v.product_num_pv, 0) AS product_num_pv,
--     COALESCE(v.product_stopover_avg_ts, 0) AS product_stopover_avg_ts,
--     COALESCE(v.product_info_page_jump_rate, 0) AS product_info_page_jump_rate,
--
--     -- 行为指标
--     COALESCE(f.product_trove_num, 0) AS product_trove_num,
--     COALESCE(c.product_buy_more_num_pv, 0) AS product_buy_more_num_pv,
--     COALESCE(c.product_buy_more_num_uv, 0) AS product_buy_more_num_uv,
--     CASE
--         WHEN COALESCE(v.product_num_uv, 0) = 0 THEN 0
--         ELSE ROUND(COALESCE(f.product_trove_num, 0) * 100.0 / v.product_num_uv, 2)
--     END AS product_trove_rate,
--
--     -- 交易指标
--     COALESCE(o.order_buyer_num, 0) AS order_buyer_num,
--     COALESCE(o.order_item_num, 0) AS order_item_num,
--     COALESCE(o.order_amount, 0) AS order_amount,
--     CASE
--         WHEN COALESCE(v.product_num_uv, 0) = 0 THEN 0
--         ELSE ROUND(COALESCE(o.order_buyer_num, 0) * 100.0 / v.product_num_uv, 2)
--     END AS order_conversion_rate,
--
--     COALESCE(pay.payment_buyer_num, 0) AS payment_buyer_num,
--     COALESCE(pay.payment_item_num, 0) AS payment_item_num,
--     COALESCE(pay.payment_amount, 0) AS payment_amount,
--     CASE
--         WHEN COALESCE(o.order_buyer_num, 0) = 0 THEN 0
--         ELSE ROUND(COALESCE(pay.payment_buyer_num, 0) * 100.0 / o.order_buyer_num, 2)
--     END AS payment_conversion_rate,
--
--     -- 客户分析
--     COALESCE(pay.new_payment_buyer_num, 0) AS new_payment_buyer_num,
--     COALESCE(pay.old_payment_buyer_num, 0) AS old_payment_buyer_num,
--     COALESCE(pay.old_buyer_payment_amount, 0) AS old_buyer_payment_amount,
--     CASE
--         WHEN COALESCE(pay.payment_buyer_num, 0) = 0 THEN 0
--         ELSE ROUND(COALESCE(pay.payment_amount, 0) / pay.payment_buyer_num, 2)
--     END AS customer_unit_price,
--
--     -- 售后指标
--     COALESCE(r.successful_refund_return_amount, 0) AS successful_refund_return_amount,
--
--     -- 综合指标
--     COALESCE(pay.payment_amount, 0) AS annual_payment_amount, -- 简化计算，实际应按年累计
--     CASE
--         WHEN COALESCE(v.product_num_uv, 0) = 0 THEN 0
--         ELSE ROUND(COALESCE(pay.payment_amount, 0) / v.product_num_uv, 2)
--     END AS visitor_avg_value,
--
--     COALESCE(comp.competitiveness_score, 0) AS competitiveness_score,
--     COALESCE(m.product_micro_detail_visitor_num, 0) AS product_micro_detail_visitor_num,
--
--     -- 元数据
--     'ODS' AS source_system,
--     'ods_*' AS source_table,
--     CURRENT_TIMESTAMP() AS load_time
-- FROM product_base p
-- CROSS JOIN date_dimension d
-- LEFT JOIN visit_metrics v ON p.product_no = v.product_no AND p.store_no = v.store_no AND d.time_dimension = v.time_dimension
-- LEFT JOIN cart_metrics c ON p.product_no = c.product_no AND p.store_no = c.store_no AND d.time_dimension = c.time_dimension
-- LEFT JOIN favorite_metrics f ON p.product_no = f.product_no AND p.store_no = f.store_no AND d.time_dimension = f.time_dimension
-- LEFT JOIN order_metrics o ON p.product_no = o.product_no AND p.store_no = o.store_no AND d.time_dimension = o.time_dimension
-- LEFT JOIN payment_metrics pay ON p.product_no = pay.product_no AND p.store_no = pay.store_no AND d.time_dimension = pay.time_dimension
-- LEFT JOIN refund_metrics r ON p.product_no = r.product_no AND p.store_no = r.store_no AND d.time_dimension = r.time_dimension
-- LEFT JOIN micro_visit_metrics m ON p.product_no = m.product_no AND d.time_dimension = m.time_dimension
-- LEFT JOIN competitiveness comp ON p.product_no = comp.product_no AND p.store_no = comp.store_no;




create external table dev_offline_electronic_business.dws_flow_rate_source_1df(
   flow_rate_source string,
   flow_rate_uv int,
   payment_trove_rate double
)row format delimited fields terminated by "\t"
location '/origin_data/dev_offline_electronic_business/dws/dws_flow_rate_source_1df';

select
    flow_source,
    count(distinct user_id),

from ods_flow_rate_source_v1 group by flow_source;









create external table dev_offline_electronic_business.dws_sku_info_1df(
   sku_color string,
   sku_payment_num int,
   sku_now_inventory int,
   sku_inventory_sell_day int
)row format delimited fields terminated by "\t"
location '/origin_data/dev_offline_electronic_business/dws/dws_sku_info_1df';




create external table dev_offline_electronic_business.dws_search_terms_1df(
   search_terms string,
   payment_num int
)row format delimited fields terminated by "\t"
location '/origin_data/dev_offline_electronic_business/dws/dws_search_terms_1df';





create external table dev_offline_electronic_business.dws_product_info_1df(
   shop_no int,
   product_no int,
   product_uv int,
   product_pv int,
    product_details_uv int,
    product_details_jop_rate double,
    product_buy_more int
)partitioned by (ds string)
    row format delimited fields terminated by "\t"
location '/origin_data/dev_offline_electronic_business/dws/dws_product_info_1df';

