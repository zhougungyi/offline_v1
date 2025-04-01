from faker import Faker
import csv
import random
from datetime import datetime, timedelta

fake = Faker('zh_CN')

# 全局配置
DAYS_RANGE = 30
BASE_DATE = datetime.now() - timedelta(days=DAYS_RANGE)
STORES = [f"ST{str(i).zfill(5)}" for i in range(1, 21)]
PRODUCTS = [f"PD{str(i).zfill(8)}" for i in range(1, 101)]
USERS = [f"USER{str(i).zfill(10)}" for i in range(1, 501)]

def random_time():
    """生成随机时间戳"""
    return BASE_DATE + timedelta(
        days=random.randint(0, DAYS_RANGE-1),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )

# 生成商品基础信息
with open('ods_product_info.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(['product_no','product_name','store_no','source_system','source_table','load_time'])
    for product in PRODUCTS:
        writer.writerow([
            product,
            f"商品{fake.color_name()}{fake.random_int(100,999)}",
            random.choice(STORES),
            'ERP系统',
            'prod_base_info',
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])

# 生成访问行为数据
with open('ods_product_visit.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(['log_id','store_no','product_no','user_id','event_time','event_type','stay_duration','source_system','source_table','load_time'])
    for i in range(500):
        event_time = random_time()
        writer.writerow([
            i+1,
            random.choice(STORES),
            random.choice(PRODUCTS),
            random.choice(USERS),
            event_time.strftime('%Y-%m-%d %H:%M:%S'),
            random.choices(['pv','uv'], weights=[7,3])[0],
            random.randint(10, 1800),
            'WEB日志系统',
            'user_click_log',
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])

# 生成订单数据（核心事务表）
orders = []
with open('ods_order_transaction.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(
        ['order_id', 'user_id', 'store_no', 'product_no', 'order_time', 'quantity', 'amount', 'order_status',
         'is_new_buyer', 'source_system', 'source_table', 'load_time'])
    for i in range(500):
        order_time = random_time()
        order_id = f"ORDER{order_time.strftime('%Y%m%d%H%M%S')}{random.randint(1000, 9999)}"
        order_status = random.choices(['已支付', '待支付', '已取消'], weights=[8, 1, 1])[0]  # 提前生成状态
        amount = round(random.uniform(50, 2000), 2)

        # 需要记录的关键字段
        orders.append({
            'order_id': order_id,
            'order_time': order_time,
            'amount': amount,
            'order_status': order_status  # 新增状态存储
        })

        writer.writerow([
            order_id,
            random.choice(USERS),
            random.choice(STORES),
            random.choice(PRODUCTS),
            order_time.strftime('%Y-%m-%d %H:%M:%S'),
            random.randint(1, 5),
            amount,
            order_status,  # 使用已生成的状态
            random.choice([True, False]),
            '交易系统',
            'order_main',
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])

# 生成支付数据（需关联订单）
with open('ods_payment_record.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(['payment_id','order_id','payment_time','payment_amount','payment_channel','source_system','source_table','load_time'])
    for i, order in enumerate(orders):
        if order['order_status'] == '已支付':  # 现在可以正确访问状态
            writer.writerow([
                f"PAY{order['order_id']}",
                order['order_id'],
                (order['order_time'] + timedelta(minutes=random.randint(1,60))).strftime('%Y-%m-%d %H:%M:%S'),
                round(order['amount'] * random.uniform(0.9, 1.0),2),
                random.choice(['支付宝','微信','银联']),
                '支付系统',
                'payment_flow',
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ])
# 生成购物车数据
with open('ods_cart_behavior.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(['cart_id','user_id','product_no','store_no','quantity','action_time','source_system','source_table','load_time'])
    for i in range(500):
        action_time = random_time()
        writer.writerow([
            i+1,
            random.choice(USERS),
            random.choice(PRODUCTS),
            random.choice(STORES),
            random.randint(1,3),
            action_time.strftime('%Y-%m-%d %H:%M:%S'),
            '交易系统',
            'cart_action_log',
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])

# 生成退款数据
with open('ods_refund_record.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(['refund_id','order_id','refund_amount','refund_time','refund_type','refund_status','source_system','source_table','load_time'])
    refund_orders = random.sample([o for o in orders if o['order_status'] == '已支付'], 50)  # 假设10%的退款率
    for i, order in enumerate(refund_orders):
        writer.writerow([
            f"REFUND{order['order_id']}",
            order['order_id'],
            round(order['amount'] * random.uniform(0.1, 1.0),2),
            (order['order_time'] + timedelta(days=random.randint(1,7))).strftime('%Y-%m-%d %H:%M:%S'),
            random.choice(['仅退款','退货退款']),
            random.choice(['已完成','处理中']),
            '售后系统',
            'refund_apply',
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])

# 生成收藏数据
with open('ods_product_favorites.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(['fav_id','user_id','product_no','store_no','fav_time','source_system','source_table','load_time'])
    for i in range(500):
        writer.writerow([
            i+1,
            random.choice(USERS),
            random.choice(PRODUCTS),
            random.choice(STORES),
            random_time().strftime('%Y-%m-%d %H:%M:%S'),
            '用户行为系统',
            'favorite_log',
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])

# 生成竞争力数据
with open('ods_product_competitiveness.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(['store_no','product_no','score_source','raw_score','calc_time','source_system','source_table','load_time'])
    for i in range(500):
        writer.writerow([
            random.choice(STORES),
            random.choice(PRODUCTS),
            random.choice(['算法评分','人工评分']),
            round(random.uniform(3.0, 5.0),1),
            random_time().strftime('%Y-%m-%d %H:%M:%S'),
            'BI系统',
            'competitiveness_calculation',
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])

# 生成微详情访问数据
with open('ods_micro_detail_visit.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(['visit_id','product_no','user_id','visit_time','stay_duration','source_system','source_table','load_time'])
    for i in range(500):
        visit_time = random_time()
        writer.writerow([
            i+1,
            random.choice(PRODUCTS),
            random.choice(USERS),
            visit_time.strftime('%Y-%m-%d %H:%M:%S'),
            random.randint(5, 600),
            'APP日志系统',
            'micro_detail_log',
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])