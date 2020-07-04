from common import constants as const
from common import helper_functions as hf
import pyspark.sql.types as T

def get_a(spark, region, date, dfs):

    query = '''
    with t0_edit as (
    select *
    from (select *, ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY ctime desc, id desc) as txn_desc
            from t0
            where date(from_unixtime(case when '{region}' in ('A', 'B', 'C') 
                then ctime - 3600 
                else ctime end)) <= date('{date}')
            )
    where txn_desc = 1
    ),
    
    t1_edit as (
    select *
    from (select *, ROW_NUMBER() OVER(PARTITION BY flight_id ORDER BY ctime desc, id desc) as txn_desc
            from t1
            where date(from_unixtime(case when '{region}' in ('A', 'B', 'C') 
                then ctime - 3600 
                else ctime end)) <= date('{date}')
            )
    where txn_desc = 1
    )
    
    SELECT '{region}' as country,
            date(from_unixtime((case when '{region}' in ('A', 'B', 'C') 
                    then pay_a.ctime - 3600 
                    else pay_a.ctime end))) as date_id,
            t_prov.entity_id,
            trans.user_id as user_id,
            cast(user.username as string) as user_name,
            prov.entity_id as a_id,
            city.city_name,
            pay.amount as supplier_amount,
            pay_c.description as supplier_flight_channel,
            pay.channel_ref as supplier_flight_channel_ref,
            pay.channel_ref2 as supplier_flight_channel_ref2,
            trans.transaction_id as supplier_transaction_id,
            pay.flight_id as supplier_flight_id,
            'SUCCESS' as supplier_flight_status,
            cast(get_json_object(trans.extra_data,'$.producer_transaction_id') as bigint) as producer_transaction_id,
            cast(get_json_object(trans.extra_data,'$.producer_flight_id') as bigint) as producer_flight_id,
            t_pay_c.description as producer_flight_channel,
            coalesce(get_json_object(pc.extra_data, '$.description'), pc.description) AS flight_type,
            t_pay.channel_ref as producer_flight_channel_ref,
            t_pay.channel_ref2 as producer_flight_channel_ref2,
            CASE WHEN t_taudit.new_status = 0 then 'INITIAL' 
                WHEN t_taudit.new_status = 1 then 'PAYMENT_ONGOING' 
                WHEN t_taudit.new_status = 2 then 'PAYMENT_INCOMPLETE' 
                WHEN t_taudit.new_status = 4 then 'PAYMENT_RECONCILING' 
                WHEN t_taudit.new_status = 6 then 'PAYMENT_OVERCOMPLETE' 
                WHEN t_taudit.new_status = 10 then 'PAYMENT_COMPLETE' 
                WHEN t_taudit.new_status = -12 then 'CANCEL_READY' 
                WHEN t_taudit.new_status = 12 then 'CANCELLED' 
                WHEN t_taudit.new_status = 14 then 'BLOCKED' 
                WHEN t_taudit.new_status = 16 then 'ACTION_REQUIRED'
                WHEN t_taudit.new_status = 18 then 'ACTION_FULFILLED' 
                WHEN t_taudit.new_status = 20 then 'EXPIRED' 
                END as producer_transaction_status,
            from_unixtime(case when '{region}' in ('A', 'B', 'C') 
                            then t_pay.mtime - 3600 
                            else t_pay.mtime end) as producer_transaction_update_time,
            CASE WHEN t_pay_a.new_status = 0 then 'INITIAL' 
                WHEN t_pay_a.new_status = 2 then 'PAYMENT_INIT' 
                WHEN t_pay_a.new_status = 4 then 'AUTHORIZED' 
                WHEN t_pay_a.new_status = 6 then 'USER_PROCESSING' 
                WHEN t_pay_a.new_status = 8 then 'PENDING' 
                WHEN t_pay_a.new_status = 10 then 'BLOCKED' 
                WHEN t_pay_a.new_status = 12 then 'ACTION_REQUIRED' 
                WHEN t_pay_a.new_status = 14 then 'RECONCILING' 
                WHEN t_pay_a.new_status = 20 then 'SUCCESS' 
                WHEN t_pay_a.new_status = 22 then 'FAILED'
                WHEN t_pay_a.new_status = 24 then 'LATE_SUCCESS' 
                WHEN t_pay_a.new_status = 39 then 'UNDERPAID' 
                WHEN t_pay_a.new_status = 40 then 'OVERPAID' 
                WHEN t_pay_a.new_status = 52 then 'VOID_READY' 
                WHEN t_pay_a.new_status = 51 then 'CANCEL_READY' 
                WHEN t_pay_a.new_status = 50 then 'CANCELLED' 
                WHEN t_pay_a.new_status = 100 then 'FRAUD' 
                WHEN t_pay_a.new_status = 999 then 'REJECTED' 
                END as producer_flight_status
            
    from 
        (
            select *
            from 
                (
                    select *, ROW_NUMBER() OVER(PARTITION BY flight_id ORDER BY ctime asc) as rank  --requested by LY, need to dedupe and choose the one with min(ctime)
                    from t1
                    where  new_status in ({SUCCESS}) --mps_constants.PaymentStatus.SUCCESS
                    and old_status not in ({SUCCESS}) --mps_constants.PaymentStatus.SUCCESS
                )
            where rank = 1
        ) pay_a 
    left join t2 pay 
        on pay_a.flight_id = pay.flight_id
    left join t3 prov
        on prov.transaction_id = pay.transaction_id
    left join t4 prov_c
        on prov.channel_id = prov_c.channel_id
    left join t5 pay_c 
        on pay.channel_id = pay_c.channel_id
    left join t6 trans 
        on pay.transaction_id = trans.transaction_id
    left join t7 user 
        on trans.user_id = user.userid
    left join t8 as city 
        on trans.city_id = city.city_id 
        
    left join t2 t_pay 
        on cast(get_json_object(trans.extra_data,'$.producer_transaction_id') as bigint) = t_pay.transaction_id
        and cast(get_json_object(trans.extra_data,'$.producer_flight_id') as bigint) = t_pay.flight_id
    left join t5 t_pay_c
        on t_pay.channel_id = t_pay_c.channel_id
    left join 
        (
            select t_prov.transaction_id, t_prov.entity_id
            from t3 t_prov 
            join t4 t_prov_c on t_prov.channel_id = t_prov_c.channel_id and t_prov_c.category not in (9,10,13) --filter away those fake checkouts, it's a bug in mps admin
        ) t_prov  on cast(get_json_object(trans.extra_data,'$.producer_transaction_id') as bigint) = t_prov.transaction_id
    left join t0_edit t_taudit
        on cast(get_json_object(trans.extra_data,'$.producer_transaction_id') as bigint) = t_taudit.transaction_id
    left join t1_edit t_pay_a
        on cast(get_json_object(trans.extra_data,'$.producer_transaction_id') as bigint) = t_pay_a.transaction_id
        and cast(get_json_object(trans.extra_data,'$.producer_flight_id') as bigint) = t_pay_a.flight_id
    left join t9 as co 
        on co.checkoutid = t_prov.entity_id
    left join t10 pc  
        on pc.channelid = co.checkout_info.checkout_flight_info.channelid -- flight channelid
        
    where prov_c.category in ({AUTO_SUPPLIER}, {SERVER_INITIATED_SUPPLIER})
    and date(from_unixtime((case when '{region}' in ('A', 'B', 'C') 
            then pay_a.ctime - 3600 
            else pay_a.ctime end))) = date('{date}')
    and t_pay_c.description != 'JKO COD'
    '''
    query = query.format(region=region, date=date,
                         SUCCESS=const.SPM.PaymentStatus.SUCCESS,
                         AUTO_SUPPLIER=const.SPM.ProvisionChannelCategory.AUTO_SUPPLIER,
                         SERVER_INITIATED_SUPPLIER=const.SPM.ProvisionChannelCategory.SERVER_INITIATED_SUPPLIER)
    print query
    df = spark.sql(query)
    # df.cache()
    pprice_udf = hf.pprice_udf
    df = df.withColumn("supplier_amount", pprice_udf(df["supplier_amount"]))
    df.cache()
    return df

def se_a(spark, region, date, dfs):
    query = '''
    WITH t0 AS
        (
            SELECT
                t1.country, t1.date_id, t1.entity_id, t1.user_id, t1.user_name, t1.a_id, t1.city_name, 
                t1.supplier_amount, t1.supplier_flight_channel, t1.supplier_flight_channel_ref, t1.supplier_flight_channel_ref2,
                t1.supplier_transaction_id, t1.supplier_flight_id, t1.supplier_flight_status, t1.producer_transaction_id, t1.producer_flight_id,
                t1.producer_flight_channel, t1.flight_type, t1.producer_flight_channel_ref, t1.producer_flight_channel_ref2, t1.producer_transaction_status,
                t1.producer_transaction_update_time, t1.producer_flight_status,
                AVG(od.cb_option) AS checkout_cb_option
            FROM total_a t1
            LEFT JOIN t12 od ON t1.entity_id = od.checkoutid
            WHERE t1.city_name like '%SE%'
            --and producer_flight_channel not in ('SE Credit')
            AND (CASE WHEN producer_flight_channel like '%SE Credit%'
                    THEN 0 ELSE 1 END) = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
        )
    
    select country, date_id, concat_ws(',',collect_set(cast(entity_id as string))) as checkout_ids, user_id, user_name, a_id, city_name, 
            supplier_amount, supplier_flight_channel, supplier_flight_channel_ref, supplier_flight_channel_ref2,
            supplier_transaction_id, supplier_flight_id, supplier_flight_status, producer_transaction_id, producer_flight_id,
            producer_flight_channel, flight_type, producer_flight_channel_ref, producer_flight_channel_ref2, producer_transaction_status,
            producer_transaction_update_time, producer_flight_status,
            concat_ws(',',collect_set(checkout_type)) as checkout_type
    from
        (
            SELECT *,
                    CASE
                        WHEN t0.checkout_cb_option = 0 THEN 'local'
                        WHEN t0.checkout_cb_option = 1 THEN 'cb'
                        WHEN t0.checkout_cb_option < 1 AND t0.checkout_cb_option > 0 THEN 'mixed'
                        ELSE ''
                    END AS checkout_type
            FROM t0
        )
    group by 1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
    '''
    print query
    df = spark.sql(query)
    df.cache()
    return df

def se_market_a(spark, region, date, dfs):
    df = dfs['se_a']
    df_seMarket = df.filter(~(df.city_name.like('%SEB%') | df.city_name.like('%SE B%')))
    df_seMarket.cache()
    return df_seMarket


def se_b_a(spark, region, date, dfs):
    df = dfs['se_a']
    df_seB = df.filter(df.city_name.like('%SEB%') | df.city_name.like('%SE B%'))
    df_seB = df_seB.drop(*['flight_type', 'checkout_type'])
    df_seB.cache()
    return df_seB


def di_a(spark, region, date, dfs):
    if region == 'C':
        dfs['t13_a'].registerTempTable('t13')
        dfs['t14_a'].registerTempTable('t14')
        dfs['t15_a'].registerTempTable('t15')

    query = '''
    select t1.country, t1.date_id, concat_ws(',',collect_set(cast(t1.entity_id as string))) as checkout_ids, t1.user_id, t1.user_name, t1.a_id, t1.city_name, 
            t1.supplier_amount, t1.supplier_flight_channel, t1.supplier_flight_channel_ref, t1.supplier_flight_channel_ref2,
            t1.supplier_transaction_id, t1.supplier_flight_id, t1.supplier_flight_status, t1.producer_transaction_id, t1.producer_flight_id,
            t1.producer_flight_channel, t1.producer_flight_channel_ref, t1.producer_flight_channel_ref2, t1.producer_transaction_status,
            t1.producer_transaction_update_time, t1.producer_flight_status,
            concat_ws(',',collect_set(c2.name)) as sub_category1, 
            concat_ws(',',collect_set(c1.name)) as sub_category2,
            concat_ws(',',collect_set(i.carrier_name)) as sub_category3,
            get_json_object(o.details,'$.merchant.id') as merchant_id,
            get_json_object(o.details,'$.merchant.name') as merchant_name,
            concat_ws(',',collect_set(mps.name)) as checkout_flight_channel
             
            
    from total_a t1
    left join t13 o on t1.entity_id = o.order_id
    left join t15 mps on mps.transaction_id = t1.producer_transaction_id
    left join t14 i ON i.item_id = o.item_id
    left join t15 c1 ON c1.category_id = i.category_id
    left join t15 c2 ON c2.category_id = c1.parent_category
    
    where t1.city_name like '%DI%'
    group by 1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,26,27
    '''
    print query
    df = spark.sql(query)
    df.cache()
    return df


def f_a(spark, region, date, dfs):
    query = '''
    select t1.country, t1.date_id, concat_ws(',',collect_set(cast(t1.entity_id as string))) as checkout_ids, t1.user_id, t1.user_name, t1.a_id, t1.city_name, 
            t1.supplier_amount, t1.supplier_flight_channel, t1.supplier_flight_channel_ref, t1.supplier_flight_channel_ref2,
            t1.supplier_transaction_id, t1.supplier_flight_id, t1.supplier_flight_status, t1.producer_transaction_id, t1.producer_flight_id,
            t1.producer_flight_channel, t1.producer_flight_channel_ref, t1.producer_flight_channel_ref2, t1.producer_transaction_status,
            t1.producer_transaction_update_time, t1.producer_flight_status
    from total_a t1
    where t1.city_name like '%FO%'
    group by 1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
    '''
    print query
    df = spark.sql(query)
    df.cache()
    return df
    

def mr_a(spark, region, date, dfs):
    query = '''
    with t1 as (
    select a.*, b.out_trade_no, b.user_id as mr_user_id
    from total_a a
    left join t16 c 
        on a.entity_id = c.flight_order_id
    left join t17 b 
        on c.order_id = b.order_id
    where a.city_name like '%MR%'
    )
    
    select t1.country, t1.date_id, concat_ws(',',collect_set(cast(t1.entity_id as string))) as checkout_ids,
            concat_ws(',',collect_set(cast(t1.out_trade_no as string))) as order_ids, 
            t1.user_id, t1.user_name, t1.mr_user_id, t1.a_id, t1.city_name, 
            t1.supplier_amount, t1.supplier_flight_channel, t1.supplier_flight_channel_ref, t1.supplier_flight_channel_ref2,
            t1.supplier_transaction_id, t1.supplier_flight_id, t1.supplier_flight_status, t1.producer_transaction_id, t1.producer_flight_id,
            t1.producer_flight_channel, t1.producer_flight_channel_ref, t1.producer_flight_channel_ref2, t1.producer_transaction_status,
            t1.producer_transaction_update_time, t1.producer_flight_status
    from t1
    group by 1,2,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
    '''
    print query
    df = spark.sql(query)
    df.cache()
    return df


def sv_a(spark, region, date, dfs):
    query = '''
    SELECT 
        t1.country, 
        t1.date_id, 
        CONCAT_WS(',',COLLECT_SET(CAST(t1.entity_id AS STRING))) AS checkout_ids, 
        t1.user_id, 
        t1.user_name, 
        t1.a_id, 
        t1.city_name, 
        t1.supplier_amount, 
        t1.supplier_flight_channel, 
        t1.supplier_flight_channel_ref,
        t1.supplier_flight_channel_ref2,
        t1.supplier_transaction_id, 
        t1.supplier_flight_id, 
        t1.supplier_flight_status, 
        t1.producer_transaction_id, 
        t1.producer_flight_id,
        t1.producer_flight_channel, 
        t1.producer_flight_channel_ref, 
        t1.producer_flight_channel_ref2, 
        t1.producer_transaction_status,
        t1.producer_transaction_update_time, 
        t1.producer_flight_status
    FROM 
        total_a t1
    WHERE 
        t1.city_name LIKE '%SV%'
    GROUP BY 
        1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
    '''
    print query
    df = spark.sql(query)
    df.cache()
    return df
