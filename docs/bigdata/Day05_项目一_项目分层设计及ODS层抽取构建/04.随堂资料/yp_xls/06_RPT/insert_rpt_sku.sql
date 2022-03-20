--商品销量TOPN
insert into yp_rpt.rpt_goods_sale_topN
select
    '2020-08-09' dt,
    sku_id,
    payment_count
from
    yp_dws.dws_sku_daycount
-- where
--     dt='2020-08-09'
order by payment_count desc
limit 10;


--商品收藏TOPN
insert into yp_rpt.rpt_goods_favor_topN
select
    '2020-08-09' dt,
    sku_id,
    favor_count
from
    yp_dws.dws_sku_daycount
-- where
--     dt='2020-08-09'
order by favor_count desc
limit 10;


--商品加入购物车TOPN
insert into yp_rpt.rpt_goods_cart_topN
select
    '2020-08-09' dt,
    sku_id,
    cart_num
from
    yp_dws.dws_sku_daycount
-- where
--     dt='2021-08-31'
order by cart_num desc
limit 10;

--商品退款率TOPN
insert into yp_rpt.rpt_goods_refund_topN
select
    '2020-08-09',
    sku_id,
    cast(
      cast(refund_last_30d_count as DECIMAL(38,4)) / cast(payment_last_30d_count as DECIMAL(38,4))
      * 100
      as DECIMAL(5,2)
   ) refund_ratio
from yp_dm.dm_sku
where payment_last_30d_count!=0
order by refund_ratio desc
limit 10;