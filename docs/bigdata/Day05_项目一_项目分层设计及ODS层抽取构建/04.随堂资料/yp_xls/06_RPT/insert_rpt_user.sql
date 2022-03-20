--用户数量统计
insert into yp_rpt.rpt_user_count
select
    '2019-05-07',
    sum(if(login_date_last='2019-05-07',1,0)),
    sum(if(login_date_first='2019-05-07',1,0)),
    sum(if(payment_date_first='2019-05-07',1,0)),
    sum(if(payment_count>0,1,0)),
    count(*),
    if(
        sum(if(login_date_last = '2019-05-07', 1, 0)) = 0,
        null,
        cast(sum(if(login_date_last = '2019-05-07', 1, 0)) as DECIMAL(38,4))
    )/count(*),
    if(
        sum(if(payment_count>0,1,0)) = 0,
        null,
        cast(sum(if(payment_count>0,1,0)) as DECIMAL(38,4))
    )/count(*),
    if(
        sum(if(login_date_first='2019-05-07',1,0)) = 0,
        null,
        cast(sum(if(login_date_first='2019-05-07',1,0)) as DECIMAL(38,4))
    )/sum(if(login_date_last='2019-05-07',1,0))
from yp_dm.dm_user;