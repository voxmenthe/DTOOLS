------------------------ Temp table of visitor & cookie ids-------------------------------
-- assumption is we only need to know which cookie id goes with a visitor one time
-- capturing cookie id along with visitor id - kind of a mapping of visitor ids to cookie ids



drop table if exists default.m948957_temp
go
create table default.m948957_temp as
select distinct
vstr_id
,cookie_id
from (
    select
    max(prtn_dt) as dt
    ,vstr_id
    ,cookie_id
    from trfc.prsntn_prod_fact
    where prtn_dt between 20180821 and 20181231 --Date cookie id started being captured at pixel presented
    and cookie_id is not null
    group by vstr_id, cookie_id
) as a
go



----------------------Remove duplicate visitor ids-------------------------------------------------------.
-- cleaning the above table in case a visitor id has more than one cookie id
drop table if exists default.m948957_cookie_fact
go
create table default.m948957_cookie_fact as
select
a.vstr_id
,a.cookie_id
from default.m948957_temp as a
where vstr_id not in (
        select
        vstr_id
        from default.m948957_temp
        group by vstr_id
        having count(vstr_id) != 1
) 
go



--------------------------Create table of associated experiments per day per cookie id----------------------
-- analyze an individual experiment
-- map cookie ids to individual experiment by date
-- then go back to core metrics tables and segment everything out

drop table if exists default.m948957_prsnt_fact
go

create table default.m948957_prsnt_fact as
select distinct
prst.prtn_dt
,prst.exprmnt_key
,prst.rqst_ctrl_id
,prst.vstr_id
,ck.cookie_id
from `trfc.prsntn_prod_fact` as prst
left join default.m948957_cookie_fact as ck on
    prst.vstr_id = ck.vstr_id
where prst.prtn_dt between 20181101 and 20181231   -- Repalce with experiment date range
//and prst.exprmnt_key in ( 
//    '294818c4-bcea-4bf0-817b-afe06b40566d'
//    ,'5ae12b2a-4d43-4e52-9413-77b0d224c591'
//)
and prst.exprmnt_key != '-1'
and prst.vstr_id != 'VISITORID_MALFORMED'
and prst.dlvr_id is not null
and prst.rqst_ctrl_id != '-1'
go


--------------------------------Temp table for PDP views by expirment id---------------------------------
-- extracting primary kpis from product view table
-- in this case getting 

drop table if exists default.m948957_t2
go
create table default.m948957_t2 as
select 
prst.prtn_dt
,prst.exprmnt_key
,prst.rqst_ctrl_id
,prst.vstr_id
,prst.cookie_id as prstCookie
,vw.cookie_id as pdpCookie
,vw.session_id
,vw.product_id as viewedProd
,vw.product_view_attribute_1 as atr1Txt
//,split(vw.product_view_attribute_1, ' \\| ')[1] as vwModel
,CASE
    WHEN vw.product_view_attribute_1 LIKE '%|%' THEN COALESCE(TRIM(split (vw.product_view_attribute_1,'\\|')[1]),-1)
    WHEN vw.product_view_attribute_1 LIKE '%%20%%' THEN COALESCE(TRIM(split (vw.product_view_attribute_1,'\\%20')[2]),-1)
END AS vwModel
from default.m948957_prsnt_fact as prst
left join ltrfc.cm_product_view as vw on
    prst.cookie_id = vw.cookie_id
    and prst.prtn_dt = vw.dt
    and vw.dt between 20181101 and 20181231   -- Repalce with experiment date range
go

--------------------------------Temp table for ATB events by expirment id---------------------------------
drop table if exists default.m948957_t3
go
create table default.m948957_t3 as
select 
prst.prtn_dt
,prst.exprmnt_key
,prst.rqst_ctrl_id
,prst.vstr_id
,atb.cookie_id as atbCookie
,atb.session_id
,atb.product_id as atbProd 
,atb.order_id
,atb.cart_attribute_1 as atbAtr1Txt
,CASE
    WHEN atb.cart_attribute_1 LIKE '%|%' THEN COALESCE(TRIM(split (atb.cart_attribute_1,'\\|')[1]),-1)
    WHEN atb.cart_attribute_1 LIKE '%%20%%' THEN COALESCE(TRIM(split (atb.cart_attribute_1,'\\%20')[2]),-1)
END AS atbModel
from default.m948957_prsnt_fact as prst
join ltrfc.cm_cart_item_addition as atb on
    prst.cookie_id = atb.cookie_id
    and prst.prtn_dt = atb.dt
    and atb.dt between 20181101 and 20181231   -- Repalce with experiment date range
go




--------------------------------Temp table for cart item purchase events by expirment id---------------------------------
drop table if exists default.m948957_t4
go
create table default.m948957_t4 as
select 
prst.prtn_dt
,prst.exprmnt_key
,prst.rqst_ctrl_id
,prst.vstr_id
,pur.cookie_id as purCookie
,pur.session_id
,pur.product_id as purProd
,pur.purchase_attribute_1 as purAtr1Txt
//,split(pur.purchase_attribute_1, ' \\| ')[1] as purModel
,CASE
    WHEN pur.purchase_attribute_1 LIKE '%|%' THEN COALESCE(TRIM(split (pur.purchase_attribute_1,'\\|')[1]),-1)
    WHEN pur.purchase_attribute_1 LIKE '%%20%%' THEN COALESCE(TRIM(split (pur.purchase_attribute_1,'\\%20')[2]),-1)
END AS purModel
,(pur.quantity * pur.base_price) as saleAmt
from default.m948957_prsnt_fact as prst
join ltrfc.cm_cart_item_purchase as pur on
    prst.cookie_id = pur.cookie_id
    and prst.prtn_dt = pur.dt
    and pur.dt between 20181101 and 20181231   -- Repalce with experiment date range
where (pur.quantity * pur.base_price) < 2500
go


------------------------Summary table of PDP, ATB, & purchase events by experiment id---------------------
select
a.prtn_dt as dt
,a.exprmnt_key as expKey
,a.rqst_ctrl_id as rqst_ctrl_id
,ex.exprmnt_nm as `zone`
,ex.rcmd_model_desc as model
,a.dstnctVstrIds as distinctVisitorIds
,a.dstnctCookies as distinctCookies
,a.dstnctPdpCookies as distinctPdpCookies
,a.sessions as pdpSessions
,a.directView as directPdpViews
,a.prosView as allProsPdpViews
,a.pdpViews as pdpViews
,b.dstnctCookies as distinctAtbCookies
,b.sessions as distinctAtbSessions
,b.directAtb as directAtbs
,b.prosAtb as prosAtbs
,b.atbs as atbEvents
,c.dstnctCookies as purCookies
,c.sessions as purSessions
,c.directItmPurch as directItemPurch
,c.directSales as directSales
,c.prosItmPurch as prosItmPurchases
,c.prosSales as allProsSales
,c.itmPurchases as allItemPurchases
,c.sales as allSales
from (
    select
    prtn_dt
    ,exprmnt_key
    ,rqst_ctrl_id
    ,count(distinct vstr_id) as dstnctVstrIds
    ,count(distinct prstCookie) as dstnctCookies
    ,count(distinct pdpCookie) as dstnctPdpCookies
    ,count(distinct session_id) as sessions
    ,count(viewedProd) as pdpViews
    ,count(vwmodel) as prosView
    ,sum(case when vwmodel = rqst_ctrl_id then 1 else 0 end) as directView
    from default.m948957_t2
    group by prtn_dt
    ,exprmnt_key
    ,rqst_ctrl_id
) as a
join (
    select
    prtn_dt
    ,exprmnt_key
    ,rqst_ctrl_id
    ,count(distinct vstr_id) as dstnctVstrIds
    ,count(distinct atbCookie) as dstnctCookies
    ,count(distinct session_id) as sessions
    ,count(atbProd) as atbs
    ,count(atbmodel) as prosAtb
    ,sum(case when atbmodel = rqst_ctrl_id then 1 else 0 end) as directAtb
    from default.m948957_t3
    group by prtn_dt
    ,exprmnt_key
    ,rqst_ctrl_id
) as b on
    a.prtn_dt = b.prtn_dt
    and a.exprmnt_key = b.exprmnt_key
join (
    select
    prtn_dt
    ,exprmnt_key
    ,rqst_ctrl_id
    ,count(distinct vstr_id) as dstnctVstrIds
    ,count(distinct purCookie) as dstnctCookies
    ,count(distinct session_id) as sessions
    ,count(purprod) as itmPurchases
    ,sum(saleamt) as sales
    ,count(purmodel) as prosItmPurch
    ,sum(case when purmodel is not null then saleamt else 0 end) as prosSales
    ,sum(case when purmodel = rqst_ctrl_id then 1 else 0 end) as directItmPurch
    ,sum(case when purmodel = rqst_ctrl_id then saleamt else 0 end) as directSales
    from default.m948957_t4
    group by prtn_dt
    ,exprmnt_key
    ,rqst_ctrl_id
) as c on 
    a.prtn_dt = c.prtn_dt
    and a.exprmnt_key = c.exprmnt_key
left join trfc.exprmnt_dim_active as ex on
    a.exprmnt_key = ex.exprmnt_key
    and a.prtn_dt = ex.prtn_dt


