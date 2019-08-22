set hivevar:MIN_DATE=20190810;
set hivevar:MAX_DATE=20190815;
!echo "Settings:";
!echo "Min date: ${MIN_DATE}";
!echo "Max date: ${MAX_DATE}";

-- Purchase
DROP TABLE IF EXISTS purchase_cnts;
CREATE TEMPORARY TABLE purchase_cnts AS
SELECT product_id, count(*) as purch_count
FROM mcom.t_ddf_cart_item_purchase
WHERE dt>=${MIN_DATE} AND dt<=${MAX_DATE}
group by 1;

-- View
DROP TABLE IF EXISTS view_counts;
CREATE TEMPORARY TABLE view_counts AS
SELECT product_id, count(*) as view_count
FROM mcom.t_ddf_product_view
WHERE dt>=${MIN_DATE} AND dt<=${MAX_DATE}
group by 1;

-- ATB
DROP TABLE IF EXISTS atb_counts;
CREATE TEMPORARY TABLE atb_counts AS
SELECT product_id,count(*) as atb_count
FROM mcom.t_ddf_cart_item_addition
WHERE dt>=${MIN_DATE} AND dt<=${MAX_DATE}
group by 1;

DROP TABLE IF EXISTS views_atb_purch_counts;
CREATE TEMPORARY TABLE views_atb_purch_counts AS
SELECT a.*,b.atb_count,c.purch_count
from view_counts as a
left join atb_counts as b
on a.product_id = b.product_id

left join purchase_cnts as c
on a.product_id = c.product_id;