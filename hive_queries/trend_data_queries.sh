hive -e 'set hive.cli.print.header=true;

DROP TABLE IF EXISTS purchase_cnts;
CREATE TEMPORARY TABLE purchase_cnts AS
SELECT product_id, count(*) as purch_count
FROM mcom.t_ddf_cart_item_purchase
WHERE dt>=20190811 AND dt<=20190815
group by 1;

DROP TABLE IF EXISTS view_counts;
CREATE TEMPORARY TABLE view_counts AS
SELECT product_id, count(*) as view_count
FROM mcom.t_ddf_product_view
WHERE dt>=20190811 AND dt<=20190815
group by 1;

DROP TABLE IF EXISTS atb_counts;
CREATE TEMPORARY TABLE atb_counts AS
SELECT product_id,count(*) as atb_count
FROM mcom.t_ddf_cart_item_addition
WHERE dt>=20190811 AND dt<=20190815
group by 1;

DROP TABLE IF EXISTS views_atb_purch_counts;
CREATE TEMPORARY TABLE views_atb_purch_counts AS
SELECT a.*,b.atb_count,c.purch_count
from view_counts as a
left join atb_counts as b
on a.product_id = b.product_id

left join purchase_cnts as c
on a.product_id = c.product_id;

set hive.cli.print.header=true;
select * from views_atb_purch_counts' >> trenddata.csv

########
on 007:
########

hive -e 'select product_id, product_name, product_category_id, product_category, event_date,time from  ltrfc.cm_product_view 
where product_id in (select product_id from  pros.macys_product_attribute where actv_web_cat_ids LIKE 5449) and dt >= 20190810 and dt <= 20190816' >> 5449.csv


hive -e 'set hive.cli.print.header=true;select product_id, product_name, product_category_id, product_category, event_date,time from  ltrfc.cm_product_view where dt >= 20190811 and dt <= 20190815' >> cm_prod_view_20190811_20190815.csv

hive -e 'set hive.cli.print.header=true;select product_id, product_name, product_category_id, product_category, event_date,time from  ltrfc.cm_cart_item_addition where dt >= 20190811 and dt <= 20190815' >> cm_cart_add_20190812_20190815.csv

hive -e 'set hive.cli.print.header=true;select product_id, product_name, product_category_id, product_category, event_date,time from  ltrfc.cm_cart_item_purchase where dt >= 20190811 and dt <= 20190815' >> cm_cart_purch_20190812_20190815.csv