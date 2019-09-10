

### INCOMPLETE WIP!!!!


-- set hivevar:DATE_DAY=20190301;
-- set hivevar:OUT_FILE=trending_data_${DATE_DAY}.csv;
-- set hivevar:CATEGORY=5449;

--!echo 'Getting all the views at a product level for category ${CATEGORY} and date ${DATE_DAY}';
--!echo 'Exporting sample data for ${DATE_DAY} to ${OUT_FILE}';

select count* from (
select product_id, product_name, product_category_id, product_category, event_date,time
from  ltrfc.cm_product_view
where product_id in (select product_id from  pros.macys_product_attribute
where actv_web_cat_ids LIKE '%${CATEGORY}%') and dt >= ${DATE_DAY} and dt <= ${DATE_DAY}
)

select product_id, product_name, product_category_id, product_category, event_date,time
from  ltrfc.cm_cart_item_addition
where product_category_id in (118,118143,60448) and dt >= ${DATE_DAY} and dt <= ${DATE_DAY}

select product_id, product_name, product_category_id, product_category, event_date,time
from  ltrfc.cm_cart_item_purchase
where product_category_id in (118,118143,60448) and dt >= ${DATE_DAY} and dt <= ${DATE_DAY}