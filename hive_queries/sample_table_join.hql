-- set hivevar:DATE_DAY=20190301;
-- set hivevar:OUT_FILE = sample_table_${DATE_DAY}.csv;

--!echo 'Exporting sample data for ${DATE_DAY} to ${OUT_FILE}';

DROP TABLE IF EXISTS sample_table_join;
CREATE TEMPORARY TABLE sample_table_join AS
SELECT A.session_id, A.cookie_id, A.time_stamp AS pdp_timestamp, A.dt, A.product_id, A.product_category_id, A.product_category_bottom,
    CASE WHEN B.product_id IS NOT NULL AND B.earliest_atb_timestamp > A.time_stamp THEN 1 ELSE 0 END AS atb_flag,
    CASE WHEN B.earliest_atb_timestamp > A.time_stamp THEN NVL(B.base_price, -1) ELSE -1 END AS base_price,
    CASE WHEN B.earliest_atb_timestamp > A.time_stamp THEN NVL(B.quantity, -1) ELSE -1 END AS quantity
FROM (
    SELECT *
    FROM mcom.t_ddf_product_view
    WHERE dt = ${DATE_DAY}) A
LEFT JOIN (
    SELECT B1.session_id, B1.cookie_id, B1.dt, B1.earliest_atb_timestamp, B1.product_id, B1.base_price,
        B1.quantity-NVL(B2.quantity,0) AS quantity
    FROM (
        SELECT session_id, cookie_id, dt, product_id, AVG(base_price) AS base_price, SUM(quantity) AS quantity, MIN(time_stamp) AS earliest_atb_timestamp
        FROM mcom.t_ddf_cart_item_addition
        WHERE dt = ${DATE_DAY}
        GROUP BY session_id, cookie_id, dt, product_id ) B1
    LEFT JOIN (
        SELECT session_id, cookie_id, dt, product_id, SUM(quantity) AS quantity
        FROM mcom.t_ddf_cart_abandonment
        WHERE dt = ${DATE_DAY}
        GROUP BY session_id, cookie_id, dt, product_id ) B2
    ON B1.dt=B2.dt AND B1.session_id=B2.session_id AND B1.product_id=B2.product_id
    WHERE B1.quantity-NVL(B2.quantity,0) > 0 ) B
ON A.dt=B.dt AND A.session_id=B.session_id AND A.product_id=B.product_id;


-- Export all the tables
set hive.exec.compress.output=false;
set mapred.reduce.tasks=1;

INSERT OVERWRITE LOCAL DIRECTORY './sample_tables/' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * FROM sample_table_join;
