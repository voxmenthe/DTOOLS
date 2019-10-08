#!/usr/bin/env bash

LIM=30000

hive -e"select * FROM ltrfc.cm_cart_item_addition WHERE dt > $1 LIMIT $2" >> cm_cart_item_addition_$1_$2.csv
hive -e"select * FROM ltrfc.cm_cart_item_purchase WHERE dt > $1 LIMIT $2" >> cm_cart_item_purchase_$1_$2.csv
hive -e"select * FROM ltrfc.cm_order WHERE dt > $1 LIMIT $2" >> cm_order_$1_$2.csv
hive -e"select * FROM ltrfc.cm_page_view WHERE dt > $1 LIMIT $2" >> cm_page_view_$1_$2.csv
hive -e"select * FROM ltrfc.cm_product_view WHERE dt > $1 LIMIT $2" >> cm_product_view_$1_$2.csv
hive -e"select * FROM ltrfc.cm_session_first_page_view WHERE dt > $1 LIMIT $2" >> cm_session_first_page_view_$1_$2.csv
hive -e"select * FROM ltrfc.cm_technical_properties WHERE dt > $1 LIMIT $2" >> cm_technical_properties_$1_$2.csv

hive -e"select * FROM mcom.t_pdm_product_history LIMIT $LIM" >> mcom_t_pdm_product_history_$LIM.csv 
hive -e"select * FROM mcom.t_sdeagg_pa_attr_history LIMIT $LIM" >> mcom_t_sdeagg_pa_attr_history_$LIM.csv 
hive -e"select * FROM mcom.t_ddf_cart_item_purchase LIMIT $LIM" >> mcom_t_ddf_cart_item_purchase_$LIM.csv 
hive -e"select * FROM mcom.t_ddf_product_view LIMIT $LIM" >> mcom_t_ddf_product_view_$LIM.csv 
hive -e"select * FROM mcom.t_ddf_cart_item_addition LIMIT $LIM" >> mcom_t_ddf_cart_item_addition_$LIM.csv 
hive -e"select * FROM mcom.t_cim_merchandise LIMIT $LIM" >> mcom_t_cim_merchandise_$LIM.csv 
