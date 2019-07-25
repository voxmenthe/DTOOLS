#!/usr/bin/env bash

hive -e"select * FROM ltrfc.cm_cart_item_addition WHERE dt > $1 LIMIT $2" >> cm_cart_item_addition_$1_$2.csv
hive -e"select * FROM ltrfc.cm_cart_item_purchase WHERE dt > $1 LIMIT $2" >> cm_cart_item_purchase_$1_$2.csv
hive -e"select * FROM ltrfc.cm_order WHERE dt > $1 LIMIT $2" >> cm_order_$1_$2.csv
hive -e"select * FROM ltrfc.cm_page_view WHERE dt > $1 LIMIT $2" >> cm_page_view_$1_$2.csv
hive -e"select * FROM ltrfc.cm_product_view WHERE dt > $1 LIMIT $2" >> cm_product_view_$1_$2.csv
hive -e"select * FROM ltrfc.cm_session_first_page_view WHERE dt > $1 LIMIT $2" >> cm_session_first_page_view_$1_$2.csv
hive -e"select * FROM ltrfc.cm_technical_properties WHERE dt > $1 LIMIT $2" >> cm_technical_properties_$1_$2.csv