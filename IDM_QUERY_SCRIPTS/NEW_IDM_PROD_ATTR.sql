SELECT * FROM (

  SELECT DISTINCT p.web_prod_id AS prod_id, a.attr_nm AS attr_name, a.ATTR_VAL_TXT AS attr_val FROM mcmn.web_prod_t p JOIN 
  (SELECT web_prod_id, attr_id, attr_nm, attr_val_txt FROM mcmn.search_prod_attr_t sa JOIN MCMN.ATTR_DEFN_T ad ON sa.ATTR_DEFN_ID=ad.ATTR_ID
   UNION ALL
   SELECT web_prod_id, attr_id, attr_nm, attr_val_txt FROM mcmn.brand_prod_attr_t ba JOIN MCMN.ATTR_DEFN_T ad ON ba.ATTR_DEFN_ID=ad.ATTR_ID
   UNION ALL
   SELECT web_prod_id, -1, 'FOB', fob_nm FROM mcmn.web_prod_t pp JOIN mcmn.prod_typ_t pt ON pp.PROD_TYP_ID = pt.PROD_TYP_ID JOIN mcmn.fob_t ft ON pt.fob_id=ft.fob_id) a
   ON p.WEB_PROD_ID = a.WEB_PROD_ID
   WHERE a.ATTR_VAL_TXT NOT IN ('false', 'N')
   AND attr_id NOT IN (11, 14, 17, 1006, 273, 24, 246, 1349, 5, 12, 267, 97, 210, 13, 109, 110, 139, 284, 15, 293, 821, 1833, 1836, 421, 217, 417, 801, 1343, 1342, 11048, 10909, 1445, 369, 371,
                       141, 31, 1813, 157, 1271, 29, 10415, 10565, 14319, 14321, 1810, 25, 10293, 14320, 10383, 272, 719, 145, 10294, 938, 235, 962, 1801, 10332, 908, 259, 280, 205, 1512, 106, 489,
                       10597, 10598, 10599, 1073, 1168, 1169, 817, 86, 216, 215, 89, 1120, 1610, 295, 294, 11049, 289, 11044, 10573, 10016, 1241, 1268, 10589, 10710, 10560, 11042, 1284, 1303, 482, 
                       10531, 430, 1024, 1020, 204, 1119, 1235, 1234, 1236, 1313, 10540, 1185, 1113, 1114, 1115, 10348, 1304, 288, 144, 802, 10297, 727, 739, 14470, 483, 778, 10692, 728, 10716, 220,
                       491, 492, 493, 494, 498, 720, 722, 729, 730, 736, 737, 738, 808, 970, 1075, 1121, 1129, 1210, 1231, 1239, 1805, 10313, 14322, 14328, 14423, 14424, 14425, 14469, 14484)
 
 UNION ALL
 
   SELECT DISTINCT u.web_prod_id AS prod_id, a.attr_nm AS attr_name, a.ATTR_VAL_TXT AS attr_val FROM mcmn.item_t u JOIN 
  (SELECT upc_id, ad.attr_id, attr_nm, attr_val_txt FROM mcmn.item_attr_t ua JOIN MCMN.ATTR_DEFN_T ad ON ua.attr_id=ad.ATTR_ID
   UNION ALL
   SELECT upc_id, ad.attr_id, attr_nm, attr_val_txt FROM mcmn.item_addl_attr_t aa JOIN MCMN.ATTR_DEFN_T ad ON aa.attr_id=ad.ATTR_ID
   UNION ALL
   SELECT upc_id, -2, 'NRF_ID', CAST(nrf_clr_nbr AS VARCHAR) FROM mcmn.item_t) a
   ON u.upc_id = a.upc_id
   WHERE a.ATTR_VAL_TXT NOT IN ('false', 'N')
   AND a.attr_id IN (-2, 893)

)
ORDER BY 1,2,3 WITH UR;
