SELECT DISTINCT p.web_prod_id AS prod_id, a.attr_nm AS attr_name, a.ATTR_VAL_TXT AS attr_val FROM mcmn.web_prod_t p JOIN 
(SELECT web_prod_id, attr_id, attr_nm, attr_val_txt FROM mcmn.copy_prod_attr_t ca JOIN MCMN.ATTR_DEFN_T ad ON ca.ATTR_DEFN_ID=ad.ATTR_ID
 UNION ALL
 SELECT web_prod_id, attr_id, attr_nm, attr_val_txt FROM mcmn.search_prod_attr_t sa JOIN MCMN.ATTR_DEFN_T ad ON (sa.ATTR_DEFN_ID=ad.ATTR_ID AND ad.ATTR_ID=369)
 UNION ALL
 SELECT web_prod_id, -3, 'PRODUCT_NAME', web_prod_desc FROM mcmn.web_prod_t
 UNION ALL
 SELECT web_prod_id, -4, 'PRODUCT_TYPE_HIERARCHY', prod_typ_path_txt FROM mcmn.web_prod_t pp JOIN mcmn.prod_typ_t pt ON pp.PROD_TYP_ID = pt.PROD_TYP_ID) a
 ON p.WEB_PROD_ID = a.WEB_PROD_ID
 WHERE attr_id NOT IN (252, 253, 255, 258, 259, 263, 266, 270, 1836) 
ORDER BY 1,2,3 WITH UR;
