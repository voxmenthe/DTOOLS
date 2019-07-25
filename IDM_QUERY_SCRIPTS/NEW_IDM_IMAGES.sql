WITH
      -- COLORS view provides a dictionary from the full color name to color normal, NRF code, and NRF description,
      -- based on the most frequently used associations in the data. This table is for fallback use only
      -- (i.e. use in COALESCE) for those cases where UPC record with the color data is unavailable.
      colors AS (SELECT clrdict.ucolor, ncolor, nrf, nrf_desc FROM
                  -- COLOR_NORMAL to COLOR
                  (
                    SELECT DISTINCT ncolor, ucolor FROM (

                           SELECT *, ROW_NUMBER () OVER (PARTITION BY ucolor ORDER BY cnt DESC) AS rn FROM (

                                  SELECT count(uid) AS cnt, ncolor, ucolor FROM (

                                         SELECT uid, ncolor, ucolor FROM (

                                                SELECT upc.upc_id as uid, upc.upc_clr_nm as ucolor, ua.attr_val_txt as ncolor
                                                FROM mcmn.item_t upc
                                                LEFT OUTER JOIN mcmn.item_attr_t ua ON (upc.upc_id=ua.upc_id AND ua.attr_id=893)

                                                UNION

                                                SELECT upc.upc_id as uid, uadd.attr_val_txt as ucolor, ua.attr_val_txt as ncolor
                                                FROM mcmn.item_t upc
                                                LEFT OUTER JOIN MCMN.ITEM_ADDL_ATTR_T uadd ON (upc.upc_id=uadd.upc_id AND uadd.attr_id=1)
                                                LEFT OUTER JOIN mcmn.item_attr_t ua ON (upc.upc_id=ua.upc_id AND ua.attr_id=893)

                                          ) WITH UR

                                  ) GROUP BY ncolor, ucolor HAVING ncolor IS NOT NULL

                          )

                    ) WHERE rn = 1

                  ) AS clrdict

                  LEFT OUTER JOIN
                  -- NRF code to COLOR
                  (
                    SELECT DISTINCT nrf, ucolor FROM (

                           SELECT *, ROW_NUMBER () OVER (PARTITION BY ucolor ORDER BY cnt DESC) AS rn FROM (

                                  SELECT count(uid) AS cnt, nrf, ucolor FROM (

                                         SELECT uid, nrf, ucolor FROM (

                                                SELECT upc.upc_id as uid, upc.upc_clr_nm as ucolor, upc.NRF_CLR_NBR as nrf
                                                FROM mcmn.item_t upc

                                                UNION

                                                SELECT upc.upc_id as uid, uadd.attr_val_txt as ucolor, upc.NRF_CLR_NBR as nrf
                                                FROM mcmn.item_t upc
                                                LEFT OUTER JOIN MCMN.ITEM_ADDL_ATTR_T uadd ON (upc.upc_id=uadd.upc_id AND uadd.attr_id=1)

                                          ) WITH UR

                                  ) GROUP BY nrf, ucolor HAVING nrf IS NOT NULL

                          )

                    ) WHERE rn = 1
                  ) AS nrfdict
                  ON clrdict.UCOLOR = nrfdict.UCOLOR

                  LEFT OUTER JOIN
                  -- NRF code to NRF description
                  (
                    SELECT DISTINCT nrf2, nrf_desc FROM (
                           SELECT nrf2, nrf_desc, ROW_NUMBER () OVER (PARTITION BY nrf2 ORDER BY cnt DESC) as rn FROM (
                                  SELECT count(*) as cnt, upc.nrf_clr_nbr as nrf2, upc.nrf_clr_desc as nrf_desc
                                  FROM mcmn.item_t upc GROUP BY upc.nrf_clr_nbr, upc.nrf_clr_desc HAVING upc.nrf_clr_desc IS NOT NULL
                                  WITH UR
                           )
                    ) WHERE rn=1
                  ) AS clrdesc

                  ON nrfdict.nrf = clrdesc.nrf2
             ),

      -- UPC view selects the first UPC record for each colorway+color name combination
      upc AS (SELECT * FROM (SELECT *, ROW_NUMBER () OVER (PARTITION BY clrway_id, upc_clr_nm) AS rn FROM mcmn.item_t WITH UR) WHERE rn=1),

      -- Product Image view selects the first Image record for every product ID+image ID combination
      pi AS (SELECT *  FROM
                           (SELECT web_prod_id, it.img_id, how_shot_txt, img_desc, CONCAT(it.img_id, file_ext_txt) as img_file_nm,
                                   ROW_NUMBER () OVER (PARTITION BY web_prod_id, it.img_id) AS rn
                            FROM mcmn.prod_img_t pit JOIN mcmn.img_t it ON pit.img_id = it.img_id WITH UR)
                       WHERE rn=1)
-- Main SELECT
SELECT pc.web_prod_id AS product_id, CAST(SUBSTR(pci.img_file_nm, 1, LOCATE('.', pci.img_file_nm)-1) AS INTEGER) AS image_id, pci.clrway_img_role_typ_cd AS colorway_image_role_type,
       COALESCE(au.attr_val_txt, colors.ncolor) AS color_normal, pc.dsply_clr_nm AS color,
       COALESCE(upc.nrf_clr_nbr, colors.nrf) AS nrf_color_code, COALESCE(upc.nrf_clr_desc, colors.nrf_desc) AS nrf_color,
       upc.upc_desc, pi.img_desc, pi.how_shot_txt

FROM mcmn.prd_clrway_t pc JOIN mcmn.prod_clrway_img_t pci ON pc.clrway_id = pci.clrway_id

LEFT OUTER JOIN pi ON (pc.web_prod_id = pi.web_prod_id AND pci.img_file_nm = pi.img_file_nm)

LEFT OUTER JOIN upc ON (pc.clrway_id = upc.clrway_id AND pc.dsply_clr_nm = upc.upc_clr_nm)

-- COLOR_NORMAL is 893
LEFT OUTER JOIN mcmn.item_attr_t au ON (au.upc_id = upc.upc_id AND au.attr_id = 893)

-- for fallback in COALESCE above
LEFT OUTER JOIN colors ON (pc.dsply_clr_nm = colors.ucolor)

-- we only want primary and secondary images (CPRI and CADD), not swatches (CSW)
WHERE pci.clrway_img_role_typ_cd!='CSW'

WITH UR;
