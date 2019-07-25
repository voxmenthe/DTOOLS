

SELECT s.pa_indiv_key AS indiv_id, v.product_id AS product_id, max(v.time_stamp) AS dt ,concat(v.session_id,s.cookie_id) AS visit_id
                           FROM ltrfc.cm_product_view AS v
                                LEFT JOIN pros.usersession_temp_champ s
                                ON s.cookie_id=v.cookie_id
                                INNER JOIN (
                                        SELECT product_id
                                        FROM pros.macys_product_attribute
                                        WHERE availability_flag = 'Y' 
                                ) prod ON prod.product_id=v.product_id                          
                                WHERE v.dt>={} AND v.dt<{} AND v.product_id IS NOT NULL AND (v.cookie_id IS NOT NULL OR s.cookie_id IS NOT NULL) 
                                group by s.pa_indiv_key, v.product_id, v.session_id, s.cookie_id



SELECT s.pa_indiv_key AS indiv_id, v.product_id AS product_id, max(v.time_stamp) AS dt
                           FROM ltrfc.cm_product_view AS v
                                LEFT JOIN pros.usersession_temp_champ s
                                ON v.cookie_id
                                INNER JOIN (
                                        SELECT product_id
                                        FROM pros.macys_product_attribute
                                        WHERE availability_flag = 'Y' 
                                ) prod ON prod.product_id=v.product_id                          
                                WHERE v.dt>={} AND v.dt<{} AND v.product_id IS NOT NULL AND v.cookie_id IS NOT NULL 
                                group by s.pa_indiv_key, v.product_id, v.session_id, v.cookie_id