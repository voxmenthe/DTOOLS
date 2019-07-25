-- set hivevar:DATE_DAY=20190515;
-- set hivevar:DATE_BEG=20190301;
-- set hivevar:DATE_END=20190303;
-- set hivevar:OUT_FILE = mcom_browse_sample_${DATE_DAY}.csv;

--!echo 'Exporting sample data for ${DATE_DAY} to ${OUT_FILE}';

-- Export all the tables
set hive.exec.compress.output=false;
set mapred.reduce.tasks=1;

drop table if exists t_tmp_kgabani_reco_input_mcom_browse_order_set_${DATE_DAY}_temp;
create table t_tmp_kgabani_reco_input_mcom_browse_order_set_${DATE_DAY}_temp as
select distinct a.visit_id, a.product_id, action
from (
  select concat_ws('+', cookie_id, session_id) as visit_id
  , cast(pa_product_id as int) as product_id
  , cookie_id, session_id, dt,
   case when type in ('browse', 'a2b')                 then 'view'
        when type = 'order'                            then 'buy'
        else 'unknown' end                             as   action

  from mcom.t_sdeagg_product_history
  where ${DATE_BEG} <= dt and dt <= ${DATE_END}
  and type in ('browse', 'a2b', 'order')
) a

--## joining a with b on session_id and cookie_id - normal join - default behavior?
join (
  select cookie_id, session_id, dt
  from mcom.t_sdeagg_pa_attr_history
  where ${DATE_BEG} <= dt and dt <= ${DATE_END}
    and bf_pa_indiv_key > 10
) b
on a.session_id = b.session_id and a.cookie_id=b.cookie_id

join t_tmp_kgabani_reco_input_reco_prod_list_${DATE_DAY} c
on a.product_id = c.product_id
;

INSERT OVERWRITE LOCAL DIRECTORY './sample_tables/' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * FROM t_tmp_kgabani_reco_input_mcom_browse_order_set_${DATE_DAY}_temp;
