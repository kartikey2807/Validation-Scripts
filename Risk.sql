-- Attributed Lives
create temporary table plans as
select 
distinct 
sstp as plan_name
from l2.pd_attribution;
select
p.plan_name,
coalesce(c.attributed_lives, 0) as old_l5,
coalesce(a.attributed_lives, 0) as new_l5,
coalesce(b.attributed_lives, 0) as new_l3
from plans p
left join
(
select 
p.plan_name,
count(distinct a.empi) as attributed_lives
from l5_backup.attribution a 
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE'::date
group by 1) c on 
p.plan_name = c.plan_name
left join
(
select 
p.plan_name,
count(distinct a.empi) as attributed_lives
from l5.attribution a 
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE'::date
group by 1) a on 
p.plan_name = a.plan_name
left join
(
select 
a.sstp as plan_name,
count(distinct empi)
as attributed_lives
from l2.pd_attribution a
where 
date_trunc('month', atrdt) = 'DATE' 
and inprsq = 'primary' 
and prvid <> 'N/A'
group by 1) b on 
p.plan_name = b.plan_name
order by 1;
-- Current Risk Recapture Rate %
drop table if  exists  risk_patient_temp;
create temporary table risk_patient_temp
as
select 
risk.empi,
date_trunc('month', risk.measure_date) month_of_measurement,
risk.risk_model_sub_type,
risk.period_mode,
risk.measure_config_id,
risk.risk_value,
cdi_risk.cdi_risk_value,
sus_risk.suspect_risk_value,
risk.plan_name 
from l3.risk_output risk
left join 
(
select 
avg(coalesce(risk_value_chronic, 0)) cdi_risk_value,
empi,
measure_date,
period_mode,
risk_model_sub_type,
cdi_config_id
from l3.cdi_output a
group by 2,3,4,5,6
) cdi_risk on 
cdi_risk.empi = risk.empi
and cdi_risk.measure_date = risk.measure_date
and cdi_risk.risk_model_sub_type = risk.risk_model_sub_type 
and cdi_risk.period_mode = risk.period_mode 
and cdi_risk.cdi_config_id = risk.measure_config_id 
left join 
(
select 
avg(coalesce(risk_value, 0)) suspect_risk_value,
empi,
measure_date,
'ytd' period_mode,
risk_model_sub_type 
from l3.suspect_output b
group by 2,3,4,5
) sus_risk on 
sus_risk.empi = risk.empi
and sus_risk.measure_date = risk.measure_date 
and sus_risk.risk_model_sub_type = risk.risk_model_sub_type 
and risk.period_mode = 'ytd';
alter table risk_patient_temp
add column recapture_numerator numeric(20, 6);
alter table risk_patient_temp
add column recapture_denominator numeric(20, 6);
update risk_patient_temp 
set recapture_numerator = t1.recapture_numerator
from
(
select 
count(distinct risk_factor) recapture_numerator,
empi,
date_trunc('month',measure_date)::date month_of_measurement,
period_mode,
risk_model_sub_type,
cdi_config_id
from l3.cdi_group t1
where 
t1.captured_risk_factor_flag is true
and t1.chronic_flag is true 
and t1.risk_factor_value != 0
group by 2,3,4,5,6) t1
where 
t1.empi = risk_patient_temp.empi 
and t1.month_of_measurement = risk_patient_temp.month_of_measurement
and t1.risk_model_sub_type = risk_patient_temp.risk_model_sub_type
and t1.period_mode = risk_patient_temp.period_mode
and t1.cdi_config_id = risk_patient_temp.measure_config_id;
update risk_patient_temp
set recapture_denominator = t1.recapture_denominator
from
(
select 
count(distinct risk_factor)as recapture_denominator,
empi,
date_trunc('month',measure_date)::date month_of_measurement,
period_mode,
risk_model_sub_type,
cdi_config_id
from l3.cdi_group t1 
where 
t1.chronic_flag is true 
and t1.risk_factor_value != 0
group by 2,3,4,5,6) t1
where 
t1.empi = risk_patient_temp.empi
and t1.month_of_measurement = risk_patient_temp.month_of_measurement
and t1.risk_model_sub_type = risk_patient_temp.risk_model_sub_type
and t1.period_mode = risk_patient_temp.period_mode
and t1.cdi_config_id = risk_patient_temp.measure_config_id;
select 
p.plan_name,
coalesce(c.recapture_rate, 0) as old_l5,
coalesce(a.recapture_rate, 0) as new_l5,
coalesce(b.recapture_rate, 0) as new_l3
from plans p
left join
(
select 
p.plan_name,
round(
sum(a.recapture_numerator)::decimal / 
sum(a.recapture_denominator)::decimal*100 , 1) recapture_rate
from l5_backup.risk_core_patient a
left join public.updated_risk_active_attribution b on 
b.empi = a.empi
and b.payer_master_id = a.payer_master_id 
and b.month_of_measurement = a.month_of_measurement 
and b.risk_documentation_flag = a.risk_documentation_flag 
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and a.risk_documentation_flag = 'Undocumented'
and b.active_attribution_flag is true
and date_trunc('month', a.month_of_measurement) = 'DATE'::date
group by 1) c on 
p.plan_name = c.plan_name
left join
(
select 
p.plan_name,
round(
sum(a.recapture_numerator)::decimal / 
sum(a.recapture_denominator)::decimal*100 , 1) recapture_rate
from l5.risk_core_patient a
left join public.updated_risk_active_attribution b on 
b.empi = a.empi
and b.payer_master_id = a.payer_master_id 
and b.month_of_measurement = a.month_of_measurement 
and b.risk_documentation_flag = a.risk_documentation_flag 
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and a.risk_documentation_flag = 'Undocumented'
and b.active_attribution_flag is true
and date_trunc('month', a.month_of_measurement) = 'DATE'::date
group by 1) a on 
p.plan_name = a.plan_name
left join
(
select 
a.plan_name,
round(
sum(a.recapture_numerator)::decimal /
sum(a.recapture_denominator)::decimal*100, 1) recapture_rate
from 
(
select 
empi, 
plan_name,
month_of_measurement,
null as  recapture_numerator,
null recapture_denominator,
'Documented' risk_documentation_flag
from risk_patient_temp 
where risk_value is not null
union all 
select 
empi,
plan_name,
month_of_measurement,
recapture_numerator,
recapture_denominator,
'Undocumented' risk_documentation_flag
from risk_patient_temp
where cdi_risk_value is not null
union all
select 
empi,
plan_name,
month_of_measurement,
null as  recapture_numerator,
null recapture_denominator,
'Suspected'  as  risk_documentation_flag
from risk_patient_temp
where suspect_risk_value is not null
) a
left join 
(
select 
distinct 
prvid,
empi,
sstp,
date_trunc('month', atrdt) as  month_of_attribution 
from l2.pd_attribution x
where inprsq = 'primary') b on 
a.empi = b.empi
and a.plan_name = b.sstp
and a.month_of_measurement = b.month_of_attribution
where 
a.plan_name <> 'Not Assigned'
and date_trunc('month', a.month_of_measurement) = 'DATE'
and a.risk_documentation_flag = 'Undocumented'
and b.empi is not null
group by 1) b on
p.plan_name = b.plan_name
order by 1;
-- Average Documented Risk
drop table if exists risk_patient_temp;
create temporary table risk_patient_temp
as
select 
a.empi,
coalesce(a.risk_value, 0) risk_value,
cdi_risk_value,
suspect_risk_value,
measure_config_id,
a.plan_name,
a.measure_date 
from l3.risk_output a
left join 
(
select 
avg(coalesce(x.risk_value_chronic, 0)) as cdi_risk_value,
empi,
measure_date,
period_mode,
risk_model_sub_type,
cdi_config_id
from l3.cdi_output  x
group by 2,3,4,5,6
) cdi_risk
on cdi_risk.empi = a.empi
and cdi_risk.measure_date = a.measure_date
and cdi_risk.risk_model_sub_type = a.risk_model_sub_type
and cdi_risk.period_mode = a.period_mode 
and cdi_risk.cdi_config_id = a.measure_config_id
left join
(
select 
avg(coalesce(risk_value, 0)) as suspect_risk_value,
empi,
measure_date,
'ytd' period_mode,
risk_model_sub_type 
from l3.suspect_output x
group by 2,3,4,5
) suspect_risk
on suspect_risk.empi = a.empi
and suspect_risk.measure_date = a.measure_date 
and suspect_risk.risk_model_sub_type = a.risk_model_sub_type 
and a.period_mode = 'ytd'
where 
date_trunc('month', a.measure_date) = 'DATE';
select 
p.plan_name,
coalesce(c.average_documented_risk, 0) as old_l5,
coalesce(a.average_documented_risk, 0) as new_l5,
coalesce(b.average_documented_risk, 0) as new_l3
from plans p
left join
(
select 
p.plan_name,
round(
sum(a.risk_value)::decimal / 
count(distinct a.empi), 3) as average_documented_risk
from l5_backup.risk_core_patient a
left join public.updated_risk_active_attribution b on 
b.empi = a.empi 
and b.payer_master_id = a.payer_master_id 
and b.month_of_measurement = a.month_of_measurement 
and b.risk_documentation_flag = a.risk_documentation_flag 
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and a.risk_documentation_flag = 'Documented'
and b.active_attribution_flag is true
and date_trunc('month', a.month_of_measurement) = 'DATE'::date
group by 1) c on 
p.plan_name = c.plan_name
left join
(
select 
p.plan_name,
round(
sum(a.risk_value)::decimal / 
count(distinct a.empi), 3) as average_documented_risk
from l5.risk_core_patient a
left join public.updated_risk_active_attribution b on 
b.empi = a.empi 
and b.payer_master_id = a.payer_master_id 
and b.month_of_measurement = a.month_of_measurement 
and b.risk_documentation_flag = a.risk_documentation_flag 
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and a.risk_documentation_flag = 'Documented'
and b.active_attribution_flag is true
and date_trunc('month', a.month_of_measurement) = 'DATE'::date
group by 1) a on 
p.plan_name = a.plan_name
left join
(
select 
t.plan_name,
round( 
sum(risk_value)::decimal / 
count(distinct t.empi), 3) as average_documented_risk
from 
(
select 
empi,
plan_name,
measure_date,
risk_value,
'Documented' as risk_documentation_flag
from risk_patient_temp
where risk_value is not null
union all
select 
empi,
plan_name,
measure_date,
cdi_risk_value,
'Undocumented'  risk_documentation_flag
from risk_patient_temp
where cdi_risk_value is not null
union all 
select 
empi,
plan_name,
measure_date,
suspect_risk_value,
'Suspected'  as risk_documentation_flag
from risk_patient_temp
where suspect_risk_value is not null) t
left join 
(
select 
distinct 
empi,
sstp,
date_trunc('month', atrdt) as month_of_attribution 
from l2.pd_attribution a
where inprsq = 'primary') b on 
b.empi = t.empi
and b.sstp = t.plan_name
and b.month_of_attribution = date_trunc('month', t.measure_date)
where t.risk_documentation_flag in ('Documented')
and b.empi is not null
group by 1) b on 
p.plan_name = b.plan_name
order by 1;
-- Average Potential Risk
select 
p.plan_name,
coalesce(c.average_potential_risk, 0) as old_l5,
coalesce(a.average_potential_risk, 0) as new_l5,
coalesce(b.average_potential_risk, 0) as new_l3
from plans p
left join 
(
select 
p.plan_name,
round(
sum(a.risk_value)::decimal /
count(distinct a.empi) , 3) as average_potential_risk
from l5_backup.risk_core_patient a
left join public.updated_risk_active_attribution b on 
b.empi = a.empi 
and b.payer_master_id = a.payer_master_id 
and b.month_of_measurement = a.month_of_measurement 
and b.risk_documentation_flag = a.risk_documentation_flag 
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and b.active_attribution_flag is true
and date_trunc('month', a.month_of_measurement) = 'DATE'::date
group by 1) c on 
p.plan_name = c.plan_name
left join
(
select 
p.plan_name,
round(
sum(a.risk_value)::decimal /
count(distinct a.empi) , 3) as average_potential_risk
from l5.risk_core_patient a
left join public.updated_risk_active_attribution b on 
b.empi = a.empi 
and b.payer_master_id = a.payer_master_id 
and b.month_of_measurement = a.month_of_measurement 
and b.risk_documentation_flag = a.risk_documentation_flag 
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and b.active_attribution_flag is true
and date_trunc('month', a.month_of_measurement) = 'DATE'::date
group by 1) a on 
p.plan_name = a.plan_name
left join 
(
select 
t.plan_name,
round( 
sum(risk_value)::decimal / 
count(distinct t.empi), 3) as average_potential_risk
from 
(
select 
empi,
plan_name,
measure_date,
risk_value,
'Documented' as risk_documentation_flag
from risk_patient_temp
where risk_value is not null
union all
select 
empi,
plan_name,
measure_date,
cdi_risk_value,
'Undocumented'  risk_documentation_flag
from risk_patient_temp
where cdi_risk_value is not null
union all 
select 
empi,
plan_name,
measure_date,
suspect_risk_value,
'Suspected'  as risk_documentation_flag
from risk_patient_temp
where suspect_risk_value is not null) t
left join 
(
select 
distinct 
empi,
sstp,
date_trunc('month', atrdt) as month_of_attribution 
from l2.pd_attribution a
where inprsq = 'primary') b on 
b.empi = t.empi
and b.sstp = t.plan_name
and b.month_of_attribution = date_trunc('month', t.measure_date)
where t.risk_documentation_flag in ('Documented','Undocumented')
and b.empi is not null
group by 1) b on 
p.plan_name = b.plan_name
order by 1;