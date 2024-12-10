-- Attribution Members
create temporary table plans as
select 
distinct 
sstp as plan_name
from l2.pd_attribution;
select 
p.plan_name,
coalesce(c.attributed_members,0) as old_l5,
coalesce(a.attributed_members,0) as new_l5,
coalesce(b.attributed_members,0) as new_l3
from plans p
left join 
(
select 
p.plan_name,
count(distinct empi) as attributed_members
from l5_backup.attribution a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE'
group by 1) c on
p.plan_name = c.plan_name
left join
(
select 
p.plan_name,
count(distinct empi) as attributed_members
from l5.attribution a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE'
group by 1) a on 
p.plan_name = a.plan_name
left join 
(
select 
plnm as plan_name,
count(distinct empi) as attributed_members
from l2.pd_attribution a
where date_trunc('month' , atrdt)::date  =  'DATE'::date 
and inprsq = 'primary'
and prvid is not null
group by 1) b on 
p.plan_name = b.plan_name
order by 1;
-- Quality Performance %
select 
p.plan_name,
coalesce(c.quality_performance,0) as old_l5,
coalesce(a.quality_performance,0) as new_l5,
coalesce(b.quality_performance,0) as new_l3
from plans p
left join
(
select 
p.plan_name,
round(
sum(a.numerator_count)::decimal / 
sum(denominator_count) * 100 , 1) as quality_performance
from l5_backup.quality_pcp a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_measurement) = 'DATE' 
group by 1) c on
p.plan_name = c.plan_name
left join
(
select 
p.plan_name,
round(
sum(a.numerator_count)::decimal / 
sum(denominator_count) * 100 , 1) as quality_performance
from l5.quality_pcp a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_measurement) = 'DATE' 
group by 1) a on 
p.plan_name = a.plan_name
left join 
(
select 
plnm as plan_name,
round(sum(mv1)::decimal / 
sum(mv2)*100, 1) as quality_performance
from l3.quality a
where date_trunc('month' , a.mdt)::date  =  'DATE'::date
group by 1) b on 
p.plan_name = b.plan_name
order by 1;
-- Visit Performance %
drop table  if  exists L5_visit_percent_backup;
create temporary table L5_visit_percent_backup
as
select 
a.plan_name,
round
(
a.numerator::decimal / b.denominator*100, 1) as  visit_percent
from 
(
select 
p.plan_name,
sum(a.visit_count) as numerator
from l5_backup.quality_visit_pcp a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_measurement) = 'DATE'
and measure_code in ('NUM')
group by 1) a 
join
(
select 
p.plan_name,
sum(a.visit_count) as denominator
from l5_backup.quality_visit_pcp a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_measurement) = 'DATE'
and measure_code in ('NUM', 'DEN')
group by 1) b on 
a.plan_name = b.plan_name;
drop table  if  exists L5_visit_percent;
create temporary table L5_visit_percent
as
select 
a.plan_name,
round
(
a.numerator::decimal / b.denominator*100, 1) as  visit_percent
from 
(
select 
p.plan_name,
sum(a.visit_count) as numerator
from l5.quality_visit_pcp a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_measurement) = 'DATE'
and measure_code in ('NUM')
group by 1) a 
join
(
select 
p.plan_name,
sum(a.visit_count) as denominator
from l5.quality_visit_pcp a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_measurement) = 'DATE'
and measure_code in ('NUM', 'DEN')
group by 1) b on 
a.plan_name = b.plan_name;
drop table  if  exists L3_visit_percent;
create temporary table L3_visit_percent
as
select 
a.plan_name,
round
(
a.numerator::decimal / b.denominator*100, 1) as  visit_percent
from 
(
select 
plan_name,
count(visit_id) as numerator
from l3.quality_visit_output a
where date_trunc('month', measure_date)::date  =  'DATE'
and measure_code in ('NUM')
group by 1) a
join 
(
select 
plan_name,
count(visit_id) as denominator
from l3.quality_visit_output a
where date_trunc('month', measure_date)::date  =  'DATE'
and measure_code in ('NUM', 'DEN')
group by 1) b on 
a.plan_name = b.plan_name;
select 
p.plan_name,
coalesce(c.visit_percent,0) as old_l5,
coalesce(a.visit_percent,0) as new_l5,
coalesce(b.visit_percent,0) as new_l3
from plans p
left join  
L5_visit_percent_backup c on 
p.plan_name = c.plan_name
left join 
L5_visit_percent a on 
p.plan_name = a.plan_name
left join 
L3_visit_percent b on 
p.plan_name = b.plan_name
order by 1;
-- AWV Score %
drop table  if  exists L5_awv_goal_backup;
create temporary table L5_awv_goal_backup
as
select 
a.plan_name,
round(a.numerator::decimal / b.denominator*100, 1) as awv_goal
from 
(
select 
p.plan_name,
count(distinct empi) as numerator
from l5_backup.attribution a 
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
left join l5_backup.patient_segment ps on 
ps.segment_master_id = a.segment_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE'
and ps.awv_status in ('Completed')
group by 1) a
join 
(
select 
p.plan_name,
count(distinct empi) as denominator
from l5_backup.attribution a 
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
left join l5_backup.patient_segment ps on 
ps.segment_master_id = a.segment_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE'
and ps.awv_status in ('Completed', 'Due')
group by 1) b on 
a.plan_name = b.plan_name;
drop table  if  exists L5_awv_goal;
create temporary table L5_awv_goal 
as
select 
a.plan_name,
round(a.numerator::decimal / b.denominator*100, 1) as awv_goal
from 
(
select 
p.plan_name,
count(distinct empi) as numerator
from l5.attribution a 
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
left join l5.patient_segment ps on 
ps.segment_master_id = a.segment_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE'
and ps.awv_status in ('Completed')
group by 1) a
join 
(
select 
p.plan_name,
count(distinct empi) as denominator
from l5.attribution a 
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
left join l5.patient_segment ps on 
ps.segment_master_id = a.segment_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE'
and ps.awv_status in ('Completed', 'Due')
group by 1) b on 
a.plan_name = b.plan_name;
drop table if  exists  awv_status_temp;
create temporary table awv_status_temp
as
select 
distinct 
empi,
plnm,
date_trunc('month', atrdt) as month_of_date,
case when lob='' then 'N/A'
else coalesce(lob,'N/A') end as lob
from l2.pd_attribution a
where inprsq = 'primary';
alter table awv_status_temp
add column awv_status varchar;
update awv_status_temp t1
set awv_status = 'Completed'
from 
(
select 
distinct
empi,
primary_procedure,
first_date_of_service,
last_date_of_service,
service_provider_npi,
date_trunc('month', first_date_of_service) month_of_visit
from l3.claims_output 
where
primary_procedure in ('G0438', 'G0402', 'G0439', 'G0468')
and lower(claim_type) not like '%invalid claim%'
and lower(claim_type) not like '%denied claim%')
t2 
where t1.month_of_date >= t2.month_of_visit
and t2.month_of_visit >= date_trunc('year', t1.month_of_date)
and t1.empi = t2.empi 
and t1.lob in ('Medicare', 'Medicare Advantage');
update awv_status_temp t1
set awv_status = 'Completed'
from 
(
select 
distinct
empi,
primary_procedure,
first_date_of_service,
last_date_of_service,
service_provider_npi,
date_trunc('month', first_date_of_service) month_of_visit
from l3.claims_output 
where
primary_procedure in (
'G0438',
'G0402',
'G0439',
'G0468',
'99381',
'99382',
'99383',
'99384',
'99385',
'99386',
'99387',
'99391',
'99392',
'99393',
'99394',
'99395',
'99396',
'99397')
and lower(claim_type) not like '%invalid claim%'
and lower(claim_type) not like '%denied claim%')
t2 
where t1.month_of_date >= t2.month_of_visit
and t2.month_of_visit >= date_trunc('year', t1.month_of_date)
and t1.empi = t2.empi 
and t1.lob in ('Commercial', 'Medicaid');
update awv_status_temp 
set awv_status = 'Due'
where awv_status is null;
drop table  if  exists L3_awv_goal;
create temporary table L3_awv_goal
as
select 
a.plan_name,
round(a.numerator::decimal/ b.denominator*100,1) as awv_goal
from 
(
select 
plnm as plan_name,
count(distinct empi) as numerator
from awv_status_temp
where month_of_date = 'DATE' 
and awv_status in ('Completed')
group by 1) a
join
(
select 
plnm as plan_name,
count(distinct empi) as denominator
from awv_status_temp
where month_of_date = 'DATE' 
and awv_status in ('Completed', 'Due')
group by 1) b on 
a.plan_name = b.plan_name
order by 1;
select 
p.plan_name,
coalesce(c.awv_goal,0) as old_l5,
coalesce(a.awv_goal,0) as new_l5,
coalesce(b.awv_goal,0) as new_l3
from plans p
left join
L5_awv_goal_backup c on 
p.plan_name = c.plan_name
left join 
L5_awv_goal a on 
p.plan_name = a.plan_name
left join
L3_awv_goal b on 
p.plan_name = b.plan_name
order by 1;