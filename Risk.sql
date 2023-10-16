drop  table  if  exists empi_count_l5;
create temporary table  empi_count_l5 
as
select 
p.plan_name,
count(empi) as "count"
from l5.attribution a 
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.plan_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1;
drop  table  if  exists empi_count_l3;
create temporary table  empi_count_l3
as
select 
a.sstp as plan_name,
count(empi) as "count"
from l2.pd_attribution a
where 
inprsq = 'primary'
and prvid<>'N/A' and date_trunc('month', atrdt) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1;
drop  table if exists  total_visit_amount_l5;
create temporary table total_visit_amount_l5 
as
select 
p.plan_name,
sum(visit_amount) as amount
from l5.visit_pcp x 
left join l5.payer p on 
p.payer_master_id = x.payer_master_id 
where 
x.org_hierarchy_master_id is not null
and x.org_hierarchy_master_id <>'N/A'
and p.plan_name <> 'Not Assigned'
and date_trunc('month', x.month_of_visit::date) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1;
drop  table if exists  total_visit_amount_l3;
create temporary table total_visit_amount_l3 
as
select 
t.sstp as plan_name,
sum(t.visit_amount) as amount
from
(
select 
*,
row_number () over(partition by a.visit_id, b.sub_source_type) rn
from l3.aggregate_output a
left join l3.claims_output b on 
b.visit_id = a.visit_id 
left join (
select 
empi,
sstp,
date_trunc('month', atrdt) month_of_attribution
from l2.pd_attribution pa 
where inprsq = 'primary'
group by 1,2,3
) x on 
x.sstp = b.sub_source_type 
and x.empi = b.empi 
and x.month_of_attribution = date_trunc('month', a.visit_start_date) 
where 
date_trunc('month', a.visit_start_date)::date between date_trunc('year', 'DATE'::Date)  and  'DATE'
and x.sstp is not null
and a.visit_type <> 'Denied Claim') t 
where rn = '1'
group by 1;
drop  table if exists  total_visit_amount_l2;
create temporary table total_visit_amount_l2
as
select 
sstp as plan_name,
sum(pih) as amount
from 
(
select
a.sstp,
cid,
pih
from l2.pd_activity a
left join 
(
select 
distinct 
prvid,
sstp,
empi,
date_trunc('month', atrdt) as month_of_attribution 
from l2.pd_attribution
where inprsq = 'primary') b on 
a.empi = b.empi 
and a.sstp = b.sstp
and date_trunc('month', a.efdt) = b.month_of_attribution 
where 
b.prvid <> 'N/A'
and date_trunc('month', a.efdt::date) between  date_trunc('year', 'DATE'::date)  and  'DATE'::date
group by 1,2,3) test group by 1;
drop  table if exists  ip_visit_amount_l5;
create temporary table ip_visit_amount_l5
as
select 
p.plan_name,
sum(visit_amount) as amount
from l5.readmit_pcp x
left join l5.payer p on 
p.payer_master_id = x.payer_master_id 
where
x.org_hierarchy_master_id is not null
and x.org_hierarchy_master_id <>'N/A'
and p.plan_name <> 'Not Assigned'
and date_trunc('month', x.month_of_visit::date) between date_trunc('year', 'DATE'::date) and 'DATE' 
group by 1;
drop  table if exists  ip_visit_amount_l3;
create temporary table ip_visit_amount_l3 
as
select 
t.sstp as plan_name,
sum(t.visit_amount) as amount
from
(
select 
*,
row_number () over(partition by a.visit_id, b.sub_source_type) rn
from l3.aggregate_output a
left join l3.claims_output b on 
b.visit_id = a.visit_id 
left join (
select 
empi,
sstp,
date_trunc('month', atrdt) month_of_attribution
from l2.pd_attribution pa 
where inprsq = 'primary'
group by 1,2,3
) x on 
x.sstp = b.sub_source_type 
and x.empi = b.empi 
and x.month_of_attribution = date_trunc('month', a.visit_start_date) 
where 
date_trunc('month', a.visit_start_date)::date between date_trunc('year', 'DATE'::Date)  and  'DATE'
and x.sstp is not null
and a.visit_sub_type in (
'Inpatient - Others',
'Inpatient - Surgical',
'Inpatient Maternity - Normal Newborn',
'Inpatient Maternity - Other Neonates with problems',
'Inpatient- Medical',
'Inpatient - Substance Use Disorder',
'Inpatient - Psychiatric',
'Inpatient - Rehabilitation')
) t 
where rn = '1'
group by 1;
drop  table if exists  snf_visit_amount_l5;
create temporary table snf_visit_amount_l5
as
select 
p.plan_name,
sum(visit_amount) as amount
from l5.snf_pcp x
left join l5.payer p on 
p.payer_master_id = x.payer_master_id 
where
x.org_hierarchy_master_id is not null
and x.org_hierarchy_master_id <>'N/A'
and p.plan_name <> 'Not Assigned'
and date_trunc('month', x.month_of_visit::date) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1;
drop  table if exists  snf_visit_amount_l3;
create temporary table snf_visit_amount_l3
as
select 
t.sstp as plan_name,
sum(t.visit_amount) as amount
from
(
select 
*,
row_number () over(partition by a.visit_id, b.sub_source_type) rn
from l3.aggregate_output a
left join l3.claims_output b on 
b.visit_id = a.visit_id 
left join (
select 
empi,
sstp,
date_trunc('month', atrdt) month_of_attribution
from l2.pd_attribution pa 
where inprsq = 'primary'
group by 1,2,3
) x on 
x.sstp = b.sub_source_type 
and x.empi = b.empi 
and x.month_of_attribution = date_trunc('month', a.visit_start_date) 
where 
date_trunc('month', a.visit_start_date)::date between date_trunc('year', 'DATE'::Date)  and  'DATE'
and x.sstp is not null
and lower(a.visit_sub_type) ilike '%snf%'
) t 
where rn = '1'
group by 1;
drop  table if exists  imaging_visit_amount_l5;
create temporary table imaging_visit_amount_l5
as
select 
p.plan_name,
sum(visit_amount) as amount
from l5.imaging_pcp x
left join l5.payer p on 
p.payer_master_id = x.payer_master_id 
where
x.org_hierarchy_master_id is not null
and x.org_hierarchy_master_id <>'N/A'
and p.plan_name <> 'Not Assigned'
and date_trunc('month', x.month_of_visit::date) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1;
drop  table if exists  imaging_visit_amount_l3;
create temporary table imaging_visit_amount_l3
as
select 
t.sstp as plan_name,
sum(t.visit_amount) as amount
from
(
select 
*,
row_number () over(partition by a.visit_id, b.sub_source_type) rn
from l3.aggregate_output a
left join l3.claims_output b on 
b.visit_id = a.visit_id 
left join (
select 
empi,
sstp,
date_trunc('month', atrdt) month_of_attribution
from l2.pd_attribution pa 
where inprsq = 'primary'
group by 1,2,3
) x on 
x.sstp = b.sub_source_type 
and x.empi = b.empi 
and x.month_of_attribution = date_trunc('month', a.visit_start_date) 
where 
date_trunc('month', a.visit_start_date)::date between date_trunc('year', 'DATE'::Date)  and  'DATE' 
and x.sstp is not null
and a.visit_sub_type in ('Professional - Imaging Others',
'Professional - Radiology - CT',
'Professional - Radiology - MRI',
'Professional - Radiology - Mammography',
'Professional - Radiology - Nuclear',
'Professional - Radiology - PET',
'Professional - Radiology - Therapeutic',
'Professional - Radiology - X Ray')
) t 
where rn = '1'
group by 1;
drop  table if exists  ed_visit_amount_l5;
create temporary table ed_visit_amount_l5 
as
select 
p.plan_name,
sum(visit_amount) as amount
from l5.ed_pcp x 
left join l5.payer p on 
p.payer_master_id = x.payer_master_id 
where 
x.org_hierarchy_master_id is not null
and x.org_hierarchy_master_id <>'N/A'
and p.plan_name <> 'Not Assigned'
and date_trunc('month', x.month_of_visit::date) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1;
drop  table if exists  ed_visit_amount1_l3;
create temporary table ed_visit_amount1_l3
as
select 
t.sstp as plan_name,
sum(t.visit_amount) as amount
from
(
select 
*,
row_number () over(partition by a.visit_id, b.sub_source_type) rn
from l3.aggregate_output a
left join l3.claims_output b on 
b.visit_id = a.visit_id 
left join (
select 
empi,
sstp,
date_trunc('month', atrdt) month_of_attribution
from l2.pd_attribution pa 
where inprsq = 'primary'
group by 1,2,3
) x on 
x.sstp = b.sub_source_type 
and x.empi = b.empi 
and x.month_of_attribution = date_trunc('month', a.visit_start_date) 
where 
date_trunc('month', a.visit_start_date)::date between date_trunc('year', 'DATE'::Date)  and  'DATE' 
and x.sstp is not null
and b.claim_type = 'Acute Inpatient' 
and b.cost_centre2 = 'Emergency Department Visits'
) t 
where rn = '1'
group by 1;
drop  table if exists  ed_visit_amount2_l3;
create temporary table ed_visit_amount2_l3
as
select 
t.sstp as plan_name,
sum(t.visit_amount) as amount
from
(
select 
*,
row_number () over(partition by a.visit_id, b.sub_source_type) rn
from l3.aggregate_output a
left join l3.claims_output b on 
b.visit_id = a.visit_id 
left join (
select 
empi,
sstp,
date_trunc('month', atrdt) month_of_attribution
from l2.pd_attribution pa 
where inprsq = 'primary'
group by 1,2,3
) x on 
x.sstp = b.sub_source_type 
and x.empi = b.empi 
and x.month_of_attribution = date_trunc('month', a.visit_start_date) 
where 
date_trunc('month', a.visit_start_date)::date between date_trunc('year', 'DATE'::Date)  and  'DATE'
and x.sstp is not null
and b.cost_centre1 = 'Emergency Department Visits'
) t 
where rn = '1'
group by 1;
drop  table if exists  ip_visit_count_l5;
create temporary table ip_visit_count_l5 
as
select 
p.plan_name,
sum(visit_count) as "count"
from l5.readmit_pcp x
left join l5.payer p on 
p.payer_master_id = x.payer_master_id 
where
x.org_hierarchy_master_id is not null
and x.org_hierarchy_master_id <>'N/A'
and p.plan_name <> 'Not Assigned'
and date_trunc('month', x.month_of_visit::date) between date_trunc('year', 'DATE'::date) and 'DATE'  
group by 1;
drop  table if exists  ip_visit_count_l3;
create temporary table ip_visit_count_l3 
as
select 
t.sstp as plan_name,
count(visit_id_aggregate) as "count"
from
(
select 
*,
a.visit_id as visit_id_aggregate,
row_number () over(partition by a.visit_id, b.sub_source_type) rn
from l3.aggregate_output a
left join l3.claims_output b on 
b.visit_id = a.visit_id 
left join (
select 
empi,
sstp,
date_trunc('month', atrdt) month_of_attribution
from l2.pd_attribution pa 
where inprsq = 'primary'
group by 1,2,3
) x on 
x.sstp = b.sub_source_type 
and x.empi = b.empi 
and x.month_of_attribution = date_trunc('month', a.visit_start_date) 
where 
date_trunc('month', a.visit_start_date)::date between date_trunc('year', 'DATE'::Date)  and  'DATE' 
and x.sstp is not null
and a.visit_sub_type in (
'Inpatient - Others',
'Inpatient - Surgical',
'Inpatient Maternity - Normal Newborn',
'Inpatient Maternity - Other Neonates with problems',
'Inpatient- Medical',
'Inpatient - Substance Use Disorder',
'Inpatient - Psychiatric',
'Inpatient - Rehabilitation')
) t 
where rn = '1'
group by 1;
drop  table if exists  snf_visit_count_l5;
create temporary table snf_visit_count_l5
as
select 
p.plan_name,
sum(visit_count) as "count"
from l5.snf_pcp x
left join l5.payer p on 
p.payer_master_id = x.payer_master_id 
where
x.org_hierarchy_master_id is not null
and x.org_hierarchy_master_id <>'N/A'
and p.plan_name <> 'Not Assigned'
and date_trunc('month', x.month_of_visit::date) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1;
drop  table if exists  snf_visit_count_l3;
create temporary table snf_visit_count_l3
as
select 
t.sstp as plan_name,
count(visit_id_aggregate) as "count"
from
(
select 
*,
a.visit_id as visit_id_aggregate,
row_number () over(partition by a.visit_id, b.sub_source_type) rn
from l3.aggregate_output a
left join l3.claims_output b on 
b.visit_id = a.visit_id 
left join (
select 
empi,
sstp,
date_trunc('month', atrdt) month_of_attribution
from l2.pd_attribution pa 
where inprsq = 'primary'
group by 1,2,3
) x on 
x.sstp = b.sub_source_type 
and x.empi = b.empi 
and x.month_of_attribution = date_trunc('month', a.visit_start_date) 
where 
date_trunc('month', a.visit_start_date)::date between date_trunc('year', 'DATE'::Date)  and  'DATE' 
and x.sstp is not null
and lower(a.visit_sub_type) ilike '%snf%'
) t 
where rn = '1'
group by 1;
drop  table if exists  imaging_visit_count_l5;
create temporary table imaging_visit_count_l5
as
select 
p.plan_name,
sum(visit_count) as "count"
from l5.imaging_pcp x
left join l5.payer p on 
p.payer_master_id = x.payer_master_id 
where
x.org_hierarchy_master_id is not null
and x.org_hierarchy_master_id <>'N/A'
and p.plan_name <> 'Not Assigned'
and date_trunc('month', x.month_of_visit::date) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1;
drop  table if exists  imaging_visit_count_l3;
create temporary table imaging_visit_count_l3
as
select 
t.sstp as plan_name,
count(visit_id_aggregate) as "count"
from
(
select 
*,
a.visit_id as visit_id_aggregate,
row_number () over(partition by a.visit_id, b.sub_source_type) rn
from l3.aggregate_output a
left join l3.claims_output b on 
b.visit_id = a.visit_id 
left join (
select 
empi,
sstp,
date_trunc('month', atrdt) month_of_attribution
from l2.pd_attribution pa 
where inprsq = 'primary'
group by 1,2,3
) x on 
x.sstp = b.sub_source_type 
and x.empi = b.empi 
and x.month_of_attribution = date_trunc('month', a.visit_start_date) 
where 
date_trunc('month', a.visit_start_date)::date between date_trunc('year', 'DATE'::Date)  and  'DATE' 
and x.sstp is not null
and a.visit_sub_type in (
'Professional - Imaging Others',
'Professional - Radiology - CT',
'Professional - Radiology - MRI',
'Professional - Radiology - Mammography',
'Professional - Radiology - Nuclear',
'Professional - Radiology - PET',
'Professional - Radiology - Therapeutic',
'Professional - Radiology - X Ray')
) t 
where rn = '1'
group by 1;
drop  table if exists  ed_visit_count_l5;
create temporary table ed_visit_count_l5
as
select 
p.plan_name,
sum(visit_count) as "count"
from l5.ed_pcp x
left join l5.payer p on 
p.payer_master_id = x.payer_master_id 
where
x.org_hierarchy_master_id is not null
and x.org_hierarchy_master_id <>'N/A'
and p.plan_name <> 'Not Assigned'
and date_trunc('month', x.month_of_visit::date) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1;
drop  table if exists  ed_visit_count1_l3;
create temporary table ed_visit_count1_l3 
as
select 
t.sstp as plan_name,
count(visit_id_aggregate) as "count"
from
(
select 
*,
a.visit_id as visit_id_aggregate,
row_number () over(partition by a.visit_id, b.sub_source_type) rn
from l3.aggregate_output a
left join l3.claims_output b on 
b.visit_id = a.visit_id 
left join (
select 
empi,
sstp,
date_trunc('month', atrdt) month_of_attribution
from l2.pd_attribution pa 
where inprsq = 'primary'
group by 1,2,3
) x on 
x.sstp = b.sub_source_type 
and x.empi = b.empi 
and x.month_of_attribution = date_trunc('month', a.visit_start_date) 
where 
date_trunc('month', a.visit_start_date)::date between date_trunc('year', 'DATE'::Date)  and  'DATE' 
and x.sstp is not null
and b.claim_type = 'Acute Inpatient'
and b.cost_centre2 = 'Emergency Department Visits'
) t 
where rn = '1'
group by 1;
drop  table if exists  ed_visit_count2_l3;
create temporary table ed_visit_count2_l3 
as
select 
t.sstp as plan_name,
count(visit_id_aggregate) as "count"
from
(
select 
*,
a.visit_id as visit_id_aggregate,
row_number () over(partition by a.visit_id, b.sub_source_type) rn
from l3.aggregate_output a
left join l3.claims_output b on 
b.visit_id = a.visit_id 
left join (
select 
empi,
sstp,
date_trunc('month', atrdt) month_of_attribution
from l2.pd_attribution pa 
where inprsq = 'primary'
group by 1,2,3
) x on 
x.sstp = b.sub_source_type 
and x.empi = b.empi 
and x.month_of_attribution = date_trunc('month', a.visit_start_date) 
where 
date_trunc('month', a.visit_start_date)::date between date_trunc('year', 'DATE'::Date)  and  'DATE' 
and x.sstp is not null
and b.claim_type = 'Outpatient'
and b.cost_centre1 = 'Emergency Department Visits'
) t 
where rn = '1'
group by 1;
drop  table if exists  patient_l3;
create temporary table patient_l3
as
select 
*
from
(
select 
*,
row_number()over(partition by empi order by ingestion_datetime desc)
as row_num
from l2.empi e) test
where row_num = '1';
-- Person Years
select
a.plan_name,
a.person_years as L5_person_years,
b.person_years as L3_person_years
from
(
select 
p.plan_name,
round(count(a.empi)::decimal / 
count(distinct a.month_of_attribution), 0) as person_years
from l5.attribution a 
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.plan_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1) a join 
(
select 
a.sstp as  plan_name,
round(
count(empi)::decimal / 
count(distinct atrdt)) as person_years
from l2.pd_attribution a
where 
inprsq = 'primary'
and prvid<>'N/A' and date_trunc('month', atrdt) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1) b on 
a.plan_name = b.plan_name
order by 1;
-- PMPM
select
a.plan_name,
a.pmpm as L5_pmpm,
b.pmpm as L3_pmpm,
c.pmpm as L2_pmpm
from 
(
select 
a.plan_name,
round(a.amount::decimal / b."count", 0) as pmpm
from total_visit_amount_l5 a
join empi_count_l5 b on 
a.plan_name = b.plan_name) a 
join
(
select 
a.plan_name,
round(a.amount::decimal / b."count", 0) as pmpm
from total_visit_amount_l3 a
join empi_count_l3 b on 
a.plan_name = b.plan_name) b on 
a.plan_name = b.plan_name
join
(
select 
a.plan_name,
round(a.amount::decimal / b."count", 0) as pmpm
from total_visit_amount_l2 a
join empi_count_l3 b on 
a.plan_name = b.plan_name) c on 
a.plan_name = c.plan_name
order by 1;
-- IP PMPM
select 
a.plan_name,
a.ip_pmpm as L5_ip_pmpm,
b.ip_pmpm as L3_ip_pmpm
from
(
select 
a.plan_name,
round(a.amount::decimal / b."count",0) as ip_pmpm
from ip_visit_amount_l5 a
join empi_count_l5 b on 
a.plan_name = b.plan_name) a join
(
select 
a.plan_name,
round(a.amount::decimal / b."count",0) as ip_pmpm
from ip_visit_amount_l3 a
join empi_count_l3 b on 
a.plan_name = b.plan_name) b on 
a.plan_name = b.plan_name
order by 1;
-- SNF PMPM
select 
a.plan_name,
a.snf_pmpm as L5_snf_pmpm,
b.snf_pmpm as L3_snf_pmpm
from 
(
select 
a.plan_name,
round(a.amount::decimal / b."count",0) as snf_pmpm
from snf_visit_amount_l5 a
join empi_count_l5 b on 
a.plan_name = b.plan_name) a join 
(
select 
a.plan_name,
round(a.amount::decimal / b."count",0) as snf_pmpm
from snf_visit_amount_l3 a
join empi_count_l3 b on 
a.plan_name = b.plan_name) b on 
a.plan_name = b.plan_name
order by 1;
-- Imaging PMPM
select 
a.plan_name,
a.imaging_pmpm as L5_imaging_pmpm,
b.imaging_pmpm as L3_imaging_pmpm
from
(
select 
a.plan_name,
round(a.amount::decimal / b."count",0) as imaging_pmpm
from imaging_visit_amount_l5 a
join empi_count_l5 b on 
a.plan_name = b.plan_name) a join 
(
select 
a.plan_name,
round(a.amount::decimal / b."count",0) as imaging_pmpm
from imaging_visit_amount_l3 a
join empi_count_l3 b on 
a.plan_name = b.plan_name) b on 
a.plan_name = b.plan_name
order by 1;
-- ED PMPM
select 
a.plan_name,
a.ed_pmpm as L5_ed_pmpm,
b.ed_pmpm as L3_ed_pmpm
from
(
select 
a.plan_name,
round(a.amount::decimal / b."count",0) as ed_pmpm
from ed_visit_amount_l5 a
join empi_count_l5 b on 
a.plan_name = b.plan_name) a join 
(
select 
a.plan_name,
round((coalesce(a.amount,0)::decimal + coalesce(c.amount,0)::decimal) /
b."count",0) as ed_pmpm
from ed_visit_amount2_l3 a
left join ed_visit_amount1_l3 c on 
a.plan_name = c.plan_name
join empi_count_l3 b on 
a.plan_name = b.plan_name) b on 
a.plan_name = b.plan_name
order by 1;
-- IP/1000
select 
a.plan_name,
a.ip_by_1000 as L5_ip_by_1000,
b.ip_by_1000 as L3_ip_by_1000
from 
(
select 
a.plan_name,
round((a."count"::decimal / b."count")*12000, 0) as ip_by_1000
from ip_visit_count_l5 a
join empi_count_l5 b on 
a.plan_name = b.plan_name) a join 
(
select 
a.plan_name,
round((a."count"::decimal / b."count")*12000, 0) as ip_by_1000
from ip_visit_count_l3 a
join empi_count_l3 b on 
a.plan_name = b.plan_name) b on 
a.plan_name = b.plan_name
order by 1;
-- SNF/1000
select 
a.plan_name,
a.snf_by_1000 as L5_snf_by_1000,
b.snf_by_1000 as L3_snf_by_1000
from
(
select 
a.plan_name,
round((a."count"::decimal / b."count")*12000, 0) as snf_by_1000
from snf_visit_count_l5 a
join empi_count_l5 b on 
a.plan_name = b.plan_name) a join 
(
select 
a.plan_name,
round((a."count"::decimal / b."count")*12000, 0) as snf_by_1000
from snf_visit_count_l3 a
join empi_count_l3 b on 
a.plan_name = b.plan_name) b on 
a.plan_name = b.plan_name
order by 1;
-- Imaging/1000
select 
a.plan_name,
a.imaging_by_1000 as L5_imaging_by_1000,
b.imaging_by_1000 as L3_imaging_by_1000
from
(
select 
a.plan_name,
round((a."count"::decimal / b."count")*12000, 0) as imaging_by_1000
from imaging_visit_count_l5 a
join empi_count_l5 b on 
a.plan_name = b.plan_name) a join 
(
select 
a.plan_name,
round((a."count"::decimal / b."count")*12000, 0) as imaging_by_1000
from imaging_visit_count_l3 a
join empi_count_l3 b on 
a.plan_name = b.plan_name) b on 
a.plan_name = b.plan_name
order by 1;
-- ED/1000
select 
a.plan_name,
a.ed_by_1000 as L5_ed_by_1000,
b.ed_by_1000 as L3_ed_by_1000
from
(
select 
a.plan_name,
round((a."count"::decimal / b."count")*12000, 0) as ed_by_1000
from ed_visit_count_l5 a
join empi_count_l5 b on 
a.plan_name = b.plan_name) a join
(
select 
a.plan_name,
round(((coalesce(a."count", 0)::decimal + coalesce(c."count", 0)::decimal) / b."count")*12000, 0) as ed_by_1000
from ed_visit_count2_l3 a
left join ed_visit_count1_l3 c on 
a.plan_name = c.plan_name
join empi_count_l3 b on 
a.plan_name = b.plan_name) b on 
a.plan_name = b.plan_name
order by 1;
-- Average Risk
select 
a.plan_name,
a.average_risk as L5_average_risk,
b.average_risk as L3_average_risk
from
(
select 
p.plan_name,
round(
sum(risk_value)::decimal / 
sum(patient_count),3) as average_risk
from l5.risk_pcp x
left join l5.payer p on 
p.payer_master_id = x.payer_master_id 
where 
x.org_hierarchy_master_id is not null
and x.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', x.month_of_measurement) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1) a join 
(
select 
plan_name,
round(
sum(risk_value)::decimal /
sum(patient_count),3) average_risk
from
(
select 
plan_name,
sum(risk_value) risk_value,
count(distinct empi) patient_count
from 
(
select 
a.empi,
a.risk_value,
a.pcp_npi,
a.plan_name,
date_trunc('month', a.measure_date) as month_of_measurement,
md5(COALESCE(measure_id,'N/A')||':'||
COALESCE(measure_version_id,'N/A')||':'||
COALESCE(period_mode,'Not Assigned')||':'||
COALESCE(risk_model_name,'Not Assigned')||':'||
COALESCE(risk_model_sub_type,'Not Assigned')||':'||
COALESCE(cast(risk_model_year as varchar),'Not Assigned')) AS measure_master_id,
measure_config_id,
b.prvid org_hierarchy_master_id,
c.zip,
a.metal_level
from l3.risk_output a
left join patient_l3 c on 
c.empi = a.empi
left join 
(
select 
prvid,
empi,
sstp,
date_trunc('month', atrdt) month_of_attribution 
from l2.pd_attribution where inprsq = 'primary'
group by 1,2,3,4) b on
a.empi = b.empi
and a.plan_name = b.sstp
and date_trunc('month', a.measure_date) = b.month_of_attribution 
where date_trunc('month', measure_date)::date   between date_trunc('year', 'DATE'::date) and 'DATE'
) x
group by
pcp_npi,
plan_name,
month_of_measurement,
measure_master_id,
measure_config_id,
org_hierarchy_master_id,
zip,
metal_level) test
group by 1) b on 
a.plan_name = b.plan_name
order by 1;