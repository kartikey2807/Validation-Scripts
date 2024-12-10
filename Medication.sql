-- Number of Patients
create temporary table plans as
select 
distinct 
sstp as plan_name
from l2.pd_attribution;
select 
p.plan_name,
coalesce(d.patients, 0) as old_l5,
coalesce(a.patients, 0) as new_l5,
coalesce(b.patients, 0) as new_l3,
coalesce(c.patients, 0) as new_l2
from plans p
left join
(
select 
p.plan_name,
count(distinct a.empi) as patients
from l5_backup.medication_management_patient a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', visit_start_date) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1) d on 
p.plan_name = d.plan_name
left join
(
select 
p.plan_name,
count(distinct a.empi) as patients
from l5.medication_management_patient a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', visit_start_date) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1) a on 
p.plan_name = a.plan_name
left join 
(
select 
sstp as plan_name,
count(distinct a_empi) as patients
from 
(
select 
*,
a.empi as a_empi,
row_number() over(partition by a.visit_id, c.sub_source_type) rn
from l3.aggregate_output a
join l3.claims_output c on 
a.visit_id = c.visit_id 
join 
(
select 
empi,
sstp,
prvid,
date_trunc('month', atrdt) month_of_attribution 
from l2.pd_attribution  pa
where inprsq = 'primary'
group by 1,2,3,4) b on 
a.empi = b.empi
and c.sub_source_type = b.sstp
and date_trunc('month', a.visit_start_date) = b.month_of_attribution
where b.prvid is not null
and date_trunc('month',a.visit_start_date) between date_trunc('year','DATE'::date) and 'DATE'
and a.visit_type in ('Medication Claim')
and b.sstp is not null) t
where rn = '1'
group by 1) b on 
p.plan_name = b.plan_name
left join 
(
select 
distinct 
sstp as plan_name,
count(distinct empi) as patients
from l2.pd_activity pac
where cltc='Medication'
and date_trunc('month', efdt)::date between date_trunc('year', 'DATE'::date) and 'DATE'::date
group by 1) c on 
p.plan_name = c.plan_name
order by 1;
-- Number of Prescriptions
select 
p.plan_name,
coalesce(d.prescriptions, 0) as old_l5,
coalesce(a.prescriptions, 0) as new_l5,
coalesce(b.prescriptions, 0) as new_l3,
'0'::decimal as new_l2
from plans p
left join
(
select 
p.plan_name,
count(distinct a.visit_id) as prescriptions
from l5_backup.medication_management_patient a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', visit_start_date) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1) d on 
p.plan_name = d.plan_name
left join
(
select 
p.plan_name,
count(distinct a.visit_id) as prescriptions
from l5.medication_management_patient a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', visit_start_date) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1) a on 
p.plan_name = a.plan_name
left join 
(
select 
sstp as plan_name,
count(distinct a_visit_id) as prescriptions
from 
(
select 
*,
a.visit_id as a_visit_id,
row_number() over(partition by a.visit_id, c.sub_source_type) rn
from l3.aggregate_output a
join l3.claims_output c on 
a.visit_id = c.visit_id 
join 
(
select 
empi,
sstp,
prvid,
date_trunc('month', atrdt) month_of_attribution 
from l2.pd_attribution  pa
where inprsq = 'primary'
group by 1,2,3,4) b on 
a.empi = b.empi
and c.sub_source_type = b.sstp
and date_trunc('month', a.visit_start_date) = b.month_of_attribution
where b.prvid is not null
and date_trunc('month',a.visit_start_date) between date_trunc('year','DATE'::date) and 'DATE'
and a.visit_type in ('Medication Claim')
and b.sstp is not null) t
where rn = '1'
group by 1) b on 
p.plan_name = b.plan_name
order by 1;
-- Total Medication Expenditure
select 
p.plan_name,
coalesce(d.amount, 0) as old_l5,
coalesce(a.amount, 0) as new_l5,
coalesce(b.amount, 0) as new_l3,
coalesce(c.amount, 0) as new_l2
from plans p
left join
(
select 
p.plan_name,
round(sum(a.visit_amount), 0) as amount
from l5_backup.medication_management_patient a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', visit_start_date) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1) d on 
p.plan_name = d.plan_name
left join
(
select 
p.plan_name,
round(sum(a.visit_amount), 0) as amount
from l5.medication_management_patient a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', visit_start_date) between date_trunc('year', 'DATE'::date) and 'DATE'
group by 1) a on
p.plan_name = a.plan_name
left join 
(
select 
sstp as plan_name,
round(sum(a_visit_amount), 0) as amount
from 
(
select 
*,
a.visit_amount as a_visit_amount,
row_number() over(partition by a.visit_id, c.sub_source_type) rn
from l3.aggregate_output a
join l3.claims_output c on 
a.visit_id = c.visit_id 
join 
(
select 
empi,
sstp,
prvid,
date_trunc('month', atrdt) month_of_attribution 
from l2.pd_attribution  pa
where inprsq = 'primary'
group by 1,2,3,4) b on 
a.empi = b.empi
and c.sub_source_type = b.sstp
and date_trunc('month', a.visit_start_date) = b.month_of_attribution
where b.prvid is not null
and date_trunc('month',a.visit_start_date) between date_trunc('year','DATE'::date) and 'DATE'
and a.visit_type in ('Medication Claim')
and b.sstp is not null) t
where rn = '1'
group by 1) b on 
p.plan_name = b.plan_name
left join 
(
select 
sstp as plan_name,
round(sum(pih), 0) as amount
from 
(
select 
distinct 
sstp,
cid,
coalesce(pih,0) pih
from l2.pd_activity pac
where cltc='Medication'
and date_trunc('month', efdt)::date between date_trunc('year', 'DATE'::date) and 'DATE'::date
) t group by 1) c on 
p.plan_name = c.plan_name
order by 1;
-- Number of Patients (Drug Dependence)
select 
p.plan_name,
coalesce(d.patients, 0) as old_l5,
coalesce(a.patients, 0) as new_l5,
coalesce(b.patients, 0) as new_l3,
coalesce(c.patients, 0) as new_l2
from plans p
left join 
(
select 
p.plan_name,
count(distinct a.empi) as patients
from l5_backup.medication_management_patient a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
left join l5_backup.ndc_data nd on 
nd.ndc = a.ndc_code 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', visit_start_date) between date_trunc('year', 'DATE'::date) and 'DATE'
and nd.dea_class_code_description <> 'DEA Class Code is not applicable'
group by 1) d on 
p.plan_name = d.plan_name
left join
(
select 
p.plan_name,
count(distinct a.empi) as patients
from l5.medication_management_patient a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
left join l5.ndc_data nd on 
nd.ndc = a.ndc_code 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', visit_start_date) between date_trunc('year', 'DATE'::date) and 'DATE'
and nd.dea_class_code_description <> 'DEA Class Code is not applicable'
group by 1) a on 
p.plan_name = a.plan_name
left join 
(
select 
sstp as plan_name,
count(distinct a_empi) as patients
from 
(
select 
*,
a.empi as a_empi,
row_number() over(partition by a.visit_id, c.sub_source_type) rn
from l3.aggregate_output a
join l3.claims_output c on 
a.visit_id = c.visit_id 
join 
(
select 
empi,
sstp,
prvid,
date_trunc('month', atrdt) month_of_attribution 
from l2.pd_attribution  pa
where inprsq = 'primary'
group by 1,2,3,4) b on 
a.empi = b.empi
left join l2.mf2ndc m on 
m.ndc_upc_hri = c.medication_code 
and c.sub_source_type = b.sstp
and date_trunc('month', a.visit_start_date) = b.month_of_attribution
where b.prvid is not null
and date_trunc('month',a.visit_start_date) between date_trunc('year','DATE'::date) and 'DATE'
and a.visit_type in ('Medication Claim')
and m.dea_class_code is not null
and b.sstp is not null) t
where rn = '1'
group by 1) b on 
p.plan_name = b.plan_name
left join 
(
select 
distinct 
sstp as plan_name,
count(distinct empi) as patients
from l2.pd_activity pac
join l2.mf2ndc m on 
m.ndc_upc_hri = pac.rxc
where cltc='Medication'
and date_trunc('month', efdt)::date between date_trunc('year', 'DATE'::date) and 'DATE'::date
and m.dea_class_code is not null
group by 1) c on 
p.plan_name = c.plan_name
order by 1;
-- Number of Prescriptions (Drug Dependence)
select 
p.plan_name,
coalesce(d.prescriptions, 0) as old_l5,
coalesce(a.prescriptions, 0) as new_l5,
coalesce(b.prescriptions, 0) as new_l3,
'0'::decimal as new_l2
from plans p
left join
(
select 
p.plan_name,
count(distinct a.visit_id) as prescriptions
from l5_backup.medication_management_patient a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
left join l5_backup.ndc_data nd on 
nd.ndc = a.ndc_code 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', visit_start_date) between date_trunc('year', 'DATE'::date) and 'DATE'
and nd.dea_class_code_description <> 'DEA Class Code is not applicable'
group by 1) d on 
p.plan_name = d.plan_name
left join 
(
select 
p.plan_name,
count(distinct a.visit_id) as prescriptions
from l5.medication_management_patient a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
left join l5.ndc_data nd on 
nd.ndc = a.ndc_code 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', visit_start_date) between date_trunc('year', 'DATE'::date) and 'DATE'
and nd.dea_class_code_description <> 'DEA Class Code is not applicable'
group by 1) a on 
p.plan_name = a.plan_name
left join 
(
select 
sstp as plan_name,
count(distinct a_visit_id) as prescriptions
from 
(
select 
*,
a.visit_id as a_visit_id,
row_number() over(partition by a.visit_id, c.sub_source_type) rn
from l3.aggregate_output a
join l3.claims_output c on 
a.visit_id = c.visit_id 
join 
(
select 
empi,
sstp,
prvid,
date_trunc('month', atrdt) month_of_attribution 
from l2.pd_attribution  pa
where inprsq = 'primary'
group by 1,2,3,4) b on 
a.empi = b.empi
left join l2.mf2ndc m on 
m.ndc_upc_hri = c.medication_code 
and c.sub_source_type = b.sstp
and date_trunc('month', a.visit_start_date) = b.month_of_attribution
where b.prvid is not null
and date_trunc('month',a.visit_start_date) between date_trunc('year','DATE'::date) and 'DATE'
and a.visit_type in ('Medication Claim')
and m.dea_class_code is not null
and b.sstp is not null) t
where rn = '1'
group by 1) b on 
p.plan_name = b.plan_name
order by 1;
-- Total Medication Expenditure (Drug Dependence)
select 
p.plan_name,
coalesce(d.amount, 0) as old_l5,
coalesce(a.amount, 0) as new_l5,
coalesce(b.amount, 0) as new_l3,
coalesce(c.amount, 0) as new_l2
from plans p
left join
(
select 
p.plan_name,
round(sum(a.visit_amount), 0) as amount
from l5_backup.medication_management_patient a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
left join l5_backup.ndc_data nd on 
nd.ndc = a.ndc_code 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', visit_start_date) between date_trunc('year', 'DATE'::date) and 'DATE'
and nd.dea_class_code_description <> 'DEA Class Code is not applicable'
group by 1) d on 
p.plan_name = d.plan_name
left join 
(
select 
p.plan_name,
round(sum(a.visit_amount), 0) as amount
from l5.medication_management_patient a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
left join l5.ndc_data nd on 
nd.ndc = a.ndc_code 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', visit_start_date) between date_trunc('year', 'DATE'::date) and 'DATE'
and nd.dea_class_code_description <> 'DEA Class Code is not applicable'
group by 1) a on 
p.plan_name = a.plan_name
left join 
(
select 
sstp as plan_name,
round(sum(a_visit_amount), 0) as amount
from 
(
select 
*,
a.visit_amount as a_visit_amount,
row_number() over(partition by a.visit_id, c.sub_source_type) rn
from l3.aggregate_output a
join l3.claims_output c on 
a.visit_id = c.visit_id 
join 
(
select 
empi,
sstp,
prvid,
date_trunc('month', atrdt) month_of_attribution 
from l2.pd_attribution  pa
where inprsq = 'primary'
group by 1,2,3,4) b on 
a.empi = b.empi
left join l2.mf2ndc m on 
m.ndc_upc_hri = c.medication_code 
and c.sub_source_type = b.sstp
and date_trunc('month', a.visit_start_date) = b.month_of_attribution
where b.prvid is not null
and date_trunc('month',a.visit_start_date) between date_trunc('year','DATE'::date) and 'DATE'
and a.visit_type in ('Medication Claim')
and m.dea_class_code is not null
and b.sstp is not null) t
where rn = '1'
group by 1) b on 
p.plan_name = b.plan_name
left join 
(
select 
sstp as plan_name,
round(sum(pih), 0) as amount
from 
(
select 
distinct 
sstp,
cid,
coalesce(pih,0) pih
from l2.pd_activity pac
join l2.mf2ndc m on 
m.ndc_upc_hri = pac.rxc
where cltc='Medication'
and date_trunc('month', efdt)::date between date_trunc('year', 'DATE'::date) and 'DATE'::date
and m.dea_class_code is not null
) t group by 1) c on 
p.plan_name = c.plan_name
order by 1;
