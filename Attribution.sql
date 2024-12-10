-- Attributed Members
create temporary table plans as
select 
distinct 
sstp as plan_name
from l2.pd_attribution;
select 
p.plan_name,
coalesce(c.attributed_members,0) as old_l5,
coalesce(a.attributed_members,0) as new_l5,
coalesce(b.attributed_members,0) as new_l2
from plans p 
left join
(
select 
p.plan_name,
count(distinct empi) as attributed_members
from l5_backup.attribution_churn a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE' 
and churn_status in ('Continued', 'Added')
group by 1) c on 
p.plan_name = c.plan_name
left join 
(
select 
p.plan_name,
count(distinct empi) as attributed_members
from l5.attribution_churn a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE' 
and churn_status in ('Continued', 'Added')
group by 1) a on 
p.plan_name = a.plan_name
left join 
(
select 
a.plnm as plan_name,
count(distinct empi) as attributed_members
from l2.pd_attribution a
where date_trunc('month', atrdt)::date  =  'DATE'::date
and inprsq = 'primary'
and prvid is not null
group by 1) b on 
p.plan_name = b.plan_name
order by 1;
-- Newly Enrolled
select 
p.plan_name,
coalesce(c.newly_enrolled,0) as old_l5,
coalesce(a.newly_enrolled,0) as new_l5,
coalesce(b.newly_enrolled,0) as new_l2
from plans p
left join
(
select 
p.plan_name,
count(distinct empi)as newly_enrolled
from l5_backup.attribution_churn a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE' 
and churn_status in ('Added')
group by 1) c on 
p.plan_name = c.plan_name
left join
(
select 
p.plan_name,
count(distinct empi)as newly_enrolled
from l5.attribution_churn a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE' 
and churn_status in ('Added')
group by 1) a on
p.plan_name = a.plan_name
left join 
(
select 
plnm as plan_name,
count(distinct empi)as newly_enrolled
from 
(
select 
distinct
plnm,
empi
from l2.pd_attribution a
where date_trunc('month', atrdt)::date  =  'DATE'::date
and inprsq = 'primary'
and prvid is not null
except
select 
distinct
plnm,
empi
from l2.pd_attribution a
where date_trunc('month', atrdt)::date  =  ('DATE'::date - interval '1 month')::date
and inprsq = 'primary'
and prvid is not null) test
group by 1) b on 
p.plan_name = b.plan_name
order by 1;
-- Disenrolled
select 
p.plan_name,
coalesce(c.disenrolled,0) as old_l5,
coalesce(a.disenrolled,0) as new_l5,
coalesce(b.disenrolled,0) as new_l2
from plans p
left join
(
select 
p.plan_name,
count(distinct empi) as disenrolled
from l5_backup.attribution_churn a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE' 
and churn_status in ('Dropped')
group by 1) c on 
p.plan_name = c.plan_name
left join 
(
select 
p.plan_name,
count(distinct empi) as disenrolled
from l5.attribution_churn a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE' 
and churn_status in ('Dropped')
group by 1) a on
p.plan_name = a.plan_name
left join
(
select 
plnm as plan_name,
count(distinct empi) as disenrolled
from 
(
select 
distinct
plnm,
empi,
dod
from l2.pd_attribution a
where date_trunc('month', atrdt)::date  =  ('DATE'::date - interval '1 month')::date
and inprsq = 'primary'
and prvid is not null
except
select 
distinct
plnm,
empi,
dod
from l2.pd_attribution a
where date_trunc('month', atrdt)::date  =  'DATE'::date
and inprsq = 'primary'
and prvid is not null) test
where dod  >  'DATE' 
or dod is null
group by 1) b on 
p.plan_name = b.plan_name
order by 1;
-- Deceased
select 
p.plan_name,
coalesce(c.deceased,0) as old_l5,
coalesce(a.deceased,0) as new_l5,
coalesce(b.deceased,0) as new_l2
from plans p
left join
(
select 
p.plan_name,
count(distinct empi) as deceased
from l5_backup.attribution_churn a
left join l5_backup.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE' 
and churn_status in ('Deceased')
group by 1) c on 
p.plan_name = c.plan_name
left join
(
select 
p.plan_name,
count(distinct empi) as deceased
from l5.attribution_churn a
left join l5.payer p on 
p.payer_master_id = a.payer_master_id 
where 
a.org_hierarchy_master_id is not null
and a.org_hierarchy_master_id <>'N/A'
and p.payer_name <> 'Not Assigned'
and date_trunc('month', a.month_of_attribution) = 'DATE' 
and churn_status in ('Deceased')
group by 1) a on
p.plan_name = a.plan_name
left join 
(
select 
plnm as plan_name,
count(distinct empi) as deceased
from 
(
select 
distinct
plnm,
empi,
dod
from l2.pd_attribution a
where date_trunc('month', atrdt)::date  =  ('DATE'::date - interval '1 month')::date
and inprsq = 'primary'
and prvid is not null
except
select 
distinct
plnm,
empi,
dod
from l2.pd_attribution a
where date_trunc('month', atrdt)::date  =  'DATE'::date
and inprsq = 'primary'
and prvid is not null) test
where dod <= 'DATE'
group by 1) b on 
p.plan_name = b.plan_name
order by 1;
