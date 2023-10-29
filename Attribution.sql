-- Attributed Members
select 
a.plan_name,
a.attributed_members as L5_attributed_members,
b.attributed_members as L2_attributed_members
from 
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
group by 1) a 
join 
(
select 
a.plnm as plan_name,
count(distinct empi) as attributed_members
from l2.pd_attribution a
where date_trunc('month', atrdt)::date  =  'DATE'::date
and inprsq = 'primary'
and prvid is not null
group by 1) b on 
a.plan_name = b.plan_name
order by 1;
-- Newly Enrolled
select 
a.plan_name,
a.newly_enrolled as L5_newly_enrolled,
b.newly_enrolled as L2_newly_enrolled
from 
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
group by 1) a 
join 
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
a.plan_name = b.plan_name
order by 1;
-- Disenrolled
select 
a.plan_name,
a.disenrolled as L5_disenrolled,
b.disenrolled as L2_disenrolled
from 
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
group by 1) a 
join 
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
a.plan_name = b.plan_name
order by 1;
-- Deceased
select 
a.plan_name,
a.deceased as L5_deceased,
b.deceased as L2_deceased
from 
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
group by 1) a 
join 
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
a.plan_name = b.plan_name
order by 1;