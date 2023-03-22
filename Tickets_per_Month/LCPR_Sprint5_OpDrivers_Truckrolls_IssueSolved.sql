
SELECT
count(*), 
count(distinct job_no_ojb)
FROM(
SELECT 
-- create_dte_ojb, 
-- job_no_ojb

-- count(*), 
create_dte_ojb, 
job_no_ojb, 
sub_acct_no_sbb

FROM
(
SELECT 
create_dte_ojb, 
job_no_ojb, 
sub_acct_no_sbb
FROM "lcpr.stage.dev"."truckrolls" 
WHERE substr(create_dte_ojb, 1, 1) != '"'
)
UNION ALL
(
SELECT
substr(create_dte_ojb, 2, 10) as create_dte_ojb, 
job_no_ojb, 
sub_acct_no_sbb
FROM "lcpr.stage.dev"."truckrolls"
WHERE substr(create_dte_ojb, 1, 1) = '"'
)
)
WHERE 
    cast(job_no_ojb as varchar) not in ('',  ' ') and job_no_ojb is not null
    and date_trunc('month', date(create_dte_ojb)) = date_trunc('month', date('2023-02-01'))
