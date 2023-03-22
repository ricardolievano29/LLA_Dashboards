
WITH
parameters as (
SELECT date_trunc('month', date('2023-01-01')) as input_month)

, table_part1 as (
SELECT 
    create_dte_ojb, 
    job_no_ojb, 
    sub_acct_no_sbb
FROM "lcpr.stage.dev"."truckrolls" 
WHERE substr(create_dte_ojb, 1, 1) != '"'
)

, table_part2 as (
SELECT
    substr(create_dte_ojb, 2, 10) as create_dte_ojb, 
    job_no_ojb, 
    sub_acct_no_sbb
FROM "lcpr.stage.dev"."truckrolls"
WHERE substr(create_dte_ojb, 1, 1) = '"'
)

, tables_union as (
SELECT
    create_dte_ojb, 
    job_no_ojb, 
    sub_acct_no_sbb
FROM table_part1
UNION ALL (SELECT * FROM table_part2)
)

SELECT
    *
FROM tables_union
WHERE
    cast(job_no_ojb as varchar) not in ('',  ' ') 
    and job_no_ojb is not null
    and date_trunc('month', date(create_dte_ojb)) = (SELECT input_month FROM parameters)
