-- Создаём слой STG DWH
-- Таблицы STG DWH будут находиться в Google Cloud Storage (внешние по отношению к Greenplum)

-- Подключение. Источник billing
DROP EXTERNAL TABLE IF EXISTS sperfilyev.stg_t_billing;
CREATE EXTERNAL TABLE sperfilyev.stg_t_billing (
    user_id INT,
    billing_period TEXT,
    service TEXT,
    tariff TEXT,
    "sum" TEXT,
    created_at TEXT
    )
    LOCATION (
        'pxf://rt-2021-03-25-16-47-29-sfunu-final-project/billing/*/?PROFILE=gs:parquet'
        )
    FORMAT 'CUSTOM' (
        FORMATTER = 'pxfwritable_import'
        );

-- Проверки. Источник billing
select * from sperfilyev.stg_t_billing limit 100;
select count(*) from sperfilyev.stg_t_billing;

select distinct extract(month from cast(created_at as date)) as created_month,
                extract(year from cast(created_at as date)) as created_year
from sperfilyev.stg_t_billing
order by created_year, created_month;

select count(*), extract(year from cast(created_at as date)) as created_year
from sperfilyev.stg_t_billing
group by created_year
order by created_year;

-- Подключение. Источник issue
DROP EXTERNAL TABLE IF EXISTS sperfilyev.stg_t_issue;
CREATE EXTERNAL TABLE sperfilyev.stg_t_issue (
    user_id TEXT,
    start_time TEXT,
    end_time TEXT,
    title TEXT,
    description TEXT,
    service TEXT
    )
    LOCATION (
        'pxf://rt-2021-03-25-16-47-29-sfunu-final-project/issue/*/?PROFILE=gs:parquet'
        )
    FORMAT 'CUSTOM' (
        FORMATTER = 'pxfwritable_import'
        );

-- Проверки. Источник issue
select * from sperfilyev.stg_t_issue limit 100;
select count(*) from sperfilyev.stg_t_issue;

select distinct extract(month from cast(start_time as date)) as start_month,
                extract(year from cast(start_time as date)) as start_year
from sperfilyev.stg_t_issue
order by start_year, start_month;

select count(*), extract(year from cast(start_time as date)) as start_year
from sperfilyev.stg_t_issue
group by start_year
order by start_year;

select min(cast(start_time as date)), max(cast(start_time as date))
from sperfilyev.stg_t_issue;

select min(cast(end_time as date)), max(cast(end_time as date))
from sperfilyev.stg_t_issue;

-- Подключение. Источник payment
DROP EXTERNAL TABLE IF EXISTS sperfilyev.stg_t_payment;
CREATE EXTERNAL TABLE sperfilyev.stg_t_payment (
    user_id INT,
    pay_doc_type TEXT,
    pay_doc_num INT,
    account TEXT,
    phone TEXT,
    billing_period TEXT,
    pay_date TEXT,
    "sum" FLOAT
    )
    LOCATION (
        'pxf://rt-2021-03-25-16-47-29-sfunu-final-project/payment/*/?PROFILE=gs:parquet'
        )
    FORMAT 'CUSTOM' (
        FORMATTER = 'pxfwritable_import'
        );

-- Проверки. Источник payment
select * from sperfilyev.stg_t_payment limit 100;
select count(*) from sperfilyev.stg_t_payment;

select distinct extract(month from cast(pay_date as date)) as pay_month,
                extract(year from cast(pay_date as date)) as pay_year
from sperfilyev.stg_t_payment
order by pay_year, pay_month;

select count(*), extract(year from cast(pay_date as date)) as pay_year
from sperfilyev.stg_t_payment
group by pay_year
order by pay_year;

-- Подключение. Источник traffic
DROP EXTERNAL TABLE IF EXISTS sperfilyev.stg_t_traffic;
CREATE EXTERNAL TABLE sperfilyev.stg_t_traffic (
    user_id INT,
    "timestamp" BIGINT,
    device_id TEXT,
    device_ip_addr TEXT,
    bytes_sent INT,
    bytes_received INT
    )
    LOCATION (
        'pxf://rt-2021-03-25-16-47-29-sfunu-final-project/traffic/*/?PROFILE=gs:parquet'
        )
    FORMAT 'CUSTOM' (
        FORMATTER = 'pxfwritable_import'
        );

-- Проверки. Источник traffic
select * from sperfilyev.stg_t_traffic limit 100;
select count(*) from sperfilyev.stg_t_traffic;

select min(to_timestamp("timestamp"/1000)), max(to_timestamp("timestamp"/1000))
from sperfilyev.stg_t_traffic;

select count(*), extract(year from to_timestamp("timestamp"/1000)) as traffic_year
from sperfilyev.stg_t_traffic
group by traffic_year
order by traffic_year;

select min(bytes_received), max(bytes_received), min(bytes_sent), max(bytes_sent)
from sperfilyev.stg_t_traffic;

select distinct device_id from sperfilyev.stg_t_traffic;
