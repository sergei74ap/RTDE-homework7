-- TODO: АВТОМАТИЗИРОВАТЬ В AIRFLOW

-- Тестируем заполнение слоя ODS данными из STG/MDM

-- Очистить всё
TRUNCATE sperfilyev.ods_t_billing;
TRUNCATE sperfilyev.ods_t_issue;
TRUNCATE sperfilyev.ods_t_payment;
TRUNCATE sperfilyev.ods_t_traffic;
TRUNCATE sperfilyev.ods_t_user;

-- Загрузить. Источник billing
INSERT INTO sperfilyev.ods_t_billing (
    SELECT user_id,
           billing_period,
           service,
           tariff,
           cast("sum" AS NUMERIC(10, 2)),
           cast(created_at AS DATE)
    FROM sperfilyev.stg_t_billing);

-- Загрузить. Источник issue
INSERT INTO sperfilyev.ods_t_issue (
    SELECT cast(user_id AS INT),
           cast(start_time AS TIMESTAMP),
           cast(end_time AS TIMESTAMP),
           title,
           description,
           service
    FROM sperfilyev.stg_t_issue);

-- Загрузить. Источник payment
INSERT INTO sperfilyev.ods_t_payment (
    SELECT user_id,
           pay_doc_type,
           cast(pay_doc_num AS INT),
           account,
           phone,
           billing_period,
           cast(pay_date AS DATE),
           cast("sum" AS NUMERIC(10, 2))
    FROM sperfilyev.stg_t_payment);

-- Загрузить. Источник traffic
INSERT INTO sperfilyev.ods_t_traffic (
    SELECT user_id,
           to_timestamp("timestamp" / 1000),
           device_id,
           device_ip_addr,
           bytes_sent,
           bytes_received
    FROM sperfilyev.stg_t_traffic);

-- Загрузить. Источник MDM user
INSERT INTO sperfilyev.ods_t_user
    SELECT *
    FROM mdm."user";

-- ======================================================
-- Проверки корректности загрузки данных в ODS из STG/MDM

-- Проверить. Источник billing
select count(*) from sperfilyev.ods_t_billing;
select * from sperfilyev.ods_t_billing limit 10;

select extract(year from created_at) as billing_year, count(*)
from sperfilyev.ods_t_billing
group by billing_year
order by billing_year;

select distinct service from sperfilyev.ods_t_billing;
select distinct tariff from sperfilyev.ods_t_billing;
select count(distinct billing_period) from sperfilyev.ods_t_billing;
select min(billing_sum), max(billing_sum), avg(billing_sum), sum(billing_sum) from sperfilyev.ods_t_billing;

-- Определим PK. Проверим уникальность начислений
select count(distinct (user_id,
                       billing_period,
                       service,
                       tariff,
                       created_at))
from sperfilyev.ods_t_billing;
-- Уникальности нет, 9980 уникальных записей из 10000

-- Как побороть записи-дубликаты?
-- Вариант 1. Внесём уникальность искусственно, добавив нумерацию:
create view sperfilyev.ods_v_billing_numbered as (
with numbered_recs as (
    select *,
           row_number() over (
               partition by
                   user_id,
                   billing_period,
                   service,
                   tariff
               order by
                   created_at
           ) as row_num
    from sperfilyev.ods_t_billing)
select * from numbered_recs
order by user_id, billing_period, service, tariff, created_at);

select * from sperfilyev.ods_v_billing_numbered;
select count(distinct (user_id, billing_period, service, tariff, row_num)) from sperfilyev.ods_v_billing_numbered;
-- Уникальность достигнута. Но для DWH такой "синтетический" ключ плохо подходит,
-- т.к. при перезаливках из систем-источников непонятно, новые будут записи или нет
drop view sperfilyev.ods_v_billing_numbered;

-- Вариант 2. Исходить из гипотезы, что записи-дубликаты являются перерасчётами-доначислениями,
-- сливать их в одну запись при загрузке ODS => DDS. Например, так:
with grouped_recs as (
    select user_id, billing_period, service, tariff, sum(billing_sum) as billing_sum, created_at
    from sperfilyev.ods_t_billing
    group by user_id, billing_period, service, tariff, created_at
)
select count (distinct (user_id, billing_period, service, tariff, created_at)),
       count (distinct billing_sum),
       sum(billing_sum)
from grouped_recs;
-- В дальнейшем при построении DWH используем Вариант 2

-- Проверить. Источник issue
select count(*) from sperfilyev.ods_t_issue;
select * from sperfilyev.ods_t_issue limit 10;

select extract(year from start_time) as start_year, count(*)
from sperfilyev.ods_t_issue
group by start_year
order by start_year;

select min(start_time), max(start_time), min(end_time), max(end_time) from sperfilyev.ods_t_issue;
select distinct service from sperfilyev.ods_t_issue;
select count(distinct title), count(distinct description) from sperfilyev.ods_t_issue;

-- Определим PK. Проверим уникальность обращений
select count(distinct (user_id, start_time)) from sperfilyev.ods_t_issue;
-- УНИКАЛЬНОСТЬ!

-- Проверить. Источник payment
select count(*) from sperfilyev.ods_t_payment;
select * from sperfilyev.ods_t_payment limit 10;

-- Определим PK. Проверим уникальность платежей
select count(distinct (user_id,
                       billing_period,
                       pay_date,
                       pay_doc_type))
from sperfilyev.ods_t_payment;
-- Уникальности нет, 9962 уникальных записей из 10000

with grouped_by_year as (
    select extract(year from pay_date) as pay_year,
           count(distinct (user_id,
                           billing_period, pay_date,
                           pay_doc_type)) as cnt
    from sperfilyev.ods_t_payment
    group by pay_year
    order by pay_year
) select sum(cnt) from grouped_by_year;
-- Уникальности в рамках года нет, 9962 уникальных записей из 10000

with grouped_by_month as (
    select extract(year from pay_date) as pay_year,
           extract(month from pay_date) as pay_month,
           count(distinct (user_id,
                           billing_period, pay_date,
                           pay_doc_type)) as cnt
    from sperfilyev.ods_t_payment
    group by pay_year, pay_month
    order by pay_year, pay_month
) select sum(cnt) from grouped_by_month;
-- Уникальности в рамках месяца нет, 9962 уникальных записей из 10000

-- ВОТ ОНА УНИКАЛЬНОСТЬ:
select count (distinct (pay_doc_type, pay_doc_num)) from sperfilyev.ods_t_payment;

select extract(year from pay_date) as pay_year, count(*)
from sperfilyev.ods_t_payment
group by pay_year
order by pay_year;

select count(distinct account), count(distinct phone), count(distinct billing_period) from sperfilyev.ods_t_payment;
select distinct pay_doc_type from sperfilyev.ods_t_payment;
select min(pay_sum), max(pay_sum), avg(pay_sum) from sperfilyev.ods_t_payment;

-- Проверить. Источник traffic
select count(*) from sperfilyev.ods_t_traffic;
select * from sperfilyev.ods_t_traffic limit 10;

select extract(year from traffic_time) as traffic_year, count(*)
from sperfilyev.ods_t_traffic
group by traffic_year
order by traffic_year;

select count(distinct user_id), count(distinct device_id), count(distinct device_ip_addr)
from sperfilyev.ods_t_traffic;

select min(bytes_sent), max(bytes_received), min(bytes_received), max(bytes_received)
from sperfilyev.ods_t_traffic;

-- Определим PK. Проверим уникальность сессий передачи данных:
select count(distinct (user_id, device_id, traffic_time)) from sperfilyev.ods_t_traffic;
-- УНИКАЛЬНОСТЬ!

-- Проверить. Источник MDM user
select count(*) from ods_t_user;
select * from sperfilyev.ods_t_user order by user_id;
select count(distinct user_id), min(user_id), max(user_id) from sperfilyev.ods_t_user;
select legal_type, count(*) from sperfilyev.ods_t_user group by legal_type;
select district, count(*) as cnt from sperfilyev.ods_t_user group by district order by cnt desc;
select min(registered_at), max(registered_at) from sperfilyev.ods_t_user;
select billing_mode, count(*) from sperfilyev.ods_t_user group by billing_mode;
select is_vip, count(*) from sperfilyev.ods_t_user group by is_vip;
