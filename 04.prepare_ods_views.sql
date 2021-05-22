-- Подготовим VIEW с расчётом хэшей в слое ODS для DATA VAULT 2.0

-- Вьюшка. Источник billing
-- Т.к. в данных от источника есть дубликаты, будем агрегировать данные для устранения дублирования
DROP VIEW IF EXISTS sperfilyev.ods_v_billing_etl CASCADE;
CREATE VIEW sperfilyev.ods_v_billing_etl AS (
WITH grouped_recs as (
    SELECT user_id,
           billing_period,
           service,
           tariff,
           sum(billing_sum) AS billing_sum,
           created_at
    FROM sperfilyev.ods_t_billing
    GROUP BY user_id, billing_period, service, tariff, created_at
),
derived_columns AS (
    SELECT *,
           user_id::TEXT            AS user_key,
           billing_period::TEXT     AS billing_period_key,
           service::TEXT            AS service_key,
           tariff::TEXT             AS tariff_key
    FROM grouped_recs
),
     hashed_columns AS (
         SELECT *,
                cast(md5(nullif(upper(trim(cast(user_id AS VARCHAR))), '')) AS TEXT)        AS user_pk,
                cast(md5(nullif(upper(trim(cast(billing_period AS VARCHAR))), '')) AS TEXT) AS billing_period_pk,
                cast(md5(nullif(upper(trim(cast(service AS VARCHAR))), '')) AS TEXT)        AS service_pk,
                cast(md5(nullif(upper(trim(cast(tariff AS VARCHAR))), '')) AS TEXT)         AS tariff_pk,
                cast(md5(nullif(concat_ws('||',
                                          coalesce(nullif(upper(trim(cast(user_id AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(billing_period AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(service AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(tariff AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(created_at AS VARCHAR))), ''), '^^')
                                    ), '^^||^^||^^')) AS TEXT)                              AS billing_pk,
                cast(md5(nullif(upper(trim(cast(billing_sum AS VARCHAR))), '')) AS TEXT)    AS billing_hashdiff
         FROM derived_columns
     )
SELECT user_key,
       billing_period_key,
       service_key,
       tariff_key,
       user_pk,
       billing_period_pk,
       service_pk,
       tariff_pk,
       billing_pk,
       billing_hashdiff,
       billing_sum,
       'BILLNG_DATALAKE'::TEXT AS rec_source,
       created_at AS effective_from
FROM hashed_columns
    );

-- Проверка
select count(*) from sperfilyev.ods_v_billing_etl;
select * from sperfilyev.ods_v_billing_etl order by user_key, effective_from limit 100;
select * from sperfilyev.ods_v_billing_etl order by user_key desc, effective_from desc limit 100;

-- Проверим уникальность ключей:
select count(distinct user_pk),
       count(distinct billing_period_pk),
       count(distinct service_pk),
       count(distinct tariff_pk),
       count(distinct billing_pk),
       count(distinct billing_hashdiff),
       sum(billing_sum)
from sperfilyev.ods_v_billing_etl;

-- Вьюшка. Источник issue
DROP VIEW IF EXISTS sperfilyev.ods_v_issue_etl CASCADE;
CREATE VIEW sperfilyev.ods_v_issue_etl AS
(
WITH derived_columns AS (
    SELECT *,
           user_id::TEXT            AS user_key,
           service::TEXT            AS service_key
    FROM sperfilyev.ods_t_issue
),
     hashed_columns AS (
         SELECT *,
                cast(md5(nullif(upper(trim(cast(user_id AS VARCHAR))), '')) AS TEXT)        AS user_pk,
                cast(md5(nullif(upper(trim(cast(service AS VARCHAR))), '')) AS TEXT)        AS service_pk,
                cast(md5(nullif(concat_ws('||',
                                          coalesce(nullif(upper(trim(cast(user_id AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(start_time AS VARCHAR))), ''), '^^')
                                    ), '^^||^^||^^')) AS TEXT)                              AS issue_pk,
                cast(md5(nullif(concat_ws('||',
                                          coalesce(nullif(upper(trim(cast(title AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(description AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(end_time AS VARCHAR))), ''), '^^')
                                    ), '^^||^^||^^')) AS TEXT)                              AS issue_hashdiff
         FROM derived_columns
     )
SELECT user_key,
       service_key,
       user_pk,
       service_pk,
       issue_pk,
       issue_hashdiff,
       title,
       description,
       end_time,
       'ISSUE_DATALAKE'::TEXT AS rec_source,
       start_time AS effective_from
FROM hashed_columns
    );

-- Проверка
select count(*) from sperfilyev.ods_v_issue_etl;
select * from sperfilyev.ods_v_issue_etl order by user_key, effective_from limit 100;
select * from sperfilyev.ods_v_issue_etl order by user_key desc, effective_from desc limit 100;

-- Проверим уникальность ключей и комбинаций атрибутов:
select count(distinct issue_pk),
       count(distinct issue_hashdiff),
       count(distinct user_pk),
       count(distinct service_pk)
from sperfilyev.ods_v_issue_etl;

-- Вьюшка. Источник payment
DROP VIEW IF EXISTS sperfilyev.ods_v_payment_etl CASCADE;
CREATE VIEW sperfilyev.ods_v_payment_etl AS
(
WITH derived_columns AS (
    SELECT *,
           user_id::TEXT            AS user_key,
           account::TEXT            AS account_key,
           billing_period::TEXT     AS billing_period_key
    FROM sperfilyev.ods_t_payment
),
     hashed_columns AS (
         SELECT *,
                cast(md5(nullif(upper(trim(cast(user_id AS VARCHAR))), '')) AS TEXT)        AS user_pk,
                cast(md5(nullif(upper(trim(cast(account AS VARCHAR))), '')) AS TEXT)        AS account_pk,
                cast(md5(nullif(upper(trim(cast(billing_period AS VARCHAR))), '')) AS TEXT) AS billing_period_pk,
                cast(md5(nullif(concat_ws('||',
                                          coalesce(nullif(upper(trim(cast(pay_doc_type AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(pay_doc_num AS VARCHAR))), ''), '^^')
                                    ), '^^||^^||^^')) AS TEXT)                              AS pay_pk,
                cast(md5(nullif(upper(trim(cast(phone AS VARCHAR))), '')) AS TEXT)          AS user_hashdiff,
                cast(md5(nullif(upper(trim(cast(pay_sum AS VARCHAR))), '')) AS TEXT)        AS pay_hashdiff
         FROM derived_columns
     )
SELECT user_key,
       account_key,
       billing_period_key,
       user_pk,
       account_pk,
       billing_period_pk,
       pay_pk,
       user_hashdiff,
       pay_hashdiff,
       pay_doc_type,
       pay_doc_num,
       pay_sum,
       phone,
       'PAYMENT_DATALAKE'::TEXT AS rec_source,
       pay_date AS effective_from
FROM hashed_columns
    );

-- Проверка
select count(*) from sperfilyev.ods_v_payment_etl;
select * from sperfilyev.ods_v_payment_etl order by user_key, effective_from limit 100;
select * from sperfilyev.ods_v_payment_etl order by user_key desc, effective_from desc limit 100;
-- Проверим уникальность ключей:
select count(distinct pay_pk),
       count(distinct user_pk),
       count(distinct account_pk),
       count(distinct billing_period_pk)
from sperfilyev.ods_v_payment_etl;

-- Вьюшка. Источник traffic
DROP VIEW IF EXISTS sperfilyev.ods_v_traffic_etl CASCADE;
CREATE VIEW sperfilyev.ods_v_traffic_etl AS
(
WITH derived_columns AS (
    SELECT *,
           user_id::TEXT            AS user_key,
           device_id::TEXT          AS device_key
    FROM sperfilyev.ods_t_traffic
),
     hashed_columns AS (
         SELECT *,
                cast(md5(nullif(upper(trim(cast(user_id AS VARCHAR))), '')) AS TEXT)        AS user_pk,
                cast(md5(nullif(upper(trim(cast(device_id AS VARCHAR))), '')) AS TEXT)      AS device_pk,
                cast(md5(nullif(concat_ws('||',
                                          coalesce(nullif(upper(trim(cast(user_id AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(device_id AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(traffic_time AS VARCHAR))), ''), '^^')
                                    ), '^^||^^||^^')) AS TEXT)                              AS traffic_pk,
                cast(md5(nullif(upper(trim(cast(device_ip_addr AS VARCHAR))), '')) AS TEXT) AS device_hashdiff,
                cast(md5(nullif(concat_ws('||',
                                          coalesce(nullif(upper(trim(cast(bytes_sent AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(bytes_received AS VARCHAR))), ''), '^^')
                                    ), '^^||^^||^^')) AS TEXT)                              AS traffic_hashdiff

         FROM derived_columns
     )
SELECT user_key,
       device_key,
       user_pk,
       device_pk,
       traffic_pk,
       traffic_hashdiff,
       device_hashdiff,
       device_ip_addr,
       bytes_sent,
       bytes_received,
       'TRAFFIC_DATALAKE'::TEXT AS rec_source,
       traffic_time AS effective_from
FROM hashed_columns
    );

-- Проверка
select count(*) from sperfilyev.ods_v_traffic_etl;
select * from sperfilyev.ods_v_traffic_etl order by user_key, effective_from limit 100;
select * from sperfilyev.ods_v_payment_etl order by user_key desc, effective_from desc limit 100;
-- Проверим уникальность ключей и комбинаций атрибутов:
select count(distinct user_pk),
       count(distinct device_pk),
       count(distinct traffic_pk),
       count(distinct traffic_hashdiff),
       count(distinct device_hashdiff)
from sperfilyev.ods_v_traffic_etl;

-- Вьюшка. Источник MDM user
DROP VIEW IF EXISTS sperfilyev.ods_v_user_etl CASCADE;
CREATE VIEW sperfilyev.ods_v_user_etl AS (
WITH derived_columns AS (
    SELECT *, user_id::TEXT AS user_key
    FROM sperfilyev.ods_t_user
),
     hashed_columns AS (
         SELECT *,
                CAST(md5(nullif(upper(trim(cast(user_id AS VARCHAR))), '')) AS TEXT) AS user_pk,
                CAST(md5(nullif(concat_ws('||',
                                          coalesce(nullif(upper(trim(cast(legal_type AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(district AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(billing_mode AS VARCHAR))), ''), '^^'),
                                          coalesce(nullif(upper(trim(cast(is_vip AS VARCHAR))), ''), '^^')
                                    ), '^^||^^||^^')) AS TEXT)                       AS user_hashdiff

         FROM derived_columns
     )
SELECT user_key,
       user_pk,
       user_hashdiff,
       legal_type,
       district,
       billing_mode,
       is_vip,
       'MASTER_DATA'::TEXT AS rec_source,
       registered_at       AS effective_from
FROM hashed_columns);

-- Проверка
select count(*) from sperfilyev.ods_v_user_etl;
select * from sperfilyev.ods_v_user_etl order by user_key;
-- Проверим уникальность ключей и комбинаций атрибутов:
select count(distinct user_pk),
       count(distinct user_hashdiff)
from sperfilyev.ods_v_user_etl;
