-- TODO: ЗАГРУЗИТЬ ДАННЫЕ В DATA VAULT (ODS => DDS)
-- TODO: СДЕЛАТЬ ДЛЯ ВСЕХ 5 ИСТОЧНИКОВ
-- TODO: АВТОМАТИЗИРОВАТЬ В AIRFLOW

-- Тестируем ETL-процедуры: заполняем данными за один год



-- ========================================================================
-- Создаём доп.таблицы расширенные для хранения ключей, хэшдиффов итд

-- Таблица с расчитанными хэшами. Источник payment
DROP TABLE IF EXISTS sperfilyev.ods_t_payment_hashed CASCADE;
CREATE TABLE sperfilyev.ods_t_payment_hashed AS (
    SELECT v.*,
           current_timestamp AS load_dts
    FROM sperfilyev.ods_v_payment_etl AS v);

-- Проверка
select count(*) from sperfilyev.ods_t_payment_hashed;
select * from sperfilyev.ods_t_payment_hashed order by user_key, effective_from;
select count(distinct pay_pk) from sperfilyev.ods_t_payment_hashed;

-- Таблица с расчитанными хэшами. Источник MDM user
DROP TABLE IF EXISTS sperfilyev.ods_t_user_hashed CASCADE;
CREATE TABLE sperfilyev.ods_t_user_hashed AS (
    SELECT v.*,
           current_timestamp AS load_dts
    FROM sperfilyev.ods_v_user_etl AS v);

-- Проверка
select count(*) from sperfilyev.ods_t_user_hashed;
select * from sperfilyev.ods_t_user_hashed order by user_key;
select count(distinct user_pk) from sperfilyev.ods_t_user_hashed;



-- Заполнение хэшей. Источник payment
insert into sperfilyev.ods_t_payment_hashed
select v.*,
       '2013-01-01'::date as load_dts
from sperfilyev.ods_v_payment_etl as v
where extract(year from effective_from)=2013;

-- Проверка. Источник payment
select count(*) from sperfilyev.ods_t_payment_hashed;
select * from sperfilyev.ods_t_payment_hashed limit 10;

select count(distinct account_key) from sperfilyev.ods_t_payment_hashed;
select count(distinct billing_period_key) from sperfilyev.ods_t_payment_hashed;

select extract(year from effective_from) as pay_year, count(*)
from sperfilyev.ods_t_payment_hashed
group by pay_year
order by pay_year;

select load_dts, count(*)
from sperfilyev.ods_t_payment_hashed
group by load_dts
order by load_dts;

-- ===============================================================
-- Загрузка в слой DDS

-- Создаём вьюшку для добавления новых данных в хаб user
DROP VIEW IF EXISTS sperfilyev.dds_v_hub_user_mdm_etl;
CREATE VIEW sperfilyev.dds_v_hub_user_mdm_etl AS (
WITH users_numbered AS (
    SELECT user_pk,
           user_key,
           load_dts,
           rec_source,
           row_number() OVER (PARTITION BY user_pk ORDER BY load_dts ASC) AS row_num
    FROM sperfilyev.ods_t_user_hashed),
     users_rank_1 AS (
         SELECT user_pk, user_key, load_dts, rec_source
         FROM users_numbered
         WHERE row_num = 1),
     records_to_insert AS (
         SELECT a.*
         FROM users_rank_1 AS a
                  LEFT JOIN sperfilyev.dds_t_hub_user AS h
                            ON a.user_pk = h.user_pk
         WHERE h.user_pk IS NULL
     )
SELECT *
FROM records_to_insert
    );

-- Проверка. Должно появиться 3 новых юзера, которых не было
-- в источнике payment, с идентификаторами 20590, 20600, 20610
select * from sperfilyev.dds_v_hub_user_mdm_etl order by user_key;
select count(*) from sperfilyev.dds_v_hub_user_mdm_etl;

-- Прогружаем данные из MDM в хаб user
INSERT INTO sperfilyev.dds_t_hub_user SELECT * FROM sperfilyev.dds_v_hub_user_mdm_etl;

-- Проверка. Должно быть 126 юзеров
select count(*) from sperfilyev.dds_t_hub_user;
select count(distinct user_pk) from sperfilyev.dds_t_hub_user;
select count(*), load_dts from sperfilyev.dds_t_hub_user group by load_dts order by load_dts desc;

-- Создаём новый сателлит для User (атрибуты из MDM)
DROP TABLE IF EXISTS sperfilyev.dds_t_sat_user_mdm CASCADE;
CREATE TABLE sperfilyev.dds_t_sat_user_mdm
(
    user_pk        TEXT,
    user_hashdiff  TEXT,
    legal_type     TEXT,
    district       TEXT,
    billing_mode   TEXT,
    is_vip         BOOLEAN,
    effective_from DATE,
    load_dts       TIMESTAMP,
    rec_source     TEXT
)  DISTRIBUTED RANDOMLY;

-- Создаём вьюшку для добавления новых данных в сателлит user_mdm
DROP VIEW IF EXISTS sperfilyev.dds_v_sat_user_mdm_etl;
CREATE VIEW sperfilyev.dds_v_sat_user_mdm_etl AS (
WITH source_data AS (
    SELECT user_pk,
           user_hashdiff,
           legal_type,
           district,
           billing_mode,
           is_vip,
           effective_from,
           load_dts,
           rec_source
    FROM sperfilyev.ods_t_user_hashed
),
     update_records AS (
         SELECT s.*
         FROM sperfilyev.dds_t_sat_user_mdm AS s
                  JOIN source_data AS src
                       ON s.user_pk = src.user_pk
     ),
     latest_records AS (
         SELECT *
         FROM (
                  SELECT user_pk,
                         user_hashdiff,
                         load_dts,
                         rank() OVER (PARTITION BY user_pk ORDER BY load_dts DESC) AS row_rank
                  FROM update_records
              ) AS ranked_recs
         WHERE row_rank = 1),
     records_to_insert AS (
         SELECT DISTINCT a.*
         FROM source_data AS a
                  LEFT JOIN latest_records
                            ON latest_records.user_hashdiff = a.user_hashdiff AND
                               latest_records.user_pk = a.user_pk
         WHERE latest_records.user_hashdiff IS NULL
     )
SELECT * FROM records_to_insert);

-- Проверка. Должны выводиться записи для всех 126 юзеров
select count(*) from sperfilyev.dds_v_sat_user_mdm_etl;
select * from sperfilyev.dds_v_sat_user_mdm_etl;

-- Загружаем данные в сателлит user_mdm
INSERT INTO sperfilyev.dds_t_sat_user_mdm SELECT * FROM sperfilyev.dds_v_sat_user_mdm_etl;
select count(*) from sperfilyev.dds_t_sat_user_mdm;
select * from sperfilyev.dds_t_sat_user_mdm;

-- Проверка ещё раз. Теперь не должно быть записей для добавления
select count(*) from sperfilyev.dds_v_sat_user_mdm_etl;

-- Проверим как данные из нового сателлита стыкуются с хабом:
drop view if exists sperfilyev.dds_v_user_detailed;
create view sperfilyev.dds_v_user_detailed as (
select h.user_pk as hub_pk,
       h.user_key as user_id,
       h.load_dts as hub_loaddts,
       h.rec_source as hub_recsrc,
       s.*
from sperfilyev.dds_t_hub_user h
join sperfilyev.dds_t_sat_user_mdm s
on h.user_pk=s.user_pk);

select * from sperfilyev.dds_v_user_detailed order by user_id;


