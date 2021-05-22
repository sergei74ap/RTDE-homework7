-- Создаём структуру для слоя ODS DWH

-- Создать. Источник billing
DROP TABLE IF EXISTS sperfilyev.ods_t_billing CASCADE;
CREATE TABLE sperfilyev.ods_t_billing
(
    user_id        INT,
    billing_period TEXT,
    service        TEXT,
    tariff         TEXT,
    billing_sum    DECIMAL(10, 2),
    created_at     DATE
)
DISTRIBUTED RANDOMLY;

-- Создать. Источник issue
DROP TABLE IF EXISTS sperfilyev.ods_t_issue CASCADE;
CREATE TABLE sperfilyev.ods_t_issue
(
    user_id        INT,
    start_time     TIMESTAMP,
    end_time       TIMESTAMP,
    title          TEXT,
    description    TEXT,
    service        TEXT
)
DISTRIBUTED RANDOMLY;

-- Создать. Источник payment
DROP TABLE IF EXISTS sperfilyev.ods_t_payment CASCADE;
CREATE TABLE sperfilyev.ods_t_payment
(
    user_id        INT,
    pay_doc_type   TEXT,
    pay_doc_num    INT,
    account        TEXT,
    phone          TEXT,
    billing_period TEXT,
    pay_date       DATE,
    pay_sum        DECIMAL(10, 2)
)
DISTRIBUTED RANDOMLY;

-- Создать. Источник traffic
DROP TABLE IF EXISTS sperfilyev.ods_t_traffic CASCADE;
CREATE TABLE sperfilyev.ods_t_traffic
(
    user_id        INT,
    traffic_time   TIMESTAMP,
    device_id      TEXT,
    device_ip_addr TEXT,
    bytes_sent     INT,
    bytes_received INT
)
DISTRIBUTED RANDOMLY;

-- Создать. Источник MDM user
DROP TABLE IF EXISTS sperfilyev.ods_t_user CASCADE;
CREATE TABLE sperfilyev.ods_t_user
(
    user_id        INT,
    legal_type     TEXT,
    district       TEXT,
    registered_at  TIMESTAMP,
    billing_mode   TEXT,
    is_vip         BOOLEAN
)
DISTRIBUTED RANDOMLY;
