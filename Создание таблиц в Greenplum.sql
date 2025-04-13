-- Создадим рабочую схему std11_12

CREATE schema std11_12;

-- Создадим таблицы из внешних источников
-- загрузим по протоколу gpfdist из локальных файлов CSV


-- Магазины(СПРАВОЧНИК)
CREATE TABLE std11_12.stores (
	plant text NULL,
	txt text NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

--Пример запроса
SELECT * FROM std11_12.stores;

--Купоны (ФАКТЫ)

CREATE TABLE std11_12.coupons (
	store_id text NULL,
	calday date NULL,
	coupon_name text NULL,
	promo_id text NULL,
	product_id bigint NULL,
	billnum bigint NOT NULL,
	billitem bigint NULL,
	price bigint NULL,
	promo_type bigint NULL,
	discount text NULL
)
DISTRIBUTED BY(billnum)


--Акции (СПРАВОЧНИК)

CREATE TABLE std11_12.promos (
	promo_id text NULL,
	promo_name text NULL,
	promo_type int NULL,
	product_id bigint NULL,
	discount int NULL
)
WITH (
	appendonly=true,  -- Включаем append-only режим для улучшения производительности.
	orientation=row,  -- Row-ориентация подходит для справочных данных, так как они часто читаются построчно.
	compresstype=zstd,  -- Используем Zstandard для сжатия данных.
	compresslevel=1     -- Низкий уровень сжатия для баланса между скоростью и компрессией.
)
DISTRIBUTED REPLICATED;

--Типы акций (СПРАВОЧНИК)

CREATE TABLE std11_12.promo_types(
	promo_type int NULL,
	txt text NULL
)
WITH (
	appendonly=true,  -- Включаем append-only режим для улучшения производительности.
	orientation=row,  -- Row-ориентация подходит для справочных данных, так как они часто читаются построчно.
	compresstype=zstd,  -- Используем Zstandard для сжатия данных.
	compresslevel=1     -- Низкий уровень сжатия для баланса между скоростью и компрессией.
)
DISTRIBUTED REPLICATED;

--  Остальные таблицы загрузим из БД PostgreSQL через pxf
--Таблица bills_head (ФАКТЫ)

CREATE TABLE std11_12.bills_head (
    billnum int NOT NULL,
    plant text NULL,
    calday date NULL
)
WITH (
    appendonly=true,          -- Включаем append-only режим.
    orientation=row,          -- Row-ориентация для справочных данных.
    compresstype=zstd,        -- Используем Zstandard для сжатия.
    compresslevel=1           -- Низкий уровень сжатия.
)
DISTRIBUTED BY (billnum)      -- Распределение данных по billnum.
PARTITION BY RANGE(calday)    -- Партиционирование по столбцу calday.
(
    PARTITION ym START ('2020-01-01') END ('2023-03-01') EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION other   -- Данные, не попадающие в другие партиции.
);

--Таблица bills_item (ФАКТЫ)

CREATE TABLE std11_12.bills_item (
    billnum int8 NOT NULL,
    billitem int8 NULL,
    material int8 NULL,
    qty int8 NULL,
    netval numeric(17, 2) NULL,
    tax numeric(17, 2) NULL,
    rpa_sat numeric(17, 2) NULL,
    calday date NULL
)
WITH (
    appendonly=true,          -- Включаем append-only режим для улучшения производительности.
    orientation=row,          -- Row-ориентация подходит для справочных данных.
    compresstype=zstd,        -- Используем Zstandard для сжатия данных.
    compresslevel=1           -- Низкий уровень сжатия для баланса между скоростью и компрессией.
)
DISTRIBUTED BY (billnum)      -- Распределение данных по billnum.
PARTITION BY RANGE(calday)    -- Партиционирование по столбцу calday.
(
    PARTITION ym START ('2020-01-01') END ('2021-03-01') EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION other   -- Данные, не попадающие в другие партиции.
);

--Таблица traffic (ФАКТЫ)

CREATE TABLE std11_12.traffic (
    plant text NULL,
    "date" date NULL,               -- Изменяем тип данных на date
    "time" time NULL,               -- Изменяем тип данных на time
    frame_id text NOT NULL,  
    quantity int NULL,
)
DISTRIBUTED BY (frame_id)           -- Распределение данных по уникальному ключу
PARTITION BY RANGE(date)          -- Партиционирование по столбцу date
(
    PARTITION ym START ('2020-01-01') END ('2021-03-01') EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION other         -- Данные, не попадающие в другие партиции
);
