CREATE OR REPLACE FUNCTION std11_12.f_load_partition(p_table text, p_partition_key text, p_pxf_table text, p_user_id text, p_pass text, p_start_date timestamp, p_end_date timestamp DEFAULT NULL::timestamp without time zone)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
	
DECLARE
    v_ext_table text;
    v_temp_table text;
    v_sql text;
    v_pxf text;
    v_result int;
    v_dist_key text;
    v_params text;
    v_where text;
    v_load_interval interval;
    v_start_date date;
    v_end_date date;
	v_iter_date date;
    v_table_oid int8;
    v_cnt int8;
	v_format text;
	
	
BEGIN
    v_ext_table := p_table || '_ext';
    v_temp_table := p_table || '_tmp';

	

    SELECT c.oid
	into v_table_oid
	FROM pg_class AS c
	JOIN pg_namespace AS n ON c.relnamespace = n.oid
	WHERE n.nspname = split_part(p_table, '.', 1)  -- Берём схему из p_table
	AND c.relname = split_part(p_table, '.', 2)    -- Берём имя таблицы
	limit 1;
	

	RAISE NOTICE 'OID IS %', v_table_oid;
    IF v_table_oid = 0 OR v_table_oid IS NULL THEN
        v_dist_key := 'DISTRIBUTED RANDOMLY';
    ELSE
        v_dist_key := pg_get_table_distributedby(v_table_oid);
    END IF;


    SELECT COALESCE('with (' || ARRAY_TO_STRING(reloptions, ', ') || ')', '')
    FROM pg_class
    INTO v_params
    WHERE oid = p_table::REGCLASS;

    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table;

	--Расчет интервалов времени
    v_load_interval := '1 month'::INTERVAL;
    v_start_date := DATE_TRUNC('month', p_start_date);
	IF p_end_date IS NULL THEN
    	v_end_date := DATE_TRUNC('month', p_start_date) + v_load_interval;
	ELSE 
		v_end_date := DATE_TRUNC('month', p_end_date);
	END IF;

	RAISE NOTICE 'start: %', v_start_date;
	RAISE NOTICE 'end: %', v_end_date;

	v_iter_date = v_start_date;
	-- Проверка партиций и добавление новых 
	WHILE v_iter_date < v_end_date LOOP
		RAISE NOTICE 'date part: %', v_iter_date;
        -- Проверяем, есть ли партиция
        IF NOT EXISTS (
            SELECT 1
			FROM pg_partitions
			WHERE schemaname = 'std11_12'
			and tablename = split_part(p_table, '.', 2)
			and substring(partitionrangestart FROM 2 FOR 10)::date = v_iter_date 
        ) THEN
            -- Создаём партицию, если её нет
            EXECUTE format(
                'ALTER TABLE '||p_table||' ADD PARTITION part_%s START (''%s'') END (''%s'');',
                to_char(v_iter_date, 'YYYYMM'),
                v_iter_date,
                v_iter_date + INTERVAL '1 month'
            );
        END IF;
        RAISE NOTICE 'part - %', to_char(v_iter_date, 'YYYYMM');
        -- Переход к следующему месяцу
        v_iter_date := v_iter_date + INTERVAL '1 month';
    END LOOP;
	
	--Строка подключения PXF
    v_pxf := 'pxf://'||p_pxf_table||'?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=' 
        || p_user_id || '&PASS=' || p_pass;

    RAISE NOTICE '1. PXF CONNECTION STRING: %', v_pxf;

	-- Проверка таблицы 
	IF p_table like 'std11_12.traffic' THEN -- 
	    v_sql := 'CREATE EXTERNAL TABLE ' || v_ext_table || ' ('
           || 'plant BPCHAR(4), '
           || 'date BPCHAR(10), '
           || 'time BPCHAR(6), '
           || 'frame_id BPCHAR(10), '
           || 'quantity INT4)'
           || 'LOCATION (''' || v_pxf || ''') '
           || 'ON ALL '
           || 'FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'') '
           || 'ENCODING ''UTF8'';';
	ELSE
		v_sql := 'CREATE EXTERNAL TABLE ' || v_ext_table || ' (LIKE ' || p_table || ') '
	       || 'LOCATION (''' || v_pxf || ''') '
	       || 'ON ALL '
	       || 'FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'') '
	       || 'ENCODING ''UTF8'';';
	END IF;

    RAISE NOTICE '2. EXTERNAL TABLE IS: %', v_sql;

    EXECUTE v_sql;
	

	--DELTA PARTITION
    v_iter_date := v_start_date;
	WHILE v_iter_date < v_end_date LOOP
		RAISE NOTICE 'ALTER DATE: %', v_iter_date;

		v_where := p_partition_key || ' >= ''' || v_iter_date || 
					''' AND ' || p_partition_key || ' < ''' || v_iter_date + v_load_interval || 
					''' AND '|| p_partition_key||' IS NOT NULL;';

		RAISE NOTICE 'WHERE: %', v_where;

		--Создание временной таблицы
	    v_sql := 'DROP TABLE IF EXISTS ' || v_temp_table || '; '
	        || 'CREATE TABLE ' || v_temp_table || ' (LIKE ' || p_table || ') ' || v_params || ' ' || v_dist_key || ';';
	
	    RAISE NOTICE '3. TEMP TABLE IS: %', v_sql;
	
	    EXECUTE v_sql;
		IF p_table like 'std11_12.traffic' THEN 
				
			v_format := 'DD.MM.YYYY'::text;
			
			RAISE NOTICE 'WHERE: %', v_where;
			v_sql := 'INSERT INTO ' || v_temp_table || ' SELECT plant, TO_DATE(' || p_partition_key || ', ''' || v_format || ''') AS date,  -- Преобразование формата
		            "time"::time, frame_id, 
		            quantity FROM ' || v_ext_table || ' WHERE TO_DATE(' || p_partition_key || ', ''' || v_format || ''') >= ''' || v_iter_date || 
		            ''' AND TO_DATE(' || p_partition_key || ', ''' || v_format || ''') < ''' || v_iter_date + v_load_interval || 
		            ''' AND ' || p_partition_key || ' IS NOT NULL';

		ELSE
			v_sql := 'INSERT INTO ' || v_temp_table || ' SELECT * FROM ' || v_ext_table || ' WHERE ' || v_where;

		END IF;
		EXECUTE v_sql;
		GET DIAGNOSTICS v_cnt = ROW_COUNT;
    	RAISE NOTICE 'INSERTED ROWS: %', v_cnt;
		v_sql := 'ALTER TABLE ' || p_table || ' EXCHANGE PARTITION FOR (DATE ''' || v_iter_date || ''') WITH TABLE ' || v_temp_table || ' WITH VALIDATION';

	    RAISE NOTICE '5. EXCHANGE PARTITION SCRIPT: %', v_sql;
	
	    EXECUTE v_sql;
	
	    EXECUTE 'SELECT COUNT(1) FROM ' || p_table || ' WHERE ' || v_where INTO v_result;

    
		v_iter_date := v_iter_date + INTERVAL '1 month';
				
	END LOOP;

	v_sql := 'DROP TABLE IF EXISTS ' || v_temp_table || '; ';
	EXECUTE v_sql;												--Удаление временной таблицы

	RETURN v_result;
END;





$$
EXECUTE ON ANY;