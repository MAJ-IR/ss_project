CREATE OR REPLACE FUNCTION std11_12.f_load_gpfdist(p_table text, p_file_name text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

DECLARE
	ext_table_name text;
	sql_script text;
	gpfdist text;

BEGIN
	ext_table_name = p_table||'_ext';
	EXECUTE 'TRUNCATE TABLE '||p_table;
	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||ext_table_name;
	gpfdist = 'gpfdist://172.16.128.50/'||p_file_name||'.csv';
	sql_script = 'CREATE EXTERNAL TABLE '||ext_table_name||' (LIKE '||p_table||')
				LOCATION ('''||gpfdist||''')
				FORMAT ''CSV'' (HEADER DELIMITER '';'')
				ENCODING ''UTF8''';

	EXECUTE sql_script;
	EXECUTE 'INSERT INTO '||p_table||' SELECT * FROM '||ext_table_name;

END;


$$
EXECUTE ON ANY;
