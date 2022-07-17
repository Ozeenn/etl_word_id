CREATE OR REPLACE PROCEDURE staging.proc_insert_tb_word_id()
 LANGUAGE plpgsql
AS $procedure$
	BEGIN
		truncate sch_word_etl.tb_word_id;

		insert into sch_word_etl.tb_word_id
		select
			distinct word, word_id
		from staging.stg_word_id;
	END;
$procedure$
;
