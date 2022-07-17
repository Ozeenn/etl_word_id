CREATE OR REPLACE PROCEDURE staging.proc_insert_tb_word_files()
 LANGUAGE plpgsql
AS $procedure$
	BEGIN
		truncate sch_word_etl.tb_word_files;

		insert into sch_word_etl.tb_word_files
		select
			word_id, word, array_agg(word_archive order by word_archive)
		from staging.stg_word_id
		group by
		word, word_id;
	END;
$procedure$
;
