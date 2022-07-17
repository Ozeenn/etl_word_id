-- sch_word_etl.tb_word_files definition

-- Drop table

-- DROP TABLE sch_word_etl.tb_word_files;

CREATE TABLE sch_word_etl.tb_word_files (
	word_id int8 NOT NULL,
	word varchar(100) NULL,
	word_files _text NULL
);