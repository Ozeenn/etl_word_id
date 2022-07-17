-- staging.stg_word_id definition

-- Drop table

-- DROP TABLE staging.stg_word_id;

CREATE TABLE staging.stg_word_id (
	word_archive int4 NULL,
	word varchar(100) NULL,
	word_id int8 NOT NULL
);