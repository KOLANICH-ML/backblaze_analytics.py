-- a temporary table to store drives
CREATE TABLE temp.'drives_' (
	id INTEGER PRIMARY KEY,
	model TEXT NOT NULL,
	serial_number TEXT UNIQUE NOT NULL
);
insert into temp.`drives_` (serial_number, model) 
	SELECT DISTINCT `serial_number`, `model` FROM `drive_stats_1` where `serial_number` not in (select `serial_number` from `drives`);

-- begin transaction;
insert into `models` (`name`)
	SELECT DISTINCT temp.`drives_`.`model` from temp.`drives_`
	where temp.`drives_`.`model` not in
			(SELECT `name` as `model` from `models`);
-- commit transaction;


-- drop INDEX IF EXISTS drives_id_IDX;
insert into `drives` (`serial_number`, `model_id`)
	SELECT t1.`serial_number`, t2.`id` as `model_id` FROM temp.`drives_` t1
	inner JOIN `models` t2 on t1.`model`=t2.`name`
	where t1.`serial_number` not in (select `serial_number` from `drives`)
;
CREATE INDEX IF NOT EXISTS drives_id_IDX ON drives(id);

drop table temp."drives_";

UPDATE `models`
SET `brand_id` = (
	select br.`id` as `brand_id` from `brands` br where `models`.`name` REGEXP br.`model_name_regex`
);
-- or
-- select m.`id`, m.`name`, br.`id` as `brand_id` from `brands` br join `models` m on m.`name` REGEXP br.`model_name_regex` ;