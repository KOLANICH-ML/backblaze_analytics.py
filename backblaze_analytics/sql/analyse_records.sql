delete from `drives_analytics`;
insert into `drives_analytics`
(`id`, `first_appearance`, `last_appearance`, `failed`)
select DISTINCT `drive_id`, `oid`, 1 from `drive_stats` where `failure` = 1;


/* update `drives` set `last_appearance` = (select oid from `drive_stats` s where s.`drive_id` = `drives`.`id` and `failure` = 1 limit 10);
*/

/* with a as (
	select `oid`, `drive_id` from `drive_stats` where `failure` = 1 limit 10
)
UPDATE drives
SET last_appearance=(select `oid` from a where `drive_id` = `drives`.`id`), failed=1;
 */

-- CREATE INDEX IF NOT EXISTS drive_stats_date_IDX ON `drive_stats` (date);
-- CREATE INDEX IF NOT EXISTS drive_stats_drive_id_IDX ON `drive_stats` (drive_id);
-- CREATE INDEX IF NOT EXISTS IF NOT EXISTS drive_stats_failure ON `drive_stats` (failure);
-- CREATE INDEX IF NOT EXISTS drive_stats_date_drive_id_IDX ON `drive_stats` ("date", drive_id) ;

--- an optimized query by incognito on freenode #sqlite
-- incorrect
select a.`oid` as `failure_record`, a.`drive_id`, b.`last_appearance`, b.`first_appearance` from
(select min(`date`) oid, drive_id from drive_stats where failure=1 group by drive_id) a
join
(select min(`date`) first_appearance, max(`date`) last_appearance, drive_id from drive_stats group by drive_id) b
on b.`drive_id`=a.`drive_id` limit 1;

