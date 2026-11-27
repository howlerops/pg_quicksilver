-- baseline 0: the standby's view of the mirrors (superuser)
SELECT heap, usable, fresh FROM quicksilver.mirror_status() ORDER BY 1;
SELECT * FROM quicksilver.join_domains;
