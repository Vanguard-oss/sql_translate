with a as (select b from c) insert into table d.e PARTITION (f='g') select d from a