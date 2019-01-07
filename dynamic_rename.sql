define tbl = 'blahblah';
prompt &tbl;
create materialized view &tbl as select 'asdf' from dual;
desc &tbl;
select * from &tbl;
drop materialized view &tbl;


var tbl varchar2(100);
exec :tbl := 'blah';
create materialized view :tbl as select 'asdf' from dual;
drop materialized view :tbl;
prompt :tbl;


define tbl = 'desc spriden';
&tbl;

define tbl = select 'blah' from dual;

--******************************************************************************************
--******************************************************************************************

-----------------------------------
-- Create base table with soem data
--and a synonym
-----------------------------------
create table TEST_TABLE_ONE
(
  words VARCHAR2(1000)
);
insert into test_table_one words values ('hello');
insert into test_table_one words values ('world');
commit;
create public synonym tto for test_table_one;
select * from tto;

-----------------------------------
--get the new name for the table and the name
--of the existing table.
-----------------------------------
col old_view_name for a30 new_value var_old_view;
col new_view_name for a30 new_value var_view_name;
def var_view_name;
select decode(table_name,
              'TEST_TABLE_ONE', 'TEST_TABLE_A',
              'TEST_TABLE_A', 'TEST_TABLE_B',
              'TEST_TABLE_B', 'TEST_TABLE_A') as new_view_name,
              table_name as old_view_name
  from all_tables 
 where owner ='XUPROMMGR' 
   and table_name like 'TEST_TABLE_%';
prompt &var_view_name;
prompt &var_old_view;

-----------------------------------
--Create the new table, alter the synonym
--to point to new table and kill the old
--table
-----------------------------------
create table &var_view_name( words varchar2(1000));
insert into &var_view_name words values ('goodbye');
insert into &var_view_name words values ('universe');
commit;
drop public synonym tto;
create public synonym tto for &var_view_name;
drop table &var_old_view;
select * from tto;

-----------------------------------
--Cleanup
-----------------------------------
drop public synonym tto;
drop table &var_view_name;
