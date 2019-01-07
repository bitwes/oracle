set serveroutput on;
create or replace procedure throws_1 
is
begin
    raise_application_error(-20001, 'From throws_1');
end;
/

create or replace procedure calls_throws_1 
is
begin
    throws_1;
end;
/

create or replace procedure calls_calls_throws_1 
is
begin
    calls_throws_1;
end;
/

begin
    calls_calls_throws_1;
exception
    when others then 
        dbms_output.put_line(sqlcode || ':  ' || sqlerrm); dbms_output.put_line(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
end;
/

drop procedure throws_1;
drop procedure calls_throws_1;
drop procedure calls_calls_throws_1;
