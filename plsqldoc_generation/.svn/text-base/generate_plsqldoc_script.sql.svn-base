set serveroutput on size 1000000;
set linesize 2000;

declare  
  OBJECTS_PER_LINE  number := 5;
  MAX_OBJECTS       number := 10;
  
  -----------------------------------------------------------------------------
  --print wrapper for less typing
  -----------------------------------------------------------------------------  
  procedure p(in_text in varchar2)
  is
  begin
    dbms_output.put_line(in_text);
  end;
  
  -----------------------------------------------------------------------------
  --Generates all the "plsql" to generate documetnation for all objects of
  --the passed in type for the passed in owner.
  -----------------------------------------------------------------------------    
  procedure generate_object_type(in_owner in varchar2, in_type in varchar2) is
    l_count           number := 0;
    
    l_objects         varchar2(2000);
  begin
    p('---------------------------------------');
    p('--'||in_owner||'.'||in_type);
    p('---------------------------------------');
    
    for rec in (select object_name
                  from all_objects
                 where upper(owner) = upper(in_owner)
                   and upper(object_type) = upper(in_type)
                   and rownum < MAX_OBJECTS
                   order by object_name) 
    loop
      l_objects := l_objects || upper(in_owner)||'.'||rec.object_name || ' ';
      l_count := l_count + 1;
      
      if(l_count >= OBJECTS_PER_LINE)then
        p('plugin plsqldoc generate ' || l_objects);
        l_count := 0;
        l_objects := '';
      end if;
    end loop;
    
    if(l_count > 0)then
      p('plugin plsqldoc generate ' || l_objects);
    end if;
    
  end;

  -----------------------------------------------------------------------------
  --Generates the code to generate the documentation of the types of objects 
  --owned by one owner that we would like to have generated.
  -----------------------------------------------------------------------------    
  procedure generate_owner(in_owner in varchar2)
  is
  begin
    generate_object_type(in_owner, 'table');
    generate_object_type(in_owner, 'package');
    generate_object_type(in_owner, 'procedure');
    generate_object_type(in_owner, 'function');
    generate_object_type(in_owner, 'view');
    generate_object_type(in_owner, 'materialized view');
  end;
  
begin

  
  p('plugin plsqldoc delete');  

  generate_owner('xuprommgr');  
  generate_object_type('xupersona', 'table');
  
  p('');
  p('plugin plsqldoc rebuild');  
  p('EXIT APPLICATION');
  p('/');
end;
/
exit;
