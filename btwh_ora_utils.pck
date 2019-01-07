create or replace package btwh_ora_utils is
    type t_vc2k_table is table of varchar2(2000) index by binary_integer;
    type t_tbl_num_vc2idx is table of number index by varchar2(200);
    --Container and table for a call to a method.  The fields
    --in this type refer to the caller of a method, not the 
    --method being called.
    type t_method_call is record(
        owner        varchar2(200),
        package_name varchar2(200),
        method_name  varchar2(200),
        line_num     number,
        code         varchar2(4000));
    type t_method_calls is table of t_method_call index by binary_integer;
    
    e_too_many_owners exception;
    PRAGMA EXCEPTION_INIT(e_too_many_owners, -2001);
    e_no_owners exception;
    PRAGMA EXCEPTION_INIT(e_no_owners, -2002);
    e_view exception;
    PRAGMA EXCEPTION_INIT(e_view, -2003);

    g_butch_sysdate_date            varchar2(30) := null;
    g_bsysdate_call_count           number := 0;

    -----------------------------------------------------------------------------
    --Returns true if the character sent in is a-z or A-Z, false if not.
    -----------------------------------------------------------------------------  
    function is_letter(in_char in character) return boolean;

    -----------------------------------------------------------------------------
    --Returns true if the character is a space, carriage return, line feed, or
    --tab, false if not.
    -----------------------------------------------------------------------------  
    function is_whitespace(in_char in character) return boolean;

    -----------------------------------------------------------------------------
    --Returns true if the character is 1-0, false if not.
    -----------------------------------------------------------------------------  
    function is_number(in_char in character) return boolean;

    -----------------------------------------------------------------------------
    --Returns true if the character can be used in a method (procedure or 
    --function) name, false if not.
    -----------------------------------------------------------------------------  
    function is_method_name_char(in_char in character) return boolean;

    -----------------------------------------------------------------------------
    --Strips out leading and trailing spaces, tabs and ALL carrage return/line
    --feeds.
    -----------------------------------------------------------------------------  
    function strip_stuff(in_strip in varchar2) return varchar2;

    -----------------------------------------------------------------------------
    --Strips the first word off of io_to_strip and returns it.  io_to_strip will
    --be null if there are no remaining words in it.
    -----------------------------------------------------------------------------  
    function strip_first_word(io_to_strip in out varchar2) return varchar2;

    -----------------------------------------------------------------------------
    --Prints out all the methods that call the passed in method.
    --
    --NOTE:
    --It is always best to double check the results printed, as there are some 
    --inaccuracies in determining the name of the methods that call the passed 
    --in method.
    -----------------------------------------------------------------------------  
    procedure print_calls_to(in_object in varchar2, in_method_name in varchar2 default null);

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    procedure set_print_to_file(in_directory in varchar2, in_file in varchar2, in_mode in varchar2 default 'W');

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    procedure close_print_file;

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    procedure set_print_screen;

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    procedure d_print(in_string in varchar2, in_depth in number default 0);
    procedure d_print_vc2idx_array(in_array in t_tbl_num_vc2idx, in_depth in number default 0);
    function increment_vc2idx_array(in_key in varchar2, io_array in out t_tbl_num_vc2idx) return number;    
    -----------------------------------------------------------------------------
    --Prints the names of columns that are not being used (contain only NULL values), 
    --and the number of unique values per column.  You can optionally provide
    --a patter to match the column name and an owner.  If the owner of the table
    --is not specified, it will try to figure it out itself.
    -----------------------------------------------------------------------------    
    procedure print_column_usage_info(in_table_name in varchar2, in_col_like in varchar2 default '%',
                                      in_owner in varchar2 default null, in_distinct_thresh in number default 0);

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    procedure print_sql_select_columns(in_table_name in varchar2, in_owner in varchar2 default null,
                                       in_prefix in varchar2 default null);

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    procedure dump_query_to_csv(in_query in varchar2, p_dir in varchar2, p_filename in varchar2);
    
    -----------------------------------------------------------------------------
    --Uses an array of context varaible names and a print flag that is associated
    --with each to return a string containing all sys_context values. 
    -----------------------------------------------------------------------------    
    function get_sys_context_values return varchar2;


    procedure search_tables_for_value(in_owner in varchar2, in_value in varchar2);    

    -----------------------------------------------------------------------------    
    --Prints a query and a count for all tables that contain the specified pidm.  
    --Excludes all views and any columns named "pidm" but are not number fields.
    -----------------------------------------------------------------------------   
    function get_tbls_with_cols_like_query(in_cols in t_vc2k_table)return varchar2;    
    function get_tbls_with_cols_like_query(in_cols in t_vc2k_table, in_vals in t_vc2k_table)return varchar2;    
    function get_tbls_with_col_like_query(in_col in varchar2, in_val in varchar2 default null)return varchar2;
    
    procedure trace(in_msg in varchar2);    
    procedure clear_trace;
    procedure print_trace;
    function split(in_to_split in varchar2, in_delim in varchar2)return t_vc2k_table;
    function split(in_string in varchar2, in_size in number)return t_vc2k_table;    
    function get_calls_to(in_owner in varchar2, in_object in varchar2, in_type in varchar2, in_method_name in varchar2) return t_method_calls;    
    ---------------------------------------------------------------------------    
    ---------------------------------------------------------------------------    
    procedure export_views_to_files(in_owner in varchar2, in_directory in varchar2);
    
end btwh_ora_utils;
/
create or replace package body btwh_ora_utils is

    --set to -1 initially.  if not -1 then this package
    --won't look for the table again.
    g_butch_trace_table_count       number := -1;

    --character constants.
    CR  constant varchar2(1) := chr(13);
    TAB constant varchar2(1) := chr(9);
    LF  constant varchar2(1) := chr(10);

    C_PRINT_SCREEN constant varchar2(1) := 'S';
    C_PRINT_FILE   constant varchar2(1) := 'F';

    C_DASH_SEP    constant varchar2(80) := rpad('-', 80, '-');
    C_DASH_SEP_SM constant varchar2(30) := rpad('-', 30, '-');


    v_where_to_print varchar2(1) := C_PRINT_SCREEN;
    v_print_dir      varchar2(1000);
    v_print_file     varchar2(1000);
    v_file_handle    utl_file.file_type;

    type t_context_var is record(
        var_name     varchar2(200),
        should_print boolean);
    type t_context_vars is table of t_context_var index by binary_integer;
    g_context_vars t_context_vars;
    --All the fields from all_source for lines containing
    --in_to_search.  The search is case insensative.
    cursor c_code_containing(in_search_object in varchar2, in_object_owner in varchar2, in_type in varchar2, in_to_search_for in varchar2) is
        select *
          from all_source
         where owner = in_object_owner
           and name = in_search_object
           and lower(text) like ('%' || lower(in_to_search_for) || '%')
           and type = in_type
         order by owner, name;
    type t_code_containing is table of all_source%rowtype;

    --All fields from all_source for the lines of a package up to and
    --including in_line_num.
    cursor c_pkg_source_to_line(in_pkg_name in varchar2, in_line_num in number) is
        select *
          from all_source
         where name = upper(in_pkg_name)
           and type = 'PACKAGE BODY'
           and line <= in_line_num;




------------------
--PRIVATE
------------------

    -----------------------------------------------------------------------------  
    -----------------------------------------------------------------------------      
    procedure print_style is
    begin
        htp.print('
<style>
  table, th, td{
    border: 1px solid black;
  }
  table{
    border-collapse:collapse;
  }
  td{
    padding:10px
  }
  th{
    background-color:black;
    font-size:18px ;
    color:white;
  }
</style>');
    end;

    -----------------------------------------------------------------------------  
    -----------------------------------------------------------------------------      
    procedure fill_context_vars is
        procedure add_var(in_name in varchar2, in_print in boolean)
        
         is
        begin
            g_context_vars(g_context_vars.count).var_name := in_name;
            g_context_vars(g_context_vars.count - 1).should_print := in_print;
        end;
    begin
        add_var('ACTION', true);
        add_var('AUTHENTICATED_IDENTITY', true);
        add_var('AUTHENTICATION_DATA', true);
        add_var('AUTHENTICATION_METHOD', false); --something weird about this one, makes concat not happen, might be an unprintable in there messing things up
        add_var('BG_JOB_ID', true);
        add_var('CLIENT_IDENTIFIER', true);
        add_var('CLIENT_INFO', true);
        add_var('CURRENT_BIND', true);
        add_var('CURRENT_SCHEMAID', true);
        add_var('CURRENT_SCHEMA', true);
        add_var('CURRENT_SQL', true);
        add_var('CURRENT_SQL_LENGTH', true);
        add_var('DB_DOMAIN', true);
        add_var('DB_NAME', true);
        add_var('DB_UNIQUE_NAME', true);
        add_var('ENTRYID', true);
        add_var('ENTERPRISE_IDENTITY', true);
        add_var('FG_JOB_ID', true);
        add_var('GLOBAL_CONTEXT_MEMORY', true);
        add_var('GLOBAL_UID', true);
        add_var('HOST', true);
        add_var('IDENTIFICATION_TYPE', false); --same as authentication method
        add_var('INSTANCE', true);
        add_var('INSTANCE_NAME', true);
        add_var('IP_ADDRESS', true);
        add_var('ISDBA', true);
        add_var('LANG', true);
        add_var('LANGUAGE', true);
        add_var('MODULE', true);
        add_var('NETWORK_PROTOCOL', true);
        add_var('NLS_CALENDAR', true);
        add_var('NLS_CURRENCY', true);
        add_var('NLS_DATE_FORMAT', true);
        add_var('NLS_DATE_LANGUAGE', true);
        add_var('NLS_SORT', true);
        add_var('NLS_TERRITORY', true);
        add_var('OS_USER', true);
        add_var('POLICY_INVOKER', true);
        add_var('PROXY_ENTERPRISE_IDENTITY', true);
        add_var('PROXY_GLOBAL_UID', false); --invalid
        add_var('PROXY_USER', true);
        add_var('PROXY_USERID', true);
        add_var('SERVER_HOST', true);
        add_var('SERVICE_NAME', true);
        add_var('SESSION_USER', true);
        add_var('SESSION_USERID', true);
        add_var('SESSIONID', true);
        add_var('SID', true);
        add_var('STATEMENTID', true);
        add_var('TERMINAL', true);
    end;

    -----------------------------------------------------------------------------
    --Depth Print
    --Used to print out indented output.  in_depth will determine how far 
    --indented the output will be.
    -----------------------------------------------------------------------------  
    procedure d_print(in_string in varchar2, in_depth in number default 0) is
        l_to_print varchar2(32000);
    begin
        l_to_print := lpad(in_string, length(in_string) + (in_depth * 2), ' ');
        if (v_where_to_print = C_PRINT_SCREEN) then
            dbms_output.put_line(l_to_print);
        elsif (v_where_to_print = C_PRINT_FILE) then
            utl_file.put_line(v_file_handle, l_to_print);
        end if;
    end;

    -----------------------------------------------------------------------------  
    -----------------------------------------------------------------------------      
    procedure d_print_vc2idx_array(in_array in t_tbl_num_vc2idx, in_depth in number default 0) is
        l_key varchar2(2000);
    begin
        l_key := in_array.first;
        while (l_key is not null) loop
            d_print(l_key || ' - ' || in_array(l_key), in_depth);
            l_key := in_array.next(l_key);
        end loop;
    end;

    -----------------------------------------------------------------------------
    --Increments the value of a t_tbl_num_vc2idx array for a given key
    --and returns the new value for the key.  If the key does not exist it 
    --is added to the array.
    -----------------------------------------------------------------------------  
    function increment_vc2idx_array(in_key in varchar2, io_array in out t_tbl_num_vc2idx) return number is
    begin
        if (io_array.exists(in_key)) then
            io_array(in_key) := io_array(in_key) + 1;
        else
            io_array(in_key) := 1;
        end if;
        return io_array(in_key);
    end;

    -----------------------------------------------------------------------------
    --Gets the first word in in_text but the parsing stops not only on whitespace
    --but also any character that is not valid in a function or procedure name.
    -----------------------------------------------------------------------------  
    function get_first_word_as_method_name(in_text in varchar2) return varchar2 is
        l_found  boolean := false;
        i        number := 0;
        l_return varchar2(2000);
    begin
        l_return := strip_stuff(in_text);
        while (i <= length(in_text) and not l_found) loop
            if (not is_method_name_char(substr(l_return, i, 1))) then
                l_found := true;
            else
                i := i + 1;
            end if;
        end loop;
    
        l_return := substr(l_return, 1, i - 1);
    
        return l_return;
    end;

    -----------------------------------------------------------------------------
    --Starts at the passed in line number in the source and searches backward
    --for a procedure or function decleration line.  It parses out the name of 
    --the procedure or fuction and returns it.
    --
    --Due to this approach, if in_line_num is in the middle of a block comment
    --it could mess with the validity of the output.
    --
    --Currently this does not take into account block comments and tries it's 
    --best to take into account single line comments, but there are some edge
    --cases that could mess with the validity of the output as well.
    --
    --That said, it works pretty well.  If you don't enjoy the results, you can
    --make a full blown plsql parser to improve this...hah.
    -----------------------------------------------------------------------------  
    function get_method_containing_line(in_pkg_name in varchar2, in_line_num in number) return varchar2 is
        l_code   t_code_containing;
        l_found  boolean := false;
        i        number;
        l_return varchar2(100);
    
        l_method_found boolean;
        l_word         varchar2(4000);
        l_line         varchar2(4000);
        l_in_comment   boolean;
        --l_in_block_comment  boolean;  --not implemented yet
    begin
        open c_pkg_source_to_line(in_pkg_name, in_line_num);
        fetch c_pkg_source_to_line bulk collect
            into l_code;
        close c_pkg_source_to_line;
    
        l_found  := false;
        i        := in_line_num;
        l_return := 'NOT_FOUND';
        --l_in_block_comment := false;
    
        while i > 0 and not l_found and l_code.count > 0 loop
            l_method_found := false;
            l_in_comment   := false;
            l_line         := l_code(i).text;
        
            --parse each line of code word by word until we find a method decleration
            --or it reaches the beginning of the file.
            while (length(l_line) > 0 and not l_method_found and not l_in_comment) loop
                l_word := strip_first_word(l_line);
                --Checks for starting comment.  Not fool proof by any means, but since 
                --it is parsing by word, this should catch 90% of the cases where
                --a comment can give false results
                if (substr(l_word, 1, 2) = '--') then
                    l_in_comment := true;
                end if;
            
                if (lower(l_word) = 'procedure' or lower(l_word) = 'function') then
                    l_method_found := true;
                end if;
            end loop;
        
            --'procedure' or 'function' was found, so the next word should
            --be the name of the procedure.
            if (l_method_found) then
                l_found  := true;
                l_return := get_first_word_as_method_name(l_line);
            else
                i := i - 1;
            end if;
        
        end loop;
    
        return l_return;
    end;

    -----------------------------------------------------------------------------
    --Searches the source for the passed in method name.  This then populates
    --a t_method_calls array with all the lines of code found in the search.
    --The accuracy of the calling method name in the results can be incorrect
    --due to the quirks of get_method_containing_line.
    -----------------------------------------------------------------------------  
    function get_calls_to(in_owner in varchar2, in_object in varchar2, in_type in varchar2, in_method_name in varchar2) return t_method_calls is
        l_loc         t_code_containing;    
        l_comment_loc number;
        l_proc_loc    number;
        l_func_loc    number;
        l_end_loc     number;
    
        l_return t_method_calls;
    begin
        open c_code_containing(in_object, in_owner, in_type, in_method_name);
        fetch c_code_containing bulk collect
            into l_loc;
        close c_code_containing;
            
        for i in 1 .. l_loc.count loop
            l_comment_loc := instr(l_loc(i).text, '--');
            l_proc_loc    := instr(lower(l_loc(i).text), 'procedure');
            l_func_loc    := instr(lower(l_loc(i).text), 'function');
            l_end_loc     := instr(lower(l_loc(i).text), 'end');
            
            --ignore decleration lines and end of method lines.
            if (l_proc_loc = 0 and l_func_loc = 0 and l_end_loc = 0) then
                --ignore lines that start with a comment.
                if (l_comment_loc = 0 or l_comment_loc > instr(lower(l_loc(i).text), lower(in_method_name))) then
                    l_return(l_return.count + 1).owner := l_loc(i).owner;
                    l_return(l_return.count).package_name := l_loc(i).name;
                    l_return(l_return.count).method_name := get_method_containing_line(l_loc(i).name, l_loc(i).line);
                    l_return(l_return.count).line_num := l_loc(i).line;
                    l_return(l_return.count).code := l_loc(i).text;
                end if;
            end if;
        end loop;
        return l_return;
    end;

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    function get_table_owner(in_table_name in varchar2) return varchar2 is
        l_return     varchar2(200);
        l_table_name varchar2(200) := upper(in_table_name);
        l_count      number;
    begin
        select owner into l_return from all_tables where table_name = l_table_name;
    
        return l_return;
    exception
        when too_many_rows then
            raise e_too_many_owners;
        when no_data_found then
            select count(1) into l_count from all_views where view_name = l_table_name;
            if (l_count = 0) then
                raise e_no_owners;
            else
                raise e_view;
            end if;
    end;
    
 

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    function get_sys_context_values return varchar2 is
        l_return varchar2(4000);
        l_value  varchar2(2000);
    begin
        if (g_context_vars.count = 0) then
            fill_context_vars;
        end if;
    
        for i in 0 .. g_context_vars.count - 1 loop
            if (g_context_vars(i).should_print) then
                begin
                    l_value := SYS_CONTEXT('USERENV', g_context_vars(i).var_name);
                exception
                    when others then
                        l_value := '<INVALID USERENV>';
                end;
                l_return := l_return || rpad(g_context_vars(i).var_name, 27, ' ') || ' = ' || nvl(l_value, '<NULL>') ||
                            chr(10);
            end if;
        end loop;
    
        return l_return;
    end;

------------------
--PUBLIC GENERAL
------------------    
    -----------------------------------------------------------------------------
    --See spec
    -----------------------------------------------------------------------------  
    function is_letter(in_char in character) return boolean is
        ascii_val number;
    begin
        ascii_val := ascii(in_char);
        return(ascii_val >= 65 and ascii_val <= 90) or(ascii_val >= 97 and ascii_val <= 122);
    end;

    -----------------------------------------------------------------------------
    --See spec
    -----------------------------------------------------------------------------  
    function is_whitespace(in_char in character) return boolean is
    begin
        return in_char in(' ', CR, LF, TAB);
    end;

    -----------------------------------------------------------------------------
    --See spec
    -----------------------------------------------------------------------------  
    function is_number(in_char in character) return boolean is
        ascii_val number;
    begin
        ascii_val := ascii(in_char);
        return(ascii_val >= 48 and ascii_val <= 57);
    
    end;

    -----------------------------------------------------------------------------
    --See spec
    -----------------------------------------------------------------------------  
    function is_method_name_char(in_char in character) return boolean is
        l_return boolean := false;
    begin
        l_return := is_letter(in_char);
        if (not l_return) then
            l_return := is_number(in_char);
        end if;
        if (not l_return) then
            l_return := in_char in ('_');
        end if;
    
        return l_return;
    end;

    -----------------------------------------------------------------------------
    --See spec
    -----------------------------------------------------------------------------  
    function strip_stuff(in_strip in varchar2) return varchar2 is
        l_return varchar2(2000);
    begin
        l_return := replace(in_strip, CR, '');
        l_return := replace(l_return, chr(10), '');
        l_return := ltrim(rtrim(l_return));
        l_return := ltrim(l_return, chr(9));
        l_return := rtrim(l_return, chr(9));
        return l_return;
    end;

    -----------------------------------------------------------------------------
    --See spec
    -----------------------------------------------------------------------------  
    function strip_first_word(io_to_strip in out varchar2) return varchar2 is
        l_found  boolean := false;
        i        number := 0;
        l_return varchar2(2000);
    begin
        io_to_strip := strip_stuff(io_to_strip);
        while (i <= length(io_to_strip) and not l_found) loop
            if (is_whitespace(substr(io_to_strip, i, 1))) then
                l_found := true;
            else
                i := i + 1;
            end if;
        end loop;
    
        l_return    := substr(io_to_strip, 1, i - 1);
        io_to_strip := substr(io_to_strip, i);
    
        return l_return;
    end;

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    procedure set_print_to_file(in_directory in varchar2, in_file in varchar2, in_mode in varchar2 default 'W') is
    begin
        v_where_to_print := C_PRINT_FILE;
        v_print_dir      := in_directory;
        v_print_file     := in_file;
        v_file_handle    := utl_file.fopen(in_directory, in_file, in_mode);
    end;

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    procedure close_print_file is
    begin
        utl_file.fclose(v_file_handle);
        v_where_to_print := C_PRINT_SCREEN;
    end;

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    procedure set_print_screen is
    begin
        v_where_to_print := C_PRINT_SCREEN;
    end;

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    procedure dump_query_to_csv(in_query in varchar2, p_dir in varchar2, p_filename in varchar2) is
        l_output      utl_file.file_type;
        l_the_cursor   integer default dbms_sql.open_cursor;
        l_column_value varchar2(4000);
        l_status      integer;
        l_col_count      number := 0;
        l_separator   varchar2(1);
        l_desc_table     dbms_sql.desc_tab;
    begin
        l_output := utl_file.fopen(p_dir, p_filename, 'w');
        execute immediate 'alter session set nls_date_format=''dd-mon-yyyy hh24:mi:ss''';
    
        dbms_sql.parse(l_the_cursor, in_query, dbms_sql.native);
        dbms_sql.describe_columns(l_the_cursor, l_col_count, l_desc_table);
    
        for i in 1 .. l_col_count loop
            utl_file.put(l_output, l_separator || '"' || l_desc_table(i).col_name || '"');
            dbms_sql.define_column(l_the_cursor, i, l_column_value, 4000);
            l_separator := ',';
        end loop;
        utl_file.new_line(l_output);
        
        --status is never used by have to assign function to something.
        l_status := dbms_sql.execute(l_the_cursor);
    
        while (dbms_sql.fetch_rows(l_the_cursor) > 0) loop
            l_separator := '';
            for i in 1 .. l_col_count loop
                dbms_sql.column_value(l_the_cursor, i, l_column_value);
                utl_file.put(l_output, l_separator || l_column_value);
                l_separator := ',';
            end loop;
            utl_file.new_line(l_output);
        end loop;
        dbms_sql.close_cursor(l_the_cursor);
        utl_file.fclose(l_output);    

        execute immediate 'alter session set nls_date_format=''dd-MON-yy'' ';
    exception
        when others then
            execute immediate 'alter session set nls_date_format=''dd-MON-yy'' ';
            raise;
    end;    

---------------------
--Schema related
---------------------

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    procedure print_column_usage_info(in_table_name in varchar2, in_col_like in varchar2 default '%',
                                      in_owner in varchar2 default null, in_distinct_thresh in number default 0) is
        type t_column is record(
            column_name     varchar2(100),
            data_type       varchar2(100),
            distinct_values number,
            comment         varchar2(2000));
        type t_columns is table of t_column index by binary_integer;
        type t_vc2 is table of varchar2(100) index by binary_integer;
        type t_num is table of number index by binary_integer;
    
        l_count           number;
        l_sql             varchar2(1000);
        l_owner           all_tables.owner%type;
        l_table_name      varchar2(100);
        l_col_like        varchar2(100);
        l_distinct_values t_vc2;
        l_distinct_count  t_num;
        l_row_count       number;
    
        l_unused_cols t_columns;
        l_bad_dt_cols t_columns;
        l_used_cols   t_columns;
    
        cursor c_cols(in_cur_table_name in varchar2, in_cur_owner in varchar2, in_cur_col_like in varchar2) is
            select com.column_name, data_type, comments
              from all_tab_cols 
              left join all_col_comments com
                on com.table_name = all_tab_cols.table_name
               and com.column_name = all_tab_cols.column_name
               and com.owner = all_tab_cols.owner
             where all_tab_cols.table_name = upper(in_cur_table_name)
               and all_tab_cols.owner = upper(in_cur_owner)
               and all_tab_cols.column_name like (in_cur_col_like)
             order by column_name;
    
        -------------------------------------------------------------------
        --Adds an entry into a t_columns variable.           
        -------------------------------------------------------------------    
        procedure add_column(in_add_col in varchar2, in_add_datatype in varchar2, in_add_count in number, in_add_comment in varchar2,
                             io_to_this in out t_columns) is
        begin
            io_to_this(io_to_this.count + 1).column_name := in_add_col;
            io_to_this(io_to_this.count).data_type := in_add_datatype;
            io_to_this(io_to_this.count).distinct_values := in_add_count;
            io_to_this(io_to_this.count).comment := in_add_comment;
        end;
    
    begin
        l_col_like   := upper(in_col_like);
        l_table_name := upper(in_table_name);
    
        --Determine the owner of the table if 
        --not supplied.
        if (in_owner is null) then
            l_owner := get_table_owner(l_table_name);
        else
            l_owner := in_owner;
        end if;
    
        l_table_name := l_owner || '.' || l_table_name;
    
        --Pri nt Heading
        d_print(C_DASH_SEP);
        d_print('Getting usage for ' || l_table_name || ' with columns like ' || l_col_like || '.');
    
        --get the number of columns found
        l_sql := 'select count(1) from all_tab_cols ' || 'where table_name = :tbl ' ||
                 'and owner = :own and column_name like (:col_like) ';
        execute immediate l_sql
            into l_count
            using upper(in_table_name), l_owner, l_col_like;
        d_print('Found ' || l_count || ' matching columns in ' || l_table_name || '.');
    
        execute immediate 'select count(1) from ' || l_table_name
            into l_row_count;
        d_print(l_row_count || ' rows in table.');
    
        if (in_distinct_thresh <> 0) then
            d_print('Retrieviing values for columns with ' || in_distinct_thresh ||
                    ' or less distinct values.  < 0 = All');
        end if;
        d_print(C_DASH_SEP || CR);
    
        --Get all information about the columns and sort into proper array
        for rec in c_cols(in_table_name, l_owner, l_col_like) loop
            --avoid trying to count uncountable datatypes
            if (rec.data_type not in ('BLOB', 'CLOB', 'RAW')) then
                l_sql := 'select count(1) from ' || l_table_name || ' where ' || rec.column_name || ' is not null ';
                execute immediate l_sql
                    into l_count;
            
                if (l_count = 0) then
                    --0 non null columns found
                    add_column(rec.column_name, rec.data_type, 0, rec.comments, l_unused_cols);
                else
                    --get distinct values for columns that are not all NULL values.
                    l_sql := 'select count(1) from (select unique ' || rec.column_name || ' from ' || l_table_name || ')';
                    execute immediate l_sql
                        into l_count;
                    add_column(rec.column_name, rec.data_type, l_count, rec.comments, l_used_cols);
                end if;
            else
                --uncountable data types.
                add_column(rec.column_name, rec.data_type, 0, rec.comments, l_bad_dt_cols);
            end if;
        end loop;
    
        --Print unused columns
        if (l_unused_cols.count > 0) then
            d_print(l_table_name || ' Unused Columns (All NULL values)', 1);
            d_print(C_DASH_SEP_SM, 1);
            for i in 1 .. l_unused_cols.count loop
                d_print(rpad(l_unused_cols(i).column_name, 50)||l_unused_cols(i).comment, 3);
            end loop;
        else
            d_print('0 Unused columns', 1);
        end if;
    
        d_print('');
        --Print used columns
        if (l_used_cols.count > 0) then
            d_print(l_table_name || ' Unique Values Counts', 1);
            d_print(C_DASH_SEP_SM, 1);
            for i in 1 .. l_used_cols.count loop
                d_print(rpad(l_used_cols(i).column_name, 40) || rpad(l_used_cols(i).distinct_values, 10) || l_used_cols(i).comment, 3);
                --If the number of distinct values for the column is less or equal to the
                --threshold then get the distinct values and print them out.
                if (l_used_cols(i).distinct_values <= in_distinct_thresh or in_distinct_thresh < 0) then
                    l_sql := 'select nvl(substr(to_char(distinct_val),1, 50), ''NULL''), cnt from ' ||
                             '(select distinct ' || l_used_cols(i).column_name || ' as distinct_val, count(1) as cnt ' ||
                             'from ' || l_table_name || ' group by ' || l_used_cols(i).column_name ||
                             ') order by cnt desc';
                    execute immediate l_sql bulk collect
                        into l_distinct_values, l_distinct_count;
                
                    for j in 1 .. l_distinct_values.count loop
                        d_print(rpad(rpad(j || '.', 5) || l_distinct_values(j), 51) || lpad(l_distinct_count(j), 10) ||
                                to_char(((l_distinct_count(j) / l_row_count) * 100), '9999.9999') || '%', 5);
                    end loop;
                end if;
            end loop;
        else
            d_print('0 Used columns', 1);
        end if;
    
        --Print uncountable data type columns
        if (l_bad_dt_cols.count > 0) then
            d_print('');
            d_print(l_table_name || ' Uncountable Data Type Columns', 1);
            d_print(C_DASH_SEP_SM, 1);
            for i in 1 .. l_bad_dt_cols.count loop
                d_print(rpad(l_bad_dt_cols(i).column_name, 40) || rpad(l_bad_dt_cols(i).data_type, 10) || l_bad_dt_cols(i).comment, 3);
            end loop;
        end if;
    
    exception
        when e_too_many_owners then
            raise_application_error(-20999, l_table_name || ' has too many owners.  You must specifiy one.');
        when e_no_owners then
            raise_application_error(-20999, l_table_name || ' does not have an owner.  You probably misspelled it.');
        when e_view then
            raise_application_error(-20999, l_table_name || ' is a view, I cannot process views.');
    end;

    -----------------------------------------------------------------------------
    -----------------------------------------------------------------------------    
    procedure print_sql_select_columns(in_table_name in varchar2, in_owner in varchar2 default null,
                                       in_prefix in varchar2 default null) is
        l_sql        varchar2(10000);
        l_table_name varchar2(200) := upper(in_table_name);
        l_owner      varchar2(200);
        l_prefix     varchar2(100);
        l_cols       t_vc2k_table;
    begin
        if (in_owner is null) then
            l_owner := get_table_owner(l_table_name);
        else
            l_owner := upper(in_owner);
        end if;
    
        if (in_prefix is not null) then
            l_prefix := in_prefix || '.';
        end if;
    
        select column_name bulk collect
          into l_cols
          from all_tab_cols
         where table_name = l_table_name
           and owner = l_owner
           order by column_name;
    
        for i in 1 .. l_cols.count loop
            l_sql := l_sql || l_prefix || l_cols(i);
            if (i <> l_cols.count) then
                l_sql := l_sql || ', ';
            end if;
        end loop;
    
        l_sql := 'SELECT ' || l_sql || ' FROM ' || l_owner || '.' || l_table_name || ' ' || in_prefix;
    
        d_print(l_sql);
    end;

    -----------------------------------------------------------------------------
    --See spec
    -----------------------------------------------------------------------------  
    procedure print_calls_to(in_object in varchar2, in_method_name in varchar2 default null) is

        
        l_to_print          t_method_calls;
        l_last_method       varchar2(100) := 'asdf';

        l_to_search_for     varchar2(100);
    begin
        if(in_method_name is null)then
            l_to_search_for := in_object;
        else
            l_to_search_for := in_object||'.'||in_method_name;
        end if;
        
        for depend_rec in (select * 
                             from all_dependencies 
                            where referenced_name = upper(in_object) 
                            order by owner, type, name) 
        loop
            l_to_print := get_calls_to(depend_rec.owner, depend_rec.name, depend_rec.type, l_to_search_for);
            d_print('Searching '||depend_rec.owner||'.'||depend_rec.name||'('||depend_rec.type||')');
                                            
            for i in 1 .. l_to_print.count loop                    
                if(l_to_print(i).method_name <> l_last_method)then
                    d_print(l_to_print(i).method_name, 2);
                    l_last_method := l_to_print(i).method_name;
                end if;
                d_print(rpad('(' || l_to_print(i).line_num || ')', 12, ' ') || strip_stuff(l_to_print(i).code), 3);
            end loop;

            dbms_output.put_line('');
        end loop;
                
        if (v_where_to_print = C_PRINT_FILE) then
            utl_file.fflush(v_file_handle);
        end if;
    end;
    
    procedure search_tables_for_value(in_owner in varchar2, in_value in varchar2)
    is
        l_table             varchar2(100);
        l_sql               varchar2(10000);
        l_match_col_count   number := 0;
        l_count             number;
    begin
        for tables in (select * from all_tables where owner = in_owner) loop
            l_table := tables.owner||'.'||tables.table_name;
            
            l_sql := 'Select count(1) from '||l_table||' where ';            
            l_match_col_count := 0;
            for cols in (select * from all_tab_cols where table_name = tables.table_name and data_type = 'VARCHAR2') loop
                if(l_match_col_count > 0) then
                    l_sql := l_sql ||' or ';
                end if;
                l_sql := l_sql ||' lower('||cols.column_name||') like ''%'||lower(in_value)||'%'' ';
                l_match_col_count := l_match_col_count + 1;
            end loop;
            
            if(l_match_col_count > 0)then
                execute immediate l_sql into l_count;
                if(l_count > 0)then
                    dbms_output.put_line('--'||l_table);
                    dbms_output.put_line(l_sql||';');
                end if;
            end if;
        end loop;
    end;
    
    
    -----------------------------------------------------------------------------
    --This was a crazy idea, but it is working.  It finds all tables that have columns
    --that match wildcard patters passed in through in_cols.  You can also 
    --optionally (through the overload) specify values for these columns.  The
    --query that is returned will list all the tables that match and have at least
    --one row in them.  It will also list a query that will get all values out of
    --that table that match the values you supplied.
    --
    --This essentially writes a dynamically generated query that includes a dynamcally 
    --generated query in its results.
    -----------------------------------------------------------------------------    
    function get_tbls_with_cols_like_query(in_cols in t_vc2k_table, in_vals in t_vc2k_table)return varchar2    
    is
        l_sql_select     varchar2(5000) := '';
        l_sql_from       varchar2(5000) := '';
        l_sql_where      varchar2(5000) := '';
        l_sql_orderby    varchar2(500) := '';
        l_tab_prefix    varchar2(2);
        
        function get_val_for_index(in_index in number)return varchar2
        is
            l_return        varchar2(200) := '';
        begin

            if(in_index > in_vals.count)then
                l_return := '';
            elsif(in_vals(in_index) is not null)then
                if(in_index > 1)then
                    l_return := '||'' and ''';
                else
                    l_return := '||'' where ''';
                end if;
                
                l_tab_prefix := chr(64 + in_index);     
                l_return := l_return || ' || ' || l_tab_prefix || '.column_name ||'' like('''''||replace(in_vals(in_index), '''', '''''')||''''')'' ';
        
            end if;
            
            return l_return;
        end;
    begin    
        l_sql_select := 'select A.owner, A.table_name, A.column_name ';
        l_sql_from := ' from all_tab_cols A';       
        l_sql_where := ' where ';
        l_sql_orderby := ' order by A.owner, A.table_name, A.column_name;';
        
        for i in 1..in_cols.count loop
            --A = 65
            l_tab_prefix := chr(64 + i);
            
            if(i > 1)then
                l_sql_select := l_sql_select || ', '||l_tab_prefix||'.column_name';
                l_sql_from := l_sql_from || ' left join all_tab_cols '||l_tab_prefix||
                                              ' on A.table_name = '||l_tab_prefix||'.table_name '||
                                             ' and A.owner = '||l_tab_prefix||'.owner';
                l_sql_where := l_sql_where || ' and ';
            end if;
            
            l_sql_where := l_sql_where || l_tab_prefix ||'.column_name like('''||upper(in_cols(i))||''')';
        end loop;

        ------------------------
        --Add dynamic SQL select statement generated by this
        --dynamic SQL.
        l_sql_select := l_sql_select ||', ''select ''''''||A.owner||''_''||A.table_name ||'''''' table_name, tbl.* from ''||A.owner||''.''||A.table_name||'' tbl ''';
        for i in 1..in_cols.count loop
            l_sql_select := l_sql_select || get_val_for_index(i);
        end loop;
        l_sql_select := l_sql_select || '||''; --''||comm.comments';
        --------------
                
        l_sql_from := l_sql_from || ' left join all_tab_comments comm '||
                                       'on comm.owner = A.owner and '||
                                          'comm.table_name = A.table_name and '||
                                          'comments is not null ';
        l_sql_where := l_sql_where || ' and not exists (select ''a'' from all_views where view_name = A.table_name) ';

                                     
        ------------------------------------------------------
        --Exclude tables that don't have any results.
        declare            
            l_other_sql     varchar2(4000);
            l_tables        t_vc2k_table;
            l_more_sql      varchar2(4000);
            l_count         number;        
        begin
            l_other_sql := 'select distinct A.owner||''.''||A.table_name '||l_sql_from||l_sql_where;
            execute immediate (l_other_sql) bulk collect into l_tables;
            
            for i in 1..l_tables.count loop
                begin
                    execute immediate('select count(1) from '||l_tables(i)||' where rownum = 1') into l_count;            
                    if(l_count = 0)then
                        if(l_more_sql is null)then
                            l_more_sql := ' and A.owner||''.''||A.table_name not in ('''||l_tables(i)||'''';
                         else
                            l_more_sql := l_more_sql ||', '''||l_tables(i)||'''';
                         end if;
                     end if;
                exception
                    when others then
                        null;
                end;
            end loop;
            
            if(l_more_sql is not null)then
                l_more_sql := l_more_sql ||')';
            end if;
            l_sql_where := l_sql_where || ' ' ||l_more_sql;
        end;
        
        return l_sql_select||l_sql_from||l_sql_where||l_sql_orderby;    
    end;       
    
    -----------------------------------------------------------------------------    
    -----------------------------------------------------------------------------               
    function get_tbls_with_cols_like_query(in_cols in t_vc2k_table)return varchar2
    is
        l_vals      t_vc2k_table;
    begin
        return get_tbls_with_cols_like_query(in_cols, l_vals);
    end;    
    
    -----------------------------------------------------------------------------    
    -----------------------------------------------------------------------------               
    function get_tbls_with_col_like_query(in_col in varchar2, in_val in varchar2 default null)return varchar2    
    is
        l_cols      t_vc2k_table;
        l_vals      t_vc2k_table;        
    begin
        l_cols(1) := in_col;
        if(in_val is not null)then
            l_vals(1) := in_val;
        end if;
        return get_tbls_with_cols_like_query(l_cols, l_vals);
    end;
    
----------------------
--Tracing/Logging
----------------------      
    procedure create_log_table_if_dne
    is

    begin
        --if we haven't checked to see if the table exists yet then
        --check, and if it doesn't exist then make it.
        if(g_butch_trace_table_count = -1)then
            select count(1)
              into g_butch_trace_table_count
              from all_tables 
             where table_name = 'BUTCH_TRACE';
                 
            if(g_butch_trace_table_count = 0)then
                execute immediate ('#');
            end if;
        end if;        
    end;
    
    ---------------------------------------------------------------------------
    ---------------------------------------------------------------------------    
    procedure trace(in_msg in varchar2)
    is
        PRAGMA AUTONOMOUS_TRANSACTION;    
    begin
        create_log_table_if_dne;
        execute immediate('insert into butch_trace(msg, insert_date) values(:msg, sysdate)')using in_msg;
        commit;
    end;
    
    ---------------------------------------------------------------------------
    ---------------------------------------------------------------------------    
    procedure print_trace
    is
    begin
        null;
        /*
        for rec in (select * from butch_trace order by insert_date) loop
            dbms_output.put_line(to_char(rec.insert_date, 'mmddyy hh:mi:ss')||'  '||rec.msg);
        end loop;
        */
    end;
    
    ---------------------------------------------------------------------------
    ---------------------------------------------------------------------------    
    procedure clear_trace
    is
        PRAGMA AUTONOMOUS_TRANSACTION;    
    begin
        
        execute immediate('delete from butch_trace');
        commit;
    end;

    ---------------------------------------------------------------------------
    ---------------------------------------------------------------------------    
    function split(in_to_split in varchar2, in_delim in varchar2)return t_vc2k_table
    is
        l_return        t_vc2k_table;
        l_loc           number := 0;
        l_split         varchar2(2000);
    begin
        l_split := in_to_split;
        
        l_loc := instr(l_split, in_delim, l_loc + 1);
        while(l_loc <> 0)loop
            l_return(l_return.count + 1) := substr(l_split, 1, l_loc -1);
            l_split := substr(l_split, l_loc + 1);
            l_loc := instr(l_split, in_delim, 1);            
        end loop;
        l_return(l_return.count + 1) := l_split;
        return l_return;
    end;
    
    ---------------------------------------------------------------------------
    ---------------------------------------------------------------------------    
    function split(in_string in varchar2, in_size in number)return t_vc2k_table
    is
        l_return        t_vc2k_table;
        l_start         number := 1;
    begin
        while(l_start < length(in_string))loop
            l_return(l_return.count + 1) := substr(in_string, l_start, in_size);
            l_start := l_start + in_size;
        end loop;
        
        return l_return;
    end;
    

    ---------------------------------------------------------------------------
    ---------------------------------------------------------------------------    
    procedure export_view_code_to_file(in_handle in utl_file.file_type, in_view_name in varchar2, in_text in long)
    is
    begin
        utl_file.put_line(in_handle, 'create or replace view '||in_view_name||' as');
        utl_file.put(in_handle, in_text);
        utl_file.put(in_handle, ';');        
    end;
    
    ---------------------------------------------------------------------------
    ---------------------------------------------------------------------------    
    procedure export_view_comments_to_file(in_handle in utl_file.file_type, in_view_name in varchar2)
    is
        l_comment       varchar2(4000);
    begin
        begin
            select comments 
              into l_comment
              from all_tab_comments 
             where table_name = in_view_name;
             
             utl_file.put_line(in_handle, 'comment on table '||in_view_name||' is '''||l_comment||''';');
        exception
            when no_data_found then
                null;
        end;

        for rec in (select * from all_col_comments where table_name = in_view_name)loop
            utl_file.put_line(in_handle, 'comment on column '||rec.table_name||'.'||rec.column_name||' is ''' ||
                                         rec.comments||''';');
        end loop;
    end;
    
    ---------------------------------------------------------------------------    
    ---------------------------------------------------------------------------    
    procedure export_views_to_files(in_owner in varchar2, in_directory in varchar2)
    is
        l_fhandle       utl_file.file_type;
        l_filename      varchar2(2000);            
    begin
        dbms_output.put_line('Exporting '||in_owner||'''s views to '||in_directory);
        dbms_output.put_line('-------------------------------------------------');
        for rec in (select * from all_views where owner = in_owner)loop        
            l_filename := 'create_'||lower(rec.view_name)||'_view.sql';
            l_fhandle := utl_file.fopen(in_directory, l_filename, 'w');
            
            export_view_code_to_file(l_fhandle, rec.view_name, rec.text);
            utl_file.put_line(l_fhandle, '');
            utl_file.put_line(l_fhandle, '');
            export_view_comments_to_file(l_fhandle, rec.view_name);        
            
            utl_file.fclose(l_fhandle);
            dbms_output.put_line('Exported '||rec.view_name||' to '||l_filename);
        end loop;

    end;
end btwh_ora_utils;
/
