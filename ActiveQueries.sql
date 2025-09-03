
use master;

declare @sql nvarchar(max)
declare @database nvarchar(255) = null;

set @sql = 
'select 
r.request_id
,r.session_id
,s.login_name
,d.name as databaseName
,r.start_time
,r.command
,t.text
,p.query_plan
,case r.blocking_session_id
	when 0 then null 
	when -2 then ''The blocking resource is owned by an orphaned distributed transaction.''
	when -3 then ''The blocking resource is owned by a deferred recovery transaction.''
	when -4 then ''session_id of the blocking latch owner couldn''''t be determined at this time because of internal latch state transitions.''
	when -5 then ''session_id of the blocking latch owner couldn''''t be determined because it isn''''t tracked for this latch type (for example, for an SH latch).''
	else try_cast(r.blocking_session_id as varchar)
	end AS blocking_session_id
,r.wait_type
,r.wait_time
,r.last_wait_type
,r.wait_resource
,r.open_transaction_count
,r.cpu_time
,r.total_elapsed_time
,r.reads
,r.writes
,r.logical_reads
,r.row_count
,r.granted_query_memory
,r.dop
from sys.dm_exec_requests r
cross apply sys.dm_exec_sql_text(r.sql_handle) t
cross apply sys.dm_exec_query_plan(r.plan_handle) p
inner join sys.databases d on d.database_id = r.database_id 
inner join sys.dm_exec_sessions s on s.session_id = r.session_id
where r.[status] in (''Running'', ''runnable'', ''suspended'')
and r.session_id <> @@spid'

if @database is not null
begin 
set @sql = @sql + CHAR(13) + CHAR(10) + 'and d.name = @databaseName'
end 

EXECUTE sp_executesql @sql, N'@databaseName nvarchar(255)', @databaseName=@database
