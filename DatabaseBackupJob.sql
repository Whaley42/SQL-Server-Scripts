/**************************************************************************************************
 Script Name    : DatabaseBackupJob.sql


-------------------------------------------------------------------

 Variable Name    : @databases
 Description      : Databases to backup.
 Valid Values     : N/A
 Required         : Yes
 Default Value    : NULL

 Example(s)       : 'sourceDB_input1'							Backup sourceDB_input1
					'sourceDB_input1,sourceDB_input2'			Backup sourceDB_input1 and sourceDB_input2
					'%input%'									Backup all databases containing input
					'%input%,-sourceDB_input1'					Backup all databases containing input except sourceDB_input1
					'%input%,%conv%,-%Cache%'					Backup all databases containing input or conv except if they contain cache
					'ALL_PRODUCTION'							Backup all production database and inputs

-------------------------------------------------------------------

 Variable Name    : @backupLocation
 Description      : Location to backup to.
 Valid Values     : N/A
 Required         : Yes
 Default Value    : ''

-------------------------------------------------------------------

 Variable Name    : @numDaysToKeepBackup
 Description      : How many days to keep backup before deleting. 
					This is ignored for @scheduleType = 'One Time'
 Valid Values     : N/A
 Required         : No
 Default Value    : 3

-------------------------------------------------------------------

 Variable Name    : @remotePath
 Description      : Location to place the backup copy in Tech Services.
 Valid Values     : N/A
 Required         : No
 Default Value    : ''

-------------------------------------------------------------------

 Variable Name    : @scheduleType
 Description      : Type of schedule for the job.
 Valid Values     : 'One Time', 'Recurring'
 Required         : No
 Default Value    : ''

 Example(s)		  : DECLARE @scheduleType NVARCHAR(50) = 'One Time'

-------------------------------------------------------------------

 Variable Name    : @occursAtTime
 Description      : Time the job starts at. 
 Valid Values     : Time related values. View example.
 Required         : Yes
 Default Value    : NULL

 Example(s)		  : DECLARE @occursAtTime NVARCHAR(15) = '2:00AM'
					DECLARE @occursAtTime NVARCHAR(15) = '4:00PM'
					
-------------------------------------------------------------------

 Variable Name    : @startDate
 Description      : Date the job starts at. 
 Valid Values     : Date related values. View example.
 Required         : Yes
 Default Value    : NULL

 Example(s)		  : DECLARE @startDate NVARCHAR(15) = '3/29/2025'

-------------------------------------------------------------------

 Variable Name    : @endDate
 Description      : Date the job ends at. 
 Valid Values     : Date related values. View example.
 Required         : No
 Default Value    : NULL

 Example(s)		  : DECLARE @endDate NVARCHAR(15) = '3/30/2025'


-------------------------------------------------------------------

 Variable Name    : @frequency
 Description      : How often the job runs. 
 Valid Values     : 'Daily', 'Weekly'
 Required         : No
 Default Value    : NULL

 Example(s)		  : DECLARE @frequency NVARCHAR(50) = 'Daily'


-------------------------------------------------------------------

 Variable Name    : @occursEvery
 Description      : How often the job runs. Only applicable for Weekly schedule.
 Valid Values     : 'Monday,', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
 Required         : No
 Default Value    : NULL

 Example(s)		  : DECLARE @occursEvery NVARCHAR(500) = 'Monday'
					DECLARE @occursEvery NVARCHAR(500) = 'Monday,Tuesday,Wednesday,Thursday,Friday'

						
***************************************************************************************************/

SET NOCOUNT ON

USE [master] --Change the database to store procs and tables.


--User Defined Variables
DECLARE @jobName NVARCHAR(500) = 'Database Backup Job'

--Backup Variables
DECLARE @databases NVARCHAR(MAX) = '' --
DECLARE @backupLocation NVARCHAR(500) = ''
DECLARE @numDaysToKeepBackup INT = 2

--Copy Backup to TechServices Folder Variables
DECLARE @CopyToRemote BIT = 0
DECLARE @proxy NVARCHAR(500) = 'PowershellProxy'
DECLARE @state NVARCHAR(2) = NULL
DECLARE @remotePath NVARCHAR(500) = ''

--Job Scheduling Variables
DECLARE @scheduleType NVARCHAR(50) = 'Recurring'
DECLARE @occursAtTime NVARCHAR(15) = '2:00AM'  
DECLARE @startDate NVARCHAR(15) = '4/07/2025'
DECLARE @endDate NVARCHAR(15) = NULL
DECLARE @frequency NVARCHAR(50) = 'Weekly'
DECLARE @occursEvery NVARCHAR(500) = 'Monday,Tuesday,Wednesday,Thursday,Friday'

DROP TABLE IF EXISTS #DatabasesToCopy
CREATE TABLE #DatabasesToCopy( 
	jobName NVARCHAR(500),
	databaseName NVARCHAR(50), 
	filePath NVARCHAR(MAX)
	)


INSERT INTO #DatabasesToCopy(jobName, databaseName, filePath)
SELECT @jobName, '', ''


--Static Variables
DECLARE @server NVARCHAR(500) = @@SERVERNAME
DECLARE @logDatabase NVARCHAR(50) = (SELECT DB_NAME())
DECLARE @loginName NVARCHAR(500) = (SELECT ORIGINAL_LOGIN())
DECLARE @databaseBackupJobStep NVARCHAR(MAX) 
DECLARE @powershellcmd NVARCHAR(MAX)
DECLARE @sql NVARCHAR(MAX)
DECLARE @successAction INT 
DECLARE @jobStep INT = 1
DECLARE @freqType INT
DECLARE @freqInterval INT
DECLARE @jobTime INT
DECLARE @jobStartDate INT
DECLARE @jobEndDate INT

---------------------------
--Error Handling
---------------------------

IF @scheduleType = 'Recurring' AND (@numDaysToKeepBackup <= 0 OR @numDaysToKeepBackup > 7)
BEGIN
	RAISERROR('@numDaysToKeepBackup must be between 1 and 7 when recurring', 15, 0)
	GOTO finish	
END

IF @scheduleType = 'Recurring' AND @numDaysToKeepBackup IS NULL
BEGIN
	RAISERROR('@numDaysToKeepBackup must be declared when using @scheduleType = Recurring', 15, 0)
	GOTO finish	
END

IF @scheduleType = 'Recurring' AND @frequency = 'Weekly' AND @occursEvery IS NULL
BEGIN
	RAISERROR('@occursEvery must be declared when using @frequency = Weekly', 15, 0)
	GOTO finish	
END

IF @scheduleType = 'Recurring' AND @frequency IS NULL
BEGIN
	RAISERROR('@frequency must be declared when using @scheduleType = Recurring', 15, 0)
	GOTO finish	
END

--Invalid Time
IF TRY_CAST(@occursAtTime AS TIME) IS NULL
BEGIN
	RAISERROR('Invalid @startDate. It is not a valid date.', 15, 0)
	GOTO finish	
END

--Invalid Start Date
IF TRY_CAST(@startDate AS DATE) IS NULL AND @startDate IS NOT NULL AND @scheduleType = 'Recurring'
BEGIN
	RAISERROR('Invalid @startDate. It is not a valid date.', 15, 0)
	GOTO finish	
END

--Invalid End Date
IF TRY_CAST(@endDate AS DATE) IS NULL AND @endDate IS NOT NULL
BEGIN
	RAISERROR('Invalid @endDate. It is not a valid date.', 15, 0)
	GOTO finish	
END

IF TRY_CAST(@endDate AS DATE) < TRY_CAST(@startDate AS DATE)
BEGIN
	RAISERROR('@endDate must be after @startDate', 15, 0)
	GOTO finish	
END

--Invalid Schedule Type
IF @scheduleType NOT IN ('One Time', 'Recurring')
BEGIN
	RAISERROR('Invalid input in @frequency.', 15, 0)
	GOTO finish	
END

--Invalid Frequency
IF @frequency NOT IN ('Daily', 'Weekly')
BEGIN
	RAISERROR('Invalid input in @frequency.', 15, 0)
	GOTO finish	
END

--Invalid Day
IF EXISTS (SELECT 1 
			FROM STRING_SPLIT(@occursEvery, ',') x 
			WHERE x.[value] NOT IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday' ) )
	AND @frequency = 'Weekly'
BEGIN
	RAISERROR('Invalid input in @occursEvery.', 15, 0)
	GOTO finish	
END

--No Valid dataabses
IF NOT EXISTS(SELECT 1 FROM sys.databases d
				WHERE (d.name NOT IN ('master', 'model', 'msdb', 'tempdb', 'csm')
				AND EXISTS(SELECT 1 FROM STRING_SPLIT(REPLACE(@databases, ' ', ''), ',') x WHERE d.[name] LIKE x.[value] AND x.[value] NOT LIKE '-%')
				AND NOT EXISTS(SELECT 1 FROM STRING_SPLIT(REPLACE(@databases, ' ', ''), ',') x WHERE d.[name] LIKE SUBSTRING(x.[value], 2, LEN(x.[value])) AND x.[value] LIKE '-%'))
				OR EXISTS (SELECT 1 FROM STRING_SPLIT(REPLACE(@databases, ' ', ''), ',') x WHERE x.value = 'ALL_PRODUCTION'))

BEGIN
	RAISERROR('Please have at least one valid database.', 15, 0)
	GOTO finish	
END

--No Valid state entered
IF @state IS NULL
BEGIN
	RAISERROR('State variable can''t be null.', 15, 0)
	GOTO finish	
END

--No Valid state entered
IF @jobName IS NULL
BEGIN
	RAISERROR('Job name variable can''t be null.', 15, 0)
	GOTO finish	
END

--Do not use master database as default
IF db_name() = 'master'
BEGIN
	RAISERROR('Please update the database name from master to a user database.', 15, 0)
	GOTO finish
END

IF NOT EXISTS(SELECT 1 FROM sys.credentials x WHERE x.credential_identity = @loginName) AND @CopyToRemote = 1
BEGIN
	RAISERROR('Credentials and Proxy must be created for user if copying to tech services. Please use command below to generate', 15, 0)
	SET @sql = 
	'
	USE [master]
	GO
	CREATE CREDENTIAL [PowershellJob] WITH IDENTITY = N''' + @loginName + ''', SECRET = N'''' /* Windows Password */
	GO
	USE [msdb]
	GO
	EXEC msdb.dbo.sp_add_proxy @proxy_name=N''PowershellProxy'',@credential_name=N''PowershellJob'', 
		@enabled=1
	GO
	EXEC msdb.dbo.sp_grant_proxy_to_subsystem @proxy_name=N''PowershellProxy'', @subsystem_id=12
	GO
	'
	PRINT @sql
	GOTO finish
END 

--------------------------
--Set up
-------------------------

IF OBJECT_ID('DatabasesToCopy') IS NULL
BEGIN
	CREATE TABLE DatabasesToCopy( 
	jobName NVARCHAR(500),
	databaseName NVARCHAR(50), 
	filePath NVARCHAR(MAX)
)
END

IF OBJECT_ID('DatabaseBackupConfig') IS NULL
BEGIN
	CREATE TABLE DatabaseBackupConfig (
		jobName NVARCHAR(500)
		,maxBackupNumber INT
		,currentBackupNumber INT
		,scheduleType NVARCHAR(50)
	)
END

DELETE c 
FROM DatabaseBackupConfig c 
WHERE c.jobName = @jobName

DELETE c 
FROM DatabasesToCopy c 
WHERE c.jobName = @jobName

INSERT INTO DatabasesToCopy(jobName, databaseName, filePath)
SELECT dc.jobName, dc.databaseName, dc.filePath
FROM #DatabasesToCopy dc 
WHERE NOT EXISTS(SELECT 1 FROM DatabasesToCopy x WHERE x.jobName = dc.jobName AND x.databaseName = dc.databaseName)

INSERT INTO DatabaseBackupConfig(jobName, maxBackupNumber, currentBackupNumber, scheduleType)
SELECT @jobName, @numDaysToKeepBackup,1, @scheduleType 

--Create Database Backup Stored Procedure
SET @sql = 
'
CREATE OR ALTER PROCEDURE DatabaseBackup
	@databases NVARCHAR(MAX)
	,@backupLocation NVARCHAR(500)
	,@scheduleType NVARCHAR(50)
	,@jobName NVARCHAR(500)
AS 
BEGIN

DECLARE @databaseValues TABLE ([value] NVARCHAR(4000))
DECLARE @databasesToBackup TABLE (ID INT IDENTITY(1,1), [database] NVARCHAR(500))
DECLARE @databaseCount INT
DECLARE @currentDatabaseID INT 
DECLARE @currentDatabaseName NVARCHAR(500)
DECLARE @sqlcmd NVARCHAR(MAX)
DECLARE @backupName NVARCHAR(500)
DECLARE @currentBackupNumber NVARCHAR(50)
DECLARE @backupLocationNetwork NVARCHAR(500)
DECLARE @backupFilePath NVARCHAR(500)

INSERT INTO @databaseValues([value])
SELECT t.[value]
FROM STRING_SPLIT(REPLACE(@databases, '' '', ''''), '','') t

INSERT INTO @databasesToBackup([database])
SELECT d.[name]
FROM sys.databases d 
WHERE EXISTS (SELECT 1 FROM @databaseValues x WHERE x.[value] = ''ALL_PRODUCTION'')
AND NOT EXISTS(SELECT 1 FROM @databaseValues x WHERE d.[name] LIKE SUBSTRING(x.[value], 2, LEN(x.[value])) AND x.[value] LIKE ''-%'')
AND d.[name] not like ''%_conv%''
AND d.is_read_committed_snapshot_on = 1 

INSERT INTO @databasesToBackup([database])
SELECT n.prodInput
FROM @databasesToBackup db
CROSS APPLY (SELECT db.[database] + ''_input'' as prodInput) n
WHERE EXISTS(SELECT 1 FROM sys.databases x WHERE x.[name] = n.prodInput)
AND NOT EXISTS(SELECT 1 FROM @databasesToBackup x WHERE n.prodInput = x.[database])
AND NOT EXISTS(SELECT 1 FROM @databaseValues x WHERE n.prodInput LIKE SUBSTRING(x.[value], 2, LEN(x.[value])) AND x.[value] LIKE ''-%'')

INSERT INTO @databasesToBackup([database])
SELECT d.[name]
FROM sys.databases d
WHERE d.name NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''csm'')
AND EXISTS(SELECT 1 FROM @databaseValues x WHERE d.[name] LIKE x.[value] AND x.[value] NOT LIKE ''-%'' )
AND NOT EXISTS(SELECT 1 FROM @databaseValues x WHERE d.[name] LIKE SUBSTRING(x.[value], 2, LEN(x.[value])) AND x.[value] LIKE ''-%'')
AND NOT EXISTS(SELECT 1 FROM @databasesToBackup x WHERE d.[name] = x.[database])

SELECT *
INTO ##BackupList 
FROM @databasesToBackup

SET @databaseCount = (SELECT COUNT(1) FROM @databasesToBackup)
SET @currentDatabaseID = 1 

WHILE @currentDatabaseID <= @databaseCount
BEGIN
	
	SET @currentDatabaseName = (SELECT x.[database] FROM @databasesToBackup x WHERE x.ID = @currentDatabaseID)
	IF @scheduleType = ''One Time''
	BEGIN
		SET @backupName = @currentDatabaseName +  ''-OT-'' + FORMAT(GETDATE(), ''MM-dd-yy'') + ''.bak'' 
	END

	IF @scheduleType = ''Recurring''
	BEGIN
		SET @currentBackupNumber = (SELECT x.currentBackupNumber FROM DatabaseBackupConfig x WHERE x.jobName = @jobName)
		SET @backupName = @currentDatabaseName + ''_'' + @currentBackupNumber + ''.bak'' 
	END

	SET @sqlcmd = ''BACKUP DATABASE ['' + @currentDatabaseName + ''] TO  DISK = N'''''' + @backupLocation + ''\'' + @backupName + '''''' WITH  RETAINDAYS = 0, NOFORMAT, INIT,  NAME = N'''''' + @currentDatabaseName + ''-Full Database Backup'''''' + '', SKIP, NOREWIND, NOUNLOAD,  STATS = 10''

	BEGIN TRY
		EXECUTE master.sys.sp_executesql @sqlcmd 
	END TRY
	BEGIN CATCH
		PRINT @sqlcmd
	END CATCH 


	SET @currentDatabaseID = @currentDatabaseID + 1
END 


UPDATE c SET
	currentbackupNumber = CASE WHEN c.currentBackupNumber + 1 > c.maxBackupNumber THEN 1 ELSE c.currentBackupNumber + 1 END
FROM DatabaseBackupConfig c
WHERE c.scheduleType = ''Recurring''
AND c.jobName = @jobName


END
'
EXECUTE(@sql)

SET @databaseBackupJobStep = 'EXECUTE dbo.DatabaseBackup @databases = ''' + @databases + ''', @backupLocation = ''' + @backupLocation + ''', @scheduleType = ''' + @scheduleType + ''', @jobName = ''' + @jobName + ''''


--PowerShell Copy Setup
SET @powershellcmd = '#NOSQLPS' + CHAR(13) + CHAR(10)
SET @powershellcmd += (SELECT '$copyPaths = @{' + STRING_AGG(CONCAT('"',d.databaseName,'"', ' = ', '"', d.filePath, '"'),CHAR(13) + CHAR(10) ) + '}' 
						FROM DatabasesToCopy d) + CHAR(13) + CHAR(10)
SET @powershellcmd += '$backupPath = ' + '"' + @backupLocation + '"' + CHAR(13) + CHAR(10)
SET @powershellcmd += '$techservicesPath = ' + '"' + @remotePath + @state + '\' + '"' + CHAR(13) + CHAR(10)
SET @powershellcmd += '

	foreach ($i in $copyPaths.GetEnumerator()){
    $filter = $i.Key + "*"
    $backupFiles = (Get-ChildItem -path $backupPath -filter $filter)
    $mostRecentBackup = $backupFiles | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    $sourceFile = $backupPath + $mostRecentBackup
    $targetDestination = $techservicesPath + ($i.Value)
	try{ 

		$targetDestinationIsValid = Test-Path -Path $targetDestination
		if($targetDestinationIsValid -eq $false){
			$newError = New-Object System.IO.DirectoryNotFoundException 
			$newError.PSObject.TypeNames.Insert(0, "System.IO.DirectoryNotFoundException")
			throw $newError
		}
		
		$sourceFileIsValid = Test-Path -Path $sourceFile -PathType Leaf
		if($sourceFileIsValid -eq $false){
			$newError = New-Object System.IO.FileNotFoundException 
			$newError.PSObject.TypeNames.Insert(0, "System.IO.System.IO.FileNotFoundException")
			throw $newError
		}
		
		$sourceFileExtension = (Get-Item $sourceFile).Extension
		if($sourceFileExtension -ne ".bak"){
			$newError = New-Object System.ArgumentException 
			$newError.PSObject.TypeNames.Insert(0, "System.ArgumentException")
			throw $newError
		}
		
		Write-Host "Success! All parameters are valid. Copying file..."
		$targetDestination = $targetDestination + ($i.Key) + ".bak"
		Copy-Item -Path $sourceFile -Destination $targetDestination 
		Set-Content -Path "D:\INFINITECAMPUS\conversion\ErrorLog.txt" -Value "Success! No Errors reported."
	} catch [System.ArgumentException]{
		Write-Host "Source file was not .bak"
	} catch [System.IO.DirectoryNotFoundException]{
		Write-Host "Target destination was not found."
	} catch [System.IO.FileNotFoundException]{
		Write-Host "Source file was not found."
	} catch{
		Write-Host $_.
		Set-Content -Path "D:\INFINITECAMPUS\conversion\ErrorLog.txt" -Value "Failed! Errors reported."
	} 
}'

/*
SET @powershellcmdPath = @powershellcmdPath + REPLACE(@jobName, ' ', '') + '.ps1'
SET @powershellcmd = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@powershellcmd, CHAR(10), CHAR(32)),CHAR(13), CHAR(32)),CHAR(160), CHAR(32)),CHAR(9),CHAR(32)), '"', '""""""""'), '$', '`$'), '\"', '\\"')))
SET @powershellcmdLength = LEN(@powershellcmd) / @powershellcmdDivider

SET @powershellFileCreation = 'powershell.exe -Command "if(-not(Test-Path -Path ' +@powershellcmdPath+ ' -PathType Leaf)){ New-Item -Path ' + @powershellcmdPath + ' -ItemType File} else{ Clear-Content -Path ' + @powershellcmdPath + ' }" ' + CHAR(13) + CHAR(10)

DECLARE @i INT = 0
WHILE @i < @powershellcmdDivider
BEGIN

	WHILE SUBSTRING(@powershellcmd, @powershellcmdLength * @i, 1) = '"'
	BEGIN
		SET @powershellcmdLength += 1
	END

	IF LEN(SUBSTRING(@powershellcmd, @powershellcmdLength * @i, @powershellcmdLength)) >= 900 
	BEGIN
		RAISERROR('Too many characters for one powershell append. Please update @powershellcmdDivider to a larger number.', 15, 0)
		GOTO finish		
	END

	IF @i = @powershellcmdDivider - 1
	BEGIN
		SET @powershellFileCreation += 'powershell.exe -Command "Add-Content -Path ' + @powershellcmdPath + ' -Value """"' + SUBSTRING(@powershellcmd, @powershellcmdLength * @i, LEN(@powershellcmd) ) + '"""" " -NoNewLine' + CHAR(13) + CHAR(10) 		
	END
	ELSE
	BEGIN
		SET @powershellFileCreation += 'powershell.exe -Command "Add-Content -Path ' + @powershellcmdPath + ' -Value """"' + SUBSTRING(@powershellcmd, @powershellcmdLength * @i, @powershellcmdLength ) + '"""" " -NoNewLine' + CHAR(13) + CHAR(10) 
	END
	
	SET @i += 1
END
*/

--------------------
--Create Job
--------------------

SET @freqType = CASE WHEN @scheduleType = 'One Time' THEN 1  
					 WHEN @frequency = 'Daily' THEN 4
					 WHEN @frequency = 'Weekly' THEN 8
				END 

IF @scheduleType = 'One Time'
BEGIN
	SET @freqInterval = 1
END

IF @frequency = 'Daily'
BEGIN 
	SET @freqInterval = 1
END

IF @frequency = 'Weekly'
BEGIN 
	SET @freqInterval = (SELECT SUM(
						CASE x.[value] 
							WHEN 'Sunday'		THEN 1 
							WHEN 'Monday'		THEN 2 
							WHEN 'Tuesday'		THEN 4
							WHEN 'Wednesday'	THEN 8
							WHEN 'Thursday'		THEN 16
							WHEN 'Friday'		THEN 32
							WHEN 'Saturday'		THEN 64
						END
					)
					FROM STRING_SPLIT(REPLACE(@occursEvery, ' ', ''), ',') x )
	
END

SET @jobStartDate = REPLACE(TRY_CAST(TRY_CAST(@startDate AS DATE) AS NVARCHAR(50)), '-', '')
SET @jobEndDate = COALESCE(REPLACE(TRY_CAST(TRY_CAST(@endDate AS DATE) AS NVARCHAR(50)), '-', ''), 99991231)
SET @jobTime = TRY_CAST(REPLACE(LEFT(TRY_CAST(TRY_CAST(@occursAtTime AS TIME) AS NVARCHAR(50)), 8), ':', '') AS INT)

USE [msdb]

IF EXISTS (SELECT 1 from msdb.dbo.sysjobs x WHERE x.[name] = @jobName)
BEGIN
	EXECUTE msdb.dbo.sp_delete_job @job_name = @jobName
END

IF NOT EXISTS(SELECT 1 FROM #DatabasesToCopy)
BEGIN
	SET @CopyToRemote = 0
END

SET @occursAtTime = CASE WHEN COALESCE(TRY_CAST(@startDate as DATETIME) + TRY_CAST(@occursAtTime AS DATETIME), '1901-01-01 00:00:00.000') <= GETDATE() THEN NULL ELSE @occursAtTime END
SET @successAction = CASE WHEN @CopyToRemote = 1 THEN 3 ELSE 1 END


DECLARE @jobId BINARY(16)
EXECUTE  msdb.dbo.sp_add_job @job_name= @jobName, 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@category_name=N'Database Maintenance', 
		@owner_login_name=@loginName, @job_id = @jobId 

EXECUTE msdb.dbo.sp_add_jobserver @job_name= @jobName, @server_name = @server


EXECUTE msdb.dbo.sp_add_jobstep @job_name= @jobName, @step_name=N'Backup Databases', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=@successAction, 
		@on_fail_action=2, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=@databaseBackupJobStep, 
		@database_name=@logDatabase, 
		@flags=8

IF @CopyToRemote = 1
BEGIN

	EXECUTE msdb.dbo.sp_add_jobstep @job_name = @jobName, @step_name=N'Run PS', 
			@step_id=2, 
			@cmdexec_success_code=0, 
			@on_success_action=1, 
			@on_fail_action=2, 
			@retry_attempts=0, 
			@retry_interval=0, 
			@os_run_priority=0, @subsystem=N'Powershell', 
			@command=@powershellcmd, 
			@database_name=@logDatabase, 
			@flags=8, 
			@proxy_name=@proxy 

END

IF @scheduleType = 'Recurring' OR (@scheduleType = 'One Time' AND @occursAtTime IS NOT NULL)
BEGIN

	DECLARE @schedule_id int
	EXECUTE msdb.dbo.sp_add_jobschedule @job_name=@jobName, @name=N'Schedule', 
			@enabled=1, 
			@freq_type=@freqType,
			@freq_interval=@freqInterval, 
			@freq_subday_type=1, 
			@freq_subday_interval=0, 
			@freq_relative_interval=0, 
			@freq_recurrence_factor=1, 
			@active_start_date=@jobStartDate, 
			@active_end_date=@jobEndDate, 
			@active_start_time=@jobTime, 
			@active_end_time=235959, @schedule_id = @schedule_id 

END

IF @scheduleType = 'One Time' AND @occursAtTime IS NULL
BEGIN

	RAISERROR('One Time Job Starting...',0,1) WITH NOWAIT;
	
	EXECUTE msdb.dbo.sp_start_job @jobName 
	
	WHILE EXISTS(SELECT 1 
				FROM msdb.dbo.sysjobactivity ja 
				INNER JOIN msdb.dbo.sysjobs j on j.job_id = ja.job_id
				WHERE j.[name] = @jobName
				AND ja.stop_execution_date IS NULL)
	BEGIN
		RAISERROR('Job Still running...',0,1) WITH NOWAIT;
		WAITFOR DELAY '00:00:05'
	END
	
	EXECUTE msdb.dbo.sp_delete_job @job_name = @jobName
	
END

SELECT t.[value]
INTO #databaseValues 
FROM STRING_SPLIT(REPLACE(@databases, ' ', ''), ',') t

SELECT d.[name] as [database]
INTO #databasesToBackup
FROM sys.databases d 
WHERE EXISTS (SELECT 1 FROM #databaseValues x WHERE x.[value] = 'ALL_PRODUCTION')
AND NOT EXISTS(SELECT 1 FROM #databaseValues x WHERE d.[name] LIKE SUBSTRING(x.[value], 2, LEN(x.[value])) AND x.[value] LIKE '-%')
AND d.[name] not like '%_conv%'
AND d.is_read_committed_snapshot_on = 1 

INSERT INTO #databasesToBackup([database])
SELECT n.prodInput
FROM #databasesToBackup db
CROSS APPLY (SELECT db.[database] + '_input' as prodInput) n
WHERE EXISTS(SELECT 1 FROM sys.databases x WHERE x.[name] = n.prodInput)
AND NOT EXISTS(SELECT 1 FROM #databasesToBackup x WHERE n.prodInput = x.[database])
AND NOT EXISTS(SELECT 1 FROM #databaseValues x WHERE n.prodInput LIKE SUBSTRING(x.[value], 2, LEN(x.[value])) AND x.[value] LIKE '-%')


INSERT INTO #databasesToBackup([database])
SELECT d.[name]
FROM sys.databases d
WHERE d.name NOT IN ('master', 'model', 'msdb', 'tempdb', 'csm')
AND EXISTS(SELECT 1 FROM #databaseValues x WHERE d.[name] LIKE x.[value] AND x.[value] NOT LIKE '-%' )
AND NOT EXISTS(SELECT 1 FROM #databaseValues x WHERE d.[name] LIKE SUBSTRING(x.[value], 2, LEN(x.[value])) AND x.[value] LIKE '-%')
AND NOT EXISTS(SELECT 1 FROM #databasesToBackup x WHERE d.[name] = x.[database])

SELECT *
FROM #databasesToBackup

DROP TABLE IF EXISTS #databaseValues
DROP TABLE IF EXISTS #databasesToBackup

SELECT 'Complete!'

finish:




