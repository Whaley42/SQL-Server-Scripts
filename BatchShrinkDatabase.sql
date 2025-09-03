

DECLARE @name VARCHAR(500);
DECLARE @currentSizeMB INT;
DECLARE @physicalName VARCHAR(500)
DECLARE @spaceUsedMB INT;
DECLARE @floor INT;
DECLARE @factor FLOAT = 500 -- Size in MB
DECLARE @targetSize INT;
DECLARE @maxCount INT = 100
DECLARE @count INT = 0;
DECLARE @output VARCHAR(500)

SELECT	@name = df.name,
		@physicalName = physical_name,
		@currentSizeMB = df.size / 128 , 
		@spaceUsedMB = TRY_CAST(FILEPROPERTY(df.name, 'SpaceUsed') AS INT) / 128 
--select *
FROM sys.database_files df
WHERE df.[type] = 0 /* data file */

SELECT @physicalName as physicalName, @currentSizeMB as currentSizeMB, @spaceUsedMB as spaceUsedMB, @factor as spaceRemovedPerShrinkMB

SET @floor = @spaceUsedMB * 0.01

WHILE @currentSizeMB > @spaceUsedMB  + @floor
BEGIN

	SET @targetSize = @currentSizeMB - @factor
	SET @output = TRY_CAST(@targetSize as VARCHAR(100))
	RAISERROR(@output, 1, 10) with nowait;

	DBCC SHRINKFILE(@name, @targetSize) with no_infomsgs;

	SET @count = @count + 1

	IF @count = @maxCount 
	BEGIN
		RAISERROR('REACHED MAX LOOP COUNT', 1, 10)
		BREAK
	END 

	SELECT
		@currentSizeMB = df.size / 128 
	--select *
	FROM sys.database_files df
	WHERE df.[type] = 0 /* data file */

	SELECT @name, @currentSizeMB sizeAfterShrinkMB, @spaceUsedMB  + @floor shrinkUntilMB, GETDATE()

END