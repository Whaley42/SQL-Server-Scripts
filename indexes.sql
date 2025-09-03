/*
difference between when a nonclustered index is a unique and not unique when including uniqifier 
*/

CREATE OR ALTER PROC VisualizeIndex 
	@database VARCHAR(500)
	,@schema VARCHAR(500) = 'dbo'
	,@table VARCHAR(500)
	,@index VARCHAR(500) = NULL
	,@debug BIT = 0
WITH RECOMPILE
AS 
BEGIN
	SET NOCOUNT ON
	
	DECLARE @databaseID INT
	DECLARE @objectID BIGINT --Object ID for table
	DECLARE @indexID BIGINT
	DECLARE @sql VARCHAR(8000)
	DECLARE @errorString VARCHAR(MAX)
	DECLARE @tableDefinitionColumns VARCHAR(8000)
	DECLARE @indexColumns VARCHAR(8000)
	DECLARE @indexKeyColumns VARCHAR(8000)
	DECLARE @includedColumns VARCHAR(8000)
	DECLARE @fileID INT 
	DECLARE @pageID BIGINT 
	DECLARE @clusteredIndexID INT
	DECLARE @logicTable TABLE (tableDefinitionColumns VARCHAR(8000), includedColumns VARCHAR(8000), indexColumns VARCHAR(8000), indexKeyColumns VARCHAR(8000))
	DECLARE @batchTable TABLE ([sql] VARCHAR(MAX))
	DECLARE @uniquifierLeaf VARCHAR(250)
	DECLARE @uniquifierInternal VARCHAR(250)
	DECLARE @pageLevel INT 
	DECLARE @depth INT
	DECLARE @isClustered BIT = 0
	DECLARE @continue BIT = 1
	DECLARE @batchSize INT = 500
	DECLARE @currentBatch INT = 0
	DECLARE @batchResultsLeaf VARCHAR(MAX)
	DECLARE @batchResultsIndex VARCHAR(MAX)
	
	SET @databaseID = 
		( 
		SELECT TOP 1 x.database_id
		FROM sys.databases x 
		WHERE x.name = @database
		)


	SET @objectID = OBJECT_ID(CONCAT_WS('.',@database,@schema, @table))
	SET @indexID = 
		(
		SELECT x.index_id
		FROM sys.indexes x 
		WHERE x.[name] = @index 
		AND x.object_id = @objectID
		) 
	SET @sql = 'USE ' + @database
	EXEC(@sql)

	--Error checking 
	IF @database IS NULL
	BEGIN
		SET @errorString = CONCAT('Database: ', @database, ' was not found')
		RAISERROR(@errorString, 0, 12) WITH NOWAIT;
		RETURN;
	END

	IF NOT EXISTS(SELECT 1 FROM sys.schemas x where x.[name] = @schema)
	BEGIN
		SET @errorString = CONCAT('Schema: ', @schema, ' was not found inside database: ', @database) 
		RAISERROR(@errorString, 0, 12) WITH NOWAIT;
		RETURN;
	END

	IF @objectID IS NULL
	BEGIN
		SET @errorString = CONCAT('Table: ', @table, ' was not found inside ', @database + '.', @schema)
		RAISERROR(@errorString, 0, 12) WITH NOWAIT;
		RETURN;
	END

	IF @indexID IS NULL
	BEGIN
		SET @errorString = CONCAT('Index: ', @index, ' was not found inside ', @database,'.', @schema, '.',@table)
		RAISERROR(@errorString, 0, 12) WITH NOWAIT;
		RETURN;
	END
	
	--Create index table based on index columns 
	SET @clusteredIndexID = 
		(
		SELECT i.index_id
		FROM sys.indexes i 
		WHERE i.[type_desc] = 'CLUSTERED'
		AND i.[object_id] = @objectID
		)

	--Nonclustered indexes only
	IF @clusteredIndexID = @indexID 
	BEGIN
		SET @errorString = 'Clustered Indexes are not allowed in this stored procedure.'
		RAISERROR(@errorString, 0, 12) WITH NOWAIT;
		RETURN;
	END

	ELSE
	BEGIN

		INSERT INTO @logicTable   	
		SELECT STRING_AGG(CONCAT(
						QUOTENAME(COL_NAME(@objectID, ic.column_id))
						,' '
						, t.[name]
						,CASE WHEN t.collation_name IS NOT NULL THEN '(' + TRY_CAST(c.max_length AS VARCHAR) + ')' ELSE '' END 
						)
					, ',') as cols
				,STRING_AGG(QUOTENAME(CASE WHEN ic.is_included_column = 1 THEN COL_NAME(@objectID, ic.column_id) ELSE NULL END), ',') as includedColumns
				,STRING_AGG(QUOTENAME(CASE WHEN ic.is_included_column = 0 THEN COL_NAME(@objectID, ic.column_id) ELSE NULL END), ',') 
					WITHIN GROUP(ORDER BY CASE WHEN i.[type_desc] = 'NONCLUSTERED' AND ic.key_ordinal <> 0 THEN 1 ELSE 0 END DESC,is_included_column ASC, ic.key_ordinal ASC, ic.column_id ASC) as indexColumns
				,STRING_AGG(QUOTENAME(CASE WHEN ic.is_included_column = 0 AND ic.index_id = @indexID THEN COL_NAME(@objectID, ic.column_id) ELSE NULL END), ',') as indexKeyColumns 
		--select count(1)
		--select *
		FROM sys.index_columns ic 
		INNER JOIN sys.indexes i ON i.[object_id] = ic.[object_id] AND ic.index_id = i.index_id 
		INNER JOIN sys.columns c ON c.column_id = ic.column_id AND ic.[object_id] = c.[object_id]
		INNER JOIN sys.types t ON t.system_type_id = c.system_type_id AND t.user_type_id = c.user_type_id
		WHERE ic.[object_id] = @objectID
		AND ic.index_id IN (@indexID, @clusteredIndexID)
		AND NOT EXISTS(SELECT 1 
						FROM sys.index_columns xic 
						INNER JOIN sys.indexes xi ON xi.[object_id] = xic.[object_id] and xic.index_id = xi.index_id
						WHERE xic.[object_id] = ic.[object_id] AND xic.column_id = ic.column_id AND i.[type_desc] = 'NONCLUSTERED'  AND xi.[type_desc] = 'CLUSTERED')
	
	END


	SET @tableDefinitionColumns = (SELECT x.tableDefinitionColumns FROM @logicTable x)
	SET @includedColumns = (SELECT x.includedColumns + ',' FROM @logicTable x)
	SET @indexColumns = (SELECT x.indexColumns FROM @logicTable x)

	SET @uniquifierLeaf = (SELECT 'Uniquifier,' FROM sys.indexes x WHERE x.[object_id] = @objectID AND x.index_id = 1 AND x.is_unique = 0) --Used is clustered index is not unique 
	SET @uniquifierInternal = (SELECT 'Uniquifier,' FROM sys.indexes x WHERE x.[object_id] = @objectID AND x.index_id = @indexID AND x.is_unique = 0 AND @uniquifierLeaf IS NOT NULL )
	SET @indexKeyColumns = (SELECT x.indexKeyColumns FROM @logicTable x WHERE @uniquifierInternal IS NULL AND @uniquifierLeaf IS NOT NULL )
	
	IF @debug = 1 
	BEGIN
		SELECT 'Logic Table Info'
				,@tableDefinitionColumns AS tableDefinitionColumns
				, @includedColumns AS includedColumns
				, @indexColumns AS indexColumns
				, @indexKeyColumns as indexKeyColumns
				, @uniquifierLeaf AS uniquifierLeaf
				, @uniquifierInternal AS uniquifierInternal
		SELECT 'Other Variables'
				, @database AS databaseName
				, @schema AS [schema]
				, @table AS [table]
				, @indexID AS indexID
				, @clusteredIndexID AS clusteredIndexID

		SELECT ic.*
		FROM sys.index_columns ic 
		INNER JOIN sys.indexes i ON i.[object_id] = ic.[object_id] AND ic.index_id = i.index_id 
		INNER JOIN sys.columns c ON c.column_id = ic.column_id AND ic.[object_id] = c.[object_id]
		INNER JOIN sys.types t ON t.system_type_id = c.system_type_id AND t.user_type_id = c.user_type_id
		WHERE ic.[object_id] = @objectID
		AND ic.index_id IN (@indexID, @clusteredIndexID)
		AND NOT EXISTS(SELECT 1 
						FROM sys.index_columns xic 
						INNER JOIN sys.indexes xi ON xi.[object_id] = xic.[object_id] and xic.index_id = xi.index_id
						WHERE xic.[object_id] = ic.[object_id] AND xic.column_id = ic.column_id AND i.[type_desc] = 'NONCLUSTERED'  AND xi.[type_desc] = 'CLUSTERED')
	END

	
	DROP TABLE IF EXISTS ##IndexResult 
	SET @sql = 'CREATE TABLE ##IndexResult(
					fileID INT
					, pageID BIGINT
					, [row] BIGINT
					, [level] INT
					, childFieldID INT
					, childPageID BIGINT
					, ' + @tableDefinitionColumns + '
					, uniquifier BIGINT
					, keyHashValue VARCHAR(8000)
					, rowSize INT
					) '
	EXEC(@sql)

	IF @debug = 1
	BEGIN
		SELECT @sql 
	END

	
	DROP TABLE IF EXISTS ##PageAllocations 
	SELECT ROW_NUMBER() OVER(PARTITION BY CASE WHEN pa.page_level > 0 THEN 1 ELSE 0 END ORDER BY (SELECT NULL)) as rowNum, pa.database_id as databaseID, pa.allocated_page_file_id as fileID, pa.allocated_page_page_id as pageID, pa.page_level as pageLevel, 
		pa.previous_page_file_id, pa.previous_page_page_id, pa.next_page_file_id, pa.next_page_page_id
	INTO ##PageAllocations
	FROM sys.dm_db_database_page_allocations(@databaseID, @objectID, NULL, NULL, 'DETAILED') pa
	WHERE pa.index_id = @indexID
	AND pa.is_allocated = 1
	AND pa.is_iam_page = 0


	/*
	--Batch Insert

	--Clustered Index Logic
	IF @isClustered = 1
	BEGIN
		WHILE @continue = 1
		BEGIN

			SELECT @batchResultsLeaf = STRING_AGG(CASE WHEN pa.pageLevel = 0 THEN TRY_CAST(CONCAT(
								'INSERT INTO ##DataPageResults 
								EXEC(''DBCC PAGE('
								,pa.databaseID, ','
								,pa.fileID, ','
								,pa.pageID, ', 3)with tableresults '') ') AS VARCHAR(MAX)) ELSE NULL END, ';')
				,@batchResultsIndex = STRING_AGG(CASE WHEN pa.pageLevel > 0 THEN TRY_CAST(CONCAT(	
					'INSERT INTO ##IndexResult(fileID, pageID, [row], [level],childFieldID, childPageID, ' + @indexColumns + ', ' + COALESCE(@uniquifier, '') + 'keyHashValue, rowSize)
					EXEC(''DBCC PAGE(' 
					,pa.databaseID, ','
					,pa.fileID, ','
					,pa.pageID, ', 3)'') ') AS VARCHAR(MAX)) ELSE NULL END, ';')
			FROM ##PageAllocations pa
			WHERE 
			pa.rowNum > @currentBatch
			AND pa.rowNum <= @currentBatch + @batchSize

			IF @batchResultsLeaf IS NOT NULL
			BEGIN
				--SELECT @batchResultsLeaf
				EXEC(@batchResultsLeaf)
			END

			IF @batchResultsIndex IS NOT NULL
			BEGIN 
				--SELECT @batchResultsIndex
				EXEC(@batchResultsIndex)
			END

			IF @batchResultsIndex IS NULL AND @batchResultsLeaf IS NULL
			BEGIN
				SET @continue = 0
			END

			SET @currentBatch = @currentBatch + @batchSize

		END
	END

	--Non Clustered Index Logic
	ELSE
	BEGIN


		select 1

	END
	*/
	
	DECLARE cursorPageAllocation CURSOR FOR
		SELECT pa.fileID, pa.pageID, pa.pageLevel
		FROM ##PageAllocations pa

	OPEN cursorPageAllocation

	FETCH NEXT FROM cursorPageAllocation INTO 
		@fileID
		,@pageID
		,@pageLevel

	WHILE @@FETCH_STATUS = 0
	BEGIN


		IF @pageLevel > 0
		BEGIN
			SET @sql = CONCAT(
				'INSERT INTO ##IndexResult(fileID, pageID, [row], [level],childFieldID, childPageID, '  + COALESCE(@indexKeyColumns, @indexColumns) + ',' + COALESCE(@uniquifierInternal, '') + 'keyHashValue, rowSize)
					EXEC(''DBCC PAGE(' 
					,@database, ','
					,@fileID, ','
					,@pageID, ', 3)'') ')

			IF @debug = 1
			BEGIN 
				PRINT @sql
			END
			EXEC(@sql)
			

		END

		ELSE
		BEGIN

			--Non Clustered Index
			SET @sql = CONCAT(
				'INSERT INTO ##IndexResult(fileID, pageID, [row], [level], ' + @indexColumns + ', ' + COALESCE(@uniquifierLeaf, '') + COALESCE(@includedColumns, '') + 'keyHashValue, rowSize)
					EXEC(''DBCC PAGE(' 
					,@database, ','
					,@fileID, ','
					,@pageID, ', 3)'') ')
			
			IF @debug = 1
			BEGIN 
				PRINT @sql
			END

			BEGIN TRY
				EXEC(@sql)
			END TRY
			BEGIN CATCH
				select 'Could not insert leaf page'
			END CATCH

		


		END

		FETCH NEXT FROM cursorPageAllocation INTO
			@fileID
			,@pageID
			,@pageLevel

	END
	CLOSE cursorPageAllocation
	DEALLOCATE cursorPageAllocation
	
	IF @debug = 1
	BEGIN
		PRINT 'Past inserts'
	END

	
	SET @depth = (SELECT MAX(x.[level]) FROM ##IndexResult x)

	print @depth

	
	
	
	WHILE @depth >= 0 
	BEGIN
		WITH recursiveOrdering AS 
			(
			SELECT pa.*, 1 as n
			FROM ##PageAllocations pa
			WHERE pa.previous_page_page_id IS NULL 
			AND pa.pageLevel = @depth

			UNION ALL

			SELECT pa.*,n + 1
			FROM recursiveOrdering o
			INNER JOIN ##PageAllocations pa on pa.pageID = o.next_page_page_id AND pa.pageLevel = @depth
			)
		SELECT r.*
		FROM recursiveOrdering o
		INNER JOIN ##IndexResult r ON r.pageID = o.pageID AND r.fileID = o.fileID
		ORDER BY o.n, r.[row]
		OPTION (maxrecursion 0)

		SET @depth = @depth - 1

	END
	
	

END
GO


