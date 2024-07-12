CREATE PROCEDURE [dbo].[spEDW_StagingToEDW_GetProcessingRange]
	@SourceName					SYSNAME,
	@TaskName					VARCHAR(50),
	@MinProcessingDate			DATE,
	@LastProcessedDate			DATETIME,
	@DimDateColumnName			SYSNAME,
	@IsOverrideBatch			BIT OUTPUT,
	@AuditID					BIGINT OUTPUT,
	@Debug						BIT = 0
AS
	/***********************************************************************************************
	*	Procedure Name	: spEDW_StagingToEDW_GetProcessingRange
	*	Date			: 23-Jul-2023
	*	Author			: Suhas De
	*	Parameters		: @SourceName - Source Database Name
	*					  @TaskName - Name of the Task
	*					  @MinProcessingDate - Minimum Processing Date
	*					  @LastProcessedDate - Last Processed Date
	*					  @DimDateColumnName - Column from DimDate Table that is used
	*					  @IsOverrideBatch - Is an override batch (output parameter)
	*					  @AuditID - Audit ID (output parameter)
	*
	*	Purpose         : Procedure is used to generate processing range based on
	*						Override Information for Historical Target Reload
	*
	*	Note			: Procedure expects 2 Temporary Tables to be created by the caller as defined below.
	*						{
	*							#OriginalProcessingRange ( [BatchIdentifier] SQL_VARIANT );		=> must be populated by the call based on the original logic.
	*							#FinalProcessingRange ( <<ColumnName>> <<DATE/INT>> );			=> must be empty
	*						}
	*
	*	Change History:
	*	Date                  	  Programmer     		 Reason
	*	--------------------      -------------------    ------------------------------
	*	
	**************************************************************************************************/
	BEGIN
		SET NOCOUNT ON;

		-- Variable Declaration
		DECLARE @DimDateColumnDataType SYSNAME;
		DECLARE @EDWBatchOverrideId INT,
				@OverrideStartDate DATE,
				@OverrideEndDate DATE,
				@EDWBatchOverrideAuditId BIGINT;
		DECLARE @SQLQuery VARCHAR(MAX),
				@Message NVARCHAR(250);

		BEGIN TRY
			-- Get the DataType and Max Length of the DimDateColumn
			SELECT
				@DimDateColumnDataType = [T].[name]
			FROM [sys].[columns] [C] (NOLOCK)
			JOIN [sys].[types] [T] (NOLOCK)
				ON [C].[system_type_id] = [T].[system_type_id]
			WHERE [C].[name] = @DimDateColumnName;

			IF (@Debug = 1)
				BEGIN
					SELECT @Message = N'DataType of ' + @DimDateColumnName + N' is [' + @DimDateColumnDataType + N']';
					RAISERROR(@Message, 0, 0) WITH NOWAIT;
				END;
		
			--	Temp Table Definition to generate and manipulate the FinalProcessingRange
			DROP TABLE IF EXISTS #OutputProcessingRange;
			CREATE TABLE #OutputProcessingRange (
				[DateKey]				INT			NOT NULL		PRIMARY KEY,
				[FullDate]				DATE		NOT NULL,
				[OutputColumn]			SQL_VARIANT	NOT NULL,
				[BatchSplitIdentifier]	INT			NULL
			);

			-- Populate #OutputProcessingRange with all records from [dbo].[DimDate]
			SELECT @SQLQuery = 'SELECT DISTINCT [DateKey], [FullDate], ' + QUOTENAME(@DimDateColumnName) + ' FROM [BING_EDW].[dbo].[DimDate] (NOLOCK);';
			INSERT INTO #OutputProcessingRange ( [DateKey], [FullDate], [OutputColumn] )
			EXEC (@SQLQuery);

			IF (@DimDateColumnDataType = 'date')
				BEGIN
					UPDATE #OutputProcessingRange
						SET [BatchSplitIdentifier] = CONVERT(INT, CAST(DATEPART(YYYY, CONVERT(DATE, [OutputColumn])) AS [CHAR](4)) +
													 RIGHT('0' + CAST(DATEPART(M, CONVERT(DATE, [OutputColumn])) AS [VARCHAR](2)), 2) +
													 RIGHT('0' + CAST(DATEPART(D, CONVERT(DATE, [OutputColumn])) AS [VARCHAR](2)), 2));
				END;
			ELSE IF (@DimDateColumnDataType = 'int')
				BEGIN
					UPDATE #OutputProcessingRange
						SET [BatchSplitIdentifier] = CONVERT(INT, [OutputColumn]);
				END

			/*
				Check if there is an Active record in the EDWBatchOverride table for the corresponding task.
			*/
			IF EXISTS(SELECT 1 FROM [dbo].[EDWBatchOverride] (NOLOCK) WHERE [SourceName] = @SourceName AND [TaskName] = @TaskName AND [IsActive] = 1)
				BEGIN
					SELECT @IsOverrideBatch = 1;

					IF (@Debug = 1)
						BEGIN
							SELECT @Message = N'There is an Active record in the EDWBatchOverride table for the task: [' + @SourceName + N'].[' + @TaskName + N'].';
							RAISERROR(@Message, 0, 0) WITH NOWAIT;
						END;
					/*
						If there is an Active Record for the task in the EDWBatchOverride table,
						•	Read the OverrideStartDate, OverrideEndDate and AuditId column values from the EDWBatchOverride table.
						•	If OverrideStartDate is NULL, use the OverrideEndDate column value to limit the processing date range. Use MinProcessingDate to limit the start of the Processing Range.
						•	If OverrideEndDate is NULL, use the OverrideStartDate column value to limit the processing date range.
						•	If values of both OverrideStartDate and OverrideEndDate are provided, generate a processing range based on the DimDate table in BING_EDW Database.
					*/
					SELECT TOP 1
						@EDWBatchOverrideId = [EDWBatchOverrideId],
						@OverrideStartDate = [OverrideStartDate],
						@OverrideEndDate = [OverrideEndDate],
						@EDWBatchOverrideAuditId = [AuditId]
					FROM [dbo].[EDWBatchOverride] (NOLOCK)
					WHERE [SourceName] = @SourceName AND [TaskName] = @TaskName AND [IsActive] = 1
					ORDER BY [CreatedDateTime] DESC;

					IF (@Debug = 1)
						BEGIN
							SELECT @Message = N'@EDWBatchOverrideId = ' + CONVERT(NVARCHAR, @EDWBatchOverrideId) + N'; @OverrideStartDate = ' + ISNULL(CONVERT(NVARCHAR, @OverrideStartDate), N'NULL')
											+ N'; @OverrideEndDate = ' + ISNULL(CONVERT(NVARCHAR, @OverrideEndDate), N'NULL') + N'; @EDWBatchOverrideAuditId = ' + ISNULL(CONVERT(NVARCHAR, @EDWBatchOverrideAuditId), N'NULL');
							RAISERROR(@Message, 0, 0) WITH NOWAIT;
						END;

					IF ((@OverrideStartDate IS NULL) OR (@OverrideEndDate IS NULL))
						BEGIN
							IF (@DimDateColumnDataType = 'date')
								BEGIN
									DELETE #OutputProcessingRange WHERE CONVERT(DATE, [OutputColumn]) NOT IN (
										SELECT CONVERT(DATE, [BatchIdentifier]) FROM #OriginalProcessingRange
									);
								END;
							ELSE IF (@DimDateColumnDataType = 'int')
								BEGIN
									DELETE #OutputProcessingRange WHERE CONVERT(INT, [OutputColumn]) NOT IN (
										SELECT CONVERT(INT, [BatchIdentifier]) FROM #OriginalProcessingRange
									);
								END;
							ELSE
								BEGIN
									DELETE #OutputProcessingRange
									WHERE CONVERT(NVARCHAR(1000), [OutputColumn]) NOT IN (
										SELECT CONVERT(NVARCHAR(1000), [BatchIdentifier]) FROM #OriginalProcessingRange
									);
								END;

							IF (@OverrideStartDate IS NULL AND @OverrideEndDate IS NOT NULL)
								BEGIN
									DELETE #OutputProcessingRange WHERE [FullDate] > @OverrideEndDate;
									DELETE #OutputProcessingRange WHERE [FullDate] < @MinProcessingDate;
								END;
							ELSE IF (@OverrideStartDate IS NOT NULL AND @OverrideEndDate IS NULL)
								BEGIN
									DELETE #OutputProcessingRange WHERE [FullDate] < @OverrideStartDate;
								END;
							ELSE IF (@OverrideStartDate IS NULL AND @OverrideEndDate IS NULL)
								BEGIN
									DELETE #OutputProcessingRange WHERE [FullDate] < @MinProcessingDate;
								END;
						END;
					ELSE IF (@OverrideStartDate IS NOT NULL AND @OverrideEndDate IS NOT NULL)
						BEGIN
							DELETE #OutputProcessingRange WHERE [FullDate] < @OverrideStartDate;
							DELETE #OutputProcessingRange WHERE [FullDate] > @OverrideEndDate;
						END;

					/*
						If the value of AuditId is NULL, generate a new AuditId, and make an entry into the EDWAuditLog table.
					*/
					IF (@EDWBatchOverrideAuditId IS NULL)
						BEGIN
							EXEC [dbo].[spEDWBeginAuditLog]
								@AuditId = @EDWBatchOverrideAuditId OUTPUT,
								@SourceName = @TaskName,
								@LastBatchProcessDate = NULL,
								@IsOverride = @IsOverrideBatch;

							UPDATE [dbo].[EDWBatchOverride]
								SET [AuditId] = @EDWBatchOverrideAuditId
							WHERE [EDWBatchOverrideId] = @EDWBatchOverrideId;
						END;
					/*
						If the value of AuditId is NOT NULL, use the AuditId value to determine the already processed date range from the EDWBatchLoadLog table.
					*/
					ELSE IF (@EDWBatchOverrideAuditId IS NOT NULL)
						BEGIN
							UPDATE [dbo].[EDWAuditLog]
								SET [StatusCode] = 0,
									[StatusName] = 'InProcess'
							WHERE [AuditId] = @EDWBatchOverrideAuditId;

							DELETE #OutputProcessingRange
							WHERE [BatchSplitIdentifier] IN (
								SELECT [BatchSplitByValue] FROM [dbo].[EDWBatchLoadLog] (NOLOCK) WHERE [AuditId] = @EDWBatchOverrideAuditId
							);
						END;
				END;
			/*
				If there are no Active Records for the task in the EDWBatchOverride table
			*/
			ELSE
				BEGIN
					SELECT @IsOverrideBatch = 0;

					IF (@Debug = 1)
						BEGIN
							SELECT @Message = N'There are no Active records in the EDWBatchOverride table for the task: [' + @SourceName + N'].[' + @TaskName + N'].';
							RAISERROR(@Message, 0, 0) WITH NOWAIT;
						END;
					/*
						Use the processing range provided to it. Use the MinProcessingDate input parameter to limit the start of the Processing Range.
					*/
					IF (@DimDateColumnDataType = 'date')
						BEGIN
							DELETE #OutputProcessingRange WHERE CONVERT(DATE, [OutputColumn]) NOT IN (
								SELECT CONVERT(DATE, [BatchIdentifier]) FROM #OriginalProcessingRange
							);
						END;
					ELSE IF (@DimDateColumnDataType = 'int')
						BEGIN
							DELETE #OutputProcessingRange WHERE CONVERT(INT, [OutputColumn]) NOT IN (
								SELECT CONVERT(INT, [BatchIdentifier]) FROM #OriginalProcessingRange
							);
						END;
					ELSE
						BEGIN
							DELETE #OutputProcessingRange
							WHERE CONVERT(NVARCHAR(1000), [OutputColumn]) NOT IN (
								SELECT CONVERT(NVARCHAR(1000), [BatchIdentifier]) FROM #OriginalProcessingRange
							);
						END;

					DELETE #OutputProcessingRange WHERE [FullDate] < @MinProcessingDate;

					/*
						Lookup the AuditId value corresponding to the LastBatchProcessedDate column
					*/
					SELECT @EDWBatchOverrideAuditId = NULL;
					SELECT
						@EDWBatchOverrideAuditId = MAX([AuditId])
					FROM [dbo].[EDWAuditLog] (NOLOCK)
					WHERE [TaskName] = @TaskName
					AND [LastBatchProcessedDate] IS NOT NULL AND [LastBatchProcessedDate] = @LastProcessedDate;
					

					IF (@Debug = 1)
						BEGIN
							SELECT @Message = N'@EDWBatchOverrideId = NULL; @OverrideStartDate = NULL; @OverrideEndDate = NULL; @EDWBatchOverrideAuditId = ' + ISNULL(CONVERT(NVARCHAR, @EDWBatchOverrideAuditId), N'NULL');
							RAISERROR(@Message, 0, 0) WITH NOWAIT;
						END;

					IF (@EDWBatchOverrideAuditId IS NULL)
						BEGIN
							/*
								If an AuditId is not found, generate a new AuditId, and make an entry into the EDWAuditLog table
							--*/
							EXEC [dbo].[spEDWBeginAuditLog]
								@AuditId = @EDWBatchOverrideAuditId OUTPUT,
								@SourceName = @TaskName,
								@LastBatchProcessDate = @LastProcessedDate,
								@IsOverride = @IsOverrideBatch;
						END;
					ELSE
						BEGIN
							/*
								If an AuditId is found, use it to determine the already processed date range from the EDWBatchLoadLog table
							*/
							UPDATE [dbo].[EDWAuditLog]
								SET [StatusCode] = 0,
									[StatusName] = 'InProcess'
							WHERE [AuditId] = @EDWBatchOverrideAuditId;

							DELETE #OutputProcessingRange
							WHERE [BatchSplitIdentifier] IN (
								SELECT [BatchSplitByValue] FROM [dbo].[EDWBatchLoadLog] (NOLOCK) WHERE [AuditId] = @EDWBatchOverrideAuditId
							);
						END;
				END;

			SELECT @AuditID = @EDWBatchOverrideAuditId;

			IF (@Debug = 1)
				BEGIN
					SELECT @Message = N'Return: @AuditID = ' + ISNULL(CONVERT(NVARCHAR, @AuditID), N'NULL') + N'; @IsOverrideBatch = ' + ISNULL(CONVERT(NVARCHAR, @IsOverrideBatch), N'NULL');
					RAISERROR(@Message, 0, 0) WITH NOWAIT;
				END;

			IF (@DimDateColumnDataType = 'date')
				BEGIN
					INSERT INTO #FinalProcessingRange
					SELECT DISTINCT
						CONVERT(DATE, [OutputColumn]) AS [BatchSplitIdentifier]
					FROM #OutputProcessingRange ORDER BY CONVERT(DATE, [OutputColumn]);
				END;
			ELSE IF (@DimDateColumnDataType = 'int')
				BEGIN
					INSERT INTO #FinalProcessingRange
					SELECT DISTINCT
						CONVERT(INT, [OutputColumn]) AS [BatchSplitIdentifier]
					FROM #OutputProcessingRange ORDER BY CONVERT(INT, [OutputColumn]);
				END;
			ELSE
				BEGIN
					INSERT INTO #FinalProcessingRange
					SELECT DISTINCT
						CONVERT(VARCHAR(1000), [OutputColumn]) AS [BatchSplitIdentifier]
					FROM #OutputProcessingRange ORDER BY CONVERT(VARCHAR(1000), [OutputColumn]);
				END;
		END TRY
		BEGIN CATCH
			DECLARE @errorMessage NVARCHAR(MAX),
				@errorSeverity INT,
				@errorState INT,
				@errorNumber INT,
				@errorLine INT;

			SELECT   
				@errorMessage = N'spEDW_StagingToEDW_GetProcessingRange :: ' + ERROR_MESSAGE(),  
				@errorSeverity = ERROR_SEVERITY(),  
				@errorState = ERROR_STATE(),
				@errorNumber = ERROR_NUMBER(),
				@errorLine = ERROR_LINE();

			RAISERROR(@errorMessage, @errorSeverity, 1);
		END CATCH
	END;