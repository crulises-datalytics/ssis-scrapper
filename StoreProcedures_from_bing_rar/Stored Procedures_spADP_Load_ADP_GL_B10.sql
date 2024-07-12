CREATE PROCEDURE [dbo].[spADP_Load_ADP_GL_B10]
	@ExecutionID		VARCHAR(300),
	@RunDateTime		DATETIME2 = NULL,
	@DebugMode			INT       = NULL,
	@BatchSplitByName	VARCHAR(50) = NULL,
	@BatchSplitByValue	DATE = NULL
AS
	-- ================================================================================
	-- 
	-- Stored Procedure:   spADP_Load_ADP_GL_B10
	--
	-- Purpose:            Inserts / Updates data in [dbo].[ADP_GL_B10]
	--                     based on incremental changes loaded to [dbo].[ADP_GL_B0]
	--
	-- Parameters:         @RunDateTime
	--                     @DebugMode - Used just for development & debug purposes,
	--                         outputting helpful info back to the caller.  Not
	--                         required for Production, and does not affect any
	--                         core logic.
	--
	--Usage:              EXEC dbo.spADP_Load_ADP_GL_B10 @ExecutionID = 'Value'
	--
	-- --------------------------------------------------------------------------------
	--
	-- Change Log:		   
	-- ----------
	--
	-- Date				Modified By			Comments
	-- ----				-----------			--------
	-- 07/10/2023		Suhas				BI-9091: ADP_GL Loading Logic Change		 
	-- ================================================================================
	BEGIN
		SET NOCOUNT ON;

		--
		-- Housekeeping Variables
		--
		DECLARE @ProcName NVARCHAR(500) = OBJECT_NAME(@@PROCID);
		DECLARE @DebugMsg NVARCHAR(500) = '';

		--
		-- ETL variables specific to this load
		--
		DECLARE @TaskName VARCHAR(50) = 'ADP_GL_B10 Base',
				@AuditId BIGINT,
				@SourceCount INT= 0,
				@InsertCount INT= 0,
				@UpdateCount INT= 0,
				@DeleteCount INT= 0,
				@RejectCount INT = 0,
				@InterimRowCount INT = 0;

		--
		-- If we do not get an @EDWRunDateTime input, set to current date
		--
		IF @RunDateTime IS NULL
			SET @RunDateTime = GETDATE(); 
		--
		IF @DebugMode = 1
			BEGIN
				SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(25), GETDATE(), 121) + N' - Starting.';
				RAISERROR (@DebugMsg, 0, 0) WITH NOWAIT;
			END;

		BEGIN TRY;
			EXEC [dbo].[spBeginAuditLog] 
				@AuditId = @AuditId OUTPUT,
				@SourceName = @TaskName,
				@ExecutionID = @ExecutionID,
				@BatchSplitByName = @BatchSplitByName,
				@BatchSplitByValue = @BatchSplitByValue;

			SELECT @SourceCount = COUNT(*) FROM [dbo].[ADP_GL_B0] (NOLOCK) WHERE [src_file_name] = @BatchSplitByName AND [date_processed] = @BatchSplitByValue;

			-- Get list of "payroll_batch", "payroll_pe_date2" and "file_number" for changed records
			DROP TABLE IF EXISTS #ChangeIdentifier;
			CREATE TABLE #ChangeIdentifier (
				[payroll_batch] NVARCHAR(20) NOT NULL,
				[payroll_pe_date2] DATE NOT NULL,
				[file_number] INT NOT NULL,
				[src_file_name] NVARCHAR(50) NOT NULL,
				[date_processed] DATE NOT NULL,
				[B0RowCount] INT NULL,
				[B0RunDate] DATE NULL,
				[B10RowCount] INT NULL,
				[B10RunDate] DATE NULL,
				[Operation] VARCHAR(3)
			);

			INSERT INTO #ChangeIdentifier (
				[payroll_batch], [payroll_pe_date2], [file_number], [src_file_name], [date_processed], [B0RowCount], [B0RunDate]
			)
			SELECT
				[B0].[payroll_batch], [B0].[payroll_pe_date2], [B0].[file_number], [B0].[src_file_name], [B0].[date_processed], COUNT(*) AS [B0RowCount], [G].[B0RunDate]
			FROM [dbo].[ADP_GL_B0] [B0] (NOLOCK)
			JOIN (
				SELECT
					[B0].[payroll_batch], [B0].[payroll_pe_date2], [B0].[file_number], [B0].[src_file_name], [B0].[date_processed], MAX([B0].[run_date]) AS [B0RunDate]
				FROM [dbo].[ADP_GL_B0] [B0] (NOLOCK)
				WHERE [src_file_name] = @BatchSplitByName AND [date_processed] = @BatchSplitByValue
				GROUP BY [B0].[payroll_batch], [B0].[payroll_pe_date2], [B0].[file_number], [B0].[src_file_name], [B0].[date_processed]
			) AS [G]
				ON [B0].[payroll_batch] = [G].[payroll_batch]
				AND [B0].[payroll_pe_date2] = [G].[payroll_pe_date2]
				AND [B0].[file_number] = [G].[file_number]
				AND [B0].[run_date] = [G].[B0RunDate]
				AND [B0].[src_file_name] = [G].[src_file_name]
				AND [B0].[date_processed] = [G].[date_processed]
			GROUP BY [B0].[payroll_batch], [B0].[payroll_pe_date2], [B0].[file_number], [B0].[src_file_name], [B0].[date_processed], [G].[B0RunDate];
			SELECT @InterimRowCount = @@ROWCOUNT;

			IF @DebugMode = 1
				BEGIN
					SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(25), GETDATE(), 121) + N' - Rows Inserted into #ChangeIdentifier from B0 => ' + CONVERT(NVARCHAR(25), @InterimRowCount);
					RAISERROR (@DebugMsg, 0, 0) WITH NOWAIT;
				END;

			CREATE NONCLUSTERED INDEX #ixChangeIdentifier
				ON #ChangeIdentifier ( [payroll_batch], [payroll_pe_date2], [file_number], [Operation] );
			
			UPDATE [CI]
				SET [B10RowCount] = [B10D].[B10RowCount],
					[B10RunDate] = [B10D].[B10RunDate]
			FROM #ChangeIdentifier [CI]
			JOIN (
				SELECT
					[B10].[payroll_batch], [B10].[payroll_pe_date2], [B10].[file_number], COUNT(*) AS [B10RowCount], MAX([B10].[run_date]) AS [B10RunDate]
				FROM [dbo].[ADP_GL_B10] [B10] (NOLOCK)
				JOIN #ChangeIdentifier [C]
					ON [B10].[payroll_batch] = [C].[payroll_batch]
					AND [B10].[payroll_pe_date2] = [C].[payroll_pe_date2]
					AND [B10].[file_number] = [C].[file_number]
				WHERE [B10].[IsBaseDeleted] = 0
				GROUP BY [B10].[payroll_batch], [B10].[payroll_pe_date2], [B10].[file_number]
			) AS [B10D]
				ON [CI].[payroll_batch] = [B10D].[payroll_batch]
				AND [CI].[payroll_pe_date2] = [B10D].[payroll_pe_date2]
				AND [CI].[file_number] = [B10D].[file_number];
			SELECT @InterimRowCount = @@ROWCOUNT;

			IF @DebugMode = 1
				BEGIN
					SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(25), GETDATE(), 121) + N' - Rows Updated in #ChangeIdentifier from B10 => ' + CONVERT(NVARCHAR(25), @InterimRowCount);
					RAISERROR (@DebugMsg, 0, 0) WITH NOWAIT;
				END;
			
			UPDATE #ChangeIdentifier
				SET [Operation] = CASE
						WHEN [B10RowCount] IS NULL THEN 'I'
						WHEN [B10RowCount] IS NOT NULL AND [B0RowCount] != ISNULL([B10RowCount], 0) THEN 'D/I'
						WHEN [B10RunDate] IS NOT NULL AND [B0RunDate] > ISNULL([B10RunDate], '1900-01-01') THEN 'D/I'
						WHEN [B10RowCount] IS NOT NULL AND [B10RunDate] IS NOT NULL AND [B0RowCount] = [B10RowCount] AND [B0RunDate] = [B10RunDate] THEN 'D/I'
						ELSE 'NA'
					END;

			BEGIN TRAN;

			UPDATE [B10]
				SET [IsBaseDeleted] = 1,
					[BaseDeletedDate] = GETDATE(),
					[BaseDeletedBy] = SUSER_SNAME(),
					[BaseDeletedExecutionID] = @ExecutionID
			FROM [dbo].[ADP_GL_B10] [B10]
			JOIN #ChangeIdentifier [CI]
				ON [B10].[payroll_batch] = [CI].[payroll_batch]
				AND [B10].[payroll_pe_date2] = [CI].[payroll_pe_date2]
				AND [B10].[file_number] = [CI].[file_number]
			WHERE [CI].[Operation] = 'D/I'
			AND [B10].[IsBaseDeleted] = 0;
			SELECT @DeleteCount = @@ROWCOUNT;

			IF @DebugMode = 1
				BEGIN
					SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(25), GETDATE(), 121) + N' - Rows Deleted from B10 => ' + CONVERT(NVARCHAR(25), @DeleteCount);
					RAISERROR (@DebugMsg, 0, 0) WITH NOWAIT;
				END;

			INSERT INTO [dbo].[ADP_GL_B10] (
				[ADP_GL_B10_RowID], [payroll_batch], [payroll_pe_date1], [journal_description1], [accounting_date], [journal_source], [journal_category], [currency_code], [company], [cost_center],
				[account], [sub_account], [center_type], [debit_amount], [credit_amount], [description], [check_date], [payroll_pe_date2], [journal_description2], [run_date], [file_number],
				[employee_name], [dept], [home_cost_number], [cost_number_worked], [pay_element], [code], [src_file_name], [date_processed], [RowHash], [IsBaseDeleted], [BaseDeletedDate], [BaseDeletedBy]
			)
			SELECT
				[ADP_GL_B0_RowID], [B0].[payroll_batch], [payroll_pe_date1], [journal_description1], [accounting_date], [journal_source], [journal_category], [currency_code], [company], [cost_center],
				[account], [sub_account], [center_type], [debit_amount], [credit_amount], [description], [check_date], [B0].[payroll_pe_date2], [journal_description2], [run_date], [B0].[file_number],
				[employee_name], [dept], [home_cost_number], [cost_number_worked], [pay_element], [code], [B0].[src_file_name], [B0].[date_processed], [RowHash], 0, NULL, NULL
			FROM [dbo].[ADP_GL_B0] [B0] (NOLOCK)
			JOIN #ChangeIdentifier [CI]
				ON [B0].[payroll_batch] = [CI].[payroll_batch]
				AND [B0].[payroll_pe_date2] = [CI].[payroll_pe_date2]
				AND [B0].[file_number] = [CI].[file_number]
				AND [B0].[run_date] = [CI].[B0RunDate]
				AND [B0].[src_file_name] = [CI].[src_file_name]
				AND [B0].[date_processed] = [CI].[date_processed]
			WHERE [CI].[Operation] IN ('I', 'D/I')
			AND [B0].[src_file_name] = @BatchSplitByName AND [B0].[date_processed] = @BatchSplitByValue;
			SELECT @InsertCount = @@ROWCOUNT;

			IF @DebugMode = 1
				BEGIN
					SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(25), GETDATE(), 121) + N' - Rows Inserted into B10 => ' + CONVERT(NVARCHAR(25), @InsertCount);
					RAISERROR (@DebugMsg, 0, 0) WITH NOWAIT;
				END;
				
			UPDATE [B10]
				SET [payroll_batch] = [C].[payroll_batch],
					[payroll_pe_date1] = [C].[payroll_pe_date1],
					[journal_description1] = [C].[journal_description1],
					[accounting_date] = [C].[accounting_date],
					[journal_source] = [C].[journal_source],
					[journal_category] = [C].[journal_category],
					[currency_code] = [C].[currency_code],
					[company] = [C].[company],
					[cost_center] = [C].[cost_center],
					[account] = [C].[account],
					[sub_account] = [C].[sub_account],
					[center_type] = [C].[center_type],
					[debit_amount] = [C].[debit_amount],
					[credit_amount] = [C].[credit_amount],
					[description] = [C].[description],
					[check_date] = [C].[check_date],
					[journal_description2] = [C].[journal_description2],
					[run_date] = [C].[run_date],
					[file_number] = [C].[file_number],
					[employee_name] = [C].[employee_name],
					[dept] = [C].[dept],
					[home_cost_number] = [C].[home_cost_number],
					[cost_number_worked] = [C].[cost_number_worked],
					[pay_element] = [C].[pay_element],
					[code] = [C].[code],
					[src_file_name] = [C].[src_file_name],
					[date_processed] = [C].[date_processed],
					[RowHash] = [C].[RowHash],
					[BaseUpdatedDate] = GETDATE(),
					[BaseUpdatedBy] = SUSER_SNAME()
			FROM [dbo].[ADP_GL_B10] [B10]
			JOIN (
				SELECT
					[ADP_GL_B0_RowID], [B0].[payroll_batch], [payroll_pe_date1], [journal_description1], [accounting_date], [journal_source], [journal_category], [currency_code], [company], [cost_center],
					[account], [sub_account], [center_type], [debit_amount], [credit_amount], [description], [check_date], [B0].[payroll_pe_date2], [journal_description2], [run_date], [B0].[file_number],
					[employee_name], [dept], [home_cost_number], [cost_number_worked], [pay_element], [code], [B0].[src_file_name], [B0].[date_processed], [RowHash]
				FROM [dbo].[ADP_GL_B0] [B0] (NOLOCK)
				JOIN #ChangeIdentifier [CI]
					ON [B0].[payroll_batch] = [CI].[payroll_batch]
					AND [B0].[payroll_pe_date2] = [CI].[payroll_pe_date2]
					AND [B0].[file_number] = [CI].[file_number]
					AND [B0].[run_date] = [CI].[B0RunDate]
					AND [B0].[src_file_name] = [CI].[src_file_name]
					AND [B0].[date_processed] = [CI].[date_processed]
				WHERE [CI].[Operation] = 'U'
			) AS [C]
				ON [B10].[ADP_GL_B10_RowID] = [C].[ADP_GL_B0_RowID]
				AND [B10].[payroll_pe_date2] = [C].[payroll_pe_date2]
			WHERE [B10].[RowHash] != [C].[RowHash];
			SELECT @UpdateCount = @@ROWCOUNT;

			IF @DebugMode = 1
				BEGIN
					SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(25), GETDATE(), 121) + N' - Rows Updated in B10 => ' + CONVERT(NVARCHAR(25), @UpdateCount);
					RAISERROR (@DebugMsg, 0, 0) WITH NOWAIT;
				END;

			COMMIT TRAN;

			IF @DebugMode = 1
				BEGIN
					SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(25), GETDATE(), 121) + N' - Completed.';
					RAISERROR (@DebugMsg, 0, 0) WITH NOWAIT;
				END;

			UPDATE [dbo].[ADPGLFilesLookup]
				SET [IsLoadedToB0] = 1, [IsLoadedToB10] = 1
			WHERE [SrcFileName] = @BatchSplitByName AND [DateProcessed] = @BatchSplitByValue;

			EXEC [dbo].[spEndAuditLog]
				@InsertCount = @InsertCount,
				@UpdateCount = @UpdateCount,
				@DeleteCount = @DeleteCount,
				@SourceCount= @SourceCount,
				@RejectCount = @RejectCount,
				@AuditId = @AuditId;
		
		END TRY
		BEGIN CATCH
			IF (@@TRANCOUNT > 0)
				ROLLBACK TRAN;

			SELECT @InsertCount = 0,
				   @UpdateCount = 0,
				   @DeleteCount = 0;

			EXEC [dbo].[spErrorAuditLog]
				@AuditId = @AuditId;

		END CATCH;

		SELECT @SourceCount AS [SourceCount], @InsertCount AS [InsertCount], @UpdateCount AS [UpdateCount], @DeleteCount AS [DeleteCount], @RejectCount AS [RejectCount];
   	 
	END;