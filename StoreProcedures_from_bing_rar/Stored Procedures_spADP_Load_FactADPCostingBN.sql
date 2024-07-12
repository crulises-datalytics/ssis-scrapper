CREATE PROCEDURE [dbo].[spADP_Load_FactADPCostingBN]
	@AuditID BIGINT,
	@FiscalWeekStartDate DATE,
	@FiscalWeekEndDate DATE,
	@DebugMode INT = NULL
AS
	BEGIN
		-- ================================================================================
		-- 
		-- Stored Procedure:   spADP_Load_FactADPCostingBN
		--
		-- Purpose:            Fetches records from [dbo].[ADP_GL_B10], [dbo].[ADP_PS_JOB_B10],
		--					   [dbo].[ADP_PS_AL_CHK_DATA_B10],[dbo].[PER_JOBS_B0]  
		--					   for insert into FactADPCostingBN 
		--
		-- Parameters:         @AuditID-Newly Generated AudutID
		--					   @FiscalWeekStartDate
		--                     @FiscalWeekEndDate
		--                     @DebugMode - Used just for development & debug purposes,
		--                         outputting helpful info back to the caller.  Not
		--                         required for Production, and does not affect any
		--                         core logic.
		--
		-- --------------------------------------------------------------------------------
		--
		-- Change Log:		   
		-- ----------
		--
		-- Date				Modified By			    Comments
		-- ----				-----------			    --------
		-- 08/02/2023		Praveen			        BI-8931 Stored Procedure to Generate Data For Table [dbo].[FactADPCostingBN]	
		-- 11/29/2023		Suhas					BI-12039: Performance Optimization
		-- ================================================================================
		SET NOCOUNT ON;

		--
		-- Housekeeping Variables
		--
		DECLARE @ProcName NVARCHAR(500) = OBJECT_NAME(@@PROCID);
		DECLARE @DebugMsg NVARCHAR(500) = '';
		--
		-- ETL variables specific to this load
		--
		DECLARE @BatchSplitByValue DATE = @FiscalWeekEndDate;
		DECLARE @RunDateTime DATETIME2 = GETDATE();

		--
		IF @DebugMode = 1
			BEGIN
				SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(25), GETDATE(), 121) + N' - Starting.';
				RAISERROR (@DebugMsg, 0, 0) WITH NOWAIT;
			END;

		BEGIN TRY;

			DROP TABLE IF EXISTS #FactADPCostingBN;

			CREATE TABLE #FactADPCostingBN (
				[PersonID]					INT						NOT NULL,
				[AccountingFullDate]		DATE					NOT NULL,
				[CostCenterNumber]			VARCHAR(6)				NOT NULL,
				[Company]					VARCHAR(5)				NOT NULL,
				[CenterType]				VARCHAR(4)				NOT NULL,
				[Account]					VARCHAR(4)				NOT NULL,
				[SubAccount]				VARCHAR(6)				NOT NULL,
				[ADPCode]					NVARCHAR(10)			NOT NULL,
				[BatchName]					NVARCHAR(20)			NOT NULL,
				[PayPeriodEndFullDate]		DATE					NOT NULL,
				[EmployeeName]				NVARCHAR(50)			NOT NULL,
				[PayGroup]					VARCHAR(3)				NOT NULL,
				[JobCode]					VARCHAR(3)				NOT NULL,
				[JobName]					VARCHAR(700)			NOT NULL,
				[JobEntryDate]				DATE					NOT NULL,
				[CheckDate]					DATE					NOT NULL,
				[CheckNumber]				BIGINT					NOT NULL,
				[CostHome]					VARCHAR(9)				NOT NULL,
				[CostWorked]				VARCHAR(9)				NOT NULL,
				[ElementCode]				VARCHAR(5)				NOT NULL,
				[FileNumber]				INT						NOT NULL,
				[HomeDepartment]			VARCHAR(6)				NOT NULL,
				[JournalName]				NVARCHAR(20)			NOT NULL,
				[JournalDescription]		NVARCHAR(20)			NOT NULL,
				[JournalLineDescription]	NVARCHAR(50)			NOT NULL,
				[JournalCategory]			NVARCHAR(10)			NOT NULL,
				[JournalSource]				NVARCHAR(7)				NOT NULL,
				[RunFullDate]				DATE					NOT NULL,
				[TaxCode]					NVARCHAR(10)			NOT NULL,
				[UnitofMeasure]				VARCHAR(5)				NOT NULL,
				[MeasureName]				VARCHAR(300)			NOT NULL,
				[DataScenarioName]			VARCHAR(100)			NOT NULL,
				[Value1]					NUMERIC(19, 4)			NOT NULL,
				[Value2]					NUMERIC(19, 4)			NULL,
				[InsertAuditID]				BIGINT					NOT NULL,
				[InsertedDate]				DATETIME2				NOT NULL,
			);

			DROP TABLE IF EXISTS [CHKDATADump];
			CREATE TABLE [CHKDATADump] (
				[file_number] INT,
				[payroll_pe_date2] DATE,
				[paygroup] VARCHAR(3),
				[check_nbr] BIGINT,
				[CreditDebitIndicator] TINYINT,
				[RowNumber] INT
			);
			WITH [CTEGL] AS (
				SELECT DISTINCT
					[file_number], [payroll_pe_date2],
					CASE
						WHEN [debit_amount] IS NULL THEN 1
						WHEN [credit_amount] IS NULL THEN 2
						ELSE 0
					END AS [CreditDebitIndicator]
				FROM [dbo].[ADP_GL_B10]
				WHERE [payroll_pe_date2] >= @FiscalWeekStartDate AND [payroll_pe_date2] <= @FiscalWeekEndDate
				AND [IsBaseDeleted] = 0
			)
			INSERT INTO [CHKDATADump] (
				[file_number], [payroll_pe_date2], [paygroup], [check_nbr], [CreditDebitIndicator], [RowNumber]
			)
			SELECT
				[CTEGL].[file_number],
				[CTEGL].[payroll_pe_date2],
				ISNULL([CHKDATA].[paygroup], 'N/A') AS [paygroup],
				ISNULL([CHKDATA].[check_nbr], -1) AS [check_nbr],
				[CTEGL].[CreditDebitIndicator],
				ROW_NUMBER() OVER (
					PARTITION BY [CTEGL].[file_number], [CTEGL].[payroll_pe_date2], [CTEGL].[CreditDebitIndicator]
					ORDER BY [CHKDATA].[paygroup], [CHKDATA].[check_nbr]
				) AS [RowNumber]
			FROM [dbo].[ADP_PS_AL_CHK_DATA_B10] [CHKDATA]
			RIGHT OUTER JOIN [CTEGL]
				ON [CHKDATA].[file_nbr] = [CTEGL].[file_number]
				AND [CHKDATA].[pay_end_dt] = [CTEGL].[payroll_pe_date2]
				AND [CHKDATA].[entry_nbr] > 0;
			
			INSERT INTO #FactADPCostingBN (
				[PersonID]
				,[AccountingFullDate]
				,[CostCenterNumber]
				,[Company]
				,[CenterType]
				,[Account]
				,[SubAccount]
				,[ADPCode]
				,[BatchName]
				,[PayPeriodEndFullDate]
				,[EmployeeName]
				,[PayGroup]
				,[JobCode]
				,[JobName]
				,[JobEntryDate]
				,[CheckDate]
				,[CheckNumber]
				,[CostHome]
				,[CostWorked]
				,[ElementCode]
				,[FileNumber]
				,[HomeDepartment]
				,[JournalName]
				,[JournalDescription]
				,[JournalLineDescription]
				,[JournalCategory]
				,[JournalSource]
				,[RunFullDate]
				,[TaxCode]
				,[UnitofMeasure]
				,[MeasureName]
				,[DataScenarioName]
				,[Value1]
				,[Value2]
				,[InsertAuditID]
				,[InsertedDate]
			)
			SELECT DISTINCT
				ISNULL([PBN].[PersonID], -1) AS [PersonID]
				,[GL].[accounting_date]
				,[GL].[cost_center]
				,[GL].[company]
				,[GL].[center_type]
				,[GL].[account]
				,[GL].[sub_account]
				,[GL].[journal_description2]
				,[GL].[payroll_batch]
				,[GL].[payroll_pe_date2]
				,[GL].[employee_name]
				,ISNULL([CHKDATA].[paygroup], '') AS [paygroup]
				,ISNULL([EMPJOB].[jobcode], '') AS [jobcode]
				,ISNULL([EMPJOB].[name], '') AS [name]
				,ISNULL([EMPJOB].[job_entry_dt], '1900-01-01') AS [job_entry_dt]
				,[GL].[check_date]
				,ISNULL([CHKDATA].[check_nbr], 0) AS [check_nbr]
				,[GL].[home_cost_number]
				,[GL].[cost_number_worked]
				,[GL].[pay_element]
				,[GL].[file_number]
				,[GL].[dept]
				,[GL].[payroll_pe_date1]
				,[GL].[journal_description1]
				,[GL].[description]
				,[GL].[journal_category]
				,[GL].[journal_source]
				,[GL].[run_date]
				,[GL].[code]
				,[GL].[currency_code]
				,'Credit' AS [MeasureName]
				,'Actual' AS [DataScenarioName]
				,[GL].[credit_amount]
				,NULL
				,@AuditID AS [InsertAuditID]
				,@RunDateTime AS [InsertedDate]
			FROM [dbo].[ADP_GL_B10] [GL]
			LEFT OUTER JOIN [dbo].[PersonBN] [PBN]
				ON [PBN].[EmployeeNumber] = CONVERT(VARCHAR(16), [GL].[file_number])
				AND [PBN].[EffectiveStartDate] <= [GL].[payroll_pe_date2]
				AND [PBN].[ExpirationDate] >= [GL].[payroll_pe_date2]
			LEFT OUTER JOIN [CHKDATADump] [CHKDATA]
				ON [GL].[file_number] = [CHKDATA].[file_number]
				AND [GL].[payroll_pe_date2] = [CHKDATA].[payroll_pe_date2]
				AND [CHKDATA].[CreditDebitIndicator] = 1
				AND [CHKDATA].[RowNumber] = 1
			LEFT OUTER JOIN [dbo].[ADP_GL_JOB_B0] [EMPJOB]
				ON [GL].[file_number] = [EMPJOB].[file_number]
				AND [GL].[payroll_pe_date2] = [EMPJOB].[payroll_pe_date2]
			WHERE [GL].[payroll_pe_date2] >= @FiscalWeekStartDate AND [GL].[payroll_pe_date2] <= @FiscalWeekEndDate
			AND [GL].[debit_amount] IS NULL
			AND [GL].[IsBaseDeleted] = 0;

			INSERT INTO #FactADPCostingBN (
				[PersonID]
				,[AccountingFullDate]
				,[CostCenterNumber]
				,[Company]
				,[CenterType]
				,[Account]
				,[SubAccount]
				,[ADPCode]
				,[BatchName]
				,[PayPeriodEndFullDate]
				,[EmployeeName]
				,[PayGroup]
				,[JobCode]
				,[JobName]
				,[JobEntryDate]
				,[CheckDate]
				,[CheckNumber]
				,[CostHome]
				,[CostWorked]
				,[ElementCode]
				,[FileNumber]
				,[HomeDepartment]
				,[JournalName]
				,[JournalDescription]
				,[JournalLineDescription]
				,[JournalCategory]
				,[JournalSource]
				,[RunFullDate]
				,[TaxCode]
				,[UnitofMeasure]
				,[MeasureName]
				,[DataScenarioName]
				,[Value1]
				,[Value2]
				,[InsertAuditID]
				,[InsertedDate]
			)
			SELECT DISTINCT
				ISNULL([PBN].[PersonID], -1) AS [PersonID]
				,[GL].[accounting_date]
				,[GL].[cost_center]
				,[GL].[company]
				,[GL].[center_type]
				,[GL].[account]
				,[GL].[sub_account]
				,[GL].[journal_description2]
				,[GL].[payroll_batch]
				,[GL].[payroll_pe_date2]
				,[GL].[employee_name]
				,ISNULL([CHKDATA].[paygroup], '') AS [paygroup]
				,ISNULL([EMPJOB].[jobcode], '') AS [jobcode]
				,ISNULL([EMPJOB].[name], '') AS [name]
				,ISNULL([EMPJOB].[job_entry_dt], '1900-01-01') AS [job_entry_dt]
				,[GL].[check_date]
				,ISNULL([CHKDATA].[check_nbr], 0) AS [check_nbr]
				,[GL].[home_cost_number]
				,[GL].[cost_number_worked]
				,[GL].[pay_element]
				,[GL].[file_number]
				,[GL].[dept]
				,[GL].[payroll_pe_date1]
				,[GL].[journal_description1]
				,[GL].[description]
				,[GL].[journal_category]
				,[GL].[journal_source]
				,[GL].[run_date]
				,[GL].[code]
				,[GL].[currency_code]
				,'Debit' AS [MeasureName]
				,'Actual' AS [DataScenarioName]
				,[GL].[debit_amount]
				,NULL
				,@AuditID AS [InsertAuditID]
				,@RunDateTime AS [InsertedDate]
			FROM [dbo].[ADP_GL_B10] [GL]
			LEFT OUTER JOIN [dbo].[PersonBN] [PBN]
				ON [PBN].[EmployeeNumber] = CONVERT(VARCHAR(16), [GL].[file_number])
				AND [PBN].[EffectiveStartDate] <= [GL].[payroll_pe_date2]
				AND [PBN].[ExpirationDate] >= [GL].[payroll_pe_date2]
			LEFT OUTER JOIN [CHKDATADump] [CHKDATA]
				ON [GL].[file_number] = [CHKDATA].[file_number]
				AND [GL].[payroll_pe_date2] = [CHKDATA].[payroll_pe_date2]
				AND [CHKDATA].[CreditDebitIndicator] = 2
				AND [CHKDATA].[RowNumber] = 1
			LEFT OUTER JOIN [dbo].[ADP_GL_JOB_B0] [EMPJOB]
				ON [GL].[file_number] = [EMPJOB].[file_number]
				AND [GL].[payroll_pe_date2] = [EMPJOB].[payroll_pe_date2]
			WHERE [GL].[payroll_pe_date2] >= @FiscalWeekStartDate AND [GL].[payroll_pe_date2] <= @FiscalWeekEndDate
			AND [GL].[credit_amount] IS NULL
			AND [GL].[IsBaseDeleted] = 0;

			SELECT * FROM #FactADPCostingBN;

			DROP TABLE IF EXISTS #FactADPCostingBN;
			DROP TABLE IF EXISTS [CHKDATADump];

		END TRY

		BEGIN CATCH
			IF (@@TRANCOUNT > 0)
				ROLLBACK TRAN;

			--
			-- Raiserror
			--	
			DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
			SELECT @ErrMsg = ERROR_MESSAGE(),
				@ErrSeverity = ERROR_SEVERITY();
			RAISERROR(@ErrMsg, @ErrSeverity, 1);

		END CATCH;
	END