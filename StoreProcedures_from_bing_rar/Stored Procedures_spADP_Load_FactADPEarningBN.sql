CREATE PROCEDURE [dbo].[spADP_Load_FactADPEarningBN]
	@AuditID BIGINT,
	@FiscalWeekStartDate DATE,
	@FiscalWeekEndDate DATE,
	@DebugMode INT = NULL
AS
	BEGIN  
 		-- ================================================================================
		-- 
		-- Stored Procedure:   spADP_Load_FactADPEarningBN
		--
		-- Purpose:            Fetches records from [dbo].[ADP_PS_AL_CHK_DATA_B10], [dbo].[ADP_PS_AL_CHK_DED_B10],
		--					   [dbo].[ADP_PS_AL_CHK_HRS_ERN_B10],[dbo].[ADP_PS_AL_PG_ERN_DATA_B10]  
		--					   for insert into FactADPEarningBN 
		--
		-- Parameters:         @AuditID-Newly Generated AudutID
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
		-- Date				Modified By			Comments
		-- ----				-----------			--------
		-- 08/01/2023		Aniket				 BI-8933 Stored Procedure to Generate Data For Table [dbo].[FactADPEarningBN]
		--09/06/2023        Aniket               BI-10214  Base BN - Earnings Fact (SQL Procedure Only) - Change Procedure Logic
		-- 01/30/2024		Yuvaraj				 DFTP-1248 UAT - Regular Pay Should be REG not RG
		-- ================================================================================
		SET  NOCOUNT ON;
  
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

			DROP TABLE IF EXISTS #FactADPEarningBN;
			CREATE TABLE #FactADPEarningBN
			(
				[PersonId] [int] NULL, 
				[CostCenterNumber] [varchar](6) NULL, 
				[AssignmentId] [int] NOT NULL,
				[PositionID] [int] NULL,
				[CheckFullDate] [date] NOT NULL, 
				[CostNumber] [varchar](9) NULL, 
				[CheckNumber] [bigint] NOT NULL, 
				[FileNumber] [int] NOT NULL, 
				[EntryNumber] [tinyint] NOT NULL,
				[RowNumber] [tinyint] NOT NULL,
				[PayGroup] [varchar](3) NOT NULL, 
				[PayPeriodEndFullDate] [date] NOT NULL, 
				[MeasureName] [varchar](300) NOT NULL, 
				[DataScenarioName] [varchar](100) NOT NULL, 
				[Code] [varchar](3) NULL, 
				[Description] [Nvarchar](30) NULL, 
				[Value1] [NUMERIC](19, 4) NULL, 
				[Value2] [NUMERIC](19, 4) NULL, 
				[InsertAuditId] [bigint] NOT NULL, 
				[InsertedDate] [datetime2] NULL
			);
			
			DROP TABLE IF EXISTS #ADP_PS_AL_CHK_DATA_B10;
			CREATE TABLE #ADP_PS_AL_CHK_DATA_B10
			(	[paygroup] [varchar](3) NOT NULL,
				[file_nbr] [int] NOT NULL,
				[check_dt] [date] NOT NULL,
				[week_nbr] [tinyint] NOT NULL,
				[payroll_nbr] [tinyint] NOT NULL,
				[check_nbr] [bigint] NOT NULL,
				[entry_nbr] [tinyint] NOT NULL,
				[emplid] [int] NOT NULL,
				[pay_end_dt] [date] NOT NULL,
				[ck_gross] [numeric](11, 2) NULL,
				[cost_num] [varchar](9) NULL,
				[home_department] [varchar](6) NULL,
				[total_hrs_worked] [numeric](6, 2) NULL
			);

			INSERT INTO #ADP_PS_AL_CHK_DATA_B10 (
				[paygroup],
				[file_nbr],
				[check_dt],
				[week_nbr],
				[payroll_nbr],
				[check_nbr],
				[entry_nbr],
				[emplid],
				[pay_end_dt],
				[ck_gross],
				[cost_num],
				[home_department],
				[total_hrs_worked]
			)
			SELECT
				[paygroup],
				[file_nbr],
				[check_dt],
				[week_nbr],
				[payroll_nbr],
				[check_nbr],
				[entry_nbr],
				[emplid],
				[pay_end_dt],
				[ck_gross],
				[cost_num],
				[home_department],
				[total_hrs_worked]  
			FROM [dbo].[ADP_PS_AL_CHK_DATA_B10] 
			WHERE [entry_nbr] > 0 
			And [pay_end_dt]  >= @FiscalWeekStartDate  and [pay_end_dt]<=  @FiscalWeekEndDate;
			--AND [pay_end_dt] = @FiscalWeekEndDate;

			-- ALEarningHours
			INSERT INTO #FactADPEarningBN (
				[PersonID],[CostCenterNumber],[AssignmentID],[PositionID] ,[CheckFullDate], 
				[CheckNumber],[FileNumber],  [EntryNumber], [RowNumber], [paygroup], [PayPeriodEndFullDate], [MeasureName], 
				[DataScenarioName], [Code], [Description], [Value1], [Value2], [InsertAuditID], [InsertedDate]
			)
			SELECT DISTINCT
				ISNULL([PBN].[PersonID], -1) AS [PersonID],
				ISNULL([DT].[home_department], -1) AS [CostCenterNumber],
				ISNULL([ABN].[AssignmentID], -1) AS [AssignmentID],[ABN].[PositionID],
				[DT].[check_dt] AS [CheckFullDate],
				--ISNULL([DT].[cost_num], -1) AS [CostNumber],
				[DT].[check_nbr] AS [CheckNumber],
				[DT].[file_nbr] AS [FileNumber],
				[DT].[entry_nbr] AS [EntryNumber],
				[ERN].[row_nbr]  AS [RowNumber],
				[DT].[paygroup],
				[DT].[pay_end_dt] AS [PayPeriodEndFullDate],
				'ALEarningHours' AS [MeasureName],
				'Actual' AS [DataScenarioName],
				CASE
					WHEN [ERN].[row_nbr] = 1 AND [ERN].[al_hours] IS NOT NULL THEN 'REG'
					WHEN [ERN].[row_nbr] = 2 AND [ERN].[al_hours] IS NOT NULL THEN 'OT'
					ELSE [ERN].[erncd]
				END AS [Code],
				CASE
					WHEN [ERN].[row_nbr] = 1 AND [ERN].[al_hours] IS NOT NULL THEN 'Regular'
					WHEN [ERN].[row_nbr] = 2 AND [ERN].[al_hours] IS NOT NULL THEN 'OverTime'
					ELSE [ERNDT].[descr15]
				END AS [Description],
				[ERN].[al_hours] AS [Value1],
				NULL AS [Value2],
				@AuditID AS [InsertAuditId],
				@RunDateTime AS [InsertedDate]
			FROM #ADP_PS_AL_CHK_DATA_B10 [DT]
			LEFT JOIN [dbo].[ADP_PS_AL_CHK_HRS_ERN_B10] [ERN]
				ON [DT].[paygroup] = [ERN].[paygroup]
				AND [DT].[file_nbr] = [ERN].[file_nbr]
				AND [DT].[emplid] = [ERN].[emplid]
				AND [DT].[check_dt] = [ERN].[check_dt]
				AND [DT].[check_nbr] = [ERN].[check_nbr]
				AND [DT].[payroll_nbr] = [ERN].[payroll_nbr]
				AND [DT].[week_nbr] = [ERN].[week_nbr]
				AND [DT].[entry_nbr] = [ERN].[entry_nbr]
			LEFT JOIN [dbo].[ADP_PS_AL_PG_ERN_DATA_B10] [ERNDT]
				ON [ERN].[paygroup] = [ERNDT].[paygroup]
				AND [ERN].[erncd] = [ERNDT].[erncd]
				AND [ERNDT].[effdtfrom] <= [DT].[pay_end_dt]
				AND [ERNDT].[effdtto] >= [DT].[pay_end_dt]
			LEFT JOIN [dbo].[PersonBN] [PBN]
				ON [PBN].[EmployeeNumber] = CONVERT(VARCHAR(16), [DT].[emplid])
				AND [BaseEffectiveDate] <= [DT].[pay_end_dt]
				AND [BaseEndDate] >= [DT].[pay_end_dt]
			LEFT JOIN [dbo].[AssignmentBN] [ABN]
				ON [ABN].[PersonID] = [PBN].[PersonID]
				AND [ABN].[BaseEffectiveDate] <= [DT].[pay_end_dt]
				AND [ABN].[BaseEndDate] >= [DT].[pay_end_dt] 
			WHERE [ERN].[row_nbr] > 0 AND [ERN].[al_hours] <> 0.0000 
			AND [DT].[pay_end_dt]  >= @FiscalWeekStartDate  AND [DT].[pay_end_dt] <=  @FiscalWeekEndDate;

			-- CheckGross
			INSERT INTO #FactADPEarningBN (
				[PersonID],[CostCenterNumber],[AssignmentID],[PositionID],[CheckFullDate], 
				[CheckNumber], [FileNumber], [EntryNumber], [RowNumber], [paygroup], [PayPeriodEndFullDate], [MeasureName], 
				[DataScenarioName], [Code], [Description], [Value1], [Value2], [InsertAuditID], [InsertedDate]
			)
			SELECT DISTINCT
				ISNULL([PBN].[PersonID], -1) AS [PersonID],
				ISNULL([DT].[home_department], -1) AS [CostCenterNumber],
				ISNULL([ABN].[AssignmentID], -1) AS [AssignmentID],[ABN].[PositionID],
				[DT].[check_dt] AS [CheckFullDate],
			--	ISNULL([DT].[cost_num], -1) AS [CostNumber],
				[DT].[check_nbr] AS [CheckNumber],
			 	[DT].[file_nbr] AS [FileNumber],
				[DT].[entry_nbr] AS [EntryNumber],
				0 AS [RowNumber],
				[DT].[paygroup],
				[DT].[pay_end_dt] AS [PayPeriodEndFullDate],
				'CheckGross' AS [MeasureName],
				'Actual' AS [DataScenarioName],
				NULL AS [Code],
				NULL AS [Description],
				[DT].[ck_gross] AS [Value1],
				NULL AS [Value2],
				@AuditID AS [InsertAuditId],
				@RunDateTime AS [InsertedDate]
			FROM #ADP_PS_AL_CHK_DATA_B10 [DT]
			LEFT JOIN [dbo].[PersonBN] [PBN]
				ON [PBN].[EmployeeNumber] = CONVERT(VARCHAR(16), [DT].[emplid])
				AND [BaseEffectiveDate] <= [DT].[pay_end_dt]
				AND [BaseEndDate] >= [DT].[pay_end_dt]
			LEFT JOIN [dbo].[AssignmentBN] [ABN]
				ON [ABN].[PersonID] = [PBN].[PersonID]
				AND [ABN].[BaseEffectiveDate] <= [DT].[pay_end_dt]
				AND [ABN].[BaseEndDate] >= [DT].[pay_end_dt] AND [DT].[ck_gross]<>0.0000;
 
  
			-- Deductions
			INSERT INTO #FactADPEarningBN (
				[PersonID],[CostCenterNumber],[AssignmentID],[PositionID],[CheckFullDate],  
				[CheckNumber],[FileNumber],  [EntryNumber],[RowNumber], [paygroup], [PayPeriodEndFullDate], [MeasureName], 
				[DataScenarioName], [Code], [Description], [Value1], [Value2], [InsertAuditID], [InsertedDate]
			)
			SELECT DISTINCT
				ISNULL([PBN].[PersonID], -1) AS [PersonID],
				ISNULL([DT].[home_department], -1) AS [CostCenterNumber],
				ISNULL([ABN].[AssignmentID], -1) AS [AssignmentID],[ABN].[PositionID],
				[DT].[check_dt] AS [CheckFullDate],
				--ISNULL([DT].[cost_num], -1) AS [CostNumber],
				[DT].[check_nbr] AS [CheckNumber],
				[DT].[file_nbr] AS [FileNumber],
				[DT].[entry_nbr] AS [EntryNumber],
				[DED].[row_nbr] AS [RowNumber],
				[DT].[paygroup],
				[DT].[pay_end_dt] AS [PayPeriodEndFullDate],
				'Deduction' AS [MeasureName],
				'Actual' AS [DataScenarioName],
				[DED].[dedcd] AS [Code], 
				[DED].[al_descr] AS [Description],
				[DED].[al_amount] AS [Value1],
				NULL AS [Value2],
				@AuditID AS [InsertAuditId],
				@RunDateTime AS [InsertedDate]
			FROM #ADP_PS_AL_CHK_DATA_B10 [DT]
			LEFT JOIN [dbo].[ADP_PS_AL_CHK_DED_B10] [DED]
				ON [DT].[paygroup] = [DED].[paygroup]
				AND [DT].[file_nbr] = [DED].[file_nbr]
				AND [DT].[emplid] = [DED].[emplid]
				AND [DT].[check_dt] = [DED].[check_dt]
				AND [DT].[check_nbr] = [DED].[check_nbr]
				AND [DT].[payroll_nbr] = [DED].[payroll_nbr]
				AND [DT].[week_nbr] = [DED].[week_nbr]
				AND [DT].[entry_nbr] = [DED].[entry_nbr]
			LEFT JOIN [dbo].[PersonBN] [PBN]
				ON [PBN].[EmployeeNumber] = CONVERT(VARCHAR(16), [DT].[emplid])
				AND [BaseEffectiveDate] <= [DT].[pay_end_dt]
				AND [BaseEndDate] >= [DT].[pay_end_dt]
			LEFT JOIN [dbo].[AssignmentBN] [ABN]
				ON [ABN].[PersonID] = [PBN].[PersonID]
				AND [ABN].[BaseEffectiveDate] <= [DT].[pay_end_dt]
				AND [ABN].[BaseEndDate] >= [DT].[pay_end_dt]
			WHERE [DED].[row_nbr] > 0  AND [DED].[al_amount] <> 0.0000
			AND [DT].[pay_end_dt]  >= @FiscalWeekStartDate  AND [DT].[pay_end_dt] <=  @FiscalWeekEndDate;

			-- Earnings
			INSERT INTO #FactADPEarningBN (
				[PersonID],[CostCenterNumber],[AssignmentID], [PositionID],[CheckFullDate], 
				[CheckNumber],[FileNumber],  [EntryNumber], [RowNumber], [paygroup], [PayPeriodEndFullDate], [MeasureName], 
				[DataScenarioName], [Code], [Description], [Value1], [Value2], [InsertAuditID], [InsertedDate]
			)
			SELECT DISTINCT
				ISNULL([PBN].[PersonID], -1) AS [PersonID],
				ISNULL([DT].[home_department], -1) AS [CostCenterNumber],
				ISNULL([ABN].[AssignmentID], -1) AS [AssignmentID],[ABN].[PositionID],
				[DT].[check_dt] AS [CheckFullDate],
				--ISNULL([DT].[cost_num], -1) AS [CostNumber],
				[DT].[check_nbr] AS [CheckNumber],
				[DT].[file_nbr] AS [FileNumber],
				[DT].[entry_nbr] AS [EntryNumber],
				[ERN].[row_nbr] AS [RowNumber],
				[DT].[paygroup],
				[DT].[pay_end_dt] AS [PayPeriodEndFullDate],
				'Earning' AS [MeasureName],
				'Actual' AS [DataScenarioName],
				CASE
					WHEN [ERN].[row_nbr] = 1 AND [ERN].[al_hours] IS NOT NULL THEN 'REG'
					WHEN [ERN].[row_nbr] = 2 AND [ERN].[al_hours] IS NOT NULL THEN 'OT'
					ELSE [ERN].[erncd]
				END AS [Code],
				CASE
					WHEN [ERN].[row_nbr] = 1 AND [ERN].[al_hours] IS NOT NULL THEN 'Regular'
					WHEN [ERN].[row_nbr] = 2 AND [ERN].[al_hours] IS NOT NULL THEN 'OverTime'
					ELSE [ERNDT].[descr15]
				END AS [Description],
				[ERN].[earnings] AS [Value1],
				NULL AS [Value2],
				@AuditID AS [InsertAuditId],
				@RunDateTime AS [InsertedDate]
			FROM #ADP_PS_AL_CHK_DATA_B10 [DT]
			LEFT JOIN [dbo].[ADP_PS_AL_CHK_DED_B10] [DED]
				ON [DT].[paygroup] = [DED].[paygroup]
				AND [DT].[file_nbr] = [DED].[file_nbr]
				AND [DT].[emplid] = [DED].[emplid]
				AND [DT].[check_dt] = [DED].[check_dt] 
				AND [DT].[check_nbr] = [DED].[check_nbr] 
				AND [DT].[payroll_nbr] = [DED].[payroll_nbr] 
				AND [DT].[week_nbr] = [DED].[week_nbr]
				AND [DT].[entry_nbr] = [DED].[entry_nbr]
			LEFT JOIN [dbo].[ADP_PS_AL_CHK_HRS_ERN_B10] [ERN]
				ON [DT].[paygroup] = [ERN].[paygroup]
				AND [DT].[file_nbr] = [ERN].[file_nbr]
				AND [DT].[emplid] = [ERN].[emplid]
				AND [DT].[check_dt] = [ERN].[check_dt]
				AND [DT].[check_nbr] = [ERN].[check_nbr]
				AND [DT].[payroll_nbr] = [ERN].[payroll_nbr]
				AND [DT].[week_nbr] = [ERN].[week_nbr]
				AND [DT].[entry_nbr] = [ERN].[entry_nbr]
			LEFT JOIN [dbo].[ADP_PS_AL_PG_ERN_DATA_B10] [ERNDT]
				ON [ERN].[paygroup] = [ERNDT].[paygroup]
				AND [ERN].[erncd] = [ERNDT].[erncd]
				AND [ERNDT].[effdtfrom] <= [DT].[pay_end_dt]
				AND [ERNDT].[effdtto] >= [DT].[pay_end_dt]
			LEFT JOIN [dbo].[PersonBN] [PBN]
				ON [PBN].[EmployeeNumber] = CONVERT(VARCHAR(16), [DT].[emplid])
				AND [BaseEffectiveDate] <= [DT].[pay_end_dt]
				AND [BaseEndDate] >= [DT].[pay_end_dt]
			LEFT JOIN [dbo].[AssignmentBN] [ABN]
				ON [ABN].[PersonID] = [PBN].[PersonID]
				AND [ABN].[BaseEffectiveDate] <= [DT].[pay_end_dt]
				AND [ABN].[BaseEndDate] >= [DT].[pay_end_dt]
			WHERE [ERN].[row_nbr] > 0   AND [ERN].[earnings] <> 0.0000
			AND [DT].[pay_end_dt]  >= @FiscalWeekStartDate  AND [DT].[pay_end_dt] <=  @FiscalWeekEndDate;

			SELECT TEMP.[PersonId]  , 
				TEMP.[CostCenterNumber]  , 
				TEMP.[AssignmentId]  , 
				TEMP.[PositionID],
				TEMP.[CheckFullDate]  , 
			    TEMP.[CostNumber]  , 
				MAX(TEMP.[proposed_salary_n]),
				TEMP.[CheckNumber]  , 
				TEMP.[FileNumber] , 
				TEMP.[EntryNumber] ,
				TEMP.[RowNumber]  ,
				TEMP.[PayGroup]  , 
				TEMP.[PayPeriodEndFullDate] , 
				TEMP.[MeasureName] , 
				TEMP.[DataScenarioName]  , 
				TEMP.[Code]  , 
				TEMP.[Description]  , 
				Sum(TEMP.[Value1]) , 
				TEMP.[Value2]  , 
				TEMP.[InsertAuditId]   , 
				TEMP.[InsertedDate]  FROM 
			(SELECT DISTINCT  [E].[PersonId]  , 
				[E].[CostCenterNumber]  , 
				[E].[AssignmentId]  , 
				[E].[PositionID],
				[E].[CheckFullDate]  , 
				ISNULL([E].[CostNumber], -1) as [CostNumber]  , 
				[PR].[proposed_salary_n],
				[E].[CheckNumber]  , 
				[E].[FileNumber] , 
				[E].[EntryNumber] ,
				[E].[RowNumber]  ,
				[E].[PayGroup]  , 
				[E].[PayPeriodEndFullDate] , 
				[E].[MeasureName] , 
				[E].[DataScenarioName]  , 
				[E].[Code]  , 
				[E].[Description]  , 
				[E].[Value1] , 
				[E].[Value2]  , 
				[E].[InsertAuditId]   , 
				[E].[InsertedDate]  FROM #FactADPEarningBN [E]
			LEFT JOIN [dbo].[PER_PAY_PROPOSALS_B0] [PR]
				ON [E].[AssignmentID] = [PR].[assignment_id]
			AND [E].[PayPeriodEndFullDate] >= [PR].[change_date]
			AND [E].[PayPeriodEndFullDate] <= [PR].[date_to]
		    AND [E].[Value1]<>0.0000
			) TEMP 
			GROUP BY  TEMP.[PersonId]  , 
				TEMP.[CostCenterNumber]  , 
				TEMP.[AssignmentId]  , 
				TEMP.[PositionID],
				TEMP.[CheckFullDate]  , 
			    TEMP.[CostNumber]  , 
				TEMP.[CheckNumber]  , 
				TEMP.[FileNumber] , 
				TEMP.[EntryNumber] ,
				TEMP.[RowNumber]  ,
				TEMP.[PayGroup]  , 
				TEMP.[PayPeriodEndFullDate] , 
				TEMP.[MeasureName] , 
				TEMP.[DataScenarioName]  , 
				TEMP.[Code]  , 
				TEMP.[Description]  , 
				TEMP.[Value2]  , 
				TEMP.[InsertAuditId]   , 
				TEMP.[InsertedDate] 
				HAVING SUM(TEMP.[Value1])<>0.0000
			DROP TABLE IF EXISTS #FactADPEarningBN;
			DROP TABLE IF EXISTS #ADP_PS_AL_CHK_DATA_B10;
		
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