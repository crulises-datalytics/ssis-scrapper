CREATE PROCEDURE [dbo].[spFactEarningLoad]
	@InsertCount INT OUTPUT
AS
	BEGIN
		-- ================================================================================
			-- 
			-- Stored Procedure:   spFactEarningLoad
			--
			-- Purpose:            Inserting earning from [DW_Landing].[FactADPEarningBN]
			--                     to main table [DW_Mart].[dbo].[FactEarning] table.
			--
			-- Parameters:		   n/a
			--
			-- Usage:              exec dbo.spFactEarningLoad
			--
			-- --------------------------------------------------------------------------------
			--
			-- Change Log:		   
			-- ----------
			--
			-- Date				Modified By			Comments
			-- ----------		-----------			--------
			--
			-- 20231004			Yuvaraj Mane		BI-9119 - Initial version
			-- 20240112			Yuvaraj Mane		DFTP-1047 - Modify Payroll fact tables to update Person Keys in fact when there is an incremental change in DimPerson			 
			-- ================================================================================
			INSERT INTO [dbo].[FactEarning]
			   ([EarningsKey]
			   ,[PersonKey]
			   ,[PersonID]
			   ,[CostCenterKey]
			   ,[CostCenterNumber]
			   ,[AssignmentKey]
			   ,[AssignmentID]
			   ,[PositionKey]
			   ,[PositionID]
			   ,[CheckDateKey]
			   ,[CheckDate]
			   ,[CostNumber]
			   ,[Proposed_Salary_N]
			   ,[CheckNumber]
			   ,[FileNumber]
			   ,[EntryNumber]
			   ,[RowNumber]
			   ,[PayGroup]
			   ,[PayPeriodEndDateKey]
			   ,[FiscalWeekEndDateKey]
			   ,[MeasureKey]
			   ,[DataScenarioKey]
			   ,[Code]
			   ,[Description]
			   ,[Value1]
			   ,[Value2]
			   ,[InsertAuditId]
			   ,[InsertedDate]
			   )

			SELECT 
		  		[SRC].[EarningsKey]
				,-1 AS [PersonKey]
				,[SRC].[PersonID]
				,ISNULL([DIM_CCenter].[CostCenterKey], -1) AS [CostCenterKey]
				,[SRC].[CostCenterNumber]
				,-1 AS [AssignmentKey]
				,[SRC].[AssignmentID]
				,-1 AS [PositionKey]
				,[SRC].[PositionID]
				,ISNULL([CHKDT].[DateKey], -1) AS [CheckDateKey]
				,[SRC].[CheckFullDate] AS [CheckDate]
				,[SRC].[CostNumber]
				,[SRC].[Proposed_Salary_N]
				,[SRC].[CheckNumber]
				,[SRC].[FileNumber]
				,[SRC].[EntryNumber]
				,[SRC].[RowNumber]
				,[SRC].[PayGroup]
				,[FullDT].[DateKey]
				,[FullDT].[FiscalWeekEndDateKey]
				,ISNULL([DIM_MES].[MeasureKey], -1) AS [MeasureKey]
				,ISNULL([DIM_DATASC].[DataScenarioKey], -1) AS [DataScenarioKey]
				,[SRC].[Code]
				,[SRC].[Description]
				,[SRC].[Value1]
				,[SRC].[Value2]
				,[SRC].[InsertAuditId]
				,[SRC].[InsertedDate]
			FROM [DW_Landing].[dbo].[FactADPEarningBN] AS [SRC]
			LEFT JOIN [DW_Mart].[dbo].[DimCostCenter] AS [DIM_CCenter] 
				ON [SRC].[CostCenterNumber] = [DIM_CCenter].[CostCenterNumber]  
				AND [SRC].[PayPeriodEndFullDate] BETWEEN [DIM_CCenter].[EDWEffectiveFrom] AND [DIM_CCenter].[EDWEffectiveTo]
			LEFT JOIN [DW_Mart].[dbo].[DimDate] AS [CHKDT] 
				ON [SRC].[CheckFullDate] = [CHKDT].[FullDate]
			LEFT JOIN [DW_Mart].[dbo].[DimDate] AS [FullDT] 
				ON [SRC].[PayPeriodEndFullDate] = [FullDT].[FullDate]
			LEFT JOIN [DW_Mart].[dbo].[DimMeasure] AS [DIM_MES]
				ON [SRC].[MeasureName] = [DIM_MES].[MeasureName]
			LEFT JOIN [DW_Mart].[dbo].[DimDataScenario] AS [DIM_DATASC] 
				ON [SRC].[DataScenarioName] = [DIM_DATASC].[DataScenarioName];


	SET @InsertCount = @@ROWCOUNT;

	END
GO