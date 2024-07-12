CREATE PROCEDURE [dbo].[spFactCostingLoad]
	@InsertCount		INT OUTPUT,
	@AuditId			BIGINT,
	@InsertDate			DATETIME2
AS
	BEGIN
		-- ================================================================================
		-- 
		-- Stored Procedure:   spFactCostingLoad
		--
		-- Purpose:            Inserting earning from [DW_Landing].[FactADPCostingBN]
		--                     to main table [DW_Mart].[dbo].[FactCosting] table.
		--
		-- Parameters:		   n/a
		--
		-- Usage:              exec dbo.spFactCostingLoad 0, 123, GETDATE()
		--
		-- --------------------------------------------------------------------------------
		--
		-- Change Log:		   
		-- ----------
		--
		-- Date				Modified By			Comments
		-- ----------		-----------			--------
		--
		-- 20231012			Yuvaraj Mane		BI-9118 - Initial version
		-- 20240111		    Aniket Navale       DFTP-1047- Modify Payroll fact tables to update Person Keys in fact when there is an incremental change in DimPerson			 
		-- ================================================================================

		INSERT INTO [dbo].[FactCosting]
			   ([CostingKey]				
				,[PersonKey]					
				,[PersonID]					
				,[AccountingDate]			
				,[CostCenterKey]				
				,[CostCenterNumber]			
				,[Company]					
				,[CenterType]				
				,[AccountSubAccountKey]		
				,[Account]					
				,[SubAccount]				
				,[ADPCode]					
				,[BatchName]					
				,[PayPeriodEndDateKey]		
				,[FiscalWeekEndDateKey]		
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
				,[RunDate]				
				,[TaxCode]					
				,[UnitofMeasure]				
				,[MeasureKey]				
				,[DataScenarioKey]			
				,[Value1]					
				,[Value2]					
				,[InsertAuditID]				
				,[InsertedDate]
			   )

			SELECT 
		  		[SRC].[CostingKey]
				,-1 AS [PersonKey]
				,[SRC].[PersonID]
				,[SRC].[AccountingFullDate]
				,ISNULL([DIM_CCenter].[CostCenterKey], -1) AS [CostCenterKey]
				,[SRC].[CostCenterNumber]
				,[SRC].[Company]
				,[SRC].[CenterType]
				,ISNULL([SubActDT].[AccountSubaccountKey], -1) AS [AccountSubaccountKey]
				,[SRC].[Account]
				,[SRC].[SubAccount]
				,[SRC].[ADPCode]
				,[SRC].[BatchName]
				,[FullDT].[DateKey]
				,[FullDT].[FiscalWeekEndDateKey]
				,[SRC].[EmployeeName]
				,[SRC].[PayGroup]
				,[SRC].[JobCode]
				,[SRC].[JobName]
				,[SRC].[JobEntryDate]
				,[SRC].[CheckDateKey]
				,[SRC].[CheckNumber]
				,[SRC].[CostHome]
				,[SRC].[CostWorked]
				,[SRC].[ElementCode]
				,[SRC].[FileNumber]
				,[SRC].[HomeDepartment]
				,[SRC].[JournalName]
				,[SRC].[JournalDescription]
				,[SRC].[JournalLineDescription]
				,[SRC].[JournalCategory]
				,[SRC].[JournalSource]
				,[SRC].[RunFullDate]
				,[SRC].[TaxCode]
				,[SRC].[UnitofMeasure]
				,ISNULL([DIM_MES].[MeasureKey], -1) AS [MeasureKey]
				,ISNULL([DIM_DATASC].[DataScenarioKey], -1) AS [DataScenarioKey]
				,[SRC].[Value1]
				,[SRC].[Value2]					
				,@AuditId
				,@InsertDate
			FROM [DW_Landing].[dbo].[FactADPCostingBN] AS [SRC]
			LEFT JOIN [DW_Mart].[dbo].[DimCostCenter] AS [DIM_CCenter] 
				ON [SRC].[HomeDepartment] = [DIM_CCenter].[CostCenterNumber]  
				AND [SRC].[PayPeriodEndFullDate] BETWEEN [DIM_CCenter].[EDWEffectiveFrom] AND [DIM_CCenter].[EDWEffectiveTo]
			LEFT JOIN [DW_Mart].[dbo].[DimAccountSubaccount] AS [SubActDT]
				ON [SRC].[Account] = [SubActDT].[AccountID] AND [SRC].[SubAccount] = [SubActDT].[SubAccountID]
			LEFT JOIN [DW_Mart].[dbo].[DimDate] AS [FullDT] 
				ON [SRC].[PayPeriodEndFullDate] = [FullDT].[FullDate]
			LEFT JOIN [DW_Mart].[dbo].[DimMeasure] AS [DIM_MES]
				ON [SRC].[MeasureName] = [DIM_MES].[MeasureName]
			LEFT JOIN [DW_Mart].[dbo].[DimDataScenario] AS [DIM_DATASC] 
				ON [SRC].[DataScenarioName] = [DIM_DATASC].[DataScenarioName];


	SET @InsertCount = @@ROWCOUNT

	END
GO