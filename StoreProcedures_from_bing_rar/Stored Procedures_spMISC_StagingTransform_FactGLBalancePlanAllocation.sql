
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spMISC_StagingTransform_FactGLBalancePlanAllocation'
)
    DROP PROCEDURE dbo.spMISC_StagingTransform_FactGLBalancePlanAllocation;
GO
*/
CREATE PROCEDURE [dbo].[spMISC_StagingTransform_FactGLBalancePlanAllocation] (
	@EDWRunDateTime DATETIME2 = NULL
	,@FiscalPeriodNumber INT
	)
AS
-- ================================================================================
--
-- Stored Procedure:	   spMISC_StagingTransform_FactGLBalancePlanAllocation
--
-- Purpose:		   Performs the transformation logic with the source database
--					  for a given Fact or Dimension table, and returns the
--					  results set to the caller (usually for populating a
--					  temporary table).
--
-- Parameters:		   @EDWRunDateTime
--
-- Returns:		   Results set containing the transformed data ready for
--					  consumption by the ETL process for load into BING_EDW
--
-- Usage:			   INSERT #FactGLBalancePlanAllocationUpsert -- (Temporary table)
--				   EXEC dbo.spMISC_StagingTransform_FactGLBalancePlanAllocation @EDWRunDateTime
--
-- --------------------------------------------------------------------------------
--
-- Change Log:		   
-- ----------
--
-- Date	  		Modified By		Comments
-- --------		-----------		--------
--
-- 11/03/17		sburke			BNG-789  - Refactored EDW FactGLBalancePlanAllocation (LegacyDW Source) load
-- 11/16/17		Bhanu			BNG-866  - CompanyKey is not populating correctly in FactGLBalancePlanallocation(Fixed the join for CompanyID)	
-- 12/07/17		Bhanu			BNG-895  - DimOrganization is SCD2 and it was used as SCD1. Fixed it.	
-- 06/21/18		anmorales		BNG-2711 - EDW - StagingToEDW FactGLBalancePlanAllocation for 2018 - New Plan Allocation Process
-- 11/07/18		anmorales		BNG-4162 - Plan data are not the same between [Fact GL Plan Allocation] vs [Fact GL Balance]
--											Fix an issue that can be caused by the slow changing dimesion
-- 08/15/19     banandesi       BNG-1861 - Duplicate data for company codes.
-- 03/31/2022   banandesi       BNG-5703 - Removed spreading for License Capcity AccountSubaccountID = '0001.000004'
-- 10/17/2022	VishalSawnat	BI-6802 - Remvoed spreading for License Capcity in allocation percentage '0001.000004'
-- ================================================================================
BEGIN
	SET NOCOUNT ON;
	--
	-- Housekeeping Variables
	--
	DECLARE @ProcName NVARCHAR(500) = OBJECT_NAME(@@PROCID);
	DECLARE @DebugMsg NVARCHAR(500);
	DECLARE @RowCount INT;
	--
	-- If we do not get an @EDWRunDateTime input, set to current date
	--
	IF @EDWRunDateTime IS NULL
		SET @EDWRunDateTime = GETDATE();
	--
	-- Execute the extract / transform from the Staging database source
	--
	BEGIN TRY
		IF OBJECT_ID('tempdb..#FactGlBalancePlanAllocation') IS NOT NULL
			DROP TABLE #FactGlBalancePlanAllocation;
		WITH CTE_FactGlBalance
		AS (
			SELECT COALESCE(dm_cctr.CostCenterNumber,'-1') AS CostCenterNumber
				,COALESCE(dm_acc_sub.AccountSubaccountID,'-1') AS AccountSubaccountID
				,COALESCE(gl.period_id,-1) AS FiscalPeriodNumber
				,COALESCE(dm_Mtyp.GLMetricTypeName,'Unknown') AS GLMetricTypeName
				,COALESCE(CASE WHEN dm_Mtyp.GLMetricTypeName = 'Dollars' THEN (-1*dm_acc_sub.AccountTypeUnary) ELSE (1)  END * gl.balance_amt,0) AS GLBalanceAmount
				,COALESCE(dm_org.OrgKey, -1)AS OrgKey
				,COALESCE(dm_loc.LocationKey, -1) AS LocationKey
				,COALESCE(dm_cmp.CompanyKey, -1) AS CompanyKey
				,COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey
				,COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey
				,COALESCE(dm_acc_sub.AccountSubaccountKey, -1) AS AccountSubaccountKey
				,COALESCE(dm_Mtyp.GLMetricTypeKey, -1) AS GLMetricTypeKey
			FROM GL_Staging.dbo.vGLBalances AS gl
			JOIN BING_EDW.dbo.DimDataScenario AS dm_ds ON dm_ds.GLActualFlag = gl.actual_flag
				AND COALESCE(dm_ds.GLBudgetVersionID, - 1) = COALESCE(gl.budget_version_id, - 1)
			LEFT JOIN BING_EDW.dbo.DimCompany AS dm_cmp ON dm_cmp.CompanyID = gl.company_id
			LEFT JOIN BING_EDW.dbo.DimCostCenter AS dm_cctr ON dm_cctr.CostCenterNumber = gl.cost_center_id
				AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
			LEFT JOIN BING_EDW.dbo.DimOrganization AS dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
				AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
			LEFT JOIN BING_EDW.dbo.DimLocation AS dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
				AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
			LEFT JOIN BING_EDW.dbo.DimCostCenterType AS dm_cctyp ON dm_cctyp.CostCenterTypeID = gl.cost_center_type_id
			LEFT JOIN BING_EDW.dbo.DimAccountSubaccount AS dm_acc_sub ON dm_acc_sub.AccountSubaccountID = gl.account_subaccount_id
			LEFT JOIN BING_EDW.dbo.DimGLMetricType AS dm_Mtyp ON dm_Mtyp.GLMetricTypeCode = gl.currency_code
			WHERE [dm_ds].[DataScenarioName] = 'Plan'
				AND [gl].[period_id] = @FiscalPeriodNumber
			)
		SELECT wk.FiscalWeekEndDateKey
			,src.OrgKey
			,src.LocationKey
			,src.CompanyKey
			,src.CostCenterTypeKey
			,src.CostCenterKey
			,src.AccountSubaccountKey
			,src.GLMetricTypeKey
			,src.GLBalanceAmount
			,src.FiscalPeriodNumber
			--===============BI-6802 Changes start ===================================================
			--,pln.AllocationPercentage END AS ProvidedAllocationPercentage
			,CASE WHEN src.AccountSubaccountID = '0001.000004'
			 THEN 1
			 ELSE pln.AllocationPercentage END AS ProvidedAllocationPercentage
			 --=============== BI-6802 Changes End  ===================================================
			,CASE WHEN src.AccountSubaccountID = '0001.000004'
			 THEN 1
			 ELSE
			 CONVERT(NUMERIC(19, 8), 1.0 / MAX(wk.FiscalWeekOfPeriodNumber) OVER (PARTITION BY wk.FiscalPeriodNumber))
			 END AS DynamicAllocationPercentage
			,ISNULL(SUM(pln.AllocationPercentage) OVER (
					PARTITION BY wk.FiscalPeriodNumber
					,src.CostCenterNumber
					,src.AccountSubAccountID
					,src.GLMetricTypeName
					), 0) AS ProvidedPeriodAllocation
			,CONVERT(NUMERIC(19, 8), NULL) AS GLBalancePlanAllocationAmount
		INTO #FactGlBalancePlanAllocation
		FROM CTE_FactGlBalance AS src
		JOIN BING_EDW.dbo.vDimFiscalWeek AS wk ON wk.FiscalPeriodNumber = src.FiscalPeriodNumber
		LEFT JOIN dbo.WeeklyPlanAllocationCurrent AS pln ON pln.CostCenterNumber = src.CostCenterNumber
			AND pln.AccountSubAccountID = src.AccountSubaccountID
			AND pln.FiscalWeekNumber = wk.FiscalWeekNumber
			AND pln.GLMetricTypeName = src.GLMetricTypeName;
		-- Set the GLBalancePlanAllocationAmount when we can set it correctly
		UPDATE #FactGlBalancePlanAllocation
		SET GLBalancePlanAllocationAmount = GLBalanceAmount * ISNULL(ProvidedAllocationPercentage, 0)
		WHERE ABS(ProvidedPeriodAllocation - 1) <= 0.001;
		SELECT @RowCount = @@ROWCOUNT;
		SELECT @DebugMsg = CONVERT(NVARCHAR(20), GETDATE()) + ' -  ' + CONVERT(NVARCHAR(20), @RowCount) + ' FactGLBalancePlanAllocation records with direct FactGLBalance -> WeeklyPlanAllocationCurrent matches.';
		PRINT @DebugMsg;
		-- Fallback to dynamic values if you have an issue
		UPDATE #FactGlBalancePlanAllocation
		SET GLBalancePlanAllocationAmount = GLBalanceAmount * DynamicAllocationPercentage
		WHERE ABS(ProvidedPeriodAllocation - 1) > 0.001;
		SELECT @RowCount = @@ROWCOUNT;
		SELECT @DebugMsg = CONVERT(NVARCHAR(20), GETDATE()) + ' -  ' + CONVERT(NVARCHAR(20), @RowCount) + ' FactGLBalancePlanAllocation records generated for unmatched records.';
		PRINT @DebugMsg;
		-- Ouptu the results to be consumed by other processes
		SELECT FiscalWeekEndDateKey
			,OrgKey
			,LocationKey
			,CompanyKey
			,CostCenterTypeKey
			,CostCenterKey
			,AccountSubaccountKey
			,GLMetricTypeKey
			,GLBalancePlanAllocationAmount
			,GETDATE() AS EDWCreatedDate
		FROM #FactGlBalancePlanAllocation;
	END TRY
	--
	-- Catch, and throw the error back to the calling procedure or client
	--
	BEGIN CATCH
		DECLARE @ErrMsg NVARCHAR(4000)
			,@ErrSeverity INT;
		SELECT @ErrMsg = 'Sub-procedure ' + @ProcName + ' - ' + ERROR_MESSAGE()
			,@ErrSeverity = ERROR_SEVERITY();
		RAISERROR (
				@ErrMsg
				,@ErrSeverity
				,1
				);
	END CATCH;
END;