CREATE PROCEDURE [dbo].[spGL_StagingTransform_FactGLBalance]
(@EDWRunDateTime     DATETIME2 = NULL,
 @FiscalPeriodNumber INT
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingTransform_FactGLBalance
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime
    --
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #FactGLBalanceUpsert -- (Temporary table)
    --                     EXEC dbo.spGL_StagingTransform_FactGLBalance @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By        Comments
    -- ----         -----------        --------
    --
    -- 11/07/17     Bhanu              BNG-787 - Refactored EDW FactGLBalance load
    -- 11/27/17     hhebbalu           BNG-894 - DimOrganization is SCD2 and it was used as SCD1. Fixed it.
    -- 12/04/17     sburke             BNG-757 - Switch the signage of monetary values going into FactGLBalance
    --                                     This initial switching of the signage for monetary (i.e. non-Statistics)
    --                                     values is the first step of preparing Balance data for presentation.
    --                                     In addition, once the data gets to SSAS, Unary operators in the 
    --                                     DimAccountSubaccount table drive further logic implemented in DAX to
    --                                     more selectively switch the signage based on AccountType - a switch 
    --                                     we don't want to perform or store in the EDW.
    -- 	
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         
	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             SELECT COALESCE(vw_dt.FiscalPeriodStartDateKey, -1) AS DateKey,
                    COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                    COALESCE(dm_loc.LocationKey, -1) AS LocationKey,
                    COALESCE(dm_cmp.CompanyKey, -1) AS CompanyKey,
                    COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey,
                    COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
                    COALESCE(dm_acc_sub.AccountSubaccountKey, -1) AS AccountSubaccountKey,
                    COALESCE(dm_ds.DataScenarioKey, -1) AS DataScenarioKey,
                    COALESCE(dm_Mtyp.GLMetricTypeKey, -1) AS GLMetricTypeKey,
                    CASE
                        WHEN dm_Mtyp.GLMetricTypeName = 'Dollars'
                        THEN COALESCE(-fct_Blnc.balance_amt, 0) -- BNG-757 - For any monetary values, perform an initial signage reverse on balance_amt
                        ELSE COALESCE(fct_Blnc.balance_amt, 0)
                    END AS GLBalanceAmount,
                    GETDATE() AS EDWCreatedDate,
                    NULL AS Deleted
             FROM dbo.vGLBalances fct_Blnc
                  LEFT JOIN BING_EDW.dbo.vDimFiscalPeriod vw_dt ON vw_dt.FiscalPeriodNumber = fct_Blnc.period_id
                  LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cmp.CompanyID = fct_Blnc.company_id
                  LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON dm_cctr.CostCenterNumber = fct_Blnc.cost_center_id
                                                                  AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                   AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                               AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctyp.CostCenterTypeID = fct_Blnc.cost_center_type_id
                  LEFT JOIN BING_EDW.dbo.DimAccountSubaccount dm_acc_sub ON dm_acc_sub.AccountSubaccountID = fct_Blnc.account_subaccount_id
                  LEFT JOIN BING_EDW.dbo.DimGLMetricType dm_Mtyp ON dm_Mtyp.GLMetricTypeCode = fct_Blnc.currency_code
                  LEFT JOIN BING_EDW.dbo.DimDataScenario dm_ds ON dm_ds.GLActualFlag = fct_Blnc.actual_flag
                                                                  AND COALESCE(dm_ds.GLBudgetVersionID, -1) = COALESCE(fct_Blnc.budget_version_id, -1)
             WHERE fct_Blnc.period_id = @FiscalPeriodNumber; -- If we are given a specific @FiscalPeriodNumber, just return data for that period.

         END TRY
	    --
	    -- Catch, and throw the error back to the calling procedure or client
	    --
         BEGIN CATCH
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = 'Sub-procedure '+@ProcName+' - '+ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;