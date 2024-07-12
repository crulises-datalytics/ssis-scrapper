

CREATE PROCEDURE [dbo].[spPRO_StagingTransform_FactNetRevenue]
(@EDWRunDateTime    DATETIME2 = NULL,
 @FiscalWeekNumber  INT
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:	   spPRO_StagingTransform_FactNetRevenue
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
    -- Usage:			   INSERT #FactNetRevenueUpsert -- (Temporary table)
    --				   EXEC dbo.spPRO_StagingTransform_FactNetRevenue @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date		Modified By		Comments
    -- ----		-----------		--------
    --
    -- 11/03/17   harshitha		Initial version of proc, converted from SSIS logic
    -- 12/11/17   Banandesi 		Fixed the DimOrganization join(BNG-869)		 
	-- 03/08/18   Adevabhakthuni Update sp to new reference new function tfnNetRevenue() 
	-- 03/18/2022 Adevabhakthuni  Updated the transform stored proc not to load after 202109
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceSystem NVARCHAR(3)= 'PRO';
	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             SELECT	
				  COALESCE(dm_dt.DateKey,-1) AS DateKey,
				  COALESCE(dm_org.OrgKey, -1) AS OrgKey,
				  COALESCE(dm_loc.LocationKey,-1) AS LocationKey,
				  -2 AS StudentKey,
				  -2 AS SponsorKey,
				  -2 AS TuitionAssistanceProviderKey,
				  COALESCE(dm_cmp.CompanyKey,-1) AS CompanyKey,
				  COALESCE(dm_cctyp.CostCenterTypeKey,-1) AS CostCenterTypeKey,
				  COALESCE(dm_cctr.CostCenterKey,-1) AS CostCenterKey,
				  COALESCE(dm_acc_sub.AccountSubaccountKey,-1) AS AccountSubaccountKey,
				  -2 AS TransactionCodeKey,
				  -2 AS TierKey,
				  -2 AS ProgramKey,
				  -2 AS SessionKey,
				  -2 AS ScheduleWeekKey,
				  -2 AS FeeTypeKey,
				  -2 AS DiscountTypeKey,
				  -2 AS InvoiceTypeKey,
				  -2 AS CreditMemoTypeKey,
				  -2 AS LifecycleStatusKey,
				  COALESCE(fct_nrev.ReferenceID,'-1') AS TransactionNumber,
				  COALESCE(fct_nrev.NetRevenueAmount,0) AS NetRevenueAmount,
				  'PRO' AS SourceSystem,      
				  GETDATE() AS EDWCreatedDate,
				  NULL AS Deleted
             FROM dbo.tfnNetRevenue(@FiscalWeekNumber) fct_nrev
                  LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON fct_nrev.FiscalDate = dm_dt.FullDate
                  LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON fct_nrev.CostCenterID = dm_cctr.CostCenterNumber
                                                                  AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON fct_nrev.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                  LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON fct_nrev.CompanyID = dm_cmp.CompanyID
                  LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
			                                                 AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                               AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimAccountSubaccount dm_acc_sub ON fct_nrev.AccountID = dm_acc_sub.AccountID
                                                                    AND fct_nrev.SubaccountID = dm_acc_sub.SubaccountID
																	WHERE dm_dt.FiscalWeekNumber < 202109;
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