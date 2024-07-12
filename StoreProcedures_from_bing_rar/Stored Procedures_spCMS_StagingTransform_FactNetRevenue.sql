


/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCMS_StagingTransform_FactNetRevenue'
)
    DROP PROCEDURE dbo.spCMS_StagingTransform_FactNetRevenue;
GO
--*/
CREATE PROCEDURE [dbo].[spCMS_StagingTransform_FactNetRevenue]
(@EDWRunDateTime    DATETIME2 = NULL,
 @FiscalWeekEndDate DATE
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_FactNetRevenue
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime
	--                     @FiscalWeekEndDate - Proc will run for a given FiscalWeekEndDate 
    --
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #FactNetRevenueUpsert -- (Temporary table)
    --                     EXEC dbo.spCMS_StagingTransform_FactNetRevenue @FiscalWeekEndDate = '20171231'
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By        Comments
    -- ----          -----------        --------
    --
    -- 11/06/17      sburke             BNG-248 - Refactored EDW FactNetRevenue (CMS Source) load
    -- 12/10/17      Banandesi          Fixed the DimOrganization join(BNG-869)
    -- 01/09/2018    hhebbalu           BNG-997 - Added DimLifecycleStatus	
    -- 01/11/2018    hhebbalu           BNG-997 - Added another condition for DimSchdeduleWeek to avoid duplicates
    --  8/14/18      sburke             BNG-1601 - Set DiscountTypeKey to 'N/A' -2 value if there is no Discount 
    --                                      Type applied (keep 'Unknown' -1 for those Discount TYpes we cannot match 
    --                                      in our DimDiscountType dimension table)
    -- 10/03/18      sburke             BNG-3789 - Only extract data from Centers in CMS after their designated migration date.  If not, exclude.  
	-- 03/26/2020    tyj				BNG-3669 Added StudentInvoiceIsVoid field for use in Retention calculation
    --               
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceSystem NVARCHAR(3)= 'CMS';
	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             -- --------------------------------------------------------------------------------
             -- CTE to build a dataset containing all the Centers that have yet to be migrated
             --     to OneCMS as-of the current Fiscal Date, and so should be excluded from 
             --     this extract (as we will be exclusively using CSS for the Fact extract
             --     until each Center is migrated).
             --
             -- This is to prevent cases where adjustments made to CSS Centers in CMS does not
             --     get loaded into the EDW (and thus showing Facts appearing for both CSS and
             --     OneCMS for the same date).
             -- --------------------------------------------------------------------------------
             WITH CTE_ExcludeCMSCenters_NotYetMigratedFromCSS
                  AS (
                  SELECT mig.[CostCenterNumber],
                         mig.[MigrationWaveNumber],
                         mig.[MigrationDate],
                         mig.[MigrationFiscalWeekNumber]
                  FROM CSS_Staging.dbo.CenterCSSMigrations mig
                  WHERE MigrationDate >= @FiscalWeekEndDate)
			   -- Select CMS FTEs, filtering out data for Centers that have yet to be migrated from CMS

                  SELECT COALESCE(dm_dt.DateKey, -1) AS DateKey,
                         COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                         COALESCE(dm_loc.LocationKey, -1) AS LocationKey,
                         COALESCE(dm_std.StudentKey, -1) AS StudentKey,
                         COALESCE(dm_spr.SponsorKey, -1) AS SponsorKey,
                         COALESCE(dm_tap.TuitionAssistanceProviderKey, -1) AS TuitionAssistanceProviderKey,
                         COALESCE(dm_cmp.CompanyKey, -1) AS CompanyKey,
                         COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey,
                         COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
                         COALESCE(dm_acc_sub.AccountSubaccountKey, -1) AS AccountSubaccountKey,
                         -2 AS TransactionCodeKey,
                         COALESCE(TierKey, -1) AS TierKey,
                         COALESCE(dm_prg.ProgramKey, -1) AS ProgramKey,
                         COALESCE(sm_ses.SessionKey, -1) AS SessionKey,
                         COALESCE(dm_schwk.ScheduleWeekKey, -1) AS ScheduleWeekKey,
                         COALESCE(dm_ftyp.FeeTypeKey, -1) AS FeeTypeKey,
                         CASE
                             WHEN fct_nrev.idDiscount IS NOT NULL
                             THEN COALESCE(dm_dtyp.DiscountTypeKey, -1) -- BNG-1601: If there is data in the idDiscount field but no match in DimDiscount, then it is Unknown (-1)
                             ELSE-2 -- BNG-1601: Set DiscountTypeKey to -2 (N/A) if there is no discount applied (denoted as a NULL in the source)
                         END AS DiscountTypeKey,
                         COALESCE(dm_invtyp.InvoiceTypeKey, -1) AS InvoiceTypeKey,
                         COALESCE(dm_cmtyp.CreditMemoTypeKey, -1) AS CreditMemoTypeKey,
                         COALESCE(dm_lcs.LifecycleStatusKey, -1) AS LifecycleStatusKey,
                         CAST(COALESCE(fct_nrev.RefNo, '-1') AS VARCHAR(50)) AS TransactionNumber,
                         COALESCE(fct_nrev.Amount, 0) AS NetRevenueAmount,
                         @SourceSystem AS SourceSystem,
                         GETDATE() AS EDWCreatedDate,
                         NULL AS Deleted,
						 StudentInvoiceIsVoid
                  FROM [dbo].[tfnNetRevenue](@FiscalWeekEndDate) fct_nrev
                       LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON fct_nrev.FiscalDate = dm_dt.FullDate
                       LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON fct_nrev.CostCenterNumber = dm_cctr.CostCenterNumber
                                                                       AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                       LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                        AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cctr.CompanyID = dm_cmp.CompanyID
                       LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                                    AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimStudent dm_std ON fct_nrev.idStudent = dm_std.StudentID
                                                                   AND dm_std.EDWEndDate IS NULL
                                                                   AND dm_std.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimSponsor dm_spr ON fct_nrev.idSponsor = dm_spr.SponsorID
                                                                   AND dm_spr.EDWEndDate IS NULL
                                                                   AND dm_spr.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimTuitionAssistanceProvider dm_tap ON fct_nrev.idSubsidyAgency = dm_tap.TuitionAssistanceProviderID
                                                                                     AND dm_tap.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimAccountSubaccount dm_acc_sub ON fct_nrev.GLAccount = dm_acc_sub.AccountID
                                                                                 AND fct_nrev.GLSubaccount = dm_acc_sub.SubaccountID
                       LEFT JOIN BING_EDW.dbo.DimTier dm_tr ON fct_nrev.idSiteTier = dm_tr.TierID
                                                               AND dm_tr.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimProgram dm_prg ON fct_nrev.idProgram = dm_prg.ProgramID
                                                                   AND dm_prg.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimSession sm_ses ON fct_nrev.idSessiontype = sm_ses.SessionID
                                                                   AND sm_ses.SourceSystem = @SourceSystem
                                                                   AND sm_ses.EDWEndDate IS NULL -- DimSession is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimScheduleWeek dm_schwk ON fct_nrev.ScheduleDaysInWeekCount = dm_schwk.ScheduleDaysInWeekCount
                                                                          AND dm_schwk.ScheduleWeekFlags = '0000000'
                                                                          AND dm_schwk.ScheduleWeekName = 'Unknown Days(CMS)'
                       LEFT JOIN BING_EDW.dbo.DimFeeType dm_ftyp ON fct_nrev.idFees = dm_ftyp.FeeTypeID
                                                                    AND dm_ftyp.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimDiscountType dm_dtyp ON fct_nrev.idDiscount = dm_dtyp.DiscountTypeID
                       LEFT JOIN BING_EDW.dbo.DimInvoiceType dm_invtyp ON fct_nrev.idInvoiceType = dm_invtyp.InvoiceTypeID
                       LEFT JOIN BING_EDW.dbo.DimCreditMemoType dm_cmtyp ON fct_nrev.idCreditMemoType = dm_cmtyp.CreditMemoTypeID
                       LEFT JOIN BING_EDW.dbo.DimLifecycleStatus dm_lcs ON fct_nrev.LifecycleStatusKey = dm_lcs.LifecycleStatusKey
                  WHERE fct_nrev.CostCenterNumber NOT IN -- Check for Centers that are still on CMS as-of the current FiscalDate, and filter-out
                  (
                      SELECT CostCenterNumber
                      FROM CTE_ExcludeCMSCenters_NotYetMigratedFromCSS
                  );
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
GO





