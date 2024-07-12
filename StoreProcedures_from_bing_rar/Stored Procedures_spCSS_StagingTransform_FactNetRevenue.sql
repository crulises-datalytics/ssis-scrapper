/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCSS_StagingTransform_FactNetRevenue'
)
    DROP PROCEDURE dbo.spCSS_StagingTransform_FactNetRevenue;
GO
--*/

CREATE PROCEDURE [dbo].[spCSS_StagingTransform_FactNetRevenue]
(@EDWRunDateTime   DATETIME2 = NULL,
 @FiscalWeekNumber INT
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:	   spCSS_StagingTransform_FactNetRevenue
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
    --                     EXEC dbo.spCSS_StagingTransform_FactNetRevenue @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date		 Modified By		Comments
    -- ----		 -----------		--------
    --
    -- 11/03/17  hhebbalu       Initial version of proc, converted from SSIS logic
    -- 12/10/17  hhebbalu       Fixed the DimOrganization join(BNG-867)
    -- 01/09/18  hhebbalu       Added DimLifecycleStatus (BNG-997)
    -- 02/20/18  Banandesi      mappped TransactionCodeKey correctly (BNG-1251)
    -- 05/01/18  hhebbalu       Fixed the CostCenter to map to CSSCenterID (BNG-1672)	
    --  8/27/18  sburke         BNG-1601 Set Set DiscountTypeKey to 'N/A' -2 if there is no Discount applied, and -1 'Unknown' 
    --                              if there appears to be a Discount but we cannot find a match in DimDiscountType.
    -- 10/03/18  sburke         BNG-3789 - Only extract data from Centers that are still on CSS on the As-Of date.  If not, exclude them (still some Centers with 'residual' CSS transactions
    --    	
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceSystem NVARCHAR(3)= 'CSS';
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
             -- CTE to build a dataset containing all the Centers that have already been 
             --     migrated over to OneCMS as-of the current Fiscal Date, and so should be  
             --     excluded from this CSS extract (as we will be exclusively using OneCMS 
             --     going forward).
             --
             -- This is to prevent cases where Facts are loaded for both CSS and OneCMS for
             --      the same date).
             -- --------------------------------------------------------------------------------
             WITH CTE_ExcludeCSSCenters_MigratedToOneCMS
                  AS (
                  SELECT mig.[CenterCSSID],
                         mig.[MigrationWaveNumber],
                         mig.[MigrationDate],
                         mig.[MigrationFiscalWeekNumber]
                  FROM dbo.CenterCSSMigrations mig
                  WHERE MigrationFiscalWeekNumber < @FiscalWeekNumber)
			   -- Select CSS Facts, filtering out data for Centers that have already migrated to CMS

                  SELECT COALESCE(dm_dt.DateKey, -1) AS DateKey,
                         COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                         COALESCE(LocationKey, -1) AS LocationKey,
                         COALESCE(StudentKey, -1) AS StudentKey,
                         COALESCE(SponsorKey, -1) AS SponsorKey,
                         COALESCE(TuitionAssistanceProviderKey, -1) AS TuitionAssistanceProviderKey,
                         COALESCE(CompanyKey, -1) AS CompanyKey,
                         COALESCE(CostCenterTypeKey, -1) AS CostCenterTypeKey,
                         COALESCE(CostCenterKey, -1) AS CostCenterKey,
                         COALESCE(AccountSubaccountKey, -1) AS AccountSubaccountKey,
                         COALESCE(dm_trn_code.TransactionCodeKey, -1) AS TransactionCodeKey,
                         COALESCE(TierKey, -1) AS TierKey,
                         COALESCE(ProgramKey, -1) AS ProgramKey,
                         COALESCE(SessionKey, -1) AS SessionKey,
                         COALESCE(ScheduleWeekKey, -1) AS ScheduleWeekKey,
                         COALESCE(FeeTypeKey, -1) AS FeeTypeKey,
                    -- BNG-1601: Set Set DiscountTypeKey to 'N/A' -2 if there is no Discount applied, and -1 'Unknown' 
                    --           if there appears to be a Discount but we cannot find a match in BING_EDW.dbo.DimDiscountType,
                    --
                    --           This is not as simple as with CMS, where there is an explcit DiscountType field to check on, so the next
                    --           best method is to look at those records that don't join to DimDiscount, and where they have a 	
                    --           tx_type of 'DS' we mark them as an Unknown Discount Type (-1).  For other types, we set as explicitly
                    --           not a Discount.					
                         CASE
                             WHEN dm_dtyp.DiscountTypeKey IS NULL
                             THEN CASE
                                      WHEN tx_type = 'DS'
                                      THEN-1 -- Unknown
                                      ELSE-2 -- Not Applicable (i.e. we are explcitly stating this is not a Discount)
                                  END
                             ELSE dm_dtyp.DiscountTypeKey
                         END AS DiscountTypeKey,
                         -2 AS InvoiceTypeKey,
                         -2 AS CreditMemoTypeKey,
                         COALESCE(dm_lcs.LifecycleStatusKey, -1) AS LifecycleStatusKey,
                         CAST(COALESCE(serial_no, '-1') AS VARCHAR(50)) AS TransactionNumber,
                         COALESCE(Amount, 0) AS NetRevenueAmount,
                         @SourceSystem AS SourceSystem,
                         GETDATE() AS EDWCreatedDate,
                         NULL AS Deleted
                  FROM [dbo].[tfnNetRevenue](@FiscalWeekNumber) fct_nrev
                       LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON fct_nrev.FiscalWeekEndDate = dm_dt.FullDate
                       LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON fct_nrev.ctr_no = dm_cctr.CenterCSSID --(BNG-1672)
                                                                       AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                       LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                        AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cctr.CompanyID = dm_cmp.CompanyID
                       LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                                    AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimStudent dm_std ON fct_nrev.ctr_no = dm_std.CSSCenterNumber
                                                                   AND fct_nrev.fam_no = dm_std.CSSFamilyNumber
                                                                   AND fct_nrev.stu_no = dm_std.CSSStudentNumber
                                                                   AND dm_std.EDWEndDate IS NULL
                                                                   AND dm_std.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimSponsor dm_spr ON fct_nrev.ctr_no = dm_spr.CSSCenterNumber
                                                                   AND fct_nrev.fam_no = dm_spr.CSSFamilyNumber
                                                                   AND dm_spr.EDWEndDate IS NULL
                                                                   AND dm_spr.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimTuitionAssistanceProvider dm_tap ON fct_nrev.ctr_no = dm_tap.CSSCenterNumber
                                                                                     AND fct_nrev.cust_code = dm_tap.CSSCustomerCode
                                                                                     AND dm_tap.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimAccountSubaccount dm_acc_sub ON fct_nrev.GLAccount = dm_acc_sub.AccountID
                                                                                 AND fct_nrev.GLSubaccount = dm_acc_sub.SubaccountID
                       LEFT JOIN BING_EDW.dbo.DimTransactionCode dm_trn_code ON fct_nrev.tx_type = dm_trn_code.TransactionTypeCode
                                                                                AND fct_nrev.tx_code = dm_trn_code.TransactionCode
                       LEFT JOIN BING_EDW.dbo.DimTier dm_tr ON fct_nrev.Tier = dm_tr.CSSTierNumber
                                                               AND dm_tr.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimProgram dm_prg ON fct_nrev.Program = dm_prg.ProgramName
                                                                   AND dm_prg.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimSession sm_ses ON fct_nrev.Session = sm_ses.SessionName
                                                                   AND sm_ses.SourceSystem = @SourceSystem
                                                                   AND sm_ses.EDWEndDate IS NULL -- DimSession is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimScheduleWeek dm_schwk ON fct_nrev.Schedule = dm_schwk.ScheduleDaysInWeekCountName
                                                                          AND dm_schwk.ScheduleWeekFlags = '0000000'
                       LEFT JOIN BING_EDW.dbo.DimFeeType dm_ftyp ON fct_nrev.tx_type = dm_ftyp.CSSTransactionType
                                                                    AND fct_nrev.tx_code = dm_ftyp.CSSTransactionCode
                                                                    AND dm_ftyp.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimDiscountType dm_dtyp ON fct_nrev.tx_type = dm_dtyp.CSSTransactionType
                                                                         AND fct_nrev.tx_code = dm_dtyp.CSSTransactionCode
                                                                         AND dm_dtyp.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimLifecycleStatus dm_lcs ON fct_nrev.LifecycleStatusKey = dm_lcs.LifecycleStatusKey
                  WHERE fct_nrev.ctr_no NOT IN -- Only bring in Centers that are still on CSS as-of the current FiscalWeek
                  (
                      SELECT CenterCSSID
                      FROM CTE_ExcludeCSSCenters_MigratedToOneCMS
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