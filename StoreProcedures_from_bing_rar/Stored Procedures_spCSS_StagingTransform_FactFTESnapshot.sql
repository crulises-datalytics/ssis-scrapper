
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCSS_StagingTransform_FactFTESnapshot'
)
    DROP PROCEDURE dbo.spCSS_StagingTransform_FactFTESnapshot;
GO
--*/

CREATE PROCEDURE [dbo].[spCSS_StagingTransform_FactFTESnapshot]
(@EDWRunDateTime DATETIME2 = NULL,
 @FiscalWeek     INT
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_StagingTransform_FactFTESnapshot
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime
    --                     @@FiscalDate - Proc will run for a given Fiscal Date 
    --
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #FactFTESnapshotUpsert -- (Temporary table)
    --                     EXEC dbo.spCSS_StagingTransform_FactFTESnapshot @FiscalWeek = 20161125 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By        Comments
    -- --------      -----------        --------
    --
    --  1/29/18      sburke             BNG-294 - Refactored EDW FactFTESnapshot (CMS Source) load
    --  2/20/18      anmorales          BNG-249 - EDW FactFTESnapshot - CSS
    --  5/23/2018    sburke             BNG-1756 - Correct join to DimStudent to account for SCD2	
    --  8/09/18      sburke             BNG-3435 - Add EDWEndDate in join to DimSession in spCMS_StagingTransform_FactFTESnapshot (CSS Legacy Source - SCD2)
    -- 10/03/18      sburke             BNG-3789 - Only extract data from Centers that are still on CSS on the As-Of date.  If not, exclude them (still some Centers with 'residual' CSS transactions
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
                  WHERE MigrationFiscalWeekNumber < @FiscalWeek)
			   -- Select CSS Facts, filtering out data for Centers that have already migrated to CMS

                  SELECT COALESCE(dm_dt.DateKey, -1) AS DateKey,
                         COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                         COALESCE(LocationKey, -1) AS LocationKey,
                         COALESCE(StudentKey, -1) AS StudentKey,
                         COALESCE(SponsorKey, -1) AS SponsorKey,
                         -2 AS TuitionAssistanceProviderKey,
                         COALESCE(CompanyKey, -1) AS CompanyKey,
                         COALESCE(CostCenterTypeKey, -1) AS CostCenterTypeKey,
                         COALESCE(CostCenterKey, -1) AS CostCenterKey,
                         COALESCE(AccountSubaccountKey, -1) AS AccountSubaccountKey,
                         COALESCE(TransactionCodeKey, -1) AS TransactionCodeKey,
                         COALESCE(TierKey, -1) AS TierKey,
                         COALESCE(ProgramKey, -1) AS ProgramKey,
                         COALESCE(SessionKey, -1) AS SessionKey,
                         COALESCE(ScheduleWeekKey, -1) AS ScheduleWeekKey,
                         -2 AS ClassroomKey,
                         COALESCE(FeeTypeKey, -1) AS FeeTypeKey,
                         COALESCE(dm_lfcs.LifecycleStatusKey, -1) AS LifecycleStatusKey,
                         fct_fte.serial_no AS ReferenceID,
                         COALESCE(fct_fte.FTE, 0) AS FTE,
                         @SourceSystem AS SourceSystem,
                         @EDWRunDateTime AS EDWCreatedDate
                  FROM [dbo].[tfnFTE](@FiscalWeek) fct_fte
                       LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON fct_fte.FiscalDate = dm_dt.FullDate
                       LEFT JOIN BING_EDW.dbo.DimTransactionCode dm_txcode ON fct_fte.tx_type = dm_txcode.TransactionTypeCode
                                                                              AND fct_fte.tx_code = dm_txcode.TransactionCode
                       LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON fct_fte.ctr_no = dm_cctr.CenterCSSID
                                                                       AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                      --LEFT JOIN BING_EDW.dbo.DimTuitionAssistanceProvider dm_tap ON fct_fte.ctr_no = dm_tap.CSSCenterNumber
                      --                                                              AND fct_fte.cust_code = dm_tap.CSSCustomerCode -- FTE's are not associated with customers in CSS
                       LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                        AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cctr.CompanyID = dm_cmp.CompanyID
                       LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                                    AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimStudent dm_std ON fct_fte.ctr_no = dm_std.CSSCenterNumber
                                                                   AND fct_fte.fam_no = dm_std.CSSFamilyNumber
                                                                   AND fct_fte.stu_no = dm_std.CSSStudentNumber
                                                                   AND dm_std.SourceSystem = @SourceSystem
                                                                   AND dm_std.EDWEndDate IS NULL -- DimStudent is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimSponsor dm_spr ON fct_fte.ctr_no = dm_spr.CSSCenterNumber
                                                                   AND fct_fte.fam_no = dm_spr.CSSFamilyNumber
                                                                   AND dm_spr.EDWEndDate IS NULL -- DimSponsor is SCD2, so get the latest version
                                                                   AND dm_spr.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimAccountSubaccount dm_accsa ON fct_fte.GLAccount = dm_accsa.AccountID
                                                                               AND fct_fte.GLSubaccount = dm_accsa.SubaccountID
                       LEFT JOIN BING_EDW.dbo.DimTier dm_tr ON fct_fte.Tier = dm_tr.CSSTierNumber
                                                               AND dm_tr.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimProgram dm_prg ON fct_fte.Program = dm_prg.ProgramName
                                                                   AND dm_prg.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimSession dm_ssn ON fct_fte.[Session] = dm_ssn.SessionName
                                                                   AND dm_ssn.SourceSystem = @SourceSystem
                                                                   AND dm_ssn.EDWEndDate IS NULL -- DimSession is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimScheduleWeek dm_shwk ON fct_fte.Schedule = dm_shwk.ScheduleDaysInWeekCountName
                                                                         AND dm_shwk.ScheduleWeekFlags = '0000000'
                       LEFT JOIN BING_EDW.dbo.DimFeeType dm_fee ON fct_fte.tx_type = dm_fee.CSSTransactionType
                                                                   AND fct_fte.tx_code = dm_fee.CSSTransactionCode
                                                                   AND dm_fee.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimLifecycleStatus dm_lfcs ON fct_fte.LifecycleStatusKey = dm_lfcs.LifecycleStatusKey
                  WHERE fct_fte.ctr_no NOT IN -- Only bring in Centers that are still on CSS as-of the current FiscalWeek
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
GO