
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCMS_StagingTransform_FactFTESnapshot'
)
    DROP PROCEDURE dbo.spCMS_StagingTransform_FactFTESnapshot;
GO
--*/

CREATE PROCEDURE [dbo].[spCMS_StagingTransform_FactFTESnapshot]
(@EDWRunDateTime DATETIME2 = NULL,
 @FiscalDate     DATE
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_FactFTESnapshot
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
    --                     EXEC dbo.spCMS_StagingTransform_FactFTESnapshot @FiscalDate = '20171231'
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
    --  8/02/18      sburke             Add EDWEndDate in join to DimSession in spCMS_StagingTransform_FactFTESnapshot (CMS Source - SCD2)
    -- 10/03/18      sburke             BNG-3789 - Only extract data from Centers in CMS after their designated migration date.  If not, exclude.             
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
                  WHERE MigrationDate >= @FiscalDate)
			   -- Select CMS Facts, filtering out data for Centers that have yet to be migrated from CMS

                  SELECT COALESCE(dm_dt.DateKey, -1) AS DateKey,
                         COALESCE(OrgKey, -1) AS OrgKey,
                         COALESCE(LocationKey, -1) AS LocationKey,
                         COALESCE(StudentKey, -1) AS StudentKey,
                         COALESCE(SponsorKey, -1) AS SponsorKey,
                         COALESCE(TuitionAssistanceProviderKey, -1) AS TuitionAssistanceProviderKey,
                         COALESCE(CompanyKey, -1) AS CompanyKey,
                         COALESCE(CostCenterTypeKey, -1) AS CostCenterTypeKey,
                         COALESCE(CostCenterKey, -1) AS CostCenterKey,
                         COALESCE(AccountSubaccountKey, -1) AS AccountSubaccountKey,
                         -2 AS TransactionCodeKey,
                         COALESCE(TierKey, -1) AS TierKey,
                         COALESCE(ProgramKey, -1) AS ProgramKey,
                         COALESCE(SessionKey, -1) AS SessionKey,
                         COALESCE(ScheduleWeekKey, -1) AS ScheduleWeekKey,
                         COALESCE(ClassroomKey, -1) AS ClassroomKey,
                         COALESCE(FeeTypeKey, -1) AS FeeTypeKey,
                         COALESCE(fct_fte.LifecycleStatusKey, -1) AS LifecycleStatusKey,
                         fct_fte.ReferenceID AS ReferenceID,
                         COALESCE(fct_fte.FTE, 0) AS FTE,
                         @SourceSystem AS SourceSystem,
                         GETDATE() AS EDWCreatedDate
                  FROM [dbo].[tfnFTE](@FiscalDate) fct_fte
                       LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON fct_fte.BusinessDate = dm_dt.FullDate
                       LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON fct_fte.CostcenterNumber = dm_cctr.CostCenterNumber
                                                                       AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                       LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                        AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cctr.CompanyID = dm_cmp.CompanyID
                       LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                                    AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimStudent ds ON fct_fte.idStudent = ds.StudentID
                                                               AND ds.EDWEndDate IS NULL -- DimStudent is SCD2, so get the latest version
                                                               AND ds.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimSponsor dsp ON fct_fte.idSponsor = dsp.SponsorID
                                                                AND dsp.EDWEndDate IS NULL -- DimSponsor is SCD2, so get the latest version

						                                        AND dsp.SourceSystem = @SourceSystem

															
                       LEFT JOIN BING_EDW.dbo.DimTuitionAssistanceProvider dm_tap ON fct_fte.idSubsidyAgency = dm_tap.TuitionAssistanceProviderID
                                                                                     AND dm_tap.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimAccountSubaccount dm_accsa ON fct_fte.GLAccount = dm_accsa.AccountID
                                                                               AND fct_fte.GLSubaccount = dm_accsa.SubaccountID
                       LEFT JOIN BING_EDW.dbo.DimTier T ON fct_fte.idSiteTier = T.TierID
                                                           AND T.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimProgram dm_prg ON fct_fte.idProgram = dm_prg.ProgramID
                                                                   AND dm_prg.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimSession dm_ssn ON fct_fte.idSessiontype = dm_ssn.SessionID
                                                                   AND dm_ssn.SourceSystem = @SourceSystem
                                                                   AND dm_ssn.EDWEndDate IS NULL -- DimSession is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimScheduleWeek dm_shwk ON fct_fte.ScheduleWeekFlags = dm_shwk.ScheduleWeekFlags
                       LEFT JOIN BING_EDW.dbo.DimClassroom dm_cls ON fct_fte.idClassroom = dm_cls.ClassroomID
                                                                     AND dm_cls.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimFeeType dm_fee ON fct_fte.idFees = dm_fee.FeeTypeID
                                                                   AND dm_fee.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW.dbo.DimLifecycleStatus dm_lfcs ON fct_fte.LifecycleStatusKey = dm_lfcs.LifecycleStatusKey
                  WHERE fct_fte.CostCenterNumber NOT IN -- Check for Centers that are still on CSS as-of the current FiscalDate, and filter-out
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


