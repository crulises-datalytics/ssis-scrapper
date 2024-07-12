/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCMS_StagingTransform_FactARBalanceSnapshot'
)
    DROP PROCEDURE dbo.spCMS_StagingTransform_FactARBalanceSnapshot;
GO
--*/
CREATE PROCEDURE [dbo].[spCMS_StagingTransform_FactARBalanceSnapshot]
(@EDWRunDateTime    DATETIME2 = NULL,
 @FiscalWeekEndDate DATE
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_FactARBalanceSnapshot
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                         making numerous GETDATE() calls  
    --                     @FiscalWeekEndDate - this stored proc has to run for a specicifc
    --                         Fiscal Week
    --				   
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #FactARBalanceSnapshotUpsert -- (Temporary table)
    --                     EXEC dbo.spCMS_StagingTransform_FactARBalanceSnapshot @EDWRunDateTime, @FiscalWeekEndDate 
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 10/30/17    sburke              BING-247.  Initial version of stored proc for AR Balance Snapshot load
    -- 1/16/2018   Banandesi           BNG-989    Fixed the DimOrganization join to use it as SCD2 type		 
    -- 2/22/18     sburke              BNG-1211 - TransactionAmount, AppliedAmount & ARBalanceAmount are all 
    --                                     additive columns, so set to 0 when NULL
    -- 04/24/18    anmorales           BNG-1639 ETL StagingToEDW for new Dimension table - Added a new column ARAgencyTypeKey in Staging ARBalanceSnapshot
    -- 10/03/18    sburke              BNG-3789 - Only extract data from Centers in CMS after their designated migration date.  If not, exclude. 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceSystem VARCHAR(10)= 'CMS';
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

                  SELECT dm_dt_asof.DateKey AS AsOfDateKey,
                         COALESCE(dm_dt_trn.DateKey, -1) AS TransactionDateKey,
                         COALESCE(dm_dt_age.DateKey, -1) AS ARAgingDateKey,
                         COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                         COALESCE(dm_loc.LocationKey, -1) AS LocationKey,
                         COALESCE(dm_cmp.CompanyKey, -1) AS CompanyKey,
                         COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey,
                         COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
                         COALESCE(dm_std.StudentKey, -1) AS StudentKey,
                         COALESCE(dm_spr.SponsorKey, -1) AS SponsorKey,
                         COALESCE(dm_tut.TuitionAssistanceProviderKey, -1) AS TuitionAssistanceProviderKey,
                         COALESCE(bal_snp.ARBalanceType, -1) AS ARBalanceTypeKey,
                         COALESCE(dm_age_buc.ARAgingBucketKey, -1) AS ARAgingBucketKey,
                         COALESCE(dm_at.ARAgencyTypeKey, -1) AS ARAgencyTypeKey,
                    -- TransactionAmount, AppliedAmount & ARBalanceAmount are all additive columns, so set to 0 when NULL 
                         COALESCE(bal_snp.TransactionAmount, 0) AS TransactionAmount,
                         COALESCE(bal_snp.AppliedAmount, 0) AS AppliedAmount,
                         COALESCE(bal_snp.ARBalanceAmount, 0) AS ARBalanceAmount,
                         @SourceSystem AS SourceSystem,
                         @EDWRunDateTime AS EDWCreatedDate
                  FROM dbo.ARBalanceSnapshot bal_snp
                       LEFT JOIN BING_EDW.dbo.DimDate dm_dt_asof ON bal_snp.AsOfFiscalWeekEndDate = dm_dt_asof.FullDate
                       LEFT JOIN BING_EDW.dbo.DimDate dm_dt_trn ON bal_snp.TransactionDate = dm_dt_trn.FullDate
                       LEFT JOIN BING_EDW.dbo.DimDate dm_dt_age ON bal_snp.ARAgingDate = dm_dt_age.FullDate
                       LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON bal_snp.idSite = CenterCMSID
                                                                       AND EDWEndDate IS NULL -- DimCostCenter is SCD2, so get current record
                       LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                       LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                        AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                                    AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get current record
                       LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cctr.CompanyID = dm_cmp.CompanyID
                       LEFT JOIN BING_EDW.dbo.DimStudent dm_std ON @SourceSystem = dm_std.SourceSystem
                                                                   AND bal_snp.idStudent = dm_std.StudentID
                                                                   AND dm_std.EDWEndDate IS NULL -- DimStudent is SCD2, so get current record
                       LEFT JOIN BING_EDW.dbo.DimSponsor dm_spr ON @SourceSystem = dm_spr.SourceSystem
                                                                   AND bal_snp.idSponsor = dm_spr.SponsorID
                                                                   AND dm_spr.EDWEndDate IS NULL -- DimSponsor is SCD2, so get current record
                       LEFT JOIN BING_EDW.dbo.DimTuitionAssistanceProvider dm_tut ON bal_snp.idSubsidyAgency = dm_tut.TuitionAssistanceProviderID
                       LEFT JOIN BING_EDW.dbo.DimARAgencyType dm_at ON @SourceSystem = dm_at.SourceSystem
                                                                       AND CASE
                                                                               WHEN idSponsor IS NOT NULL
                                                                               THEN 'Family'
                                                                               ELSE dm_tut.TuitionAssistanceProviderType
                                                                           END = dm_at.ARAgencyTypeName
                       LEFT JOIN BING_EDW.dbo.DimARAgingBucket dm_age_buc ON bal_snp.ARAgingDays BETWEEN dm_age_buc.ARAgingDaysFrom AND dm_age_buc.ARAgingDaysTo
                  WHERE dm_dt_asof.FiscalWeekEndDate = @FiscalWeekEndDate
                        AND dm_cctr.CostCenterNumber NOT IN -- Check for Centers that are still on CMS as-of the current FiscalDate, and filter-out
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