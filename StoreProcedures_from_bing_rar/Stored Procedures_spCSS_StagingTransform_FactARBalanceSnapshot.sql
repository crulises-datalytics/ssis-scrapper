CREATE PROCEDURE [dbo].[spCSS_StagingTransform_FactARBalanceSnapshot]
(@EDWRunDateTime   DATETIME2 = NULL,
 @FiscalWeekNumber INT
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_StagingTransform_FactARBalanceSnapshot
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                         making numerous GETDATE() calls  
    --				   
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #FactARBalanceSnapshotUpsert -- (Temporary table)
    --                     EXEC dbo.spCSS_StagingTransform_FactARBalanceSnapshot @EDWRunDateTime
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
	--
    -- 1/16/2018   Banandesi           BNG-989    Fixed the DimOrganization join to use it as SCD2 type		 
	--
	-- 02/27/2018  hhebbalu			   BNG-1271 Correct ETL logic for FactARBalanceSnapshot for CSS source - changed 
	--								   the defaul value for ARBalanceAmount(additive) from -1 to 0
	--
	-- 03/08/2018  hhebbalu			   BNG-1271 Correct ETL logic for FactARBalanceSnapshot for CSS source- Added a new column ARAgingDate in Staging ARBalanceSnapshot
	--								   And used that column to join and get the DateKey for ARAgingDateKey
	--
	-- 04/24/2018  anmorales			   BNG-1639 ETL StagingToEDW for new Dimension table - Added a new column ARAgencyTypeKey in Staging ARBalanceSnapshot
	--
	--06/19/2018   banandesi              BNG-1799 EDW - Update FactARBalanceSnapshot CSS load to look at Migration Date

    -- ============================================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceSystem VARCHAR(10)= 'CSS';
	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();  
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
	    -- ================================================================================
	    -- T H I S   I S   T H E   C S S   V E R S I O N   O F   F A C T  A R  B A L A N C E   
	    --                         -----  
	    --
	    -- There is also a CMS version of the proc spCSS_StagingTransform_FactARBalanceSnapshot
	    --     which resides in CMS_Staging
	    --
	    -- ================================================================================

             SELECT dm_dt_asof.FiscalWeekEndDateKey AS AsOfDateKey,
                    COALESCE(dm_dt_trn.FiscalWeekEndDateKey, -1) AS TransactionDateKey,
                    COALESCE(dm_dt_age.FiscalWeekEndDateKey, -1) AS ARAgingDateKey,
                    COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                    COALESCE(dm_loc.LocationKey, -1) AS LocationKey,
                    COALESCE(dm_cmp.CompanyKey, -1) AS CompanyKey,
                    COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey,
                    COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
                    -2 AS StudentKey, -- Hard-coded for CSS
                    COALESCE(dm_spr.SponsorKey, -1) AS SponsorKey,
                    COALESCE(dm_tut.TuitionAssistanceProviderKey, -1) AS TuitionAssistanceProviderKey,
                    COALESCE(bal_snp.ARBalanceType, -1) AS ARBalanceTypeKey,
                    COALESCE(dm_age_buc.ARAgingBucketKey, -1) AS ARAgingBucketKey,
                    COALESCE(dm_at.ARAgencyTypeKey, -1) AS ARAgencyTypeKey,
                    0 AS TransactionAmount,
                    0 AS AppliedAmount,
                    COALESCE(bal_snp.ARBalanceAmount, 0) AS ARBalanceAmount,
                    @SourceSystem AS SourceSystem,
                    @EDWRunDateTime AS EDWCreatedDate
             FROM dbo.ARBalanceSnapshot bal_snp
                  LEFT JOIN BING_EDW.dbo.vDimFiscalWeek dm_dt_asof ON bal_snp.AsOfFiscalWeek = dm_dt_asof.FiscalWeekNumber
                  LEFT JOIN BING_EDW.dbo.vDimFiscalWeek dm_dt_trn ON bal_snp.AsOfFiscalWeek = dm_dt_trn.FiscalWeekNumber
                  LEFT JOIN BING_EDW.dbo.vDimFiscalWeek dm_dt_age ON bal_snp.ARAgingDate = dm_dt_age.FiscalWeekEndDate
                  LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON bal_snp.ctr_no = dm_cctr.CenterCSSID
                                                                  AND EDWEndDate IS NULL -- DimCostCenter is SCD2, so get current record
                  LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                  LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                   AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                               AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get current record
                  LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cctr.CompanyID = dm_cmp.CompanyID
                  LEFT JOIN BING_EDW.dbo.DimSponsor dm_spr ON @SourceSystem = dm_spr.SourceSystem
                                                              AND bal_snp.ctr_no = dm_spr.CSSCenterNumber
                                                              AND bal_snp.fam_no = dm_spr.CSSFamilyNumber
                                                              AND dm_spr.EDWEndDate IS NULL -- DimSponsor is SCD2, so get current record
                  LEFT JOIN BING_EDW.dbo.DimTuitionAssistanceProvider dm_tut ON bal_snp.ctr_no = dm_tut.CSSCenterNumber
                                                                                AND bal_snp.cust_code = dm_tut.CSSCustomerCode
                  LEFT JOIN BING_EDW.dbo.DimARAgencyType dm_at ON @SourceSystem = dm_at.SourceSystem
                                                                  AND CASE
                                                                          WHEN bal_snp.Cust_Code IS NOT NULL -- There is Tuition Assistance Provider 
                                                                          THEN 'Subsidy Agency'
                                                                          WHEN bal_snp.Fam_No IS NOT NULL -- There is a Sponsor
                                                                          THEN 'Family'
                                                                      END = dm_at.ARAgencyTypeName
                  LEFT JOIN BING_EDW.dbo.DimARAgingBucket dm_age_buc ON bal_snp.ARAgingDays BETWEEN dm_age_buc.ARAgingDaysFrom AND dm_age_buc.ARAgingDaysTo
			   LEFT JOIN dbo.CenterCSSMigrations css_mgrtn ON bal_snp.ctr_no = css_mgrtn.CenterCSSID 
             WHERE bal_snp.AsOfFiscalWeek = @FiscalWeekNumber   -- Only return records if the current FiscalWeek falls BEFORE the CSS -> CMS Migration for a given Center                   
		   AND (css_mgrtn.MigrationFiscalWeekNumber IS NULL                       
             OR   css_mgrtn.MigrationFiscalWeekNumber > @FiscalWeekNumber);
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
