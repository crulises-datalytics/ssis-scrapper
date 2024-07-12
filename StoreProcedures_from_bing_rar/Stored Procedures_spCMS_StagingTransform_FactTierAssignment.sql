CREATE PROCEDURE [dbo].[spCMS_StagingTransform_FactTierAssignment] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_FactTierAssignment
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
    -- Usage:              INSERT #FactTierAssignmentUpsert -- (Temporary table)
    --                     EXEC dbo.spCMS_StagingTransform_FactTierAssignment
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    --
    -- 11/07/17     sburke          Initial version of proc, converted from SSIS logic
    -- 12/18/17     sburke          BNG-913 - Fixed missing SCD2 join on DimLocation
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
             DECLARE @LastProcessedDate DATETIME;
             SET @LastProcessedDate =
             (
                 SELECT LastProcessedDate
                 FROM EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'FactTierAssignment'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything
             WITH stdSponsorCTE
                  AS (
                  SELECT ds.SponsorKey,
                         s.idStudent,
                         s.StgModifiedDate
                  FROM dbo.stdStudentSponsor s
                       LEFT JOIN BING_EDW.dbo.DimSponsor ds ON s.idSponsor = ds.SponsorID
															   AND ds.SourceSystem = 'CMS'
                                                               AND ds.EDWEndDate IS NULL
                  WHERE idSponsorType = 1)
                  SELECT COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                         COALESCE(dm_loc.LocationKey, -1) AS LocationKey,
                         COALESCE(dm_cmp.CompanyKey, -1) AS CompanyKey,
                         COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey,
                         COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
                         COALESCE(dm_std.StudentKey, -1) AS StudentKey,
                         COALESCE(cte_spr.SponsorKey, -1) AS SponsorKey,
                         COALESCE(dm_tier.TierKey, -1) AS TierKey,
                         COALESCE(vTier.idEnrollment, -1) AS EnrollmentID,
                         COALESCE(dm_dt_eff.DateKey, -1) AS TierAssignmentEffectiveDateKey,
                         COALESCE(dm_dt_end.DateKey, -1) AS TierAssignmentEndDateKey,
                         CASE
                             WHEN vTier.TierEndDateAltered = 1
                             THEN 'Yes'
                             ELSE 'No'
                         END AS TierDatesEDWChosen,
                         @EDWRunDateTime AS EDWCreatedDate,
                         @EDWRunDateTime AS EDWModifiedDate,
                         vTier.Deleted
                  FROM dbo.vTierAssignment vTier
                       LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON vTier.idSite = dm_cctr.CenterCMSID
                                                                       AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                       LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber 
                                                                        AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                                    AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cctr.CompanyID = dm_cmp.CompanyID
                       LEFT JOIN BING_EDW.dbo.DimStudent dm_std ON vTier.idStudent = dm_std.StudentID
                                                                   AND dm_std.SourceSystem = 'CMS'
																   AND dm_std.EDWEndDate IS NULL -- DimStudent is SCD2, so get the latest version
                       LEFT JOIN stdSponsorCTE cte_spr ON vTier.idStudent = cte_spr.idStudent
                       LEFT JOIN BING_EDW.dbo.DimTier dm_tier ON vTier.idSiteTier = dm_tier.TierID
                       LEFT JOIN BING_EDW.dbo.DimDate dm_dt_eff ON vTier.TierStartDate = dm_dt_eff.FullDate
                       LEFT JOIN BING_EDW.dbo.DimDate dm_dt_end ON vTier.TierEndDate = dm_dt_end.FullDate
                  WHERE(vTier.StgModifiedDate >= @LastProcessedDate
                        OR cte_spr.StgModifiedDate >= @LastProcessedDate);
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


