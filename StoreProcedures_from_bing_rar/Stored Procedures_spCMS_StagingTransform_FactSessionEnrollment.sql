/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCMS_StagingTransform_FactSessionEnrollment'
)
    DROP PROCEDURE dbo.spCMS_StagingTransform_FactSessionEnrollment;
GO
*/
CREATE PROCEDURE [dbo].[spCMS_StagingTransform_FactSessionEnrollment] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_FactSessionEnrollment
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
    -- Usage:              INSERT #FactSessionEnrollmentUpsert -- (Temporary table)
    --                     EXEC dbo.spCMS_StagingTransform_FactSessionEnrollment
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
                 FROM CMS_Staging..EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'FactSessionEnrollment'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

             SELECT vEnr.idEnrollment AS EnrollmentID,
                    vEnr.idProgramEnrollment AS ProgramEnrollmentID,
                    vEnr.idSessionEnrollment AS SessionEnrollmentID,
				COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                    COALESCE(dm_loc.LocationKey, -1) AS LocationKey,
				COALESCE(dm_cmp.CompanyKey, -1) AS CompanyKey,
				COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey,
				COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
                    COALESCE(dm_std.StudentKey, -1) AS StudentKey,
                    COALESCE(dm_spr.SponsorKey, -1) AS SponsorKey,
                    COALESCE(dm_pgr.ProgramKey, -1) AS ProgramKey,
                    COALESCE(dm_ses.SessionKey, -1) AS SessionKey,
                    COALESCE(dm_sch_wk.ScheduleWeekKey, -1) AS ScheduleWeekKey,
                    COALESCE(dm_cls.ClassroomKey, -1) AS ClassroomKey,
                    COALESCE(dm_dt_eff.DateKey, -1) AS SessionEnrollmentEffectiveDateKey,
                    COALESCE(dm_dt_end.DateKey, -1) AS SessionEnrollmentEndDateKey,
                    COALESCE(FTE, 0) AS DailyFTE,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate,
                    vEnr.Deleted
             FROM dbo.vEnrollment vEnr
                  LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON vEnr.idSite = dm_cctr.CenterCMSID
                                                                  AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                  LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                   AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                               AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cctr.CompanyID = dm_cmp.CompanyID
                  LEFT JOIN BING_EDW.dbo.DimStudent dm_std ON vEnr.idStudent = dm_std.StudentID
															  AND dm_std.SourceSystem = 'CMS'
                                                              AND dm_std.EDWEndDate IS NULL -- DimStudent is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimSponsor dm_spr ON vEnr.idSponsor = dm_spr.SponsorID
															  AND dm_spr.SourceSystem = 'CMS'
                                                              AND dm_spr.EDWEndDate IS NULL -- DimSponsor is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimProgram dm_pgr ON vEnr.idProgram = dm_pgr.ProgramID
                  LEFT JOIN BING_EDW.dbo.DimSession dm_ses ON vEnr.idSessionType = dm_ses.SessionID
                                                           AND dm_ses.SourceSystem = 'CMS'
                                                           AND dm_ses.EDWEndDate IS NULL -- DimSession is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimClassroom dm_cls ON vEnr.idClassroom = dm_cls.ClassroomID
                  LEFT JOIN BING_EDW.dbo.DimDate dm_dt_eff ON CAST(vEnr.EffectiveDate AS DATE) = dm_dt_eff.FullDate
                  LEFT JOIN BING_EDW.dbo.DimDate dm_dt_end ON CAST(vEnr.EndDate AS DATE) = dm_dt_end.FullDate
                  LEFT JOIN BING_EDW.dbo.DimScheduleWeek dm_sch_wk ON vEnr.ScheduleWeekFlags = dm_sch_wk.ScheduleWeekFlags
             WHERE(vEnr.EnrollmentModifiedDate >= @LastProcessedDate
                   OR vEnr.ProgramModifiedDate >= @LastProcessedDate
                   OR vEnr.SessionModifiedDate >= @LastProcessedDate
                   OR vEnr.StudentSponsorModifiedDate >= @LastProcessedDate
                   OR vEnr.SessionTypeModifiedDate >= @LastProcessedDate
                   OR vEnr.ScheduleDayModifiedDate >= @LastProcessedDate);
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


