

CREATE PROCEDURE [dbo].[spCMS_StagingTransform_DimARAgencyType]
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_DimARAgencyType
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
    -- Usage:              INSERT #TemplateUpsert -- (Temporary table)
    --                     EXEC dbo.spCMS_StagingTransform_DimARAgencyType 
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    --
    --  4/24/18     anmorales       BNG-1639 - ETL StagingToEDW for new Dimension table 
    --                                  (and update AR Balance Snapshot ETL) to support 
    --                                  AR Report measures
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
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             DECLARE @LastProcessedDate DATETIME;
             SET @LastProcessedDate =
             (
                 SELECT LastProcessedDate
                 FROM EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'DimARAgencyType - CMS'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything


                  SELECT COALESCE(NULLIF(sat.SubsidyAgencyTypeName, ''), 'Unknown Tuition Assistance Provider Type') AS ARAgencyTypeName,
                         CASE
                             WHEN sat.SubsidyAgencyTypeName IN('Military', 'Pre-K', 'Government', 'Colleges', 'Grants and Scholarships', 'Headstart', 'Tribal')
                             THEN 'Subsidy'
                             WHEN sat.SubsidyAgencyTypeName IN('Back Up Child Care', 'Partner', 'Client Subsidy')
                             THEN 'Partner'
                             ELSE 'Unknown AR Type'
                         END AS ARType,
                         'CMS' AS SourceSystem,
                         GETDATE() AS EDWCreatedDate,
                         CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy
                  FROM dbo.orgSubsidyAgencyType sat(NOLOCK)
                  WHERE sat.StgModifiedDate >= @LastProcessedDate
			   UNION 
			   SELECT 'Family' AS ARAgencyTypeName,
                         'Parent' AS ARType,
                         'CMS' AS SourceSystem,
                         GETDATE() AS EDWCreatedDate,
                         CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy;
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


