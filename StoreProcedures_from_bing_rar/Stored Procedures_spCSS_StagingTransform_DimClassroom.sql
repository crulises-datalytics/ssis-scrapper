
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCSS_StagingTransform_DimClassroom'
)
    DROP PROCEDURE dbo.spCSS_StagingTransform_DimClassroom;
GO
*/
CREATE PROCEDURE [dbo].[spCSS_StagingTransform_DimClassroom] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_StagingTransform_DimClassroom
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
    --                     EXEC dbo.spCSS_StagingTransform_DimClassroom @EDWRunDateTime
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
    -- 10/17/17    sburke              BNG-639 / BNG-640.  Refactoring StagingToEDW process
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
	    -- ================================================================================
	    -- This is the C S S version of the ETL load for DimClassroom
	    --             -----
	    -- ================================================================================
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             DECLARE @LastProcessedDate DATETIME;
             SET @LastProcessedDate =
             (
                 SELECT LastProcessedDate
                 FROM CSS_Staging.dbo.EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'DimClassroom - CSS'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

             SELECT-2 AS ClassroomID,
                   COALESCE(class_cd, 'Unknown Classroom') AS ClassroomName,
                   COALESCE(class_cap, 0) AS ClassroomCapacity,
                   'Unknown Classroom Type' AS ClassroomType,
                   COALESCE(ctr_no, '-1') AS CSSCenterNumber,
                   COALESCE('CSS', 'UNK') AS SourceSystem,
                   GETDATE() AS EDWCreatedDate,
                   CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                   GETDATE() AS EDWModifiedDate,
                   CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy,
			    NULL AS Deleted
             FROM [dbo].[Csocllcd];
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


