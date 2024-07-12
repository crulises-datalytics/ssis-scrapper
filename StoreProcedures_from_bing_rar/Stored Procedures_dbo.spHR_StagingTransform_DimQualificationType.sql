/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingTransform_DimQualificationType'
)
    DROP PROCEDURE dbo.spHR_StagingTransform_DimQualificationType;
GO
*/
CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimQualificationType] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimQualificationType
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
    -- Usage:              DECLARE @EDWRunDateTime DATETIME2 = GETDATE();              
    --                     INSERT #DimQualificationTypeUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_DimQualificationType @EDWRunDateTime
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date           Modified By         Comments
    -- ----           -----------         --------
    -- 03/15/2018     Adevabhakthuni      BNG-551 Dim QualificationType
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
             SELECT COALESCE(QualificationTypeID, -1) AS QualificationTypeID,
                    COALESCE(QualificationTypeName, 'Unknown Qualification Type') AS QualificationTypeName,
                    COALESCE(QualificationCategoryCode, 'Unknown Qualification Category') AS QualificationCategoryCode,
                    COALESCE(QualificationCategoryName, '-1') AS QualificationCategoryName,
					QualificationTypeFlexAttribute1,
					QualificationTypeFlexAttribute2,
					QualificationTypeFlexAttribute3,
					QualificationTypeFlexAttribute4,
					QualificationTypeFlexAttribute5,
                    COALESCE(QualificationTypeCreatedDate, '19000101') AS QualificationTypeCreatedDate,
                    COALESCE(QualificationTypeCreatedUser, -1) AS QualificationTypeCreatedUser,
                    COALESCE(QualificationTypeModifiedDate, '19000101') AS QualificationTypeModifiedDate,
                    COALESCE(QualificationTypeModifiedUser, -1) AS QualificationTypeModifiedUser,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate
					From dbo.vQualificationTypes;
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