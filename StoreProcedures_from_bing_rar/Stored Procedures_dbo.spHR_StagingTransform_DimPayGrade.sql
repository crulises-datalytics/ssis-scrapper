/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingTransform_DimPayGrade'
)
    DROP PROCEDURE dbo.spHR_StagingTransform_DimPayGrade;
GO
*/
CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimPayGrade] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimPayGrade
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
    --                     INSERT #DimPayGradeUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_DimPayGrade @EDWRunDateTime
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
             SELECT COALESCE(PayGradeID, -1) AS PayGradeID,
                    COALESCE(PayGradeName, 'Unknown Pay Grade') AS PayGradeName,
                    COALESCE(PayGradeSort, -1) AS PayGradeSort,
                    COALESCE(PayGradeJobTypeCode, '-1') AS PayGradeJobTypeCode,
                    COALESCE(PayGradeJobTypeName, 'Unknown Pay Grade Job Type') AS PayGradeJobTypeName,
                    COALESCE(PayGradeJobCode, '-1') AS PayGradeJobCode,
                    COALESCE(PayGradeJobName, 'Unknown Pay Grade Job') AS PayGradeJobName,
                    COALESCE(PayGradeGeoCode, '-1') AS PayGradeGeoCode,
                    COALESCE(PayGradeGeoName, 'Unknown Pay Grade Geo') AS PayGradeGeoName,
                    COALESCE(PayGradeRuleID, -1) AS PayGradeRuleID,
                    COALESCE(PayGradeRuleMinValue, '0') AS PayGradeRuleMinValue,
                    COALESCE(PayGradeRuleMidValue, '0') AS PayGradeRuleMidValue,
                    COALESCE(PayGradeRuleMaxValue, '0') AS PayGradeRuleMaxValue,
                    COALESCE(PayGradeFlexAttribute1, NULL) AS PayGradeFlexAttribute1,
                    COALESCE(PayGradeFlexAttribute2, NULL) AS PayGradeFlexAttribute2,
                    COALESCE(PayGradeFlexAttribute3, NULL) AS PayGradeFlexAttribute3,
                    COALESCE(PayGradeFlexAttribute4, NULL) AS PayGradeFlexAttribute4,
                    COALESCE(PayGradeFlexAttribute5, NULL) AS PayGradeFlexAttribute5,
                    COALESCE(PayGradeCreatedDate, '19000101') AS PayGradeCreatedDate,
                    COALESCE(PayGradeCreatedUser, '-1') AS PayGradeCreatedUser,
                    COALESCE(PayGradeModifiedDate, '19000101') AS PayGradeModifiedDate,
                    COALESCE(PayGradeModifiedUser, '-1') AS PayGradeModifiedUser,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate
             FROM dbo.vPayGrades;
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