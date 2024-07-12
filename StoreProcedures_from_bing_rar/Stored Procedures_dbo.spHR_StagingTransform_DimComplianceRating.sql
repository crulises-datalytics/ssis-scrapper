Create Procedure [dbo].[spHR_StagingTransform_DimComplianceRating] 
(@EDWRunDateTime DateTime2=Null) 
As
 
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimComplianceRating
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
    --                     EXEC dbo.spHR_StagingTransform_DimComplianceRating 
    -- 
    --
    -- --------------------------------------------------------------------------------
	-- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    --
    -- 03/15/18    Banandesi          BNG-550 - Initial version
    --			 
    -- ================================================================================
	Begin 
	 SET NOCOUNT ON;
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
		 SELECT 
		          COALESCE(ComplianceRatingID, -1) AS ComplianceRatingID,
                    COALESCE(ComplianceRatingName, 'Unknown Compliance Rating') AS ComplianceRatingName,
				COALESCE(ComplianceRatingScaleID, -1) AS ComplianceRatingScaleID,
				COALESCE(ComplianceRatingScaleName, 'Unknown Compliance Rating Scale') AS ComplianceRatingScaleName,
                    COALESCE(ComplianceRatingScaleDescription, 'Unknown Compliance Rating Scale') AS ComplianceRatingScaleDescription,
                    COALESCE(ComplianceRatingCreatedDate, '19000101') AS ComplianceRatingCreatedDate,
                    COALESCE(ComplianceRatingCreatedUser, -1) AS ComplianceRatingCreatedUser,
                    COALESCE(ComplianceRatingModifiedDate, '19000101') AS ComplianceRatingModifiedDate,
                    COALESCE(ComplianceRatingModifiedUser, -1) AS ComplianceRatingModifiedUser,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate
					From dbo.vComplianceRatings;
			End Try 
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