CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimPerformanceRating] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimPerformanceRating
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
    --                     EXEC dbo.spHR_StagingTransform_DimPerformanceRating 
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
    -- 3/12/18     valimineti          BNG-266 - Initial version
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
             SELECT  COALESCE(PerformanceRatingCode,'-1') AS PerformanceRatingCode
					,COALESCE(PerformanceRatingName,'Unknown Performance Rating') AS PerformanceRatingName
					,PerformanceRatingFlexAttribute1 AS PerformanceRatingFlexAttribute1
					,PerformanceRatingFlexAttribute2 AS PerformanceRatingFlexAttribute2
					,PerformanceRatingFlexAttribute3 AS PerformanceRatingFlexAttribute3
					,PerformanceRatingFlexAttribute4 AS PerformanceRatingFlexAttribute4
					,PerformanceRatingFlexAttribute5 AS PerformanceRatingFlexAttribute5
					,COALESCE(PerformanceRatingCreatedDate,'1900-01-01') AS PerformanceRatingCreatedDate
					,COALESCE(PerformanceRatingCreatedUser,-1) AS PerformanceRatingCreatedUser
					,COALESCE(PerformanceRatingModifiedDate,'1900-01-01') AS PerformanceRatingModifiedDate
					,COALESCE(PerformanceRatingModifiedUser,-1) AS PerformanceRatingModifiedUser
					,@EDWRunDateTime as EDWCreatedDate
					,@EDWRunDateTime as EDWModifiedDate
			FROM dbo.vPerformanceRatings;
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