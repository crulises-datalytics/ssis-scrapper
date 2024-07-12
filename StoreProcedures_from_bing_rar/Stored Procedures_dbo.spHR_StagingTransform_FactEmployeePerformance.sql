
CREATE PROCEDURE [dbo].[spHR_StagingTransform_FactEmployeePerformance]
(@EDWRunDateTime     DATETIME2 = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_FactEmployeePerformance
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
    -- Usage:              INSERT #FactEmployeePerformanceUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_FactEmployeePerformance @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By        Comments
    -- ----         -----------        --------
    --
    -- 3/14/18        valimineti        BNG-274 - Intital version of the proc
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
             SELECT 
				 COALESCE(dm_dt.DateKey,-1) AS DateKey
				,COALESCE(PersonKey,-1) AS PersonKey
				,COALESCE(PerformanceRatingKey,-1) AS PerformanceRatingKey
				,COALESCE(EmployeePerformanceID,-1) AS EmployeePerformanceID
				,COALESCE(EmployeePerformanceCreatedDate,'1900-01-01') AS EmployeePerformanceCreatedDate
				,COALESCE(EmployeePerformanceCreatedUser,-1) AS EmployeePerformanceCreatedUser
				,COALESCE(EmployeePerformanceModifiedDate,'1900-01-01') AS EmployeePerformanceModifiedDate
				,COALESCE(EmployeePerformanceModifiedUser,-1) AS EmployeePerformanceModifiedUser
				,@EDWRunDateTime AS EDWCreatedDate
			FROM vPerformanceReviews fct_empperf
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt on dm_dt.FullDate = fct_empperf.EmployeePerformanceDate
			LEFT JOIN BING_EDW.dbo.DimPerson dm_per on dm_per.PersonID = fct_empperf.PersonID AND dm_per.PersonCurrentRecordFlag='Y'
			LEFT JOIN BING_EDW.dbo.DimPerformanceRating dm_perf on dm_perf.PerformanceRatingCode = fct_empperf.PerformanceRatingCode 

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