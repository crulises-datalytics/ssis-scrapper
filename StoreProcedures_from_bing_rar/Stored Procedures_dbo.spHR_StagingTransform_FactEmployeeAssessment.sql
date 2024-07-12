
CREATE PROCEDURE [dbo].[spHR_StagingTransform_FactEmployeeAssessment]
(@EDWRunDateTime     DATETIME2 = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_FactEmployeeAssessment
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
    -- Usage:              INSERT #FactEmployeeAssessmentUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_FactEmployeeAssessment @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By        Comments
    -- ----         -----------        --------
    --
    -- 3/16/18        Banandesi        BNG-278 - Intital version of the proc
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
				,COALESCE(dm_per.PersonKey,-1) AS PersonKey
				,COALESCE(dm_Assess.AssessmentTypeKey,-1) AS AssessmentTypeKey
				,COALESCE(EmployeeAssessmentID,-1) AS EmployeeAssessmentID
				,COALESCE(AssessmentID,-1) AS AssessmentID
				,AssessmentFlexValue1 AS EmployeeAssessmentFlexValue1
				,AssessmentFlexValue2 AS EmployeeAssessmentFlexValue2
				,AssessmentFlexValue3 AS EmployeeAssessmentFlexValue3
				,AssessmentFlexValue4 AS EmployeeAssessmentFlexValue4
				,AssessmentFlexValue5 AS EmployeeAssessmentFlexValue5
                    ,COALESCE(AssessmentCreatedDate,'1/1/1900') AS EmployeeAssessmentCreatedDate
                    ,COALESCE(AssessmentCreatedUser,-1) AS EmployeeAssessmentCreatedUser
                    ,COALESCE(AssessmentModifiedDate,'1/1/1900') As EmployeeAssessmentModifiedDate
                    ,COALESCE(AssessmentModifiedUser,-1) As EmployeeAssessmentModifiedUser
                    ,@EDWRunDateTime AS EDWCreatedDate
			FROM vAssessments fact_Assessments
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON dm_dt.FullDate = fact_Assessments.AssessmentDate
			LEFT JOIN BING_EDW.dbo.DimPerson dm_per ON dm_per.PersonID = fact_Assessments.PersonID AND dm_per.PersonCurrentRecordFlag='Y' 
			LEFT JOIN BING_EDW.dbo.DimAssessmentType dm_Assess ON dm_Assess.AssessmentJobName=fact_Assessments.AssessmentJobName
			AND dm_Assess.AssessmentRatingCode = fact_Assessments.AssessmentRatingCode

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