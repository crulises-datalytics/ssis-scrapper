
CREATE PROCEDURE [dbo].[spHR_StagingTransform_FactEmployeeQualification]
(@EDWRunDateTime     DATETIME2 = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_FactEmployeeQualification
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
    -- Usage:              INSERT #FactEmployeeQualificationUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_FactEmployeeQualification @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By        Comments
    -- ----         -----------        --------
    --
    -- 3/19/18      Adevabhakthuni        BNG-959 - Intital version of the proc
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
				,COAlESCE(dm_dt1.DateKey,-1) AS QualificationAwardDateKey
				,COALESCE(PersonKey,-1) AS PersonKey
				,COALESCE(QualificationTypeKey,-1) AS QualificationTypeKey 
				,COALESCE(EmployeeQualificationID,-1) AS EmployeeQualificationID
				,EmployeeQualificationName
				,EmployeeQualificationFlexValue1
				,EmployeeQualificationFlexValue2
				,EmployeeQualificationFlexValue3
				,EmployeeQualificationFlexValue4
				,EmployeeQualificationFlexValue5
				,COALESCE(EmployeeQualificationCreatedDate,'1900-01-01') AS EmployeeQualificationCreatedDate
				,COALESCE(EmployeeQualificationCreatedUser,-1) AS EmployeeQualificationCreatedUser
				,COALESCE(EmployeeQualificationModifiedDate,'1900-01-01') AS EmployeeQualificationModifiedDate
				,COALESCE(EmployeeQualificationModifiedUser,-1) AS EmployeeQualificationModifiedUser
				,@EDWRunDateTime AS EDWCreatedDate
				,@EDWRunDateTime AS EDWModifiedDate
			FROM vQualifications fct_empqual 
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt on dm_dt.FullDate = fct_empqual.QualificationDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt1 on dm_dt1.FullDate = fct_empqual.QualificationAwardDate
			LEFT JOIN BING_EDW.dbo.DimPerson dm_prsn on dm_prsn.PersonID = fct_empqual.PersonID AND dm_prsn.PersonCurrentRecordFlag='Y'
			LEFT JOIN BING_EDW.dbo.DimQualificationType dm_qualT on dm_qualT.QualificationTypeID = fct_empqual.QualificationTypeID 

			

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