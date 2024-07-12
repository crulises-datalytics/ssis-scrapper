
CREATE PROCEDURE [dbo].[spHR_StagingTransform_FactEmployeeCompliance]
(@EDWRunDateTime     DATETIME2 = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_FactEmployeeCompliance
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
    -- Usage:              INSERT #FactPersonSpecialInfoUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_FactEmployeeCompliance @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By        Comments
    -- ----         -----------        --------
    --
    -- 3/19/18        valimineti        BNG-277 - Intital version of the proc
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
             SELECT COALESCE(dm_dt.DateKey,-1) AS EmployeeComplianceEffectiveDateKey,
				COALESCE(dm_dt1.DateKey,-1) AS EmployeeComplianceEndDateKey,
				COALESCE(EmployeeComplianceCurrentRecordFlag,'X') AS EmployeeComplianceCurrentRecordFlag,
				COALESCE(PersonKey,-1) AS PersonKey,
				COALESCE(ComplianceItemKey,-1) AS ComplianceItemKey,
				COALESCE(ComplianceRatingKey,-1) AS ComplianceRatingKey,
				COALESCE(ComplianceID,-1) AS EmployeeComplianceID,
				ComplianceValue1,
				ComplianceValue2,
				ComplianceValue3,
				ComplianceValue4,
				ComplianceValue5,
				ComplianceValue6,
				ComplianceValue7,
				ComplianceValue8,
				ComplianceValue9,
				ComplianceValue10,
				ComplianceValue11,
				ComplianceValue12,
				ComplianceValue13,
				ComplianceValue14,
				ComplianceValue15,
				ComplianceValue16,
				ComplianceValue17,
				ComplianceValue18,
				ComplianceValue19,
				ComplianceValue20,
				COALESCE(ComplianceCreatedDate,'1900-01-01') AS ComplianceCreatedDate,
				COALESCE(ComplianceCreatedUser,-1) AS ComplianceCreatedUser,
				COALESCE(ComplianceModifiedDate,'1900-01-01') AS ComplianceModifiedDate,
				COALESCE(ComplianceModifiedUser,-1) AS ComplianceModifiedUser,
				@EDWRunDateTime AS EDWCreatedDate,
				@EDWRunDateTime AS EDWModifiedDate
		  FROM vCompliance fct_empcomp
				LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON dm_dt.FullDate = fct_empcomp.ComplianceEffectiveDate
				LEFT JOIN BING_EDW.dbo.DimDate dm_dt1 ON dm_dt1.FullDate = fct_empcomp.ComplianceEndDate
				LEFT JOIN BING_EDW.dbo.DimPerson dm_per ON dm_per.PersonID = fct_empcomp.PersonID
												AND dm_per.PersonCurrentRecordFlag = 'Y'
				LEFT JOIN BING_EDW.dbo.DimComplianceItem dm_compitem ON dm_compitem.ComplianceItemID = fct_empcomp.ComplianceItemID
				LEFT JOIN BING_EDW.dbo.DimComplianceRating dm_comprtg ON dm_comprtg.ComplianceRatingID = fct_empcomp.ComplianceRatingID;

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