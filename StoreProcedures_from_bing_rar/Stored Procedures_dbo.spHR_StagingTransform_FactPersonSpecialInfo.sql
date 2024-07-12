
CREATE PROCEDURE [dbo].[spHR_StagingTransform_FactPersonSpecialInfo]
(@EDWRunDateTime     DATETIME2 = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_FactPersonSpecialInfo
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
    --                     EXEC dbo.spHR_StagingTransform_FactPersonSpecialInfo @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By        Comments
    -- ----         -----------        --------
    --
    -- 3/16/18        valimineti        BNG-276 - Intital version of the proc
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
             SELECT COALESCE(dm_dt.DateKey,-1) AS PersonSpecialInfoEffectiveDateKey,
				COALESCE(dm_dt1.DateKey,-1) AS PersonSpecialInfoEndDateKey,
				COALESCE(PersonSpecialInfoCurrentRecordFlag,'X') AS PersonSpecialInfoCurrentRecordFlag,
				COALESCE(PersonKey,-1) AS PersonKey,
				COALESCE(SpecialInfoKey,-1) AS SpecialInfoKey,
				COALESCE(PersonSpecialInfoID,-1) AS PersonSpecialInfoID,
				COALESCE(PersonSpecialInfoCreatedUser,-1) AS PersonSpecialInfoCreatedUser,
				COALESCE(PersonSpecialInfoCreatedDate,'1900-01-01') AS PersonSpecialInfoCreatedDate,
				COALESCE(PersonSpecialInfoModifiedUser,-1) AS PersonSpecialInfoModifiedUser,
				COALESCE(PersonSpecialInfoModifiedDate,'1900-01-01') AS PersonSpecialInfoModifiedDate,
				@EDWRunDateTime AS EDWCreatedDate,
				@EDWRunDateTime AS EDWModifiedDate
		  FROM vPersonSpecialInfo fct_perspl
				LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON dm_dt.FullDate = fct_perspl.PersonSpecialInfoEffectiveDate
				LEFT JOIN BING_EDW.dbo.DimDate dm_dt1 ON dm_dt1.FullDate = fct_perspl.PersonSpecialInfoEndDate
				LEFT JOIN BING_EDW.dbo.DimPerson dm_per ON dm_per.PersonID = fct_perspl.PersonID
												AND dm_per.PersonCurrentRecordFlag = 'Y'
				LEFT JOIN BING_EDW.dbo.DimSpecialInfo dm_splinfo ON dm_splinfo.SpecialInfoID = fct_perspl.SpecialInfoID;

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