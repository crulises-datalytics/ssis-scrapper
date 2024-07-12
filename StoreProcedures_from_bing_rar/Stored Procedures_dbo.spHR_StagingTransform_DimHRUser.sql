
CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimHRUser] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimHRUser
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
    --                     INSERT #DimHRUserUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_DimHRUser @EDWRunDateTime
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
    -- 12/20/2017  Adevabhakthuni              BNG-265 Initial version of proc
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
             SELECT COALESCE(HRUserEffectiveDate, '19000101') AS HRUserEffectiveDate,
                    COALESCE(HRUserEndDate, '99991231') AS HRUserEndDate,
                    COALESCE(HRUserID, -1) AS HRUserID,
                    COALESCE(HRUserCode, '-1') AS HRUserCode,
                    COALESCE(HRUserName, 'Unknown User') AS HRUserName,
                    COALESCE(HRUserEmployeeNumber, NULL) AS HRUserEmployeeNumber,
                    COALESCE(HRUserEmployeeName, NULL) AS HRUserEmployeeName,
                    COALESCE(HRUserCreatedDate, '19000101') AS HRUserCreatedDate,
                    COALESCE(HRUserCreatedUser, '-1') AS HRUserCreatedUser,
                    COALESCE(HRUserModifiedDate, 1) AS HRUserModifiedDate,
                    COALESCE(HRUserModifiedUser, '-1') AS HRUserModifiedUser,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate				
             FROM dbo.vUsers;
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