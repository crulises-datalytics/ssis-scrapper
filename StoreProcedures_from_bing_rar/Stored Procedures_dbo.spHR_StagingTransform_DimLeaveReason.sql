CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimLeaveReason] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimLeaveReason
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
    --                     INSERT #DimLeaveReasonUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_DimLeaveReason @EDWRunDateTime
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
    --  1/01/2018  sburke              BNG-553 Initial version of proc
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
             SELECT COALESCE(LeaveReasonCode, '-1') AS LeaveReasonCode,
                    COALESCE(LeaveReasonName, 'Unknown Leave Reason') AS LeaveReasonName,
                    COALESCE(LeaveReasonCreatedDate, '19000101') AS LeaveReasonCreatedDate,
                    COALESCE(LeaveReasonCreatedUser, -1) AS LeaveReasonCreatedUser,
                    COALESCE(LeaveReasonModifiedDate, '19000101') AS LeaveReasonModifiedDate,
                    COALESCE(LeaveReasonModifiedUser, -1) AS LeaveReasonModifiedUser,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate
             FROM dbo.vLeaveReasons;
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