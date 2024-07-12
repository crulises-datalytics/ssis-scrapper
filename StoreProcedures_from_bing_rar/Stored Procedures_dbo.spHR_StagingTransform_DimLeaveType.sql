CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimLeaveType] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimLeaveType
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
    --                     EXEC dbo.spHR_StagingTransform_DimLeaveType 
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
    -- 03/06/18    Adevabhakthuni          BNG-552 - Initial version
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
             SELECT COALESCE(LeaveTypeID, -1) AS LeaveTypeID,
                    COALESCE(LeaveTypeName, 'Unknown Leave Type') AS LeaveTypeName,
                    COALESCE(LeaveCategoryCode, '-1') AS LeaveCategoryCode,
                    COALESCE(LeaveCategoryName, 'Unknown Leave Category') AS LeaveCategoryName,
                    COALESCE(LeaveTimeframeCode, '-1') AS LeaveTimeframeCode,
                    COALESCE(LeaveTimeframeName, 'Unknown Leave Timeframe') AS LeaveTimeframeName,
                    LeaveTypeFlexAttribute1,
                    LeaveTypeFlexAttribute2,
                    LeaveTypeFlexAttribute3,
                    LeaveTypeFlexAttribute4,
                    LeaveTypeFlexAttribute5,
                    COALESCE(LeaveTypeCreatedDate, '19000101') AS LeaveTypeCreatedDate,
                    COALESCE(LeaveTypeCreatedUser, -1) AS LeaveTypeCreatedUser,
                    COALESCE(LeaveTypeModifiedDate, '19000101') AS LeaveTypeModifiedDate,
                    COALESCE(LeaveTypeModifiedUser, -1) AS LeaveTypeModifiedUser,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate
             FROM dbo.vLeaveTypes;
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