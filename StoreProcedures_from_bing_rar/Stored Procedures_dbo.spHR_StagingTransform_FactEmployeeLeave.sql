
CREATE PROCEDURE [dbo].[spHR_StagingTransform_FactEmployeeLeave]
(@EDWRunDateTime     DATETIME2 = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_FactEmployeeLeave
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
    -- Usage:              INSERT #FactEmployeeLeaveUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_FactEmployeeLeave @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By        Comments
    -- ----         -----------        --------
    --
    -- 3/16/18        hhebbalu        BNG-279 - Intital version of the proc
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
				 COALESCE(dm_dt.DateKey,-1) AS LeaveActualStartDateKey
				,COALESCE(dm_dt1.DateKey,-1) AS LeaveActualEndDateKey
				,COALESCE(dm_dt2.DateKey,-1) AS LeaveProjectedStartDateKey
				,COALESCE(dm_dt3.DateKey,-1) AS LeaveProjectedEndDateKey
				,COALESCE(dm_ltype.LeaveTypeKey,-1) AS LeaveTypeKey
				--,COALESCE(dm_lrsn.LeaveReasonKey,-1) AS LeaveReasonKey
				,-1 AS LeaveReasonKey		 -- the LeaveReasonCode in DimLeaveReason is varchar and the LeaveReasonID in PerAbsenceAttendances(vLeaves) is integer. So can not be joined. It's hardcoded to -1 until gets clarified from HR team/Karin
				,COALESCE(dm_prsn.PersonKey,-1) AS PersonKey
				,COALESCE(fct_Leaves.LeaveID,-1) AS EmployeeLeaveID
				,COALESCE(fct_Leaves.LeaveDays,-1) AS LeaveDays
				,COALESCE(fct_Leaves.LeaveHours,-1) AS LeaveHours
				,COALESCE(fct_Leaves.LeaveCreatedDate,'1900-01-01') AS LeaveCreatedDate
				,COALESCE(fct_Leaves.LeaveCreatedUser,-1) AS LeaveCreatedUser
				,COALESCE(fct_Leaves.LeaveModifiedDate,'1900-01-01') AS LeaveModifiedDate
				,COALESCE(fct_Leaves.LeaveModifiedUser,-1) AS LeaveModifiedUser
                ,@EDWRunDateTime AS EDWCreatedDate
				FROM vLeaves fct_Leaves
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt on dm_dt.FullDate = fct_Leaves.LeaveActualStartDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt1 on dm_dt1.FullDate = fct_Leaves.LeaveActualEndDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt2 on dm_dt2.FullDate = fct_Leaves.LeaveProjectedStartDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt3 on dm_dt3.FullDate = fct_Leaves.LeaveProjectedEndDate
			LEFT JOIN BING_EDW.dbo.DimLeaveType dm_ltype on dm_ltype.LeaveTypeID = fct_Leaves.LeaveTypeID
			--LEFT JOIN BING_EDW.dbo.DimLeaveReason dm_lrsn on dm_lrsn.LeaveReasonCode = fct_Leaves.LeaveReasonID
			LEFT JOIN BING_EDW.dbo.DimPerson dm_prsn on dm_prsn.PersonID = fct_Leaves.PersonID and dm_prsn.PersonEndDate = '12/31/4712'

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