CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimAssignmentType] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimAssignmentType
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
    --                     INSERT #DimAssignmentTypeUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_DimAssignmentType @EDWRunDateTime
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
    --  1/01/2018  sburke              BNG-261 Initial version of proc
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
             SELECT COALESCE(AssignmentStatusTypeID, -1) AS AssignmentStatusTypeID,
                    COALESCE(AssignmentStatusTypeName, 'Unknown Assignment Status Type') AS AssignmentStatusTypeName,
                    COALESCE(AssignmentNQDCFlag, 'Unknown NQDC') AS AssignmentNQDCFlag,
                    COALESCE(AssignmentBusinessTitleName, 'Unknown Business Title') AS AssignmentBusinessTitleName,
                    COALESCE(AssignmentWorkAtHomeFlag, 'Unknown Work At Home') AS AssignmentWorkAtHomeFlag,
                    COALESCE(AssignmentIVRCode, '-1') AS AssignmentIVRCode,
                    COALESCE(AssignmentESMStatusChangeReasonName, 'Unknown ESM Status Change Reason') AS AssignmentESMStatusChangeReasonName,
                    COALESCE(AssignmentBonusPercent, 0) AS AssignmentBonusPercent,
                    COALESCE(AssignmentTypeCode, '-1') AS AssignmentTypeCode,
                    COALESCE(AssignmentTypeName, 'Unknown Assignment Type') AS AssignmentTypeName,
                    COALESCE(EmploymentCategoryCode, '-1') AS EmploymentCategoryCode,
                    COALESCE(EmploymentCategoryName, 'Unknown Employment Category') AS EmploymentCategoryName,
                    COALESCE(EmploymentEligibleRehireFlag, 'Unknown Eligible Rehire') AS EmploymentEligibleRehireFlag,
                    COALESCE(EmploymentTwoWeeksNoticeFlag, 'Unknown Two Weeks Notice') AS EmploymentTwoWeeksNoticeFlag,
                    COALESCE(EmploymentTerminationRegrettableFlag, 'Unknown Termination Regrettable') AS EmploymentTerminationRegrettableFlag,
                    COALESCE(EmploymentLeavingReasonCode, '-1') AS EmploymentLeavingReasonCode,
                    COALESCE(EmploymentLeavingReasonName, 'Unknown Leaving Reason') AS EmploymentLeavingReasonName,
                    COALESCE(EmploymentLeavingReasonDescription, 'Unknown Leaving Reason') AS EmploymentLeavingReasonDescription,
                    COALESCE(EmploymentLeavingReasonTypeName, 'Unknown Leaving Reason Type') AS EmploymentLeavingReasonTypeName,
                    @EDWRunDateTime AS EDWCreatedDate
             FROM dbo.vAssignmentTypes;
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