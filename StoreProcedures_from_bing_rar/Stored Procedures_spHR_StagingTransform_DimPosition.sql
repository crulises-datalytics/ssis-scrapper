CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimPosition] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimPosition
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
    --                     INSERT #DimPositionUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_DimPosition @EDWRunDateTime
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
    -- 12/19/2017  sburke              BNG-264 Initial version of proc
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
             SELECT COALESCE(PositionEffectiveDate, '19000101') AS PositionEffectiveDate,
                    PositionEndDate AS PositionEndDate,
                    COALESCE(PositionCurrentRecordFlag, 'Y') AS PositionCurrentRecordFlag,
                    COALESCE(PositionID, -1) AS PositionID,
                    COALESCE(PositionCode, '-1') AS PositionCode,
                    COALESCE(PositionName, 'Unknown Position') AS PositionName,
                    COALESCE(RollupPositionName, 'Unknown Rollup Position') AS RollupPositionName,
                    COALESCE(PositionStatusCode, 'Unknown Position Status') AS PositionStatusCode,
                    COALESCE(PositionStatusName, 'Unknown Position Status') AS PositionStatusName,
                    COALESCE(PositionFunctionalAreaName, 'Unknown Functional Area') AS PositionFunctionalAreaName,
                    COALESCE(PositionSubfunctionalAreaName, 'Unknown Subfunctional Area') AS PositionSubfunctionalAreaName,
                    COALESCE(PositionCorporateOverheadValue, 'Unknown Corporate Overhead') AS PositionCorporateOverheadValue,
                    COALESCE(PositionBonusEligibleFlag, 'Unknown Bonus Eligible') AS PositionBonusEligibleFlag,
                    COALESCE(PositionBonusPlanName, 'Unknown Bonus Plan') AS PositionBonusPlanName,
                    COALESCE(PositionBonusTargetPercent, '0') AS PositionBonusTargetPercent,
                    COALESCE(PositionLTIPEligibleFlag, 'Unknown LTIP Eligible') AS PositionLTIPEligibleFlag,
                    COALESCE(PositionPayBasisID, -1) AS PositionPayBasisID,
                    COALESCE(PositionEntryPayGradeID, -1) AS PositionEntryPayGradeID,
                    COALESCE(JobID, -1) AS JobID,
                    COALESCE(JobCode, '-1') AS JobCode,
                    COALESCE(JobName, 'Unknown Job') AS JobName,
                    COALESCE(JobGroupCode, '-1') AS JobGroupCode,
                    COALESCE(JobGroupName, 'Unknown Job Group') AS JobGroupName,
                    COALESCE(JobCategoryID, -1) AS JobCategoryID,
                    COALESCE(JobCategoryName, 'Unknown Job Category') AS JobCategoryName,
                    COALESCE(JobCCDGroupName, 'Unknown Job CCD Group') AS JobCCDGroupName,
                    COALESCE(JobEEOCategoryID, -1) AS JobEEOCategoryID,
                    COALESCE(JobEEOCategoryName, 'Unknown EEO Category') AS JobEEOCategoryName,
                    COALESCE(JobFLSACode, '-1') AS JobFLSACode,
                    COALESCE(JobFLSAName, 'Unknown FLSA') AS JobFLSAName,
                    COALESCE(JobWorkersCompTypeCode, -1) AS JobWorkersCompTypeCode,
                    COALESCE(JobWorkersCompTypeName, 'Unknown Workers Comp Type') AS JobWorkersCompTypeName,
                    COALESCE(JobAATypeCode, -1) AS JobAATypeCode,
                    COALESCE(JobAATypeName, 'Unknown AA Type') AS JobAATypeName,
                    COALESCE(JobAACategoryName, 'Unknown AA Category') AS JobAACategoryName,
                    COALESCE(JobLevelCode, '-1') AS JobLevelCode,
                    COALESCE(JobLevelName, 'Unknown Job Level') AS JobLevelName,
                    COALESCE(JobPeopleGroupName, 'Unknown People Group') AS JobPeopleGroupName,
                    COALESCE(JobTypeName, 'Unknown Job Type') AS JobTypeName,
                    COALESCE(JobFamilyName, 'Unknown Job Family') AS JobFamilyName,
                    COALESCE(JobManagerFlag, 'Unknown Manager') AS JobManagerFlag,
                    COALESCE(JobPurchaseApprovalLevelName, 'Unknown Purchase Approval Level') AS JobPurchaseApprovalLevelName,
                    COALESCE(JobPcardApproverFlag, 'Unknown Pcard Approver') AS JobPcardApproverFlag,
                    COALESCE(PositionFlexValue1, NULL) AS PositionFlexValue1,
                    COALESCE(PositionFlexValue2, NULL) AS PositionFlexValue2,
                    COALESCE(PositionFlexValue3, NULL) AS PositionFlexValue3,
                    COALESCE(PositionFlexValue4, NULL) AS PositionFlexValue4,
                    COALESCE(PositionFlexValue5, NULL) AS PositionFlexValue5,
                    COALESCE(JobFlexValue1, NULL) AS JobFlexValue1,
                    COALESCE(JobFlexValue2, NULL) AS JobFlexValue2,
                    COALESCE(JobFlexValue3, NULL) AS JobFlexValue3,
                    COALESCE(JobFlexValue4, NULL) AS JobFlexValue4,
                    COALESCE(JobFlexValue5, NULL) AS JobFlexValue5,
                    COALESCE(PositionCreatedDate, '19000101') AS PositionCreatedDate,
                    COALESCE(PositionCreatedUser, '-1') AS PositionCreatedUser,
                    COALESCE(PositionModifiedDate, '19000101') AS PositionModifiedDate,
                    COALESCE(PositionModifiedUser, '-1') AS PositionModifiedUser,
                    COALESCE(JobCreatedDate, '19000101') AS JobCreatedDate,
                    COALESCE(JobCreatedUser, '-1') AS JobCreatedUser,
                    COALESCE(JobModifiedDate, '19000101') AS JobModifiedDate,
                    COALESCE(JobModifiedUser, '-1') AS JobModifiedUser,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate
             FROM dbo.vPositions;
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
GO