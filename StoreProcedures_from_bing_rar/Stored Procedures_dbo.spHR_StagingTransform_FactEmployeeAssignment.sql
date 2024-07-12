
CREATE PROCEDURE [dbo].[spHR_StagingTransform_FactEmployeeAssignment]
(@EDWRunDateTime     DATETIME2 = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_FactEmployeeAssignment
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
    -- Usage:              INSERT #FactEmployeeAssignmentUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_FactEmployeeAssignment @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By        Comments
    -- ----         -----------        --------
    --
    -- 3/13/18        Banandesi        BNG-273 - Intital version of the proc
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
				 COALESCE(dm_dt.DateKey,-1) AS AssignmentStartDateKey
				,COALESCE(dm_dt1.DateKey,-1) as AssignmentEndDateKey
				,COALESCE(fct_Assignments.AssignmentCurrentRecordFlag,'-1') as AssignmentCurrentRecordFlag
				,COALESCE(dm_dt2.DateKey,-1) as AssignmentPositionStartDateKey
				,COALESCE(dm_dt3.DateKey,-1) as AssignmentProjectedEndDateKey
				,COALESCE(dm_dt4.DateKey,-1) as EmploymentStartDateKey
				,COALESCE(dm_dt5.DateKey,-1) as EmploymentEndDateKey
				,COALESCE(dm_dt6.DateKey,-1) as EmploymentAdjustedServiceDateKey
				,COALESCE(dm_dt7.DateKey,-1) as EmploymentLastWorkedDateKey
				,COALESCE(dm_dt8.DateKey,-1) as EmploymentTerminationNotifiedDateKey
				,COALESCE(dm_dt9.DateKey,-1) as EmploymentTerminationAcceptedDateKey
				,COALESCE(dm_dt10.DateKey,-1) as EmploymentTerminationProjectedDateKey
				,COALESCE(dm_dt11.DateKey,-1) as EmploymentTerminationActualDateKey
				,COALESCE(dm_dt12.DateKey,-1) as EmploymentLastPayrollProcessDateKey
				,COALESCE(dm_per.PersonKey,-1) AS PersonKey
				,COALESCE(dm_per1.PersonKey,-1) AS ExecutiveAssistantPersonKey
				,COALESCE(dm_per2.PersonKey,-1) AS SupervisorPersonKey
				,COALESCE(LocationKey,-1) AS LocationKey
				,COALESCE(OrgKey,-1) AS OrgKey
				,COALESCE(CompanyKey,-1) AS CompanyKey
				,COALESCE(CostCenterTypeKey,-1) AS CostCenterTypeKey
				,COALESCE(CostCenterKey,-1) AS CostCenterKey
				,COALESCE(PositionKey,-1) AS PositionKey
				,COALESCE(PayGradeKey,-1) AS PayGradeKey
				,COALESCE(PeopleGroupKey,-1) AS PeopleGroupKey
				,COALESCE(dm_Assign.AssignmentTypeKey,-1) AS EmployeeAssignmentTypeKey
				,COALESCE(AssignmentID,-1) AS AssignmentID
				,EmploymentID
				,AssignmentNumber
				,COALESCE(AssignmentSequence,-1) AS AssignmentSequence
                    ,AssignmentPositionSequence
                    ,EmploymentTerminationComments
                    ,COALESCE(EmploymentCreatedDate,'1/1/1900') AS EmploymentCreatedDate
                    ,COALESCE(EmploymentCreatedUser,-1) AS EmploymentCreatedUser
                    ,COALESCE(EmploymentModifiedDate,'1/1/1900') As EmploymentModifiedDate
                    ,COALESCE(EmploymentModifiedUser,-1) As EmploymentModifiedUser
                    ,COALESCE(AssignmentCreatedDate,'1/1/1900') AS AssignmentCreatedDate
                    ,COALESCE(AssignmentCreatedUser,-1) AS AssignmentCreatedUser
                    ,COALESCE(AssignmentModifiedDate,'1/1/1900') AS AssignmentModifiedDate
                    ,COALESCE(AssignmentCreatedUser,-1) AS AssignmentModifiedUser
                    ,@EDWRunDateTime AS EDWCreatedDate
				,@EDWRunDateTime AS EDWModifiedDate
			FROM vAssignments fct_Assignments
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt on dm_dt.FullDate = fct_Assignments.AssignmentStartDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt1 on dm_dt1.FullDate = fct_Assignments.AssignmentEndDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt2 on dm_dt2.FullDate = fct_Assignments.AssignmentPositionStartDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt3 on dm_dt3.FullDate = fct_Assignments.AssignmentProjectedEndDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt4 on dm_dt4.FullDate = fct_Assignments.EmploymentStartDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt5 on dm_dt5.FullDate = fct_Assignments.EmploymentEndDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt6 on dm_dt6.FullDate = fct_Assignments.EmploymentAdjustedServiceDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt7 on dm_dt7.FullDate = fct_Assignments.EmploymentLastWorkedDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt8 on dm_dt8.FullDate = fct_Assignments.EmploymentTerminationNotifiedDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt9 on dm_dt9.FullDate = fct_Assignments.EmploymentTerminationAcceptedDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt10 on dm_dt10.FullDate = fct_Assignments.EmploymentTerminationProjectedDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt11 on dm_dt11.FullDate = fct_Assignments.EmploymentTerminationActualDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt12 on dm_dt12.FullDate = fct_Assignments.EmploymentLastPayrollProcessDate
			LEFT JOIN BING_EDW.dbo.DimPerson dm_per on dm_per.PersonID = fct_Assignments.PersonID AND dm_per.PersonCurrentRecordFlag='Y' 
			LEFT JOIN BING_EDW.dbo.DimPerson dm_per1 on dm_per1.PersonID = fct_Assignments.ExecutiveAssistantPersonID AND dm_per.PersonCurrentRecordFlag='Y'
			LEFT JOIN BING_EDW.dbo.DimPerson dm_per2 on dm_per2.PersonID = fct_Assignments.SupervisorPersonID AND dm_per.PersonCurrentRecordFlag='Y'
			LEFT JOIN BING_EDW.dbo.DimLocation dm_loc on dm_loc.LocationID = fct_Assignments.LocationID AND dm_loc.EDWEndDate is NULL
			LEFT JOIN BING_EDW.dbo.DimOrganization dm_org on dm_org.OrgID = fct_Assignments.OrgID AND dm_org.EDWEndDate is NULL
			LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cc on dm_org.CostCenterNumber = dm_cc.CostCenterNumber AND dm_cc.EDWEndDate is NULL
			LEFT JOIN BING_EDW.dbo.DimCostCenterType dimcct on dimcct.CostCenterTypeID = dm_cc.CostCenterTypeID AND dimcct.Deleted is null
			LEFT JOIN BING_EDW.dbo.DimCompany dm_com on dm_com.CompanyID = dm_cc.CompanyID AND dm_com.Deleted is null
			LEFT JOIN BING_EDW.dbo.DimPosition dm_pos on dm_pos.PositionID = fct_Assignments.PositionID AND dm_pos.PositionCurrentRecordFlag='Y'
			LEFT JOIN BING_EDW.dbo.DimPayGrade dm_pg on dm_pg.PayGradeID = fct_Assignments.PayGradeID  
			LEFT JOIN BING_EDW.dbo.DimPeopleGroup dm_pplgrp on dm_pplgrp.PeopleGroupID = fct_Assignments.PeopleGroupID 
			LEFT JOIN BING_EDW.dbo.DimAssignmentType dm_Assign on  dm_Assign.AssignmentStatusTypeID = fct_Assignments.AssignmentStatusTypeID 
			AND dm_Assign.AssignmentStatusTypeName = fct_Assignments.AssignmentStatusTypeName 
			AND dm_Assign.AssignmentBusinessTitleName = fct_Assignments.AssignmentBusinessTitleName 
			AND dm_Assign.AssignmentWorkAtHomeFlag = fct_Assignments.AssignmentWorkAtHomeFlag
		     AND dm_Assign.AssignmentIVRCode = fct_Assignments.AssignmentIVRCode
		     AND dm_Assign.AssignmentESMStatusChangeReasonName = fct_Assignments.AssignmentESMStatusChangeReasonName 
			AND dm_Assign.AssignmentBonusPercent = fct_Assignments.AssignmentBonusPercent
		     AND dm_Assign.AssignmentTypeCode = fct_Assignments.AssignmentTypeCode 
			AND dm_Assign.AssignmentTypeName = fct_Assignments.AssignmentTypeName 
			AND dm_Assign.EmploymentCategoryCode = fct_Assignments.EmploymentCategoryCode 
			AND dm_Assign.EmploymentCategoryName = fct_Assignments.EmploymentCategoryName
		     AND dm_Assign.EmploymentEligibleRehireFlag = fct_Assignments.EmploymentEligibleRehireFlag 
			AND dm_Assign.EmploymentTwoWeeksNoticeFlag = fct_Assignments.EmploymentTwoWeeksNoticeFlag 
			AND dm_Assign.EmploymentTerminationRegrettableFlag = fct_Assignments.EmploymentTerminationRegrettableFlag 
			AND dm_Assign.EmploymentLeavingReasonCode = fct_Assignments.EmploymentLeavingReasonCode 
			AND dm_Assign.EmploymentLeavingReasonName = fct_Assignments.EmploymentLeavingReasonName
		     AND dm_Assign.EmploymentLeavingReasonDescription = fct_Assignments.EmploymentLeavingReasonDescription
		     AND dm_Assign.EmploymentLeavingReasonTypeName = fct_Assignments.EmploymentLeavingReasonTypeName

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