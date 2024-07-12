
CREATE PROCEDURE [dbo].[spHR_StagingTransform_FactEmployeePayRate]
(@EDWRunDateTime     DATETIME2 = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_FactEmployeePayRate
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
    -- Usage:              INSERT #FactEmployeePayRateUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_FactEmployeePayRate @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By        Comments
    -- ----         -----------        --------
    --
    -- 3/2/18        valimineti        BNG-274 - Intital version of the proc
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
				 COALESCE(dm_dt.DateKey,-1) AS EmployeePayRateEffectiveDateKey
				,COALESCE(dm_dt1.DateKey,-1) as EmployeePayRateEndDateKey
				,COALESCE(EmployeePayRateCurrentRecordFlag,'X') AS EmployeePayRateCurrentRecordFlag
				,COALESCE(PersonKey,-1) AS PersonKey
				,COALESCE(LocationKey,-1) AS LocationKey
				,COALESCE(OrgKey,-1) AS OrgKey
				,COALESCE(CompanyKey,-1) AS CompanyKey
				,COALESCE(CostCenterTypeKey,-1) AS CostCenterTypeKey
				,COALESCE(CostCenterKey,-1) AS CostCenterKey
				,COALESCE(PositionKey,-1) AS PositionKey
				,COALESCE(PayGradeKey,-1) AS PayGradeKey
				,COALESCE(PeopleGroupKey,-1) AS PeopleGroupKey
				,COALESCE(PayBasisKey,-1) AS PayBasisKey
				,COALESCE(PayRateChangeReasonKey,-1) AS PayRateChangeReasonKey
				,COALESCE(EmployeePayRateID,-1) AS EmployeePayRateID
				,COALESCE(AssignmentID,-1) AS AssignmentID 
				,COALESCE(EmployeePayRateApprovedFlag,'X') AS PayRateApprovedFlag
				,COALESCE(EmployeeAnnualizedPayRate,0) AS EmployeeAnnualizedPayRate
				,COALESCE(EmployeeHourlyPayRate,NULL) AS EmployeeHourlyPayRate
				,COALESCE(EmployeeAnnualPayRate,NULL) AS EmployeeSalaryPayRate
				,COALESCE(EmployeePayRateCreatedDate,'1900-01-01') AS EmployeePayRateCreatedDate
				,COALESCE(EmployeePayRateCreatedUser,-1) AS EmployeePayRateCreatedUser
				,COALESCE(EmployeePayRateModifiedDate,'1900-01-01') AS EmployeePayRateModifiedDate
				,COALESCE(EmployeePayRateModifiedUser,-1) AS EmployeePayRateModifiedUser
				,@EDWRunDateTime AS EDWCreatedDate
				,@EDWRunDateTime AS EDWModifiedDate
			FROM vPayRates fct_payrate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt on dm_dt.FullDate = fct_payrate.EmployeePayRateEffectiveDate
			LEFT JOIN BING_EDW.dbo.DimDate dm_dt1 on dm_dt1.FullDate = fct_payrate.EmployeePayRateEndDate
			LEFT JOIN BING_EDW.dbo.DimPerson dm_per on dm_per.PersonID = fct_payrate.PersonID AND dm_per.PersonCurrentRecordFlag='Y'
			LEFT JOIN BING_EDW.dbo.DimLocation dm_loc on dm_loc.LocationID = fct_payrate.LocationID AND dm_loc.EDWEndDate is NULL
			LEFT JOIN BING_EDW.dbo.DimOrganization dm_org on dm_org.OrgID = fct_payrate.OrgID AND dm_org.EDWEndDate is NULL
			LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cc on dm_org.CostCenterNumber = dm_cc.CostCenterNumber AND dm_cc.EDWEndDate is NULL
			LEFT JOIN BING_EDW.dbo.DimCostCenterType dimcct on dimcct.CostCenterTypeID = dm_cc.CostCenterTypeID AND dimcct.Deleted is null
			LEFT JOIN BING_EDW.dbo.DimCompany dm_com on dm_com.CompanyID = dm_cc.CompanyID AND dm_com.Deleted is null
			LEFT JOIN BING_EDW.dbo.DimPosition dm_pos on dm_pos.PositionID = fct_payrate.PositionID AND dm_pos.PositionCurrentRecordFlag='Y'
			LEFT JOIN BING_EDW.dbo.DimPayGrade dm_pg on dm_pg.PayGradeID = fct_payrate.PayGradeID  
			LEFT JOIN BING_EDW.dbo.DimPeopleGroup dm_pplgrp on dm_pplgrp.PeopleGroupID = fct_payrate.PeopleGroupID 
			LEFT JOIN BING_EDW.dbo.DimPayBasis dm_pb on dm_pb.PayBasisID = fct_payrate.PayBasisID 
			LEFT JOIN BING_EDW.dbo.DimPayRateChangeReason dm_prcg on dm_prcg.PayRateChangeReasonCode = fct_payrate.PayRateChangeReasonCode

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