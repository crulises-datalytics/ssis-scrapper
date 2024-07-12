/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingTransform_DimOrganization'
)
    DROP PROCEDURE dbo.spHR_StagingTransform_DimOrganization;
GO
--*/

CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimOrganization] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimOrganization
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime - Current ETL Run Date
    --
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #DimOrganizationUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_DimOrganization @EDWRunDateTime = '20171017'
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By         Comments
    -- ----         -----------         --------
    --
    -- 10/24/17     sburke              BNG-563 - Initial version of proc
    -- 11/29/17     sburke              BNG-835 - Remove call to spHR_StagingGenerate_OrgLeaderAccess_DimOraganization,
    --                                      which has been moved into its own standalone ETL job and renamed to 
    --                                      the more generic spHR_StagingGenerate_OrgLeaderAccess.				 
    --  9/13/18     sburke              Removed the code that shifts hierarchy levels up where there are NULLs, as
    --                                      that will be done in the EDW model view layer.  By default, we populate 
    --                                      the data in DimOrganization as a fixed hierarchy (Centers will always be
    --                                      Level 11), and then have two views on the data in the model view layer, 
    --                                      which can be used to build both a Fixed and Flex (i.e. NULL levels of the
    --                                      hierarchy are filled by moving lower levels up) view of the hierarchy in AS.
    -- 03/17/2020 Adevabhakthuni      BI-3498 Added Row_number to leader CTE's in order to avoid duplicates
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
         BEGIN TRY
		     -- ================================================================================
		     -- Build Organization Leader Access Datasets 
             -- -----------------------------------------		   
		     -- Create a number of CTEs that show different slices of the data in 
		     --     HR_Staging.dbo.OrgLeaderAccess
		     -- ================================================================================
             --			 
		     -- CTE for all Orgs
             WITH Orgs
                  AS (
                  SELECT *
                  FROM dbo.vOrgs),
                  --				  
		          -- CTE for General OrgLeaderAccess (numerous other CTEs below spinning off this)
                  OrgLeaderAccess
                  AS (
                  SELECT *
                  FROM dbo.OrgLeaderAccess
                  WHERE Deleted IS NULL -- Get the 'current' records
                        AND OrgSelfDescendantsFlag = 'Y'
                        AND JobPrimaryFlag = 'Y'),
		          --				  
		          -- CTE for CenterLeader info from OrgLeaderAccess
                  CenterLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgCenterLeaderName = EmployeeFullName, ROW_NUMBER() over (Partition by ORgid order by orgid ) AS RN --added row_number to avoid duplicates 
                  FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'CENTER'
                        AND JobActingFlag = 'N'),
						
		          --				  
		          -- CTE for ActingCenterLeaders info from OrgLeaderAccess
                  ActingCenterLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgActingCenterLeaderName = EmployeeFullName,ROW_NUMBER() over (Partition by ORgid order by orgid ) AS RN --added row_number to avoid duplicates
                  FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'CENTER'
                        AND JobActingFlag = 'Y'),
		          --				  
		          -- CTE for SubGroupLeaders info from OrgLeaderAccess
                  SubGroupLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgSubGroupLeaderName = EmployeeFullName,ROW_NUMBER() over (Partition by ORgid order by orgid ) AS RN --added row_number to avoid duplicates
                  FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'SUBGROUP'
                        AND JobActingFlag = 'N'),
		          --				  
		          -- CTE for GroupLeaders info from OrgLeaderAccess
                  GroupLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgGroupLeaderName = EmployeeFullName,ROW_NUMBER() over (Partition by ORgid order by orgid ) AS RN --added row_number to avoid duplicates
                  FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'GROUP'
                        AND JobActingFlag = 'N'),
		          --				  
		          -- CTE for CampusLeaders info from OrgLeaderAccess
                  CampusLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgCampusLeaderName = EmployeeFullName,ROW_NUMBER() over (Partition by ORgid order by orgid ) AS RN --added row_number to avoid duplicates
                  FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'CAMPUS'
                        AND JobActingFlag = 'N'),
		          --				  
		          -- CTE for DistrictLeaders info from OrgLeaderAccess
                  DistrictLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgDistrictLeaderName = EmployeeFullName,ROW_NUMBER() over (Partition by ORgid order by orgid ) AS RN --added row_number to avoid duplicates
                  FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'DISTRICT'
                        AND JobActingFlag = 'N'),
		          --				  
		          -- CTE for ActingDistrictLeaders info from OrgLeaderAccess
                  ActingDistrictLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgActingDistrictLeaderName = EmployeeFullName,ROW_NUMBER() over (Partition by ORgid order by orgid ) AS RN --added row_number to avoid duplicates
                  FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'DISTRICT'
                        AND JobActingFlag = 'Y'),
		          --				  
		          -- CTE for SubmarketLeaders info from OrgLeaderAccess
                  SubmarketLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgSubMarketLeaderName = EmployeeFullName,ROW_NUMBER() over (Partition by ORgid order by orgid ) AS RN --added row_number to avoid duplicates
                  FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'SUBMARKET'
                        AND JobActingFlag = 'N'),
		          --				  
		          -- CTE for MarketLeaders info from OrgLeaderAccess
                  MarketLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgMarketLeaderName = EmployeeFullName,ROW_NUMBER() over (Partition by ORgid order by orgid ) AS RN --added row_number to avoid duplicates
                  FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'MARKET'
                        AND JobActingFlag = 'N'),
		          --				  
		          -- CTE for RegionLeaders info from OrgLeaderAccess
                  RegionLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgRegionLeaderName = EmployeeFullName,ROW_NUMBER() over (Partition by ORgid order by orgid ) AS RN --added row_number to avoid duplicates
                  FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'REGION'
                        AND JobActingFlag = 'N'),
		          --				  
		          -- CTE for DivisionLeaders info from OrgLeaderAccess
                  DivisionLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgDivisionLeaderName = EmployeeFullName,ROW_NUMBER() over (Partition by ORgid order by orgid ) AS RN --added row_number to avoid duplicates
                  FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'DIVISION'
                        AND JobActingFlag = 'N'),
                  -- --------------------------------------------------------------------------------
                  -- CTE for OrgsToDistrictLeaders info from CTEs Orgs and DistrictLeaders
                  --
                  -- This CTE gets a list of District Leaders associated with District Names, in
                  --     order to determine the Interim District Leader
                  -- --------------------------------------------------------------------------------
                  OrgsToDistrictLeaders
                  AS (
                  SELECT OrgDistrictName,
                         OrgInterimDistrictLeaderName = MIN(OrgDistrictLeaderName)
                  FROM Orgs cte_orgs
                       LEFT JOIN DistrictLeaders cte_dist_ldr ON cte_orgs.OrgID = cte_dist_ldr.OrgID
                  WHERE cte_orgs.OrgDistrictName IS NOT NULL
                  GROUP BY cte_orgs.OrgDistrictName),
                  -- --------------------------------------------------------------------------------
                  -- Pull the data we need from the different OrgLeaderAccess CTEs together  
                  -- --------------------------------------------------------------------------------
                  OrgsWithLeaders
                  AS (
                  SELECT cte_orgs.OrgID,
                         cte_orgs.OrgEffectiveDate,
                         cte_orgs.OrgEndDate,
                         cte_orgs.ParentOrgID,
                         cte_orgs.DefaultLocationID,
                         cte_orgs.CostCenterNumber,
                         OrgCenterLeaderName = COALESCE(cte_ctr_ldr.OrgCenterLeaderName, cte_act_ctr_ldr.OrgActingCenterLeaderName+' (Acting)'),
                         cte_act_ctr_ldr.OrgActingCenterLeaderName,
                         cte_orgs.OrgNumber,
                         cte_orgs.OrgName,
                         cte_orgs.OrgExecutiveFunctionName,
                         cte_orgs.OrgExecutiveFunctionLeaderName,
                         cte_orgs.OrgExecutiveSubFunctionName,
                         cte_orgs.OrgExecutiveSubFunctionLeaderName,
                         cte_orgs.OrgCorporateFunctionName,
                         cte_orgs.OrgCorporateSubFunctionName,
                         cte_orgs.OrgAllName,
                         cte_orgs.OrgDivisionName,
                         cte_orgs.OrgDivisionLeaderName,
                         cte_orgs.OrgRegionNumber,
                         cte_orgs.OrgRegionName,
                         cte_reg_ldr.OrgRegionLeaderName,
                         cte_orgs.OrgMarketNumber,
                         cte_orgs.OrgMarketName,
                         cte_mkt_ldr.OrgMarketLeaderName,
                         cte_orgs.OrgSubMarketNumber,
                         cte_orgs.OrgSubMarketName,
                         cte_submkt_ldr.OrgSubMarketLeaderName,
                         cte_orgs.OrgDistrictNumber,
                         cte_orgs.OrgDistrictName,
                         OrgDistrictLeaderName = COALESCE(cte_dst_ldr.OrgDistrictLeaderName, cte_act_dst_ldr.OrgActingDistrictLeaderName+' (Acting)', cte_org2dst_ldr.OrgInterimDistrictLeaderName+' (Interim)'),
                         cte_act_dst_ldr.OrgActingDistrictLeaderName,
                         cte_orgs.OrgInterimDistrictNumber,
                         cte_orgs.OrgInterimDistrictName,
                         cte_org2dst_ldr.OrgInterimDistrictLeaderName,
                         cte_orgs.OrgGroupNumber,
                         cte_orgs.OrgGroupName,
                         cte_grp_ldr.OrgGroupLeaderName,
                         cte_orgs.OrgSubgroupNumber,
                         cte_orgs.OrgSubGroupName,
                         cte_subgrp_ldr.OrgSubGroupLeaderName,
                         cte_orgs.OrgCampusNumber,
                         cte_orgs.OrgCampusName,
                         cte_cmp_ldr.OrgCampusLeaderName,
                         cte_orgs.OrgCategoryName,
                         cte_orgs.OrgTypeCode,
                         cte_orgs.OrgTypeName,
                         cte_orgs.OrgPartnerGroupCode,
                         cte_orgs.OrgPartnerGroupName,
                         cte_orgs.OrgCenterGroupCode,
                         cte_orgs.OrgCenterGroupName,
                         cte_orgs.OrgDivisionLegacyName,
                         cte_orgs.OrgLineOfBusinessCode,
                         cte_orgs.OrgBrandCode,
                         cte_orgs.OrgBrandName,
                         cte_orgs.OrgFlexAttribute1,
                         cte_orgs.OrgFlexAttribute2,
                         cte_orgs.OrgFlexAttribute3,
                         cte_orgs.OrgFlexAttribute4,
                         cte_orgs.OrgFlexAttribute5,
                         cte_orgs.OrgCreatedUser,
                         cte_orgs.OrgCreatedDate,
                         cte_orgs.OrgModifiedUser,
                         cte_orgs.OrgModifiedDate
                  FROM Orgs cte_orgs
                       LEFT JOIN CenterLeaders cte_ctr_ldr ON cte_orgs.OrgID = cte_ctr_ldr.OrgID and cte_ctr_ldr.RN=1					   -- Added Row_number to avoid duplicate leader name 			
                       LEFT JOIN ActingCenterLeaders cte_act_ctr_ldr ON cte_orgs.OrgID = cte_act_ctr_ldr.OrgID and cte_act_ctr_ldr.RN=1	   -- Added Row_number to avoid duplicate leader name 
                       LEFT JOIN SubGroupLeaders cte_subgrp_ldr ON cte_orgs.OrgID = cte_subgrp_ldr.OrgID and cte_subgrp_ldr.RN=1		   -- Added Row_number to avoid duplicate leader name 
                       LEFT JOIN GroupLeaders cte_grp_ldr ON cte_orgs.OrgID = cte_grp_ldr.OrgID and cte_grp_ldr.RN=1					   -- Added Row_number to avoid duplicate leader name 
                       LEFT JOIN CampusLeaders cte_cmp_ldr ON cte_orgs.OrgID = cte_cmp_ldr.OrgID and cte_cmp_ldr.RN=1					   -- Added Row_number to avoid duplicate leader name 
                       LEFT JOIN DistrictLeaders cte_dst_ldr ON cte_orgs.OrgID = cte_dst_ldr.OrgID and cte_dst_ldr.RN=1					   -- Added Row_number to avoid duplicate leader name 
                       LEFT JOIN ActingDistrictLeaders cte_act_dst_ldr ON cte_orgs.OrgID = cte_act_dst_ldr.OrgID and cte_act_dst_ldr.RN=1  -- Added Row_number to avoid duplicate leader name 
                       LEFT JOIN SubMarketLeaders cte_submkt_ldr ON cte_orgs.OrgID = cte_submkt_ldr.OrgID and cte_submkt_ldr.RN=1		   -- Added Row_number to avoid duplicate leader name 
                       LEFT JOIN MarketLeaders cte_mkt_ldr ON cte_orgs.OrgID = cte_mkt_ldr.OrgID and cte_mkt_ldr.RN=1					   -- Added Row_number to avoid duplicate leader name 
                       LEFT JOIN RegionLeaders cte_reg_ldr ON cte_orgs.OrgID = cte_reg_ldr.OrgID and cte_reg_ldr.RN=1					   -- Added Row_number to avoid duplicate leader name 
                       LEFT JOIN DivisionLeaders cte_div_ldr ON cte_orgs.OrgID = cte_div_ldr.OrgID and cte_div_ldr.RN=1					   -- Added Row_number to avoid duplicate leader name 
                       LEFT JOIN OrgsToDistrictLeaders cte_org2dst_ldr ON cte_orgs.OrgInterimDistrictName = cte_org2dst_ldr.OrgDistrictName)

		   
                  -- ================================================================================
                  -- Build the final #DimOrganization temp table, that will hold the dataset returned 
                  --     to the calling proc.
                  -- ================================================================================

                  SELECT *,
                         OrgHierarchyLevel1Name = OrgAllName,
                         OrgHierarchyLevel2Name = OrgExecutiveFunctionName+COALESCE(' - '+OrgExecutiveFunctionLeaderName, ''),
                         OrgHierarchyLevel3Name = CASE
                                                      WHEN OrgCategoryName = 'Corporate'
                                                      THEN OrgCorporateFunctionName
                                                      ELSE OrgDivisionName+COALESCE(' - '+OrgDivisionLeaderName, '')
                                                  END,
                         OrgHierarchyLevel4Name = CASE
                                                      WHEN OrgCategoryName = 'Corporate'
                                                      THEN OrgCorporateSubFunctionName
                                                      ELSE OrgRegionName+COALESCE(' - '+OrgRegionLeaderName, '')
                                                  END,
                         OrgHierarchyLevel5Name = OrgMarketName+COALESCE(' - '+OrgMarketLeaderName, ''),
                         OrgHierarchyLevel6Name = OrgSubMarketName+COALESCE(' - '+OrgSubMarketLeaderName, ''),
                         OrgHierarchyLevel7Name = OrgDistrictName+COALESCE(' - '+OrgDistrictLeaderName, ''),
                         OrgHierarchyLevel8Name = OrgGroupName+COALESCE(' - '+OrgGroupLeaderName, ''),
                         OrgHierarchyLevel9Name = OrgSubGroupName+COALESCE(' - '+OrgSubGroupLeaderName, ''),
                         OrgHierarchyLevel10Name = OrgCampusName+COALESCE(' - '+OrgCampusLeaderName, ''),
                         OrgHierarchyLevel11Name = OrgName+COALESCE(' - '+OrgCenterLeaderName, '')
                  INTO #DimOrganization
                  FROM OrgsWithLeaders;

             -- ================================================================================
             -- Finally, we can return the dataset back to the calling proc.
             -- ================================================================================

             SELECT COALESCE(OrgID, -1) AS OrgID,
                    COALESCE(NULLIF(OrgEffectiveDate, ''), '19000101') AS OrgEffectiveDate,
                    COALESCE(NULLIF(OrgEndDate, ''), '99991231') AS OrgEndDate,
                    COALESCE(NULLIF(ParentOrgID, ''), NULL) AS ParentOrgID,
                    COALESCE(NULLIF(DefaultLocationID, ''), '-1') AS DefaultLocationID,
                    COALESCE(NULLIF(CostCenterNumber, ''), '-1') AS CostCenterNumber,
                    COALESCE(NULLIF(OrgNumber, ''), '-1') AS OrgNumber,
                    COALESCE(NULLIF(OrgName, ''), 'Unknown Org') AS OrgName,
                    COALESCE(NULLIF(OrgHierarchyLevel1Name, ''), NULL) AS OrgHierarchyLevel1Name,
                    COALESCE(NULLIF(OrgHierarchyLevel2Name, ''), NULL) AS OrgHierarchyLevel2Name,
                    COALESCE(NULLIF(OrgHierarchyLevel3Name, ''), NULL) AS OrgHierarchyLevel3Name,
                    COALESCE(NULLIF(OrgHierarchyLevel4Name, ''), NULL) AS OrgHierarchyLevel4Name,
                    COALESCE(NULLIF(OrgHierarchyLevel5Name, ''), NULL) AS OrgHierarchyLevel5Name,
                    COALESCE(NULLIF(OrgHierarchyLevel6Name, ''), NULL) AS OrgHierarchyLevel6Name,
                    COALESCE(NULLIF(OrgHierarchyLevel7Name, ''), NULL) AS OrgHierarchyLevel7Name,
                    COALESCE(NULLIF(OrgHierarchyLevel8Name, ''), NULL) AS OrgHierarchyLevel8Name,
                    COALESCE(NULLIF(OrgHierarchyLevel9Name, ''), NULL) AS OrgHierarchyLevel9Name,
                    COALESCE(NULLIF(OrgHierarchyLevel10Name, ''), NULL) AS OrgHierarchyLevel10Name,
                    COALESCE(NULLIF(OrgHierarchyLevel11Name, ''), NULL) AS OrgHierarchyLevel11Name,
                    COALESCE(NULLIF(OrgAllName, ''), 'Unknown All') AS OrgAllName,
                    COALESCE(NULLIF(OrgExecutiveFunctionName, ''), 'Unknown Executive Function') AS OrgExecutiveFunctionName,
                    COALESCE(NULLIF(OrgExecutiveFunctionLeaderName, ''), 'Unknown Executive Function Leader') AS OrgExecutiveFunctionLeaderName,
                    COALESCE(NULLIF(OrgExecutiveSubFunctionName, ''), 'Unknown Executive Subfunction') AS OrgExecutiveSubFunctionName,
                    COALESCE(NULLIF(OrgExecutiveSubFunctionLeaderName, ''), 'Unknown Executive Subfunction Leader') AS OrgExecutiveSubFunctionLeaderName,
                    COALESCE(NULLIF(OrgCorporateFunctionName, ''), 'Unknown Corporate Function') AS OrgCorporateFunctionName,
                    COALESCE(NULLIF(OrgCorporateSubFunctionName, ''), 'Unknown Corporate Subfunction') AS OrgCorporateSubFunctionName,
                    COALESCE(NULLIF(OrgDivisionName, ''), 'No Division') AS OrgDivisionName,
                    COALESCE(NULLIF(OrgDivisionLeaderName, ''), 'Unknown Division Leader') AS OrgDivisionLeaderName,
                    COALESCE(NULLIF(OrgRegionNumber, ''), '-1') AS OrgRegionNumber,
                    COALESCE(NULLIF(OrgRegionName, ''), 'No Region') AS OrgRegionName,
                    COALESCE(NULLIF(OrgRegionLeaderName, ''), 'No Region Leader') AS OrgRegionLeaderName,
                    COALESCE(NULLIF(OrgMarketNumber, ''), '-1') AS OrgMarketNumber,
                    COALESCE(NULLIF(OrgMarketName, ''), 'No Market') AS OrgMarketName,
                    COALESCE(NULLIF(OrgMarketLeaderName, ''), 'No Market Leader') AS OrgMarketLeaderName,
                    COALESCE(NULLIF(OrgSubMarketNumber, ''), '-1') AS OrgSubMarketNumber,
                    COALESCE(NULLIF(OrgSubMarketName, ''), 'No Submarket') AS OrgSubMarketName,
                    COALESCE(NULLIF(OrgSubMarketLeaderName, ''), 'No Submarket Leader') AS OrgSubMarketLeaderName,
                    COALESCE(NULLIF(OrgDistrictNumber, ''), '-1') AS OrgDistrictNumber,
                    COALESCE(NULLIF(OrgDistrictName, ''), 'No District') AS OrgDistrictName,
                    COALESCE(NULLIF(OrgInterimDistrictNumber, ''), '-1') AS OrgInterimDistrictNumber,
                    COALESCE(NULLIF(OrgInterimDistrictName, ''), 'No Interim District') AS OrgInterimDistrictName,
                    COALESCE(NULLIF(OrgDistrictLeaderName, 'No District Leader'), '') AS OrgDistrictLeaderName,
                    COALESCE(NULLIF(OrgActingDistrictLeaderName, ''), 'No Acting District Leader') AS OrgActingDistrictLeaderName,
                    COALESCE(NULLIF(OrgInterimDistrictLeaderName, ''), 'No Interim District Leader') AS OrgInterimDistrictLeaderName,
                    COALESCE(NULLIF(OrgGroupNumber, ''), '-1') AS OrgGroupNumber,
                    COALESCE(NULLIF(OrgGroupName, ''), 'No Group') AS OrgGroupName,
                    COALESCE(NULLIF(OrgGroupLeaderName, ''), 'No Group Leader') AS OrgGroupLeaderName,
                    COALESCE(NULLIF(OrgSubgroupNumber, ''), '-1') AS OrgSubgroupNumber,
                    COALESCE(NULLIF(OrgSubGroupName, ''), 'No Subgroup') AS OrgSubGroupName,
                    COALESCE(NULLIF(OrgSubGroupLeaderName, ''), 'No Subgroup Leader') AS OrgSubGroupLeaderName,
                    COALESCE(NULLIF(OrgCampusNumber, ''), '-1') AS OrgCampusNumber,
                    COALESCE(NULLIF(OrgCampusName, ''), 'No Campus') AS OrgCampusName,
                    COALESCE(NULLIF(OrgCampusLeaderName, ''), 'No Campus Leader') AS OrgCampusLeaderName,
                    COALESCE(NULLIF(OrgCenterLeaderName, ''), 'No Center Leader') AS OrgCenterLeaderName,
                    COALESCE(NULLIF(OrgActingCenterLeaderName, ''), 'No Center Leader') AS OrgActingCenterLeaderName,
                    COALESCE(NULLIF(OrgCategoryName, ''), 'Unknown Category') AS OrgCategoryName,
                    COALESCE(NULLIF(OrgTypeCode, ''), 'Unknown Type Code') AS OrgTypeCode,
                    COALESCE(NULLIF(OrgTypeName, ''), 'Unknown Type') AS OrgTypeName,
                    COALESCE(NULLIF(OrgPartnerGroupCode, ''), 'Unknown Partner Group Code') AS OrgPartnerGroupCode,
                    COALESCE(NULLIF(OrgPartnerGroupName, ''), 'Unknown Partner Group') AS OrgPartnerGroupName,
                    COALESCE(NULLIF(OrgCenterGroupCode, ''), 'Unknown Center Group Code') AS OrgCenterGroupCode,
                    COALESCE(NULLIF(OrgCenterGroupName, ''), 'Unknown Center Group') AS OrgCenterGroupName,
                    COALESCE(NULLIF(OrgDivisionLegacyName, ''), 'Unknown Legacy Division') AS OrgDivisionLegacyName,
                    COALESCE(NULLIF(OrgLineOfBusinessCode, ''), 'Unknown Org Line of Business') AS OrgLineOfBusinessCode,
                    COALESCE(NULLIF(OrgBrandCode, ''), 'Unknown Brand Code') AS OrgBrandCode,
                    COALESCE(NULLIF(OrgBrandName, ''), 'Unknown Brand') AS OrgBrandName,
                    COALESCE(NULLIF(OrgFlexAttribute1, ''), NULL) AS OrgFlexAttribute1,
                    COALESCE(NULLIF(OrgFlexAttribute2, ''), NULL) AS OrgFlexAttribute2,
                    COALESCE(NULLIF(OrgFlexAttribute3, ''), NULL) AS OrgFlexAttribute3,
                    COALESCE(NULLIF(OrgFlexAttribute4, ''), NULL) AS OrgFlexAttribute4,
                    COALESCE(NULLIF(OrgFlexAttribute5, ''), NULL) AS OrgFlexAttribute5,
                    COALESCE(NULLIF(OrgCreatedUser, ''), '-1') AS OrgCreatedUser,
                    COALESCE(NULLIF(OrgCreatedDate, ''), '19000101') AS OrgCreatedDate,
                    COALESCE(NULLIF(OrgModifiedUser, ''), '-1') AS OrgModifiedUser,
                    COALESCE(NULLIF(OrgModifiedDate, ''), '19000101') AS OrgModifiedDate,
                    @EDWRunDateTime AS EDWEffectiveDate,
                    NULL AS EDWEndDate,
                    @EDWRunDateTime AS EDWCreatedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                    NULL AS Deleted
             FROM #DimOrganization
             ORDER BY 1;
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


