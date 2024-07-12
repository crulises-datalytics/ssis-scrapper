/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spGL_StagingTransform_DimCostCenter'
)
    DROP PROCEDURE dbo.spGL_StagingTransform_DimCostCenter;
GO
--*/
CREATE PROCEDURE [dbo].[spGL_StagingTransform_DimCostCenter] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingTransform_DimCostCenter
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
    -- Usage:              INSERT #DimCostCenterUpsert -- (Temporary table)
    --                     EXEC dbo.spGL_StagingTransform_DimCostCenter; @EDWRunDateTime
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
    -- 10/11/17    sburke              BNG-256 - Initial version of stored proc, 
    --                                     incorporating changes for Cost Center Master
    -- 11/29/17    sburke              BNG-835 - Changes to CostCenter Hierarchy logic to use
    --                                     similar approach to DimOrganization load
    --                                     (use HR_Staging..vOrgs rather than the defunct
    --                                      vOrgsWithLevels view)
    -- 10/10/18    sburke              BNG-4166 - Additional logic to cover for when Cost Centers
    --                                      are not correctly closed-out in the source (commented-out, see below).	
    -- 10/15/18    sburke              Reference CenterCSSMigrations table for determining
    --                                      CSS -> CMS migration date and status.  
    --                                 Also comment-out BNG-4166 code change, as this is on hold while we 
    --                                      look to Finance to rectify in the source.	
    -- 11/16/18    sburke              BNG-4422 - Remove logic for ascertaiing LicensedCapacity from DimCostCenter, as 
    --                                      this is data now held in the FactCenterStatSnapshot table. 
    -- 12/3/2021  Adevabhakthui       BI-5306 Update the stored proc to avoid Duplicate due to HR data 
	-- 01/04/2024	Suhas			  DFTP-873 Modifications to handle duplicates due to HR Org Changes
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
             -- ================================================================================
             -- Build core Cost Center Datasets
             -- -------------------------------
             -- These CTEs form the center of our final transformation query that builds
             --     the DimCostCenter dataset
             -- ================================================================================
             WITH CostCenterNumbers
                  AS (
                  --				  
                  -- CTE to create list of CostCenter Numbers

                  SELECT DISTINCT
                         CostCenterNumber = Segment3
                  FROM dbo.GLCodeCombinations
                  WHERE Segment3 IS NOT NULL
                        AND Segment3 NOT LIKE '%[A-z]%'),
                  --						
                  -- CTE for all CostCenters
                  CostCenters
                  AS (
                  SELECT *
                  FROM dbo.vCostCenters),
                  -- ================================================================================
                  -- Build core Organization Hierarchy and Org Leader Access Datasets 
                  -- ----------------------------------------------------------------
                  -- Create a number of CTEs that show different slices of the data in 
                  --     HR_Staging.dbo.OrgLeaderAccess.
                  --
                  -- These CTEs are the usage is similar to how the DimOrganization transformation
                  --     works.  For now we keep building of the OrgHierarchy & leadership for
                  --     the two Dimension tables seperate (noting the code duplication) and look at
                  --     possibly having a single process that could be leveraged by both.
                  -- ================================================================================
                  --				  
                  -- CTE for all Orgs
                  Orgs
                  AS (
                  SELECT *
                  FROM HR_Staging.dbo.vOrgs),
                  --
                  -- CTE for General OrgLeaderAccess (numerous other CTEs below spinning off this)
                  OrgLeaderAccess
                  AS (
                  SELECT *
                  FROM HR_Staging.dbo.OrgLeaderAccess
                  WHERE Deleted IS NULL -- Get the 'current' records
                        AND OrgSelfDescendantsFlag = 'Y'
                        AND JobPrimaryFlag = 'Y'),
                  --
                  -- CTE for CenterLeader info from OrgLeaderAccess
                  CenterLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgCenterLeaderName = EmployeeFullName
                  FROM (SELECT CanAccessOrgID, EmployeeFullName, ROW_NUMBER() OVER (PARTITION BY CanAccessOrgID ORDER BY JobOrgLevelSequence, EmployeeNumber) AS [RW] FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'CENTER'
                        AND JobActingFlag = 'N') AS [A] WHERE [RW] = 1),
                  --
                  -- CTE for ActingCenterLeaders info from OrgLeaderAccess
                  ActingCenterLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgActingCenterLeaderName = EmployeeFullName
                  FROM (SELECT CanAccessOrgID, EmployeeFullName, ROW_NUMBER() OVER (PARTITION BY CanAccessOrgID ORDER BY JobOrgLevelSequence, EmployeeNumber) AS [RW] FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'CENTER'
                        AND JobActingFlag = 'Y') AS [A] WHERE [RW] = 1),
                  --
                  -- CTE for SubGroupLeaders info from OrgLeaderAccess
                  SubGroupLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgSubGroupLeaderName = EmployeeFullName
                  FROM (SELECT CanAccessOrgID, EmployeeFullName, ROW_NUMBER() OVER (PARTITION BY CanAccessOrgID ORDER BY JobOrgLevelSequence, EmployeeNumber) AS [RW] FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'SUBGROUP'
                        AND JobActingFlag = 'N') AS [A] WHERE [RW] = 1),
                  -- 
                  -- CTE for GroupLeaders info from OrgLeaderAccess
                  GroupLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgGroupLeaderName = EmployeeFullName
                  FROM (SELECT CanAccessOrgID, EmployeeFullName, ROW_NUMBER() OVER (PARTITION BY CanAccessOrgID ORDER BY JobOrgLevelSequence, EmployeeNumber) AS [RW] FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'GROUP'
                        AND JobActingFlag = 'N') AS [A] WHERE [RW] = 1),
                  --  
                  -- CTE for CampusLeaders info from OrgLeaderAccess
                  CampusLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgCampusLeaderName = EmployeeFullName
                  FROM (SELECT CanAccessOrgID, EmployeeFullName, ROW_NUMBER() OVER (PARTITION BY CanAccessOrgID ORDER BY JobOrgLevelSequence, EmployeeNumber) AS [RW] FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'CAMPUS'
                        AND JobActingFlag = 'N') AS [A] WHERE [RW] = 1),
                  --  
                  -- CTE for DistrictLeaders info from OrgLeaderAccess
                  DistrictLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgDistrictLeaderName = EmployeeFullName
                  FROM (SELECT CanAccessOrgID, EmployeeFullName, ROW_NUMBER() OVER (PARTITION BY CanAccessOrgID ORDER BY JobOrgLevelSequence, EmployeeNumber) AS [RW] FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'DISTRICT'
                        AND JobActingFlag = 'N') AS [A] WHERE [RW] = 1),
                  --
                  -- CTE for ActingDistrictLeaders info from OrgLeaderAccess
                  ActingDistrictLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgActingDistrictLeaderName = EmployeeFullName
                  FROM (SELECT CanAccessOrgID, EmployeeFullName, ROW_NUMBER() OVER (PARTITION BY CanAccessOrgID ORDER BY JobOrgLevelSequence, EmployeeNumber) AS [RW] FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'DISTRICT'
                        AND JobActingFlag = 'Y') AS [A] WHERE [RW] = 1),
                  --
                  -- CTE for SubmarketLeaders info from OrgLeaderAccess
                  SubmarketLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgSubMarketLeaderName = EmployeeFullName
                  FROM (SELECT CanAccessOrgID, EmployeeFullName, ROW_NUMBER() OVER (PARTITION BY CanAccessOrgID ORDER BY JobOrgLevelSequence, EmployeeNumber) AS [RW] FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'SUBMARKET'
                        AND JobActingFlag = 'N') AS [A] WHERE [RW] = 1),
                  --
                  -- CTE for MarketLeaders info from OrgLeaderAccess
                  MarketLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgMarketLeaderName = EmployeeFullName
                  FROM (SELECT CanAccessOrgID, EmployeeFullName, ROW_NUMBER() OVER (PARTITION BY CanAccessOrgID ORDER BY JobOrgLevelSequence, EmployeeNumber) AS [RW] FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'MARKET'
                        AND JobActingFlag = 'N') AS [A] WHERE [RW] = 1),
                  --
                  -- CTE for RegionLeaders info from OrgLeaderAccess
                  RegionLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgRegionLeaderName = EmployeeFullName
                  FROM (SELECT CanAccessOrgID, EmployeeFullName, ROW_NUMBER() OVER (PARTITION BY CanAccessOrgID ORDER BY JobOrgLevelSequence, EmployeeNumber) AS [RW] FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'REGION'
                        AND JobActingFlag = 'N') AS [A] WHERE [RW] = 1),
                  -- 
                  -- CTE for DivisionLeaders info from OrgLeaderAccess
                  DivisionLeaders
                  AS (
                  SELECT OrgID = CanAccessOrgID,
                         OrgDivisionLeaderName = EmployeeFullName
                  FROM (SELECT CanAccessOrgID, EmployeeFullName, ROW_NUMBER() OVER (PARTITION BY CanAccessOrgID ORDER BY JobOrgLevelSequence, EmployeeNumber) AS [RW] FROM OrgLeaderAccess
                  WHERE JobOrgLevelName = 'DIVISION'
                        AND JobActingFlag = 'N') AS [A] WHERE [RW] = 1),
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
                       LEFT JOIN CenterLeaders cte_ctr_ldr ON cte_orgs.OrgID = cte_ctr_ldr.OrgID
                       LEFT JOIN ActingCenterLeaders cte_act_ctr_ldr ON cte_orgs.OrgID = cte_act_ctr_ldr.OrgID
                       LEFT JOIN SubGroupLeaders cte_subgrp_ldr ON cte_orgs.OrgID = cte_subgrp_ldr.OrgID
                       LEFT JOIN GroupLeaders cte_grp_ldr ON cte_orgs.OrgID = cte_grp_ldr.OrgID
                       LEFT JOIN CampusLeaders cte_cmp_ldr ON cte_orgs.OrgID = cte_cmp_ldr.OrgID
                       LEFT JOIN DistrictLeaders cte_dst_ldr ON cte_orgs.OrgID = cte_dst_ldr.OrgID
                       LEFT JOIN ActingDistrictLeaders cte_act_dst_ldr ON cte_orgs.OrgID = cte_act_dst_ldr.OrgID
                       LEFT JOIN SubMarketLeaders cte_submkt_ldr ON cte_orgs.OrgID = cte_submkt_ldr.OrgID
                       LEFT JOIN MarketLeaders cte_mkt_ldr ON cte_orgs.OrgID = cte_mkt_ldr.OrgID
                       LEFT JOIN RegionLeaders cte_reg_ldr ON cte_orgs.OrgID = cte_reg_ldr.OrgID
                       LEFT JOIN DivisionLeaders cte_div_ldr ON cte_orgs.OrgID = cte_div_ldr.OrgID
                       LEFT JOIN OrgsToDistrictLeaders cte_org2dst_ldr ON cte_orgs.OrgInterimDistrictName = cte_org2dst_ldr.OrgDistrictName),
                  -- ================================================================================
                  -- ...end of Organization Hierarchy and Org Leader Access specific CTEs 
                  -- ================================================================================
                  --			   			   				  
                  -- CTE to build dataset of Centers and their Open days
                  CMSOpenDays
                  AS (
                  SELECT CMSID = idSite,
                         CenterOpenSunFlag = pvt.[1],
                         CenterOpenMonFlag = pvt.[2],
                         CenterOpenTueFlag = pvt.[3],
                         CenterOpenWedFlag = pvt.[4],
                         CenterOpenThuFlag = pvt.[5],
                         CenterOpenFriFlag = pvt.[6],
                         CenterOpenSatFlag = pvt.[7]
                  FROM
                  (
                      SELECT idSite,
                             idDayofWeek,
                             IsOpen = 'Y'
                      FROM CMS_Staging.dbo.locSiteScheduleDay
                  ) loc_schdday PIVOT(MIN(IsOpen) FOR idDayOfWeek IN([1],
                                                                     [2],
                                                                     [3],
                                                                     [4],
                                                                     [5],
                                                                     [6],
                                                                     [7])) pvt),
                  --														   
                  -- CTE to build dataset of CostCenters and their CMS-sourced information (Center opening/closing times, Backup Care, etc)
                  CMS
                  AS (
                  SELECT CostCenterNumber = loc_ste.SiteNumber,
                         CMSID = loc_ste.idSite,
                         CenterBackupCareFlag = CASE loc_ste.isBUCCeligible
                                                    WHEN 1
                                                    THEN 'Backup Care'
                                                    WHEN 0
                                                    THEN 'Not Backup Care'
                                                    ELSE 'Unknown Backup Care'
                                                END,
                         CenterChildCareSelectFlag = CASE loc_ste.isCareSelect
                                                         WHEN 1
                                                         THEN 'Care Select'
                                                         WHEN 0
                                                         THEN 'Not Care Select'
                                                         ELSE 'Unknown Care Select'
                                                     END,
                         CenterOperationStartTime = loc_ste.OpStartTime,
                         CenterOperationEndTime = loc_ste.OpEndTime,
                         CenterPublicAllowedFlag = CASE loc_ste.AllowGeneralEnrollment
                                                       WHEN 1
                                                       THEN 'Public Allowed'
                                                       WHEN 0
                                                       THEN 'Public Not Allowed'
                                                       ELSE 'Unknown Public Allowed'
                                                   END,
                         CenterFoodProgramStartDate = CAST(loc_ste.FoodSubsidyStartDate AS DATE),
                         CenterFoodProgramEndDate = CAST(loc_ste.FoodSubsidyEndDate AS DATE),
                         CenterRegistrationType = org_reg.RegistrationType,
                         CenterStudentMinimumAge = org_age_min.AgeSegmentName,
                         CenterStudentMaximumAge = org_age_max.AgeSegmentName
                  FROM CMS_Staging.dbo.LOCSite loc_ste
                       LEFT JOIN CMS_Staging.dbo.orgRegistrationType org_reg ON loc_ste.idRegistrationType = org_reg.idRegistrationType
                       LEFT JOIN CMS_Staging.dbo.orgAgeSegment org_age_min ON loc_ste.idAgeSegmentMinimum = org_age_min.idAgeSegment
                       LEFT JOIN CMS_Staging.dbo.orgAgeSegment org_age_max ON loc_ste.idAgeSegmentMaximum = org_age_max.idAgeSegment),
                  --					   
                  -- CTE to build dataset of CostCenters and their CSS-sourced information (Center opening/closing times, Backup Care, etc)
                  CSS
                  AS (
                  SELECT CostCenterNumber = csx_ctrr.cost_ctr_no,
                         CSSID = csx_ctrr.ctr_no,
                         CenterBackupCareFlag = CASE csx_ctrr.bu_care_flag
                                                    WHEN 'Y'
                                                    THEN 'Backup Care'
                                                    WHEN 'N'
                                                    THEN 'Not Backup Care'
                                                    ELSE 'Unknown Backup Care'
                                                END,
                         CenterOperationStartTime = csx_ctrr.time_open,
                         CenterOperationEndTime = csx_ctrr.time_close,
                         CenterStudentMinimumAge = csx_ctrr.min_age, --need better name
                         CenterStudentMaximumAge = csx_ctrr.max_age, --need better name
                         CenterOpenSunFlag = CASE csx_ctrr.sun_open
                                                 WHEN 'Y'
                                                 THEN 'Open Sunday'
                                                 WHEN 'N'
                                                 THEN 'Closed Sunday'
                                                 ELSE 'Unknown Sunday'
                                             END,
                         CenterOpenMonFlag = CASE csx_ctrr.mon_open
                                                 WHEN 'Y'
                                                 THEN 'Open Monday'
                                                 WHEN 'N'
                                                 THEN 'Closed Monday'
                                                 ELSE 'Unknown Monday'
                                             END,
                         CenterOpenTueFlag = CASE csx_ctrr.tue_open
                                                 WHEN 'Y'
                                                 THEN 'Open Tuesday'
                                                 WHEN 'N'
                                                 THEN 'Closed Tuesday'
                                                 ELSE 'Unknown Tuesday'
                                             END,
                         CenterOpenWedFlag = CASE csx_ctrr.wed_open
                                                 WHEN 'Y'
                                                 THEN 'Open Wednesday'
                                                 WHEN 'N'
                                                 THEN 'Closed Wednesday'
                                                 ELSE 'Unknown Wednesday'
                                             END,
                         CenterOpenThuFlag = CASE csx_ctrr.thu_open
                                                 WHEN 'Y'
                                                 THEN 'Open Thursday'
                                                 WHEN 'N'
                                                 THEN 'Closed Thursday'
                                                 ELSE 'Unknown Thursday'
                                             END,
                         CenterOpenFriFlag = CASE csx_ctrr.fri_open
                                                 WHEN 'Y'
                                                 THEN 'Open Friday'
                                                 WHEN 'N'
                                                 THEN 'Closed Friday'
                                                 ELSE 'Unknown Friday'
                                             END,
                         CenterOpenSatFlag = CASE csx_ctrr.sat_open
                                                 WHEN 'Y'
                                                 THEN 'Open Saturday'
                                                 WHEN 'N'
                                                 THEN 'Closed Saturday'
                                                 ELSE 'Unknown Saturday'
                                             END
                  FROM CSS_Staging.dbo.CSXCTRR csx_ctrr),
                  --				  
                  -- CTE for Horizon CostCenter information
                  Horizon
                  AS (
                  SELECT HorizonID = hor_loc_ste.idSite,
                         CostCenterNumber = hor_loc_ste.SiteNumber,
                         SiteClientName = hor_loc_ste.SiteName,
                         SiteSchoolDistrict = SchoolDistrict
                  FROM Horizon_Staging.dbo.LOCSite hor_loc_ste
                       LEFT JOIN
                  (
                      SELECT idSite,
                             SchoolDistrict
                      FROM Horizon_Staging.dbo.LOCSiteYear loc_yr
                           INNER JOIN Horizon_Staging.dbo.OrgYear org_yr ON loc_yr.idYear = org_yr.idYear
                      WHERE org_yr.StartDate =
                      (
                          SELECT MaxStartDate = MAX(StartDate)
                          FROM Horizon_Staging.dbo.LOCSiteYear loc_yr_max
                               INNER JOIN Horizon_Staging.dbo.OrgYear org_yr_max ON loc_yr_max.idYear = org_yr_max.idYear
                                                                                    AND org_yr_max.StartDate < GETDATE()
                      )
                  ) hor_loc_ste_yr ON hor_loc_ste.idSite = hor_loc_ste_yr.idSite),
                  --				  
                  -- CTE for CSS -> CMS Migration details
                  CSStoCMSDates
                  AS (
                  SELECT CostCenterNumber = ctr2ste.CostCenterNumber,
                         CSSID = ctr2ste.CenterCSSID,
                         CenterCMSMigrationDate = ctr2ste.MigrationDate,
                         CenterCMSMigrationStatus = CASE
                                                        WHEN ctr2ste.MigrationDate IS NULL
                                                             OR ctr2ste.MigrationDate >= GETDATE()
                                                        THEN 'N'
                                                        ELSE 'Y'
                                                    END,
                         CenterEnrollmentSourceSystem = CASE
                                                            WHEN ctr2ste.MigrationDate IS NULL
                                                                 OR ctr2ste.MigrationDate >= GETDATE()
                                                            THEN 'CSS'
                                                            ELSE 'CMS'
                                                        END
                  FROM CSS_Staging..CenterCSSMigrations ctr2ste),
                  --				  
                  -- CTE for Facilities data for each CostCenter
                  FacilitiesContacts
                  AS (
                  SELECT TOP 1 WITH TIES CostCenterNumber = fac_ctr_cnt.Center,
                                         CenterMaintenanceSupervisorName = fac_ctr_cnt.MaintenanceSupervisor,
                                         CenterPreventativeTechnicianName = fac_ctr_cnt.PreventativeTech,
                                         CenterRegionalFacilitiesCoordinatorName = fac_ctr_cnt.RFC,
                                         CenterRegionalFacilitiesManagerName = fac_ctr_cnt.RFM
                  FROM MISC_Staging.dbo.MdmFacilitiesCenterContact fac_ctr_cnt
                  ORDER BY ROW_NUMBER() OVER(PARTITION BY fac_ctr_cnt.Center ORDER BY fac_ctr_cnt.StgCreatedDate)),
                  --				  
                  -- CTE for Lease / Esate data for each CostCenter
                  LeaseAdministration
                  AS (
                  SELECT TOP 1 WITH TIES CostCenterNumber = lse_adm.CenterNumber,
                                         CenterBuildingSquareFootage = lse_adm.BuildingSquareFootage,
                                         CenterLandSquareFootage = lse_adm.LandSquareFootage,
                                         CenterCertificateOfOccupancyIssueDate = lse_adm.CertificateOfOccupancyIssueDate,
                                         CenterCoreBasedStatisticalAreaName = lse_adm.CoreBasedStatisticalArea,
                                         CenterLandlordName = lse_adm.LandlordName,
                                         CenterLeaseControlEndMonthDate = lse_adm.LeaseControlEndMonthDate,
                                         CenterLeaseExpirationDate = lse_adm.LeaseExpirationDate,
                                         CenterLeaseExtensionOptionNoticeDate = lse_adm.LeaseExtensionOptionNoticeDate,
                                         CenterLeaseExtensionOptionsRemainingCount = lse_adm.NumberOfLeaseExtensionOptionsRemaining,
                                         CenterLeaseExtensionOptionRemainingYears = lse_adm.LeaseExtensionOptionYearsRemaining,
                                         CenterLeaseStatus = lse_adm.LeaseStatus,
                                         CenterBuildingBuiltYear = lse_adm.YearBuildingWasBuilt
                  FROM MISC_Staging.dbo.MdmLeaseAdministration lse_adm
                  ORDER BY ROW_NUMBER() OVER(PARTITION BY lse_adm.CenterNumber ORDER BY lse_adm.StgCreatedDate)),
                  --				  
                  -- CTE for geographic (latitude / longitude) data for each CostCenter
                  LatitudeLongitude
                  AS (
                  SELECT TOP 1 WITH TIES CostCenterNumber = lat_lng.Center,
                                         CenterLatitude = lat_lng.SiteLatitude,
                                         CenterLongitude = lat_lng.SiteLongitude
                  FROM MISC_Staging.dbo.MdmLatitudeAndLongitude lat_lng
                  ORDER BY ROW_NUMBER() OVER(PARTITION BY lat_lng.Center ORDER BY lat_lng.StgCreatedDate)),
                  --				  
                  -- CTE for Marketing data for each CostCenter
                  Marketing
                  AS (
                  SELECT TOP 1 WITH TIES CostCenterNumber = mrk.CenterNumber,
                                         mrk.CenterURL,
                                         mrk.CenterMenuURL,
                                         CenterHasBreakfastFlag = CASE mrk.HasBreakfastFlag
                                                                      WHEN 1
                                                                      THEN 'Has Breakfast'
                                                                      WHEN 0
                                                                      THEN 'No Breakfast'
                                                                      ELSE 'Unknown Breakfast'
                                                                  END,
                                         CenterHasMorningSlackFlag = CASE mrk.HasMorningSnackFlag
                                                                         WHEN 1
                                                                         THEN 'Has Morning Snack'
                                                                         WHEN 0
                                                                         THEN 'No Morning Snack'
                                                                         ELSE 'Unknown Morning Snack'
                                                                     END,
                                         CenterHasLunchFlag = CASE mrk.HasLunchFlag
                                                                  WHEN 1
                                                                  THEN 'Has Lunch'
                                                                  WHEN 0
                                                                  THEN 'No Lunch'
                                                                  ELSE 'Unknown Lunch'
                                                              END,
                                         CenterHasAfternoonSnackFlag = CASE mrk.HasAfternoonSnackFlag
                                                                           WHEN 1
                                                                           THEN 'Has Afternoon Snack'
                                                                           WHEN 0
                                                                           THEN 'No Afternoon Snack'
                                                                           ELSE 'Unknown Afternoon Snack'
                                                                       END,
                                         CenterSpeaksASLFlag = CASE mrk.SpeaksAmericanSignLanguageFlag
                                                                   WHEN 1
                                                                   THEN 'Speaks American Sign Language'
                                                                   WHEN 0
                                                                   THEN 'No American Sign Language'
                                                                   ELSE 'Unknown American Sign Language'
                                                               END,
                                         CenterSpeaksArabicFlag = CASE mrk.SpeaksArabicFlag
                                                                      WHEN 1
                                                                      THEN 'Speaks Arabic'
                                                                      WHEN 0
                                                                      THEN 'No Arabic'
                                                                      ELSE 'Unknown Arabic'
                                                                  END,
                                         CenterSpeaksFrenchFlag = CASE mrk.SpeaksFrenchFlag
                                                                      WHEN 1
                                                                      THEN 'Speaks French'
                                                                      WHEN 0
                                                                      THEN 'No French'
                                                                      ELSE 'Unknown French'
                                                                  END,
                                         CenterSpeaksGermanFlag = CASE mrk.SpeaksGermanFlag
                                                                      WHEN 1
                                                                      THEN 'Speaks German'
                                                                      WHEN 0
                                                                      THEN 'No German'
                                                                      ELSE 'Unknown German'
                                                                  END,
                                         CenterSpeaksHindiFlag = CASE mrk.SpeaksHindiFlag
                                                                     WHEN 1
                                                                     THEN 'Speaks Hindi'
                                                                     WHEN 0
                                                                     THEN 'No Hindi'
                                                                     ELSE 'Unknown Hindi'
                                                                 END,
                                         CenterSpeaksMandarinFlag = CASE mrk.SpeaksMandarinFlag
                                                                        WHEN 1
                                                                        THEN 'Speaks Mandarin'
                                                                        WHEN 0
                                                                        THEN 'No Mandarin'
                                                                        ELSE 'Unknown Mandarin'
                                                                    END,
                                         CenterSpeaksPunjabiFlag = CASE mrk.SpeaksPunjabiFlag
                                                                       WHEN 1
                                                                       THEN 'Speaks Punjabi'
                                                                       WHEN 0
                                                                       THEN 'No Punjabi'
                                                                       ELSE 'Unknown Punjabi'
                                                                   END,
                                         CenterSpeaksSpanishFlag = CASE mrk.SpeaksSpanishFlag
                                                                       WHEN 1
                                                                       THEN 'Speaks Spanish'
                                                                       WHEN 0
                                                                       THEN 'No Spanish'
                                                                       ELSE 'Unknown Spanish'
                                                                   END,
                                         CenterSpeaksOtherLanguages = mrk.SpeaksOtherLanguage
                  FROM MISC_Staging.dbo.MdmMarketing mrk
                  ORDER BY ROW_NUMBER() OVER(PARTITION BY mrk.CenterNumber ORDER BY mrk.StgCreatedDate)),
                  --				  
                  -- CTE for SubsidyContacts data for each CostCenter
                  SubsidyContacts
                  AS (
                  SELECT TOP 1 WITH TIES CostCenterNumber = RIGHT('000000'+sub_cnt_lst.CenterNumber, 6),
                                         CenterNutritionAndWellnessAdministratorName = sub_cnt_lst.NutritionAndWellnessAdministratorContact,
                                         CenterNutritionAndWellnessAdministratorEmail = sub_cnt_lst.NutritionAndWellnessAdministratorEmail,
                                         CenterNutritionAndWellnessAdministratorPhone = sub_cnt_lst.NutritionAndWellnessAdministratorPhoneNumber,
                                         CenterSubsidyCoordinatorName = sub_cnt_lst.SubsidyCoordinatorContact,
                                         CenterSubsidyCoordinatorEmail = sub_cnt_lst.SubsidyCoordinatorEmail,
                                         CenterSubsidyCoordinatorPhone = sub_cnt_lst.SubsidyCoordinatorPhoneNumber,
                                         CenterSubsidyManagerName = sub_cnt_lst.SubsidyManagerContact,
                                         CenterSubsidyManagerEmail = sub_cnt_lst.SubsidyManagerEmail,
                                         CenterSubsidyManagerPhone = sub_cnt_lst.SubsidyManagerPhoneNumber,
                                         CenterSubsidySupervisorName = sub_cnt_lst.SubsidySupervisorContact,
                                         CenterSubsidySupervisorEmail = sub_cnt_lst.SubsidySupervisorEmail,
                                         CenterSubsidySupervisorPhone = sub_cnt_lst.SubsidySupervisorPhoneNumber
                  FROM MISC_Staging.dbo.MdmSubsidyContactList sub_cnt_lst
                  ORDER BY ROW_NUMBER() OVER(PARTITION BY sub_cnt_lst.CenterNumber ORDER BY sub_cnt_lst.StgCreatedDate)),
                  --				  
                  -- CTE for dataset showing any CostCenters that have HmanSigma scores
                  HumanSigma
                  AS (
                  SELECT CostCenterNumber,
                         CenterCurrentHumanSigmaScore = pvt.[1],
                         CenterPreviousHumanSigmaScore = pvt.[2]
                  FROM
                  (
                      SELECT CostCenterNumber = KLC_VALUE,
                             YearRank = ROW_NUMBER() OVER(PARTITION BY KLC_VALUE ORDER BY ACTIVE_DATE DESC),
                             HumanSigmaScore = PARTNER_VALUE
                      FROM dbo.xxklcOilXrefValuesV
                      WHERE PARTNER_NAME = 'BING'
                            AND TYPE_NAME = 'Human_Sigma'
                            AND ACTIVE_DATE <= GETDATE()
                  ) hmn_sig PIVOT(MIN(HumanSigmaScore) FOR YearRank IN([1],
                                                                       [2])) pvt),
                  --																 
                  -- CTE for Quality & Accreditation data for each CostCenter
                  QualityAndAccreditation
                  AS (
                  SELECT CostCenterNumber,
                         CenterAccreditationAgencyCode = ql_accr.AccreditationAgencyCode,
                         CenterAccreditationExpirationDate = ql_accr.AccreditationExpirationDate,
                         CenterAccreditationNextActivity = ql_accr.AccreditationNextActivity,
                         CenterAccreditationNextActivityDueDate = ql_accr.AccreditationNextActivityDueDate,
                         CenterAccreditationPrimaryStatus = ql_accr.AccreditationPrimaryStatus,
                         CenterAccreditationProgramID = ql_accr.AccreditationProgramID,
                         CenterAccreditationStartDate = ql_accr.AccreditationStartDate,
                         CenterQRISRating = ql_accr.QRISRating,
                         CenterQRISRatingStartDate = ql_accr.QRISRatingStartDate,
                         CenterQRISRatingExpirationDate = ql_accr.QRISRatingExpirationDate
                  FROM vQualityAndAccreditation ql_accr)

                  -- ================================================================================
                  -- Build the final #DimCostCenter temp table, that will hold the dataset returned 
                  --     to the calling proc.
                  -- ================================================================================

                  SELECT Distinct cte_cst_ctr_nums.CostCenterNumber AS [CostCenterNumber],
                         COALESCE(cte_cst_ctrs.CostCenterName, cte_cst_ctr_nums.CostCenterNumber) AS [CostCenterName],
                         cte_cst_ctrs.CompanyID AS [CompanyID],
                         cte_cst_ctrs.CostCenterTypeID AS [CostCenterTypeID],
                         -- CostCenter Hierarchy Levels 1 - 11, with Leaders included in the HierarchyLevelName
                         cte_orgs_ldrs.OrgAllName AS [CCHierarchyLevel1Name],
                         cte_orgs_ldrs.OrgExecutiveFunctionName+COALESCE(' - '+cte_orgs_ldrs.OrgExecutiveFunctionLeaderName, '') AS [CCHierarchyLevel2Name],
                         CASE
                             WHEN cte_orgs_ldrs.OrgCategoryName = 'Corporate'
                             THEN cte_orgs_ldrs.OrgCorporateFunctionName
                             ELSE cte_orgs_ldrs.OrgDivisionName+COALESCE(' - '+cte_orgs_ldrs.OrgDivisionLeaderName, '')
                         END AS [CCHierarchyLevel3Name],
                         CASE
                             WHEN cte_orgs_ldrs.OrgCategoryName = 'Corporate'
                             THEN cte_orgs_ldrs.OrgCorporateSubFunctionName
                             ELSE cte_orgs_ldrs.OrgRegionName+COALESCE(' - '+cte_orgs_ldrs.OrgRegionLeaderName, '')
                         END AS [CCHierarchyLevel4Name],
                         cte_orgs_ldrs.OrgMarketName+COALESCE(' - '+cte_orgs_ldrs.OrgMarketLeaderName, '') AS [CCHierarchyLevel5Name],
                         cte_orgs_ldrs.OrgSubMarketName+COALESCE(' - '+cte_orgs_ldrs.OrgSubMarketLeaderName, '') AS [CCHierarchyLevel6Name],
                         cte_orgs_ldrs.OrgDistrictName+COALESCE(' - '+cte_orgs_ldrs.OrgDistrictLeaderName, '') AS [CCHierarchyLevel7Name],
                         cte_orgs_ldrs.OrgGroupName+COALESCE(' - '+cte_orgs_ldrs.OrgGroupLeaderName, '') AS [CCHierarchyLevel8Name],
                         cte_orgs_ldrs.OrgSubGroupName+COALESCE(' - '+cte_orgs_ldrs.OrgSubGroupLeaderName, '') AS [CCHierarchyLevel9Name],
                         cte_orgs_ldrs.OrgCampusName+COALESCE(' - '+cte_orgs_ldrs.OrgCampusLeaderName, '') AS [CCHierarchyLevel10Name],
                         cte_orgs_ldrs.OrgName+COALESCE(' - '+cte_orgs_ldrs.OrgCenterLeaderName, '') AS [CCHierarchyLevel11Name],
                         cte_cst_ctrs.OpenDate AS [CCOpenDate],
                         cte_cst_ctrs.ClosedDate AS [CCClosedDate],
                         cte_cst_ctrs.ReopenDate AS [CCReopenDate],
                         cte_cst_ctrs.ReopenDateType AS [CCReopenDateType],
                         cte_cst_ctrs.CostCenterClassification AS [CCClassification],
                         cte_cst_ctrs.CostCenterStatus AS [CCStatus],
                         cte_cst_ctrs.Consolidation AS [CCConsolidation],
                         cte_cst_ctrs.FlexAttribute1 AS [CCFlexAttribute1],
                         cte_cst_ctrs.FlexAttribute2 AS [CCFlexAttribute2],
                         cte_cst_ctrs.FlexAttribute3 AS [CCFlexAttribute3],
                         cte_cst_ctrs.FlexAttribute4 AS [CCFlexAttribute4],
                         cte_cst_ctrs.FlexAttribute5 AS [CCFlexAttribute5],
                         -- CMS / CSS / Horizon-specific details
                         cte_cms.CMSID AS [CenterCMSID],
                         cte_css.CSSID AS [CenterCSSID],
                         cte_hor.HorizonID AS [SiteHorizonID],
                         -- CSS -> CMS Migration details						 
                         cte_css2cms.CenterEnrollmentSourceSystem AS [CenterEnrollmentSourceSystem],
                         cte_css2cms.CenterCMSMigrationDate AS [CenterCMSMigrationDate],
                         cte_css2cms.CenterCMSMigrationStatus AS [CenterCMSMigrationStatus],
					NULL AS [CenterLicensedCapacity], -- BNG-4422 - Leave NULL for now - we will remove column from Dimension at a later date.
                         CASE
                             WHEN CenterEnrollmentSourceSystem = 'CSS'
                             THEN cte_css.CenterBackupCareFlag
                             ELSE cte_cms.CenterBackupCareFlag
                         END AS [CenterBackupCareFlag],
                         -- Center loaction-specific details (Open / close days & times, Student min / max age, etc)						 
                         CenterChildCareSelectFlag AS [CenterChildCareSelectFlag],
                         CenterPublicAllowedFlag AS [CenterPublicAllowedFlag],
                         CASE
                             WHEN CenterEnrollmentSourceSystem = 'CSS'
                             THEN cte_css.CenterOperationStartTime
                             ELSE cte_cms.CenterOperationStartTime
                         END AS [CenterOpenTime],
                         CASE
                             WHEN CenterEnrollmentSourceSystem = 'CSS'
                             THEN cte_css.CenterOperationEndTime
                             ELSE cte_cms.CenterOperationEndTime
                         END AS [CenterCloseTime],
                         CASE
                             WHEN CenterEnrollmentSourceSystem = 'CSS'
                             THEN cte_css.CenterStudentMinimumAge
                             ELSE cte_cms.CenterStudentMinimumAge
                         END AS [CenterStudentMinimumAge],
                         CASE
                             WHEN CenterEnrollmentSourceSystem = 'CSS'
                             THEN cte_css.CenterStudentMaximumAge
                             ELSE cte_cms.CenterStudentMaximumAge
                         END AS [CenterStudentMaximumAge],
                         CASE
                             WHEN CenterEnrollmentSourceSystem = 'CSS'
                             THEN cte_css.CenterOpenSunFlag
                             ELSE cte_cms_opn.CenterOpenSunFlag
                         END AS [CenterOpenSunFlag],
                         CASE
                             WHEN CenterEnrollmentSourceSystem = 'CSS'
                             THEN cte_css.CenterOpenMonFlag
                             ELSE cte_cms_opn.CenterOpenMonFlag
                         END AS [CenterOpenMonFlag],
                         CASE
                             WHEN CenterEnrollmentSourceSystem = 'CSS'
                             THEN cte_css.CenterOpenTueFlag
                             ELSE cte_cms_opn.CenterOpenTueFlag
                         END AS [CenterOpenTueFlag],
                         CASE
                             WHEN CenterEnrollmentSourceSystem = 'CSS'
                             THEN cte_css.CenterOpenWedFlag
                             ELSE cte_cms_opn.CenterOpenWedFlag
                         END AS [CenterOpenWedFlag],
                         CASE
                             WHEN CenterEnrollmentSourceSystem = 'CSS'
                             THEN cte_css.CenterOpenThuFlag
                             ELSE cte_cms_opn.CenterOpenThuFlag
                         END AS [CenterOpenThuFlag],
                         CASE
                             WHEN CenterEnrollmentSourceSystem = 'CSS'
                             THEN cte_css.CenterOpenFriFlag
                             ELSE cte_cms_opn.CenterOpenFriFlag
                         END AS [CenterOpenFriFlag],
                         CASE
                             WHEN CenterEnrollmentSourceSystem = 'CSS'
                             THEN cte_css.CenterOpenSatFlag
                             ELSE cte_cms_opn.CenterOpenSatFlag
                         END AS [CenterOpenSatFlag],
                         cte_cms.CenterFoodProgramStartDate AS [CenterFoodProgramStartDate],
                         cte_cms.CenterFoodProgramEndDate AS [CenterFoodProgramEndDate],
                         cte_cms.CenterRegistrationType AS [CenterRegistrationType],
                         cte_hor.SiteSchoolDistrict AS [SiteSchoolDistrict],
                         cte_cst_ctrs.ClassYear AS [SiteClassYear],
                         -- Marketing
                         cte_mkt.CenterURL AS [CenterURL],
                         cte_mkt.CenterMenuURL AS [CenterMenuURL],
                         cte_mkt.CenterHasBreakfastFlag AS [CenterHasBreakfastFlag],
                         cte_mkt.CenterHasMorningSlackFlag AS [CenterHasMorningSlackFlag],
                         cte_mkt.CenterHasLunchFlag AS [CenterHasLunchFlag],
                         cte_mkt.CenterHasAfternoonSnackFlag AS [CenterHasAfternoonSnackFlag],
                         cte_mkt.CenterSpeaksASLFlag AS [CenterSpeaksASLFlag],
                         cte_mkt.CenterSpeaksArabicFlag AS [CenterSpeaksArabicFlag],
                         cte_mkt.CenterSpeaksFrenchFlag AS [CenterSpeaksFrenchFlag],
                         cte_mkt.CenterSpeaksGermanFlag AS [CenterSpeaksGermanFlag],
                         cte_mkt.CenterSpeaksHindiFlag AS [CenterSpeaksHindiFlag],
                         cte_mkt.CenterSpeaksMandarinFlag AS [CenterSpeaksMandarinFlag],
                         cte_mkt.CenterSpeaksPunjabiFlag AS [CenterSpeaksPunjabiFlag],
                         cte_mkt.CenterSpeaksSpanishFlag AS [CenterSpeaksSpanishFlag],
                         cte_mkt.CenterSpeaksOtherLanguages AS [CenterSpeaksOtherLanguages],
                         -- Quality and Accreditation
                         cte_ql_acr.CenterAccreditationAgencyCode AS [CenterAccreditationAgencyCode],
                         cte_ql_acr.CenterAccreditationStartDate AS [CenterAccreditationStartDate],
                         cte_ql_acr.CenterAccreditationExpirationDate AS [CenterAccreditationExpirationDate],
                         cte_ql_acr.CenterAccreditationNextActivity AS [CenterAccreditationNextActivity],
                         cte_ql_acr.CenterAccreditationNextActivityDueDate AS [CenterAccreditationNextActivityDueDate],
                         cte_ql_acr.CenterAccreditationPrimaryStatus AS [CenterAccreditationPrimaryStatus],
                         cte_ql_acr.CenterAccreditationProgramID AS [CenterAccreditationProgramID],
                         cte_ql_acr.CenterQRISRating AS [CenterQRISRating],
                         cte_ql_acr.CenterQRISRatingStartDate AS [CenterQRISRatingStartDate],
                         cte_ql_acr.CenterQRISRatingExpirationDate AS [CenterQRISRatingExpirationDate],
                         -- Facilities Contacts
                         cte_fac.CenterMaintenanceSupervisorName AS [CenterMaintenanceSupervisorName],
                         cte_fac.CenterPreventativeTechnicianName AS [CenterPreventativeTechnicianName],
                         cte_fac.CenterRegionalFacilitiesCoordinatorName AS [CenterRegionalFacilitiesCoordinatorName],
                         cte_fac.CenterRegionalFacilitiesManagerName AS [CenterRegionalFacilitiesManagerName],
                         -- Subsidy Contacts
                         cte_sbsdy.CenterNutritionAndWellnessAdministratorName AS [CenterNutritionAndWellnessAdministratorName],
                         cte_sbsdy.CenterNutritionAndWellnessAdministratorEmail AS [CenterNutritionAndWellnessAdministratorEmail],
                         cte_sbsdy.CenterNutritionAndWellnessAdministratorPhone AS [CenterNutritionAndWellnessAdministratorPhone],
                         cte_sbsdy.CenterSubsidyCoordinatorName AS [CenterSubsidyCoordinatorName],
                         cte_sbsdy.CenterSubsidyCoordinatorEmail AS [CenterSubsidyCoordinatorEmail],
                         cte_sbsdy.CenterSubsidyCoordinatorPhone AS [CenterSubsidyCoordinatorPhone],
                         cte_sbsdy.CenterSubsidyManagerName AS [CenterSubsidyManagerName],
                         cte_sbsdy.CenterSubsidyManagerEmail AS [CenterSubsidyManagerEmail],
                         cte_sbsdy.CenterSubsidyManagerPhone AS [CenterSubsidyManagerPhone],
                         cte_sbsdy.CenterSubsidySupervisorName AS [CenterSubsidySupervisorName],
                         cte_sbsdy.CenterSubsidySupervisorEmail AS [CenterSubsidySupervisorEmail],
                         cte_sbsdy.CenterSubsidySupervisorPhone AS [CenterSubsidySupervisorPhone],
                         -- Lease Administration
                         cte_lse.CenterBuildingSquareFootage AS [CenterBuildingSquareFootage],
                         cte_lse.CenterLandSquareFootage AS [CenterLandSquareFootage],
                         cte_lse.CenterCoreBasedStatisticalAreaName AS [CenterCoreBasedStatisticalAreaName],
                         cte_lse.CenterLandlordName AS [CenterLandlordName],
                         cte_lse.CenterLeaseControlEndMonthDate AS [CenterLeaseControlEndMonthDate],
                         cte_lse.CenterLeaseExpirationDate AS [CenterLeaseExpirationDate],
                         cte_lse.CenterLeaseExtensionOptionNoticeDate AS [CenterLeaseExtensionOptionNoticeDate],
                         cte_lse.CenterLeaseExtensionOptionsRemainingCount AS [CenterLeaseExtensionOptionsRemainingCount],
                         cte_lse.CenterLeaseExtensionOptionRemainingYears AS [CenterLeaseExtensionOptionRemainingYears],
                         cte_lse.CenterLeaseStatus AS [CenterLeaseStatus],
                         -- Latitude / Longitude
                         cte_lat_lng.CenterLatitude AS [CenterLatitude],
                         cte_lat_lng.CenterLongitude AS [CenterLongitude],
                         -- Human Sigma
                         cte_hmn.CenterCurrentHumanSigmaScore AS [CenterCurrentHumanSigmaScore],
                         cte_hmn.CenterPreviousHumanSigmaScore [CenterPreviousHumanSigmaScore]
                  INTO #DimCostCenter
                  FROM CostCenterNumbers cte_cst_ctr_nums
                       LEFT JOIN CostCenters cte_cst_ctrs ON cte_cst_ctr_nums.CostCenterNumber = cte_cst_ctrs.CostCenterNumber
                       LEFT JOIN OrgsWithLeaders cte_orgs_ldrs ON cte_cst_ctr_nums.CostCenterNumber = cte_orgs_ldrs.CostCenterNumber
                       LEFT JOIN CMS cte_cms ON cte_cst_ctr_nums.CostCenterNumber = cte_cms.CostCenterNumber
                       LEFT JOIN CSS cte_css ON cte_cst_ctr_nums.CostCenterNumber = cte_css.CostCenterNumber
                       LEFT JOIN CSStoCMSDates cte_css2cms ON cte_cst_ctr_nums.CostCenterNumber = cte_css2cms.CostCenterNumber
                       LEFT JOIN FacilitiesContacts cte_fac ON cte_cst_ctr_nums.CostCenterNumber = cte_fac.CostCenterNumber
                       LEFT JOIN LeaseAdministration cte_lse ON cte_cst_ctr_nums.CostCenterNumber = cte_lse.CostCenterNumber
                       LEFT JOIN LatitudeLongitude cte_lat_lng ON cte_cst_ctr_nums.CostCenterNumber = cte_lat_lng.CostCenterNumber
                       LEFT JOIN CMSOpenDays cte_cms_opn ON cte_cms.CMSID = cte_cms_opn.CMSID
                       LEFT JOIN HumanSigma cte_hmn ON cte_cst_ctr_nums.CostCenterNumber = cte_hmn.CostCenterNumber
                       LEFT JOIN Marketing cte_mkt ON cte_cst_ctr_nums.CostCenterNumber = cte_mkt.CostCenterNumber
                       LEFT JOIN Horizon cte_hor ON cte_cst_ctr_nums.CostCenterNumber = cte_hor.CostCenterNumber
                       LEFT JOIN QualityAndAccreditation cte_ql_acr ON cte_cst_ctr_nums.CostCenterNumber = cte_ql_acr.CostCenterNumber
                       LEFT JOIN SubsidyContacts cte_sbsdy ON cte_cst_ctr_nums.CostCenterNumber = cte_sbsdy.CostCenterNumber;

             -- ================================================================================
             -- Override the CCStatus and CCClassification
             -- ------------------------------------------
             --
             -- The source is not doing a good job of staying up-to-date with the CCStatus and
             --    CCClassification, so we are seeing Centers sitting under Region 98 (all Closed
             --    Centers) but not having a Status or Classification of 'Closed'.  This then
             --    shows up incorrectly in the AS Model and in the Reports where Closed Centers
             --    should be filtered-out.
             --
             -- This logic works on the assumption that the Center rolling-up to R98 in the 
             --    source data is ALWAYS correct and should override anything already in the 
             --    CCStatus or CCClassification fields.			 
             -- ================================================================================
             --
             -- SB 10/15/18 - This logic is on-hold for now.  
             --               Ron Bishop stated he wants Finance to correct in the source...
             -- 
             -- UPDATE #DimCostCenter
             -- SET
             --   CCStatus = 'Closed',
             --   CCClassification = 'Closed'
             -- WHERE CCHierarchyLevel4Name LIKE 'R98%'
             -- AND CCClassification NOT IN('Closed', 'Unknown');

             -- ================================================================================
             -- We have to do some filling in of the blanks here
             -- Loop through each level of the hierarchy and where NULL, set to the next 
             --     level down
             -- ================================================================================
             DECLARE @Levels INT= 0;
             WHILE @Levels < 11
                 BEGIN
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel10Name = CCHierarchyLevel11Name
                     WHERE CCHierarchyLevel10Name IS NULL;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel9Name = CCHierarchyLevel10Name
                     WHERE CCHierarchyLevel9Name IS NULL;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel8Name = CCHierarchyLevel9Name
                     WHERE CCHierarchyLevel8Name IS NULL;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel7Name = CCHierarchyLevel8Name
                     WHERE CCHierarchyLevel7Name IS NULL;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel6Name = CCHierarchyLevel7Name
                     WHERE CCHierarchyLevel6Name IS NULL;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel5Name = CCHierarchyLevel6Name
                     WHERE CCHierarchyLevel5Name IS NULL;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel4Name = CCHierarchyLevel5Name
                     WHERE CCHierarchyLevel4Name IS NULL;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel3Name = CCHierarchyLevel4Name
                     WHERE CCHierarchyLevel3Name IS NULL;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel2Name = CCHierarchyLevel3Name
                     WHERE CCHierarchyLevel2Name IS NULL;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel1Name = CCHierarchyLevel2Name
                     WHERE CCHierarchyLevel1Name IS NULL;
                     SET @Levels = @Levels + 1;
                 END;

             -- ================================================================================
             -- Repeat this whole set of update statements once for each level of the hierarchy.  
             -- This "defrags" the groupings
             -- ================================================================================
             SET @Levels = 0;
             WHILE @Levels < 11
                 BEGIN
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel10Name = CCHierarchyLevel11Name
                     WHERE CCHierarchyLevel9Name = CCHierarchyLevel10Name;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel9Name = CCHierarchyLevel10Name
                     WHERE CCHierarchyLevel8Name = CCHierarchyLevel9Name;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel8Name = CCHierarchyLevel9Name
                     WHERE CCHierarchyLevel7Name = CCHierarchyLevel8Name;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel7Name = CCHierarchyLevel8Name
                     WHERE CCHierarchyLevel6Name = CCHierarchyLevel7Name;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel6Name = CCHierarchyLevel7Name
                     WHERE CCHierarchyLevel5Name = CCHierarchyLevel6Name;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel5Name = CCHierarchyLevel6Name
                     WHERE CCHierarchyLevel4Name = CCHierarchyLevel5Name;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel4Name = CCHierarchyLevel5Name
                     WHERE CCHierarchyLevel3Name = CCHierarchyLevel4Name;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel3Name = CCHierarchyLevel4Name
                     WHERE CCHierarchyLevel2Name = CCHierarchyLevel3Name;
                     UPDATE #DimCostCenter
                       SET
                           CCHierarchyLevel2Name = CCHierarchyLevel3Name
                     WHERE CCHierarchyLevel1Name = CCHierarchyLevel2Name;
                     SET @Levels = @Levels + 1;
                 END;

             -- ================================================================================
             -- Delete any duplicating Hierarchy Level data at the end (e.g. when we get to the  
             --     point where CCHierarchyLevel10Name is the same as CCHierarchyLevel9Name).
             --
             -- We do this for SSAS 2017, which can only handle ragged hierarchies when the 
             --     bottom levels are blank
             -- ================================================================================
             UPDATE #DimCostCenter
               SET
                   CCHierarchyLevel11Name = NULL
             WHERE CCHierarchyLevel10Name = CCHierarchyLevel11Name;
             UPDATE #DimCostCenter
               SET
                   CCHierarchyLevel10Name = NULL
             WHERE CCHierarchyLevel9Name = CCHierarchyLevel10Name;
             UPDATE #DimCostCenter
               SET
                   CCHierarchyLevel9Name = NULL
             WHERE CCHierarchyLevel8Name = CCHierarchyLevel9Name;
             UPDATE #DimCostCenter
               SET
                   CCHierarchyLevel8Name = NULL
             WHERE CCHierarchyLevel7Name = CCHierarchyLevel8Name;
             UPDATE #DimCostCenter
               SET
                   CCHierarchyLevel7Name = NULL
             WHERE CCHierarchyLevel6Name = CCHierarchyLevel7Name;
             UPDATE #DimCostCenter
               SET
                   CCHierarchyLevel6Name = NULL
             WHERE CCHierarchyLevel5Name = CCHierarchyLevel6Name;
             UPDATE #DimCostCenter
               SET
                   CCHierarchyLevel5Name = NULL
             WHERE CCHierarchyLevel4Name = CCHierarchyLevel5Name;
             UPDATE #DimCostCenter
               SET
                   CCHierarchyLevel4Name = NULL
             WHERE CCHierarchyLevel3Name = CCHierarchyLevel4Name;
             UPDATE #DimCostCenter
               SET
                   CCHierarchyLevel3Name = NULL
             WHERE CCHierarchyLevel2Name = CCHierarchyLevel3Name;

             -- ================================================================================
             -- Finally, we can return the dataset back to the calling proc.
             -- ================================================================================

             SELECT COALESCE(CostCenterNumber, '-1') AS CostCenterNumber,
                    COALESCE(CostCenterName, 'Unknown Cost Center') AS CostCenterName,
                    COALESCE(CompanyID, '-1') AS CompanyID,
                    COALESCE(CostCenterTypeID, '-1') AS CostCenterTypeID,
                    COALESCE(CCHierarchyLevel1Name, NULL) AS CCHierarchyLevel1Name,
                    COALESCE(CCHierarchyLevel2Name, NULL) AS CCHierarchyLevel2Name,
                    COALESCE(CCHierarchyLevel3Name, NULL) AS CCHierarchyLevel3Name,
                    COALESCE(CCHierarchyLevel4Name, NULL) AS CCHierarchyLevel4Name,
                    COALESCE(CCHierarchyLevel5Name, NULL) AS CCHierarchyLevel5Name,
                    COALESCE(CCHierarchyLevel6Name, NULL) AS CCHierarchyLevel6Name,
                    COALESCE(CCHierarchyLevel7Name, NULL) AS CCHierarchyLevel7Name,
                    COALESCE(CCHierarchyLevel8Name, NULL) AS CCHierarchyLevel8Name,
                    COALESCE(CCHierarchyLevel9Name, NULL) AS CCHierarchyLevel9Name,
                    COALESCE(CCHierarchyLevel10Name, NULL) AS CCHierarchyLevel10Name,
                    COALESCE(CCHierarchyLevel11Name, NULL) AS CCHierarchyLevel11Name,
                    COALESCE(CCOpenDate, GETDATE()) AS CCOpenDate,
                    COALESCE(CCClosedDate, NULL) AS CCClosedDate,
                    COALESCE(CCReopenDate, NULL) AS CCReopenDate,
                    COALESCE(CCReopenDateType, NULL) AS CCReopenDateType,
                    COALESCE(CCClassification, 'Unknown Classification') AS CCClassification,
                    COALESCE(CCStatus, ' Unknown Status') AS CCStatus,
                    COALESCE(CCConsolidation, 'Unknown Consolidation') AS CCConsolidation,
                    COALESCE(CCFlexAttribute1, NULL) AS CCFlexAttribute1,
                    COALESCE(CCFlexAttribute2, NULL) AS CCFlexAttribute2,
                    COALESCE(CCFlexAttribute3, NULL) AS CCFlexAttribute3,
                    COALESCE(CCFlexAttribute4, NULL) AS CCFlexAttribute4,
                    COALESCE(CCFlexAttribute5, NULL) AS CCFlexAttribute5,
                    COALESCE(CenterCMSID, NULL) AS CenterCMSID,
                    COALESCE(CenterCSSID, NULL) AS CenterCSSID,
                    COALESCE(SiteHorizonID, NULL) AS SiteHorizonID,
                    COALESCE(CenterEnrollmentSourceSystem, NULL) AS CenterEnrollmentSourceSystem,
                    COALESCE(CenterCMSMigrationDate, NULL) AS CenterCMSMigrationDate,
                    COALESCE(CenterCMSMigrationStatus, NULL) AS CenterCMSMigrationStatus,
                    COALESCE(CenterLicensedCapacity, NULL) AS CenterLicensedCapacity, -- BNG-4422 -We are actually just returning a NULL now
                    COALESCE(CenterBackupCareFlag, NULL) AS CenterBackupCareFlag,
                    COALESCE(CenterChildCareSelectFlag, NULL) AS CenterChildCareSelectFlag,
                    COALESCE(CenterPublicAllowedFlag, NULL) AS CenterPublicAllowedFlag,
                    COALESCE(CenterOpenTime, NULL) AS CenterOpenTime,
                    COALESCE(CenterCloseTime, NULL) AS CenterCloseTime,
                    COALESCE(CenterStudentMinimumAge, NULL) AS CenterStudentMinimumAge,
                    COALESCE(CenterStudentMaximumAge, NULL) AS CenterStudentMaximumAge,
                    COALESCE(CenterOpenSunFlag, NULL) AS CenterOpenSunFlag,
                    COALESCE(CenterOpenMonFlag, NULL) AS CenterOpenMonFlag,
                    COALESCE(CenterOpenTueFlag, NULL) AS CenterOpenTueFlag,
                    COALESCE(CenterOpenWedFlag, NULL) AS CenterOpenWedFlag,
                    COALESCE(CenterOpenThuFlag, NULL) AS CenterOpenThuFlag,
                    COALESCE(CenterOpenFriFlag, NULL) AS CenterOpenFriFlag,
                    COALESCE(CenterOpenSatFlag, NULL) AS CenterOpenSatFlag,
                    COALESCE(CenterFoodProgramStartDate, NULL) AS CenterFoodProgramStartDate,
                    COALESCE(CenterFoodProgramEndDate, NULL) AS CenterFoodProgramEndDate,
                    COALESCE(CenterRegistrationType, NULL) AS CenterRegistrationType,
                    COALESCE(SiteSchoolDistrict, NULL) AS SiteSchoolDistrict,
                    COALESCE(SiteClassYear, NULL) AS SiteClassYear,
                    COALESCE(CenterMenuURL, NULL) AS CenterMenuURL,
                    COALESCE(CenterHasBreakfastFlag, NULL) AS CenterHasBreakfastFlag,
                    COALESCE(CenterHasMorningSlackFlag, NULL) AS CenterHasMorningSlackFlag,
                    COALESCE(CenterHasLunchFlag, NULL) AS CenterHasLunchFlag,
                    COALESCE(CenterHasAfternoonSnackFlag, NULL) AS CenterHasAfternoonSnackFlag,
                    COALESCE(CenterSpeaksASLFlag, NULL) AS CenterSpeaksASLFlag,
                    COALESCE(CenterSpeaksArabicFlag, NULL) AS CenterSpeaksArabicFlag,
                    COALESCE(CenterSpeaksFrenchFlag, NULL) AS CenterSpeaksFrenchFlag,
                    COALESCE(CenterSpeaksGermanFlag, NULL) AS CenterSpeaksGermanFlag,
                    COALESCE(CenterSpeaksHindiFlag, NULL) AS CenterSpeaksHindiFlag,
                    COALESCE(CenterSpeaksMandarinFlag, NULL) AS CenterSpeaksMandarinFlag,
                    COALESCE(CenterSpeaksPunjabiFlag, NULL) AS CenterSpeaksPunjabiFlag,
                    COALESCE(CenterSpeaksSpanishFlag, NULL) AS CenterSpeaksSpanishFlag,
                    COALESCE(CenterSpeaksOtherLanguages, NULL) AS CenterSpeaksOtherLanguages,
                    COALESCE(CenterAccreditationAgencyCode, NULL) AS CenterAccreditationAgencyCode,
                    COALESCE(CenterAccreditationStartDate, NULL) AS CenterAccreditationStartDate,
                    COALESCE(CenterAccreditationExpirationDate, NULL) AS CenterAccreditationExpirationDate,
                    COALESCE(CenterAccreditationNextActivity, NULL) AS CenterAccreditationNextActivity,
                    COALESCE(CenterAccreditationNextActivityDueDate, NULL) AS CenterAccreditationNextActivityDueDate,
                    COALESCE(CenterAccreditationPrimaryStatus, NULL) AS CenterAccreditationPrimaryStatus,
                    COALESCE(CenterAccreditationProgramID, NULL) AS CenterAccreditationProgramID,
                    COALESCE(CenterQRISRating, NULL) AS CenterQRISRating,
                    COALESCE(CenterQRISRatingStartDate, NULL) AS CenterQRISRatingStartDate,
                    COALESCE(CenterQRISRatingExpirationDate, NULL) AS CenterQRISRatingExpirationDate,
                    COALESCE(CenterMaintenanceSupervisorName, NULL) AS CenterMaintenanceSupervisorName,
                    COALESCE(CenterPreventativeTechnicianName, NULL) AS CenterPreventativeTechnicianName,
                    COALESCE(CenterRegionalFacilitiesCoordinatorName, NULL) AS CenterRegionalFacilitiesCoordinatorName,
                    COALESCE(CenterRegionalFacilitiesManagerName, NULL) AS CenterRegionalFacilitiesManagerName,
                    COALESCE(CenterNutritionAndWellnessAdministratorName, NULL) AS CenterNutritionAndWellnessAdministratorName,
                    COALESCE(CenterNutritionAndWellnessAdministratorEmail, NULL) AS CenterNutritionAndWellnessAdministratorEmail,
                    COALESCE(CenterNutritionAndWellnessAdministratorPhone, NULL) AS CenterNutritionAndWellnessAdministratorPhone,
                    COALESCE(CenterSubsidyCoordinatorName, NULL) AS CenterSubsidyCoordinatorName,
                    COALESCE(CenterSubsidyCoordinatorEmail, NULL) AS CenterSubsidyCoordinatorEmail,
                    COALESCE(CenterSubsidyCoordinatorPhone, NULL) AS CenterSubsidyCoordinatorPhone,
                    COALESCE(CenterSubsidyManagerName, NULL) AS CenterSubsidyManagerName,
                    COALESCE(CenterSubsidyManagerEmail, NULL) AS CenterSubsidyManagerEmail,
                    COALESCE(CenterSubsidyManagerPhone, NULL) AS CenterSubsidyManagerPhone,
                    COALESCE(CenterSubsidySupervisorName, NULL) AS CenterSubsidySupervisorName,
                    COALESCE(CenterSubsidySupervisorEmail, NULL) AS CenterSubsidySupervisorEmail,
                    COALESCE(CenterSubsidySupervisorPhone, NULL) AS CenterSubsidySupervisorPhone,
                    COALESCE(CenterBuildingSquareFootage, NULL) AS CenterBuildingSquareFootage,
                    COALESCE(CenterLandSquareFootage, NULL) AS CenterLandSquareFootage,
                    COALESCE(CenterCoreBasedStatisticalAreaName, NULL) AS CenterCoreBasedStatisticalAreaName,
                    COALESCE(CenterLandlordName, NULL) AS CenterLandlordName,
                    COALESCE(CenterLeaseControlEndMonthDate, NULL) AS CenterLeaseControlEndMonthDate,
                    COALESCE(CenterLeaseExpirationDate, NULL) AS CenterLeaseExpirationDate,
                    COALESCE(CenterLeaseExtensionOptionNoticeDate, NULL) AS CenterLeaseExtensionOptionNoticeDate,
                    COALESCE(CenterLeaseExtensionOptionsRemainingCount, NULL) AS CenterLeaseExtensionOptionsRemainingCount,
                    COALESCE(CenterLeaseExtensionOptionRemainingYears, NULL) AS CenterLeaseExtensionOptionRemainingYears,
                    COALESCE(CenterLeaseStatus, NULL) AS CenterLeaseStatus,
                    COALESCE(CenterLatitude, NULL) AS CenterLatitude,
                    COALESCE(CenterLongitude, NULL) AS CenterLongitude,
                    COALESCE(CenterCurrentHumanSigmaScore, NULL) AS CenterCurrentHumanSigmaScore,
                    COALESCE(CenterPreviousHumanSigmaScore, NULL) AS CenterPreviousHumanSigmaScore,
                    @EDWRunDateTime AS EDWEffectiveDate,
                    NULL AS EDWEndDate,
                    @EDWRunDateTime AS EDWCreatedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                    NULL AS Deleted
             FROM #DimCostCenter;
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
