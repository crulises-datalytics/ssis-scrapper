 

CREATE PROCEDURE [dbo].[spHR_StagingGenerate_OrgLeaderAccess]
(@EDWRunDateTime DATETIME2 = NULL, 
 @DebugMode      INT       = NULL
)
AS
    BEGIN
        -- ================================================================================
        -- 
        -- Stored Procedure:   spHR_StagingGenerate_OrgLeaderAccess
        --
        -- Purpose:            Populates the HR_Staging 'helper' table dbo.OrgLeaderAccess,
        --                         which is leveraged by the DimOrganization Dimension
        --                         table loads
        --
        --                     Note - This proc was formally spHR_StagingTransfrom_DimOrganization
        --
        -- Populates:          Truncates and [re]loads HR_Staging..OrgLeaderAccess 
        --
        -- Usage:              EXEC dbo.spHR_StagingGenerate_OrgLeaderAccess @DebugMode = 1
        --
        -- --------------------------------------------------------------------------------
        --
        -- Change Log:		   
        -- ----------
        --
        -- Date         Modified By     Comments
        -- ----         -----------     --------
        --
        -- 10/25/17     sburke          BNG-563 - Initial version of proc
        -- 11/29/17     sburke          BNG-835 - The data in OrgLeaderAccess is now populated by this proc, 
        --                                  replacing the old spHR_StagingGenerate_OrgLeaderAccess_DimOraganization
        --                                  and cleaning up the logic (and adding AuditLog entries).
        --                              This proc will also be executed as a standalone ETL job, rather than a 
        --                                  sub-procedure called by spHR_StagingTransform_DimOrganization, as it
        --                                  is now required by both the DimOrganization and DimCostCenter loads.
        --  1/18/18     sburke          BNG-1042 - Correction to OrgLeaderAccess load coming out of PoC UAT - add
        --                                  filter on vPeople.PersonCurrentRecordFlag when linking a Leader to 
        --                                  an Org.		 
        --  6/20/18     ADevabhakthuni  BNG-1797 - Add handling for multiple people appearing as Center Directors to
        --                                  choose the correct one (to cater for HRIS source data issues when data from
        --                                  acquired entities was loaded into HRIS).  This resolves ETL load errors caused
        --                                  by duplicatation.		
        --  11/12/2019   Adevabhakthuni  BI-561 Update the join to bring all leaders name even when source doesn't have job code
		-- 03/24/2022   Adevabhakthuni BI-5704 Updated the logic to get the active people in current week 
        -- 04/12/2023	Sagar.Gangula	BI-7791: Updated Logic to get the MIN(OrgLevelSequence) instead of MAX
	--02/09/2024   Aniket Navale     DFTP-966-  Update the logic for defining Division Leader/Region Leader/etc.
        -- ================================================================================
        SET NOCOUNT ON;
        --
        -- Housekeeping Variables
        -- 
        DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
        DECLARE @DebugMsg NVARCHAR(500);
        DECLARE @SourceName VARCHAR(100)= 'OrgLeaderAccess';
        DECLARE @AuditId BIGINT;
        --
        -- ETL status Variables
        --
        DECLARE @RowCount INT;
        DECLARE @Error INT;
        DECLARE @SourceCount INT= 0;
        DECLARE @InsertCount INT= 0;
        DECLARE @UpdateCount INT= 0;
        DECLARE @DeleteCount INT= 0;	
        --
        -- If we do not get an @EDWRunDateTime input, set to current date
        --
        IF @EDWRunDateTime IS NULL
            SET @EDWRunDateTime = GETDATE();
        --
        IF @DebugMode = 1
            SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Starting.';
        PRINT @DebugMsg;

        --
        -- Write to EDW AuditLog we are starting
        --
        EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog] 
             @SourceName = @SourceName, 
             @AuditId = @AuditId OUTPUT; 		 	 
        --
        BEGIN TRY
            -- ================================================================================
            -- Get all the Org details from the vOrgs view
            -- ================================================================================

            SELECT [OrgId], 
                   [OrgEffectiveDate], 
                   [OrgEndDate], 
                   [ParentOrgID], 
                   [DefaultLocationID], 
                   [CostCenterNumber], 
                   [OrgNumber], 
                   [OrgName], 
                   [OrgAllName], 
                   [OrgExecutiveFunctionName], 
                   [OrgExecutiveSubFunctionName], 
                   [OrgCorporateFunctionName], 
                   [OrgCorporateSubFunctionName], 
                   [OrgDivisionName], 
                   [OrgRegionNumber], 
                   [OrgRegionName], 
                   [OrgMarketNumber], 
                   [OrgMarketName], 
                   [OrgSubMarketNumber], 
                   [OrgSubMarketName], 
                   [OrgDistrictNumber], 
                   [OrgDistrictName], 
                   [OrgInterimDistrictNumber], 
                   [OrgInterimDistrictName], 
                   [OrgGroupNumber], 
                   [OrgGroupName], 
                   [OrgSubgroupNumber], 
                   [OrgSubGroupName], 
                   [OrgCampusNumber], 
                   [OrgCampusName], 
                   [OrgExecutiveFunctionLeaderName], 
                   [OrgExecutiveSubFunctionLeaderName], 
                   [OrgDivisionLeaderName], 
                   [OrgCategoryName], 
                   [OrgTypeCode], 
                   [OrgTypeName], 
                   [OrgPartnerGroupCode], 
                   [OrgPartnerGroupName], 
                   [OrgCenterGroupCode], 
                   [OrgCenterGroupName], 
                   [OrgDivisionLegacyName], 
                   [OrgLineOfBusinessCode], 
                   [OrgBrandCode], 
                   [OrgBrandName], 
                   [OrgFlexAttribute1], 
                   [OrgFlexAttribute2], 
                   [OrgFlexAttribute3], 
                   [OrgFlexAttribute4], 
                   [OrgFlexAttribute5], 
                   [OrgCreatedUser], 
                   [OrgCreatedDate], 
                   [OrgModifiedUser], 
                   [OrgModifiedDate]
            INTO #Orgs
            FROM vOrgs;

            -- ================================================================================
            -- Flatten, by unpivoting, the parent-child Organization Hierarchy data in #Orgs 
            --      into 11 Levels.
            -- ================================================================================
            WITH FlattenedOrgs
                 AS (SELECT OrgID, 
                            OrgName, 
                            OrgTypeCode, 
                            HierarchyLevelID = CAST(LEFT(ID, 2) AS INT), 
                            HierarchyOrgID = CAST(RIGHT(ID, LEN(ID) - 3) AS INT)
                     FROM
                     (
                         SELECT org_lv_01.OrgID, 
                                org_lv_01.OrgName, 
                                org_lv_01.OrgTypeCode, 
                                Level1 = '01.' + CAST(org_lv_01.OrgID AS VARCHAR), 
                                Level2 = '02.' + CAST(org_lv_01.ParentOrgID AS VARCHAR), 
                                Level3 = '03.' + CAST(org_lv_03.ParentOrgID AS VARCHAR), 
                                Level4 = '04.' + CAST(org_lv_04.ParentOrgID AS VARCHAR), 
                                Level5 = '05.' + CAST(org_lv_05.ParentOrgID AS VARCHAR), 
                                Level6 = '06.' + CAST(org_lv_06.ParentOrgID AS VARCHAR), 
                                Level7 = '07.' + CAST(org_lv_07.ParentOrgID AS VARCHAR), 
                                Level8 = '08.' + CAST(org_lv_08.ParentOrgID AS VARCHAR), 
                                Level9 = '09.' + CAST(org_lv_09.ParentOrgID AS VARCHAR), 
                                Level10 = '10.' + CAST(org_lv_10.ParentOrgID AS VARCHAR), 
                                Level11 = '11.' + CAST(org_lv_11.ParentOrgID AS VARCHAR)
                         FROM #Orgs org_lv_01
                              -- Why no org_lv_2?  Because we use ParentOrgID from org_lv_1 as Level2 OrgID
                              LEFT JOIN #Orgs org_lv_03 ON org_lv_01.ParentOrgID = org_lv_03.OrgID
                              LEFT JOIN #Orgs org_lv_04 ON org_lv_03.ParentOrgID = org_lv_04.OrgID
                              LEFT JOIN #Orgs org_lv_05 ON org_lv_04.ParentOrgID = org_lv_05.OrgID
                              LEFT JOIN #Orgs org_lv_06 ON org_lv_05.ParentOrgID = org_lv_06.OrgID
                              LEFT JOIN #Orgs org_lv_07 ON org_lv_06.ParentOrgID = org_lv_07.OrgID
                              LEFT JOIN #Orgs org_lv_08 ON org_lv_07.ParentOrgID = org_lv_08.OrgID
                              LEFT JOIN #Orgs org_lv_09 ON org_lv_08.ParentOrgID = org_lv_09.OrgID
                              LEFT JOIN #Orgs org_lv_10 ON org_lv_09.ParentOrgID = org_lv_10.OrgID
                              LEFT JOIN #Orgs org_lv_11 ON org_lv_10.ParentOrgID = org_lv_11.OrgID
                     ) a UNPIVOT(ID FOR LevelID IN(Level1, 
                                                   Level2, 
                                                   Level3, 
                                                   Level4, 
                                                   Level5, 
                                                   Level6, 
                                                   Level7, 
                                                   Level8, 
                                                   Level9, 
                                                   Level10, 
                                                   Level11)) u),
                 -- ================================================================================
                 --  Add Org Hierarchy Name and Type to our Flattened Orgs CTE results set
                 -- ================================================================================
                 OrgHierarchy
                 AS (SELECT flat_orgs.*, 
                            HierarchyOrgName = orgs.OrgName, 
                            HierarchyOrgTypeCode = orgs.OrgTypeCode
                     FROM FlattenedOrgs flat_orgs
                          LEFT JOIN #Orgs orgs ON flat_orgs.HierarchyOrgID = orgs.OrgID),
                 -- ================================================================================
                 --  Get Org Type Access details from GL_Staging.  It produces a list of what 
                 --      OrgTypeCodes can access other OtherTypeCodes, and it drives our logic
                 --      for ascertaining what has access to what.   
                 -- ================================================================================
                 OrgTypeAccess
                 AS (SELECT OrgTypeCode = KLC_VALUE, 
                            CanAccessOrgTypeCode = PARTNER_VALUE
                     FROM GL_Staging.dbo.xxklcOilXrefValuesV
                     WHERE PARTNER_NAME = 'BING'
                           AND TYPE_NAME = 'OrgType_Access'),
                 -- ================================================================================
                 --  Build a dataset of Orgs that can access other Orgs
                 -- ================================================================================
                 CanAccessOrgs
                 AS (SELECT DISTINCT 
                            org_hier.OrgID, 
                            org_hier.OrgName, 
                            org_hier.OrgTypeCode, 
                            org_hier.HierarchyOrgID, 
                            org_hier.HierarchyOrgName, 
                            org_hier.HierarchyOrgTypeCode
                     FROM OrgHierarchy org_hier
                          INNER JOIN OrgTypeAccess org_typ_acc ON org_hier.OrgTypeCode = org_typ_acc.OrgTypeCode
                                                                  AND org_hier.HierarchyOrgTypeCode = org_typ_acc.CanAccessOrgTypeCode),
                 -- ================================================================================
                 --  Of those Orgs that can access other Orgs (CTE CanAccessOrgs), build a dataset
                 --      containing those that have 'LevelUp' Access, and those that have 'Normal'
                 --      Access to other Orgs
                 -- ================================================================================
                 -- Level-Up Access
                 LevelUpAccess
                 AS (SELECT DISTINCT 
                            can_acc_orgs.OrgID, 
                            can_acc_orgs.OrgName, 
                            can_acc_orgs.OrgTypeCode, 
                            CanAccessOrgID = org_hier.OrgID, 
                            CanAccessOrgName = org_hier.OrgName, 
                            CanAccessOrgTypeCode = org_hier.OrgTypeCode
                     FROM OrgHierarchy org_hier
                          INNER JOIN CanAccessOrgs can_acc_orgs ON org_hier.HierarchyOrgID = can_acc_orgs.HierarchyOrgID),
                 -- Normal Access
                 NormalAccess
                 AS (SELECT OrgID = HierarchyOrgID, 
                            OrgName = HierarchyOrgName, 
                            OrgTypeCode = HierarchyOrgTypeCode, 
                            CanAccessOrgID = OrgID, 
                            CanAccessOrgName = OrgName, 
                            CanAccessOrgTypeCode = OrgTypeCode
                     FROM OrgHierarchy)
                 -- ================================================================================
                 --  Now that we have datasets for Orgs and the Acces Levels, create a single
                 --      #temp table for all the OrgAccess data we need on the subject.
                 --
                 -- The #OrgAccess temp table shows the Org Hierarchy in terms of what Org level
                 --     has access to what (and what type of access it is).  For the three example
                 --     records shown below...			 
                 --
                 -- OrgID   OrgName              OrgTypeCode   CanAccessOrgID   CanAccessOrgName      CanAccessOrgTypeCode
                 -- -----   ------------------   -----------   --------------   ----------------      --------------------		 
                 -- 36980   340110 West R01D10   DIST          36980            340110 West R01D10    DIST
                 -- 36980   340110 West R01D10   DIST          37715            360100 West R01       REG			 			  
                 -- 36980   340110 West R01D10   DIST          37366            081050 Little Ensos   CENTER 
                 --			 
                 -- ...we see that OrgID 36980 can acceess itself (a DIST CanAccessOrgTypeCode), along
                 --     with accessing 37715 (REG) and 37366 (CENTER).  Whoever has access to OrgID 36980
                 --     will be able to see 37715 and 37366 also.  The linking of a particilar person
                 --     to an Org in this way is done later in this proc when we link #OrgAccess 
                 --     to the dataset returned by vAssignments.			 			  			 		  
                 -- ================================================================================

                 SELECT lvl_up_acc.*, 
                        OrgSelfFlag = CASE
                                          WHEN nrml_acc.OrgID = nrml_acc.CanAccessOrgID
                                          THEN 'Y'
                                          ELSE 'N'
                                      END, 
                        OrgSelfDescendantsFlag = CASE
                                                     WHEN nrml_acc.OrgID IS NOT NULL
                                                     THEN 'Y'
                                                     ELSE 'N'
                                                 END, 
                        OrgLevelUpDescendantsFlag = 'Y'
                 INTO #OrgAccess
                 FROM LevelUpAccess lvl_up_acc
                      LEFT JOIN NormalAccess nrml_acc ON lvl_up_acc.OrgID = nrml_acc.OrgID
                                                         AND lvl_up_acc.CanAccessOrgID = nrml_acc.CanAccessOrgID;

            -- ================================================================================
            --  Get the list of Jobs that pertain to Org Levels (from GL_Staging)
            --      e.g. Center Director is assigned to a Center
            --           District Leader is assigned to a District
            -- ================================================================================

            SELECT OrgDivisionName = LEFT(KLC_VALUE, CHARINDEX('.', KLC_VALUE) - 1), 
                   JobCode = LEFT(RIGHT(KLC_VALUE, LEN(KLC_VALUE) - CHARINDEX('.', KLC_VALUE)), 3), 
                    JobName =  CASE WHEN  CHARINDEX('.',RIGHT(RIGHT(KLC_VALUE, LEN(KLC_VALUE) - CHARINDEX('.', KLC_VALUE)), LEN(RIGHT(KLC_VALUE, LEN(KLC_VALUE) - CHARINDEX('.', KLC_VALUE))) -4))=0
								   THEN  RIGHT(RIGHT(KLC_VALUE, LEN(KLC_VALUE) - CHARINDEX('.', KLC_VALUE)), LEN(RIGHT(KLC_VALUE, LEN(KLC_VALUE) - CHARINDEX('.', KLC_VALUE))) -4)
								   ELSE
							 LEFT(RIGHT(RIGHT(KLC_VALUE, LEN(KLC_VALUE) - CHARINDEX('.', KLC_VALUE)), LEN(RIGHT(KLC_VALUE, LEN(KLC_VALUE) - CHARINDEX('.', KLC_VALUE))) -4),
							 CHARINDEX('.',RIGHT(RIGHT(KLC_VALUE, LEN(KLC_VALUE) - CHARINDEX('.', KLC_VALUE)), LEN(RIGHT(KLC_VALUE, LEN(KLC_VALUE) - CHARINDEX('.', KLC_VALUE))) -4))-1)
						    END,

                   OrgLevelName = LEFT(PARTNER_VALUE, CHARINDEX('.', PARTNER_VALUE) - 1), 
                   OrgLevelSequence = LEFT(RIGHT(PARTNER_VALUE, LEN(PARTNER_VALUE) - CHARINDEX('.', PARTNER_VALUE)), 1), 
                   JobActingFlag = RIGHT(PARTNER_VALUE, LEN(PARTNER_VALUE) - 2 - CHARINDEX('.', PARTNER_VALUE)),
				   OrgTypeCode =CASE WHEN CHARINDEX('.',KLC_VALUE,LEN(KLC_VALUE)-7 ) =0 then NULL
				 ELSE  RIGHT(KLC_VALUE, LEN(KLC_VALUE)-CHARINDEX('.',  KLC_VALUE,LEN(KLC_VALUE)-7 )) END 
            INTO #OrgJobs
            FROM GL_Staging.dbo.xxklcoilxrefvaluesv a
            WHERE partner_name = 'BING'
                  AND type_name = 'ORG_JOB';

            -- --------------------------------------------------------------------------------
            --  Delete specicifc Job Codes (this to be reviewed - SB 21-Nov-17)
            -- --------------------------------------------------------------------------------
            DELETE FROM #OrgJobs
            WHERE JobCode = 173
                  AND OrgLevelName = 'CENTER';
            DELETE FROM #OrgJobs
            WHERE JobCode = 605
                  AND OrgLevelName = 'CENTER';

				  

            -- ================================================================================
            -- Get all the active Assignments of Person to Org, depending on their Job. 
            --
            -- This is used to link to #OrgAccess to build a dataset of Leaders for each Org
            --     at each level.
            --
            -- BNG-1797 -
            -- ----------
            --
            -- Adding additional logic to prevent duplicates creeping into the dataset returned
            --     by this process, as HRIS upstream data is showing multiple people showing up
            --     as Center Directors when they aren't really CDs (they are actually Acting Directors 
            --     that HR had to enter into HRIS as CDs because for certain Centers the AD is a salaried
            --     position, and CD is the only salaried position at the Center level).			 
            --
            -- We de-duplicate by looking at the AssignmentPositionStartDate and breaking any
            --     ties by using the earliest date (HR users will be making the 'real' CD as having
            --     earliest Start Date).
            --			 			 			 
            -- ================================================================================
        
			SELECT * INTO #PEOPLE FROM (

SELECT  v_ppl.PersonFullName,v_ppl.PersonID,v_ppl.EmployeeNumber,v_ppl.PersonEffectiveDate,v_ppl.PersonEndDate 
FROM HR_Staging..vPeople (NOLOCK) v_ppl
WHERE (
          v_ppl.PersonEndDate <> '4712-12-31'
          AND v_ppl.PersonCurrentRecordFlag = 'N'
		  AND    v_ppl.EmployeeCurrentFlag <> 'NO' 
      )
      AND CONVERT(DATE, SYSDATETIME())
      BETWEEN DATEADD(dd, -1, PersonEffectiveDate) AND PersonEndDate
Union
SELECT v_ppl.PersonFullName,v_ppl.PersonID,v_ppl.EmployeeNumber,v_ppl.PersonEffectiveDate,v_ppl.PersonEndDate 
FROM HR_Staging..vPeople (NOLOCK) v_ppl
WHERE v_ppl.PersonCurrentRecordFlag = 'Y'
      AND CONVERT(DATE, SYSDATETIME()) 
      BETWEEN DATEADD(dd, -1, PersonEffectiveDate) AND PersonEndDate
	  AND v_ppl.EmployeeCurrentFlag <> 'NO' ) A;

			
		WITH 	
			SortedAssignments
                 AS (SELECT v_assn.AssignmentID, 
                            v_assn.OrgID, 
                            v_assn.LocationID, 
                            v_assn.PositionID, 
                            v_pos.JobID, 
                            v_pos.JobCode, 
                            v_pos.JobName, 
                            v_orgs.OrgDivisionName, 
                            tmp_orgjobs.OrgLevelName, 
                            tmp_orgjobs.OrgLevelSequence, 
                            tmp_orgjobs.JobActingFlag, 
                            v_assn.PersonID, 
                            v_ppl.EmployeeNumber, 
                            v_ppl.PersonFullName, 
                            v_orgs.OrgName, 
                            v_orgs.OrgTypeCode, 
                            v_assn.AssignmentStatusTypeName, 
                            v_assn.AssignmentStartDate, 
                            v_assn.AssignmentEndDate, 
                            v_assn.AssignmentPositionStartDate, 
                            RANK() OVER(PARTITION BY v_assn.OrgId, 
                                                     tmp_orgjobs.OrgLevelName, 
                                                     v_pos.JobName
                            ORDER BY AssignmentPositionStartDate ASC, -- Use earliest AssignmentPositionStartDate
                                     PersonFullName) AS SortOrder -- BNG-1797- Use to identify 'real' Center Directors in cases where there are multiple
                     FROM vAssignments v_assn
                          INNER JOIN vPositions v_pos ON v_assn.PositionID = v_pos.PositionID
                                                         AND CONVERT(DATE, SYSDATETIME()) BETWEEN v_pos.PositionEffectiveDate AND v_pos.PositionEndDate
                          LEFT JOIN vOrgs v_orgs ON v_assn.OrgID = v_orgs.OrgID
                          INNER JOIN #People v_ppl ON v_assn.PersonID = v_ppl.PersonID
                                                      -- AND v_assn.AssignmentStartDate BETWEEN v_ppl.PersonEffectiveDate AND v_ppl.PersonEndDate
                                                      AND CONVERT(DATE, SYSDATETIME()) BETWEEN DATEADD(dd, -1, v_ppl.PersonEffectiveDate) AND v_ppl.PersonEndDate
                                                      --AND v_ppl.PersonCurrentRecordFlag = 'Y' -- BNG-1042 - Add this filter to prevent not-curent records being included.-- Removed the filter in order to get the records that were missing as this was bing created filter 
                          LEFT JOIN #OrgJobs tmp_orgjobs ON v_orgs.OrgDivisionName = tmp_orgjobs.OrgDivisionName
                                                            AND v_pos.JobCode = tmp_orgjobs.JobCode --and tmp_orgjobs.OrgDivisionName = 'KinderCare Field'
															AND v_orgs.OrgTypeCode = ISNULL(tmp_orgjobs.OrgTypeCode, v_orgs.OrgTypeCode)
                     WHERE((CONVERT(DATE, SYSDATETIME()) BETWEEN AssignmentStartDate AND AssignmentEndDate)
                           OR AssignmentCurrentRecordFlag = 'Y')
                          --And AssignmentEndDate >= DATEADD(yy, 1000, @EDWRunDateTime)
                          AND AssignmentStartDate <= @EDWRunDateTime
                          AND AssignmentStatusTypeID <> 3)
                 --
                 -- Build the Assignments dataset, excluding what we have identified above as not 'real' Center Directors (thus preventing duplictaes)
                 --

                 SELECT AssignmentID, 
                        OrgID, 
                        LocationID, 
                        PositionID, 
                        JobID, 
                        JobCode, 
                        JobName, 
                        OrgDivisionName, 
                        OrgLevelName, 
                        OrgLevelSequence, 
                        JobActingFlag, 
                        PersonID, 
                        EmployeeNumber, 
                        PersonFullName, 
                        OrgName, 
                        OrgTypeCode, 
                        AssignmentStatusTypeName, 
                        AssignmentStartDate, 
                        AssignmentEndDate, 
                        AssignmentPositionStartDate, 
                        SortOrder
                 INTO #Assignments
                 FROM SortedAssignments
                 WHERE SortOrder = 1;

            -- ================================================================================
            --  Get all Assignments, with Acting and Primary flags set
            -- ================================================================================
            WITH SequencedAssignments
                 AS (
                 -- --------------------------------------------------------------------------------
                 -- There are multiple jobs associated with a given level of the hierarchy.  
                 -- Grab the highest sequence we have for that Org.
                 -- --------------------------------------------------------------------------------

                 SELECT OrgID, 
                        JobActingFlag, 
                        MinJobOrgLevelSequence = MIN(OrgLevelSequence)
                 FROM #Assignments
                 GROUP BY OrgID, 
                          JobActingFlag),
                 -- --------------------------------------------------------------------------------
                 -- If there are multiple people associated with that sequence at that Org, get 
                 --     the person who has been there the longest.
                 -- --------------------------------------------------------------------------------
                 LongestSequencedAssignments
                 AS (SELECT tmp_assn.OrgID, 
                            tmp_assn.OrgLevelSequence, 
                            tmp_assn.JobActingFlag, 
                            MinAssignmentPositionEffectiveDate = MIN(AssignmentPositionStartDate)
                     FROM #Assignments tmp_assn
                          INNER JOIN SequencedAssignments seq_assn ON tmp_assn.OrgID = seq_assn.OrgID
                                                                      AND tmp_assn.OrgLevelSequence = seq_assn.MinJobOrgLevelSequence
                                                                      AND tmp_assn.JobActingFlag = seq_assn.JobActingFlag
                     GROUP BY tmp_assn.OrgID, 
                              tmp_assn.OrgLevelSequence, 
                              tmp_assn.JobActingFlag)

                 -- ================================================================================
                 --  Build the #FieldAssignments results set, using the core #Assignments table
                 --      and joining to LongestSequencedAssignments CTE
                 -- ================================================================================

                 SELECT tmp_assn.OrgID, 
                        tmp_assn.OrgName, 
                        tmp_assn.OrgTypeCode, 
                        tmp_assn.OrgLevelName, 
                        tmp_assn.OrgLevelSequence, 
                        JobName, 
                        tmp_assn.JobActingFlag, 
                        JobPrimaryFlag = CASE
                                             WHEN lng_seq_assn.OrgID IS NOT NULL
                                             THEN 'Y'
                                             ELSE 'N'
                                         END, 
                        EmployeeNumber, 
                        PersonFullName, 
                        AssignmentPositionStartDate
                 INTO #FieldAssignments
                 FROM #Assignments tmp_assn
                      LEFT JOIN LongestSequencedAssignments lng_seq_assn ON tmp_assn.OrgID = lng_seq_assn.OrgID
                                                                            AND tmp_assn.OrgLevelSequence = lng_seq_assn.OrgLevelSequence
                                                                            AND tmp_assn.JobActingFlag = lng_seq_assn.JobActingFlag
                                                                            AND tmp_assn.AssignmentPositionStartDate = lng_seq_assn.MinAssignmentPositionEffectiveDate
                 ORDER BY 1, 
                          2, 
                          3;

            -- ================================================================================
            -- Build the final OrgLeaderAccess table
            -- 
            -- Combine the Org Access dataset with the Field Assignments dataset, and load
            --      into OrgLeaderAccess (after truncating - we do a kill/fill each time)
            -- ================================================================================
            --
            -- Soft Delete previous load by setting the Deleted flag to 1 
            -- We clear-down any 'Deleted' records after 7 days
            --
            UPDATE dbo.OrgLeaderAccess
              SET 
                  Deleted = 1
            WHERE Deleted IS NULL;
            SELECT @DeleteCount = @@ROWCOUNT;
            IF @DebugMode = 1
                BEGIN
                    SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Soft deleted ' + CONVERT(NVARCHAR(20), @DeleteCount) + ' from Target.';
                    PRINT @DebugMsg;
            END;

            -- Clean-up old data in the OrgLeaderAccess table
            DELETE FROM dbo.OrgLeaderAccess
            WHERE Deleted = 1
                  AND StgCreatedDate < DATEADD(d, -7, @EDWRunDateTime); -- Delete if older than 7 days
            SET @Rowcount = @@ROWCOUNT;
            IF @DebugMode = 1
                BEGIN
                    SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Cleared-down ' + CONVERT(NVARCHAR(20), @Rowcount) + ' from Target.';
                    PRINT @DebugMsg;
            END;

            -- Do the insert
            INSERT INTO dbo.OrgLeaderAccess
            (EmployeeNumber, 
             EmployeeFullName, 
             JobName, 
             JobActingFlag, 
             JobPrimaryFlag, 
             JobOrgLevelName, 
             JobOrgLevelSequence, 
             AssignmentPositionEffectiveDate, 
             OrgID, 
             OrgName, 
             OrgTypeCode, 
             CanAccessOrgID, 
             CanAccessOrgName, 
             CanAccessOrgTypeCode, 
             OrgSelfFlag, 
             OrgSelfDescendantsFlag, 
             OrgLevelUpDescendantsFlag, 
             Deleted, 
             StgCreatedDate
            )
                   SELECT tmp_fld_assn.EmployeeNumber, 
                          tmp_fld_assn.PersonFullName, 
                          tmp_fld_assn.JobName, 
                          tmp_fld_assn.JobActingFlag, 
                          tmp_fld_assn.JobPrimaryFlag, 
                          tmp_fld_assn.OrgLevelName, 
                          tmp_fld_assn.OrgLevelSequence, 
                          tmp_fld_assn.AssignmentPositionStartDate, 
                          tmp_fld_assn.OrgID, 
                          tmp_fld_assn.OrgName, 
                          tmp_fld_assn.OrgTypeCode, 
                          tmp_org_acc.CanAccessOrgID, 
                          tmp_org_acc.CanAccessOrgName, 
                          tmp_org_acc.CanAccessOrgTypeCode, 
                          tmp_org_acc.OrgSelfFlag, 
                          tmp_org_acc.OrgSelfDescendantsFlag, 
                          tmp_org_acc.OrgLevelUpDescendantsFlag, 
                          NULL, 
                          @EDWRunDateTime
                   FROM #FieldAssignments tmp_fld_assn
                        LEFT JOIN #OrgAccess tmp_org_acc ON tmp_fld_assn.OrgID = tmp_org_acc.OrgID
                   WHERE tmp_fld_assn.EmployeeNumber IS NOT NULL
                   -- The DimOrganization and DimCostCenter loads that utilize the data in OrgLeaderAccess
                   --     only require data where these 2 flags are Y, so we filter here to keep the 
                   --     dataset smaller and more manageable.
                   -- AND tmp_org_acc.OrgSelfDescendantsFlag = 'Y'
                   -- AND tmp_fld_assn.JobPrimaryFlag = 'Y'
                   -- BNG-1042 - Change the join to #OrgAccess to LEFT and remove the restriction on 
                   --     OrgSelfDescendantsFlag and JobPrimaryFlag, as they don't really add anything
                   ORDER BY tmp_fld_assn.EmployeeNumber, 
                            tmp_org_acc.OrgSelfDescendantsFlag DESC, 
                            tmp_org_acc.OrgSelfFlag DESC, 
                            tmp_org_acc.CanAccessOrgTypeCode, 
                            tmp_org_acc.CanAccessOrgName;
            -- Debug output progress

            SELECT @SourceCount = @@ROWCOUNT;
            SELECT @InsertCount = @SourceCount;
            IF @DebugMode = 1
                BEGIN
                    SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Inserted ' + CONVERT(NVARCHAR(20), @InsertCount) + ' into Target.';
                    PRINT @DebugMsg;
            END;

            --
            -- Write our successful run to the EDW AuditLog 
            --
            EXEC [BING_EDW].[dbo].[spEDWEndAuditLog] 
                 @InsertCount = @InsertCount, 
                 @UpdateCount = @UpdateCount, 
                 @DeleteCount = @DeleteCount, 
                 @SourceCount = @SourceCount, 
                 @AuditId = @AuditId;

            -- Debug output progress
            IF @DebugMode = 1
                SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Completing successfully.';
            PRINT @DebugMsg;
        END TRY
        --
        -- Catch, and throw the error back to the calling procedure or client
        --
        BEGIN CATCH
            --
            -- Write our failed run to the EDW AuditLog 
            --
            EXEC [BING_EDW].[dbo].[spEDWErrorAuditLog] 
                 @AuditId = @AuditId;
            DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
            SELECT @ErrMsg = 'Sub-procedure ' + @ProcName + ' - ' + ERROR_MESSAGE(), 
                   @ErrSeverity = ERROR_SEVERITY();
            RAISERROR(@ErrMsg, @ErrSeverity, 1);
        END CATCH;
    END;