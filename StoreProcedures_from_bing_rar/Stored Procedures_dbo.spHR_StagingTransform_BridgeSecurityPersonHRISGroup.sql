CREATE PROCEDURE [dbo].[spHR_StagingTransform_BridgeSecurityPersonHRISGroup] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_BridgeSecurityPersonHRISGroup
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
    --                     INSERT #BridgeSecurityPersonHRISGroupUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_BridgeSecurityPersonHRISGroup @EDWRunDateTime
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
    -- 1/23/2017  Banandesi              BNG-565 INitial version of proc
    -- 4/09/2018  sburke                 BNG-1593 - Remove reference to the vHRUserSecurity
    --                                       view for performance reasons.  Bring the logic
    --                                       from that view directly into the proc here, and
    --                                       break out into manageable chunks that the optimizer
    --                                       can better create an execution plan for.                 
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
             -- ================================================================================
             -- Originally the view HR_Staging..vHRUserSecurity was used for this transform,
             --     but it was a mess of nested CTEs and views that the optimizer was unable to
             --     produce a decent query plan for (it was taking 35 mins+ to return 102 rows).
             --
             -- The logic from that view is split up into its contituent parts here, using a set
             --     of #temp tables so the optimizer can produce better query plans.
             --
             -- The vHRUserSecurity view is only used for this transform, so we no longer need
             --     that view (it will be removed from the DB project, as of BNG-1593)
             -- ================================================================================
             --
		   -- HRISUsersByOrg
		   --

             SELECT OrgNumber = KLC_VALUE,
                    HRISSecurityGroupNumber = PARTNER_VALUE
             INTO #HRISUsersByOrg
             FROM GL_Staging.dbo.xxklcOilXrefValuesV
             WHERE PARTNER_NAME = 'BING'
                   AND TYPE_NAME = 'HRIS_Users_by_Org';

             --
		   -- HRISUsersByOrg
		   --

             SELECT AssignmentNumber = KLC_VALUE,
                    HRISSecurityGroupNumber = PARTNER_VALUE
             INTO #HRISUsersByAssignment
             FROM GL_Staging.dbo.xxklcOilXrefValuesV
             WHERE PARTNER_NAME = 'BING'
                   AND TYPE_NAME = 'HRIS_Users_by_Assignment';
             --
		   -- ActiveAssignments - this is the query that takes the bulk of the processing time
		   --

             SELECT a.PersonID,
                    EmployeeNumber,
                    ContingentWorkerNumber,
                    ContingentWorkerCurrentFlag,
                    PersonFullName,
                    a.AssignmentID,
                    AssignmentNumber,
                    a.OrgID,
                    OrgNumber
             INTO #HRISActiveAssignments
             FROM dbo.vAssignments a
                  INNER JOIN dbo.vOrgs b ON a.OrgID = b.OrgID
                  INNER JOIN dbo.vPeople c ON a.PersonID = c.PersonID
                                          AND a.AssignmentStartDate BETWEEN c.PersonEffectiveDate AND c.PersonEndDate
             WHERE AssignmentEndDate >= DATEADD(yy, 1000, GETDATE())
                   AND AssignmentStartDate <= GETDATE()
                   AND AssignmentStatusTypeID <> 3; --Not Terminated

             --
		   -- PersonAllAccessLevels
		   --

             SELECT HRISSecurityType = 'Org',
                    HRISSecurityGroupNumber,
                    a.PersonID
             INTO #PersonAllAccessLevels
             FROM #HRISActiveAssignments a
                  INNER JOIN #HRISUsersByOrg b ON a.OrgNumber = b.OrgNumber
             WHERE a.EmployeeNumber IS NOT NULL -- No Contigent Workers allowed to have access by Org (access by Assignment is OK)
             UNION
             SELECT HRISSecurityType = 'Assignment',
                    HRISSecurityGroupNumber,
                    a.PersonID
             FROM #HRISActiveAssignments a
                  INNER JOIN #HRISUsersByAssignment b ON a.AssignmentNumber = b.AssignmentNumber;
             --
		   -- PersonHighestAccessLevel
		   --
             --     Because each lower level is a subset of the higher levels (like a Russian doll), 
             --     we can limit the dataset to return the highest level of access they have.
             --

             SELECT PersonID,
                    HRISSecurityGroupNumber = MIN(HRISSecurityGroupNumber)
             INTO #PersonHighestAccessLevel
             FROM #PersonAllAccessLevels
             GROUP BY PersonID;

             --
		   -- HRUserSecurity
		   --

             SELECT a.HRISSecurityType,
                    a.HRISSecurityGroupNumber,
                    a.PersonID
             INTO #HRUserSecurity
             FROM #PersonAllAccessLevels a
                  INNER JOIN #PersonHighestAccessLevel b ON a.PersonID = b.PersonID
                                                            AND a.HRISSecurityGroupNumber = b.HRISSecurityGroupNumber;
             --
		   -- Final result set that gets retruned to the calling process
		   --

             SELECT vHRUS.PersonID AS PersonKey,
                    vHRUs.HRISSecurityGroupNumber AS HRISGroupNumber,
                    GETDATE() AS EDWCreatedDate
             FROM #HRUserSecurity vHRUS
             WHERE PersonID IN
             (
                 SELECT PersonID
                 FROM BING_EDW.dbo.DimPerson
             );
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