/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spGL_StagingToEDW_DimCostCenter'
)
    DROP PROCEDURE dbo.spGL_StagingToEDW_DimCostCenter;
GO
*/
CREATE PROCEDURE dbo.spGL_StagingToEDW_DimCostCenter
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingToEDW_DimCostCenter
    --
    -- Purpose:            Performs the Insert / Update (Type 2 SCD) ETL process for
    --                         the DimCostCenter table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spGL_StagingTransform_DimCostCenter, 
    --                                 and create any helper indexes
    --                         Step 3: Perform the Insert / Update (SCD2) required for this EDW
    --                                 table load
    --                             (a) Perform a Merge that inserts new rows, and updates any existing 
    --                                 current rows to be a previous version
    --                             (b) For any updated records from step 3(a), we insert those rows to 
    --                                 create a new, additional current record, in-line with a 
    --                                 Type 2 Slowly Changing Dimension				 
    --                         Step 4: Execute any automated tests associated with this EDW table load
    --                         Step 5: Output Source / Insert / Update / Delete counts to caller, 
    --                                 commit the transaction, and tidy-up
    --
    -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                         making numerous GETDATE() calls  
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --				   
    -- Returns:            Single-row results set containing the following columns:
    --                         SourceCount - Number of rows extracted from source
    --                         InsertCount - Number or rows inserted to target table
    --                         UpdateCount - Number or rows updated in target table
    --                         DeleteCount - Number or rows deleted in target table
    --
    -- Usage:              EXEC dbo.spGL_StagingToEDW_DimCostCenter @EDWRunDateTime = GETDATE(), @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 10/20/17    sburke              Initial version of proc
    -- 11/29/17    sburke              BNG-835 - Center Master Refactoring - add CCHierarchyLevel11Name to
    --                                      DimCostCenter, and change CostCenter Hierarchy logic to use 
    --                                      similar approach to DimOrganization load
    --                                     (use HR_Staging..vOrgs rather than the defunct
    --                                      vOrgsWithLevels view)
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimCostCenter';
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
	    -- Merge statement action table variable - for SCD2 we add the unique key columns inaddition to the action
	    --
         DECLARE @tblMrgeActions_SCD2 TABLE
         ([MergeAction]      [VARCHAR](250) NOT NULL,
	    -- Column(s) that make up the unique business key for the table we are loading
          [CostCenterNumber] [INT] NOT NULL,
          [EDWEffectiveDate] [DATETIME2](7) NOT NULL
         );

	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
	    --
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting.';
         PRINT @DebugMsg;

	    --
	    -- Write to EDW AuditLog we are starting
	    --
         EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
              @SourceName = @SourceName,
              @AuditId = @AuditId OUTPUT; 

	    -- --------------------------------------------------------------------------------
	    -- Extract from Source, Upserts and Deletes contained in a single transaction.  
	    --	 Rollback on error
	    -- --------------------------------------------------------------------------------
         BEGIN TRY
             BEGIN TRANSACTION;
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Beginning transaction.';
             PRINT @DebugMsg;
		   -- ================================================================================
		   --
		   -- S T E P   1.
		   --
		   -- Create temporary landing #table
		   --
		   -- ================================================================================
             CREATE TABLE #DimCostCenterUpsert
             ([CostCenterNumber]                             VARCHAR(6) NOT NULL,
              [CostCenterName]                               VARCHAR(250) NOT NULL,
              [CompanyID]                                    VARCHAR(3) NOT NULL,
              [CostCenterTypeID]                             VARCHAR(4) NOT NULL,
              [CCHierarchyLevel1Name]                        VARCHAR(250) NULL,
              [CCHierarchyLevel2Name]                        VARCHAR(250) NULL,
              [CCHierarchyLevel3Name]                        VARCHAR(250) NULL,
              [CCHierarchyLevel4Name]                        VARCHAR(250) NULL,
              [CCHierarchyLevel5Name]                        VARCHAR(250) NULL,
              [CCHierarchyLevel6Name]                        VARCHAR(250) NULL,
              [CCHierarchyLevel7Name]                        VARCHAR(250) NULL,
              [CCHierarchyLevel8Name]                        VARCHAR(250) NULL,
              [CCHierarchyLevel9Name]                        VARCHAR(250) NULL,
              [CCHierarchyLevel10Name]                       VARCHAR(250) NULL,
              [CCHierarchyLevel11Name]                       VARCHAR(250) NULL,
              [CCOpenDate]                                   DATE NOT NULL,
              [CCClosedDate]                                 DATE NULL,
              [CCReopenDate]                                 DATE NULL,
              [CCReopenDateType]                             VARCHAR(250) NULL,
              [CCClassification]                             VARCHAR(250) NOT NULL,
              [CCStatus]                                     VARCHAR(250) NOT NULL,
              [CCConsolidation]                              VARCHAR(250) NOT NULL,
              [CCFlexAttribute1]                             VARCHAR(250) NULL,
              [CCFlexAttribute2]                             VARCHAR(250) NULL,
              [CCFlexAttribute3]                             VARCHAR(250) NULL,
              [CCFlexAttribute4]                             VARCHAR(250) NULL,
              [CCFlexAttribute5]                             VARCHAR(250) NULL,
              [CenterCMSID]                                  INT NULL,
              [CenterCSSID]                                  VARCHAR(4) NULL,
              [SiteHorizonID]                                INT NULL,
              [CenterEnrollmentSourceSystem]                 VARCHAR(3) NULL,
              [CenterCMSMigrationDate]                       DATE NULL,
              [CenterCMSMigrationStatus]                     VARCHAR(2) NULL,
              [CenterLicensedCapacity]                       INT NULL,
              [CenterBackupCareFlag]                         VARCHAR(250) NULL,
              [CenterChildCareSelectFlag]                    VARCHAR(250) NULL,
              [CenterPublicAllowedFlag]                      VARCHAR(250) NULL,
              [CenterOpenTime]                               VARCHAR(8) NULL,
              [CenterCloseTime]                              VARCHAR(8) NULL,
              [CenterStudentMinimumAge]                      VARCHAR(250) NULL,
              [CenterStudentMaximumAge]                      VARCHAR(250) NULL,
              [CenterOpenSunFlag]                            VARCHAR(50) NULL,
              [CenterOpenMonFlag]                            VARCHAR(50) NULL,
              [CenterOpenTueFlag]                            VARCHAR(50) NULL,
              [CenterOpenWedFlag]                            VARCHAR(50) NULL,
              [CenterOpenThuFlag]                            VARCHAR(50) NULL,
              [CenterOpenFriFlag]                            VARCHAR(50) NULL,
              [CenterOpenSatFlag]                            VARCHAR(50) NULL,
              [CenterFoodProgramStartDate]                   DATE NULL,
              [CenterFoodProgramEndDate]                     DATE NULL,
              [CenterRegistrationType]                       VARCHAR(100) NULL,
              [SiteSchoolDistrict]                           VARCHAR(100) NULL,
              [SiteClassYear]                                INT NULL,
              [CenterMenuURL]                                VARCHAR(500) NULL,
              [CenterHasBreakfastFlag]                       VARCHAR(50) NULL,
              [CenterHasMorningSlackFlag]                    VARCHAR(50) NULL,
              [CenterHasLunchFlag]                           VARCHAR(50) NULL,
              [CenterHasAfternoonSnackFlag]                  VARCHAR(50) NULL,
              [CenterSpeaksASLFlag]                          VARCHAR(50) NULL,
              [CenterSpeaksArabicFlag]                       VARCHAR(50) NULL,
              [CenterSpeaksFrenchFlag]                       VARCHAR(50) NULL,
              [CenterSpeaksGermanFlag]                       VARCHAR(50) NULL,
              [CenterSpeaksHindiFlag]                        VARCHAR(50) NULL,
              [CenterSpeaksMandarinFlag]                     VARCHAR(50) NULL,
              [CenterSpeaksPunjabiFlag]                      VARCHAR(50) NULL,
              [CenterSpeaksSpanishFlag]                      VARCHAR(50) NULL,
              [CenterSpeaksOtherLanguages]                   VARCHAR(500) NULL,
              [CenterAccreditationAgencyCode]                VARCHAR(100) NULL,
              [CenterAccreditationStartDate]                 DATE NULL,
              [CenterAccreditationExpirationDate]            DATE NULL,
              [CenterAccreditationNextActivity]              VARCHAR(50) NULL,
              [CenterAccreditationNextActivityDueDate]       DATE NULL,
              [CenterAccreditationPrimaryStatus]             VARCHAR(50) NULL,
              [CenterAccreditationProgramID]                 VARCHAR(100) NULL,
              [CenterQRISRating]                             VARCHAR(50) NULL,
              [CenterQRISRatingStartDate]                    DATE NULL,
              [CenterQRISRatingExpirationDate]               DATE NULL,
              [CenterMaintenanceSupervisorName]              VARCHAR(250) NULL,
              [CenterPreventativeTechnicianName]             VARCHAR(250) NULL,
              [CenterRegionalFacilitiesCoordinatorName]      VARCHAR(250) NULL,
              [CenterRegionalFacilitiesManagerName]          VARCHAR(250) NULL,
              [CenterNutritionAndWellnessAdministratorName]  VARCHAR(250) NULL,
              [CenterNutritionAndWellnessAdministratorEmail] VARCHAR(250) NULL,
              [CenterNutritionAndWellnessAdministratorPhone] VARCHAR(250) NULL,
              [CenterSubsidyCoordinatorName]                 VARCHAR(250) NULL,
              [CenterSubsidyCoordinatorEmail]                VARCHAR(250) NULL,
              [CenterSubsidyCoordinatorPhone]                VARCHAR(250) NULL,
              [CenterSubsidyManagerName]                     VARCHAR(250) NULL,
              [CenterSubsidyManagerEmail]                    VARCHAR(250) NULL,
              [CenterSubsidyManagerPhone]                    VARCHAR(250) NULL,
              [CenterSubsidySupervisorName]                  VARCHAR(250) NULL,
              [CenterSubsidySupervisorEmail]                 VARCHAR(250) NULL,
              [CenterSubsidySupervisorPhone]                 VARCHAR(250) NULL,
              [CenterBuildingSquareFootage]                  INT NULL,
              [CenterLandSquareFootage]                      INT NULL,
              [CenterCoreBasedStatisticalAreaName]           VARCHAR(250) NULL,
              [CenterLandlordName]                           VARCHAR(250) NULL,
              [CenterLeaseControlEndMonthDate]               DATE NULL,
              [CenterLeaseExpirationDate]                    DATE NULL,
              [CenterLeaseExtensionOptionNoticeDate]         DATE NULL,
              [CenterLeaseExtensionOptionsRemainingCount]    INT NULL,
              [CenterLeaseExtensionOptionRemainingYears]     INT NULL,
              [CenterLeaseStatus]                            VARCHAR(250) NULL,
              [CenterLatitude]                               NUMERIC(9, 6) NULL,
              [CenterLongitude]                              NUMERIC(9, 6) NULL,
              [CenterCurrentHumanSigmaScore]                 INT NULL,
              [CenterPreviousHumanSigmaScore]                INT NULL,
              [EDWEffectiveDate]                             DATETIME2 NOT NULL,
              [EDWEndDate]                                   DATETIME2 NULL,
              [EDWCreatedDate]                               DATETIME2 NOT NULL,
              [EDWCreatedBy]                                 VARCHAR(50) NOT NULL,
              [Deleted]                                      DATETIME2 NULL
             );
          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimCostCenterUpsert
             EXEC dbo.spGL_StagingTransform_DimCostCenter
                  @EDWRunDateTime;

		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimCostCenterUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimCostCenterUpsert ON #DimCostCenterUpsert
             ([CostCenterNumber] ASC, [EDWEffectiveDate] ASC
             );



		   -- ================================================================================	
		   --
		   -- S T E P   3.
		   --
		   -- Perform the Inserts for new records, and SCD Type 2 for updated records.
		   --
		   -- The first MERGE statement performs the inserts for any new rows, and the first
		   -- part of the SCD2 update process for changed existing records, but setting the
		   -- EDWEndDate to the current run-date (an EDWEndDate of NULL means it is the current
		   -- record.
		   --
		   -- After the initial merge has completed, we collect the details of the updates from 
		   -- $action and use that to execute a second insert into the target table, this time 
		   -- creating a new record for each updated record, with an EDW EffectiveDate of the
		   -- current run date, and an EDWEndDate of NLL (current record).
		   --
		   -- ================================================================================
		   
		   --
		   -- Perform the Merge statement for insert / updates
		   --
             MERGE [BING_EDW].[dbo].[DimCostCenter] T
             USING #DimCostCenterUpsert S
             ON(S.CostCenterNumber = T.CostCenterNumber)
                 WHEN MATCHED AND S.Deleted IS NULL
                                  AND T.EDWEndDate IS NULL -- The 'current' record in target
                                  AND (S.CostCenterName <> T.CostCenterName
                                       OR S.CompanyID <> T.CompanyID
                                       OR S.CostCenterTypeID <> T.CostCenterTypeID
                                       OR S.CCHierarchyLevel1Name <> T.CCHierarchyLevel1Name
                                       OR S.CCHierarchyLevel2Name <> T.CCHierarchyLevel2Name
                                       OR S.CCHierarchyLevel3Name <> T.CCHierarchyLevel3Name
                                       OR S.CCHierarchyLevel4Name <> T.CCHierarchyLevel4Name
                                       OR S.CCHierarchyLevel5Name <> T.CCHierarchyLevel5Name
                                       OR S.CCHierarchyLevel6Name <> T.CCHierarchyLevel6Name
                                       OR S.CCHierarchyLevel7Name <> T.CCHierarchyLevel7Name
                                       OR S.CCHierarchyLevel8Name <> T.CCHierarchyLevel8Name
                                       OR S.CCHierarchyLevel9Name <> T.CCHierarchyLevel9Name
                                       OR S.CCHierarchyLevel10Name <> T.CCHierarchyLevel10Name
                                       OR S.CCHierarchyLevel11Name <> T.CCHierarchyLevel11Name
                                       OR S.CCOpenDate <> T.CCOpenDate
                                       OR S.CCClosedDate <> T.CCClosedDate
                                       OR S.CCReopenDate <> T.CCReopenDate
                                       OR S.CCReopenDateType <> T.CCReopenDateType
                                       OR S.CCClassification <> T.CCClassification
                                       OR S.CCStatus <> T.CCStatus
                                       OR S.CCConsolidation <> T.CCConsolidation
                                       OR S.CCFlexAttribute1 <> T.CCFlexAttribute1
                                       OR S.CCFlexAttribute2 <> T.CCFlexAttribute2
                                       OR S.CCFlexAttribute3 <> T.CCFlexAttribute3
                                       OR S.CCFlexAttribute4 <> T.CCFlexAttribute4
                                       OR S.CCFlexAttribute5 <> T.CCFlexAttribute5
                                       OR S.CenterCMSID <> T.CenterCMSID
                                       OR S.CenterCSSID <> T.CenterCSSID
                                       OR S.SiteHorizonID <> T.SiteHorizonID
                                       OR S.CenterEnrollmentSourceSystem <> T.CenterEnrollmentSourceSystem
                                       OR S.CenterCMSMigrationDate <> T.CenterCMSMigrationDate
                                       OR S.CenterCMSMigrationStatus <> T.CenterCMSMigrationStatus
                                       OR S.CenterLicensedCapacity <> T.CenterLicensedCapacity
                                       OR S.CenterBackupCareFlag <> T.CenterBackupCareFlag
                                       OR S.CenterChildCareSelectFlag <> T.CenterChildCareSelectFlag
                                       OR S.CenterPublicAllowedFlag <> T.CenterPublicAllowedFlag
                                       OR S.CenterOpenTime <> T.CenterOpenTime
                                       OR S.CenterCloseTime <> T.CenterCloseTime
                                       OR S.CenterStudentMinimumAge <> T.CenterStudentMinimumAge
                                       OR S.CenterStudentMaximumAge <> T.CenterStudentMaximumAge
                                       OR S.CenterOpenSunFlag <> T.CenterOpenSunFlag
                                       OR S.CenterOpenMonFlag <> T.CenterOpenMonFlag
                                       OR S.CenterOpenTueFlag <> T.CenterOpenTueFlag
                                       OR S.CenterOpenWedFlag <> T.CenterOpenWedFlag
                                       OR S.CenterOpenThuFlag <> T.CenterOpenThuFlag
                                       OR S.CenterOpenFriFlag <> T.CenterOpenFriFlag
                                       OR S.CenterOpenSatFlag <> T.CenterOpenSatFlag
                                       OR S.CenterFoodProgramStartDate <> T.CenterFoodProgramStartDate
                                       OR S.CenterFoodProgramEndDate <> T.CenterFoodProgramEndDate
                                       OR S.CenterRegistrationType <> T.CenterRegistrationType
                                       OR S.SiteSchoolDistrict <> T.SiteSchoolDistrict
                                       OR S.SiteClassYear <> T.SiteClassYear
                                       OR S.CenterMenuURL <> T.CenterMenuURL
                                       OR S.CenterHasBreakfastFlag <> T.CenterHasBreakfastFlag
                                       OR S.CenterHasMorningSlackFlag <> T.CenterHasMorningSlackFlag
                                       OR S.CenterHasLunchFlag <> T.CenterHasLunchFlag
                                       OR S.CenterHasAfternoonSnackFlag <> T.CenterHasAfternoonSnackFlag
                                       OR S.CenterSpeaksASLFlag <> T.CenterSpeaksASLFlag
                                       OR S.CenterSpeaksArabicFlag <> T.CenterSpeaksArabicFlag
                                       OR S.CenterSpeaksFrenchFlag <> T.CenterSpeaksFrenchFlag
                                       OR S.CenterSpeaksGermanFlag <> T.CenterSpeaksGermanFlag
                                       OR S.CenterSpeaksHindiFlag <> T.CenterSpeaksHindiFlag
                                       OR S.CenterSpeaksMandarinFlag <> T.CenterSpeaksMandarinFlag
                                       OR S.CenterSpeaksPunjabiFlag <> T.CenterSpeaksPunjabiFlag
                                       OR S.CenterSpeaksSpanishFlag <> T.CenterSpeaksSpanishFlag
                                       OR S.CenterSpeaksOtherLanguages <> T.CenterSpeaksOtherLanguages
                                       OR S.CenterAccreditationAgencyCode <> T.CenterAccreditationAgencyCode
                                       OR S.CenterAccreditationStartDate <> T.CenterAccreditationStartDate
                                       OR S.CenterAccreditationExpirationDate <> T.CenterAccreditationExpirationDate
                                       OR S.CenterAccreditationNextActivity <> T.CenterAccreditationNextActivity
                                       OR S.CenterAccreditationNextActivityDueDate <> T.CenterAccreditationNextActivityDueDate
                                       OR S.CenterAccreditationPrimaryStatus <> T.CenterAccreditationPrimaryStatus
                                       OR S.CenterAccreditationProgramID <> T.CenterAccreditationProgramID
                                       OR S.CenterQRISRating <> T.CenterQRISRating
                                       OR S.CenterQRISRatingStartDate <> T.CenterQRISRatingStartDate
                                       OR S.CenterQRISRatingExpirationDate <> T.CenterQRISRatingExpirationDate
                                       OR S.CenterMaintenanceSupervisorName <> T.CenterMaintenanceSupervisorName
                                       OR S.CenterPreventativeTechnicianName <> T.CenterPreventativeTechnicianName
                                       OR S.CenterRegionalFacilitiesCoordinatorName <> T.CenterRegionalFacilitiesCoordinatorName
                                       OR S.CenterRegionalFacilitiesManagerName <> T.CenterRegionalFacilitiesManagerName
                                       OR S.CenterNutritionAndWellnessAdministratorName <> T.CenterNutritionAndWellnessAdministratorName
                                       OR S.CenterNutritionAndWellnessAdministratorEmail <> T.CenterNutritionAndWellnessAdministratorEmail
                                       OR S.CenterNutritionAndWellnessAdministratorPhone <> T.CenterNutritionAndWellnessAdministratorPhone
                                       OR S.CenterSubsidyCoordinatorName <> T.CenterSubsidyCoordinatorName
                                       OR S.CenterSubsidyCoordinatorEmail <> T.CenterSubsidyCoordinatorEmail
                                       OR S.CenterSubsidyCoordinatorPhone <> T.CenterSubsidyCoordinatorPhone
                                       OR S.CenterSubsidyManagerName <> T.CenterSubsidyManagerName
                                       OR S.CenterSubsidyManagerEmail <> T.CenterSubsidyManagerEmail
                                       OR S.CenterSubsidyManagerPhone <> T.CenterSubsidyManagerPhone
                                       OR S.CenterSubsidySupervisorName <> T.CenterSubsidySupervisorName
                                       OR S.CenterSubsidySupervisorEmail <> T.CenterSubsidySupervisorEmail
                                       OR S.CenterSubsidySupervisorPhone <> T.CenterSubsidySupervisorPhone
                                       OR S.CenterBuildingSquareFootage <> T.CenterBuildingSquareFootage
                                       OR S.CenterLandSquareFootage <> T.CenterLandSquareFootage
                                       OR S.CenterCoreBasedStatisticalAreaName <> T.CenterCoreBasedStatisticalAreaName
                                       OR S.CenterLandlordName <> T.CenterLandlordName
                                       OR S.CenterLeaseControlEndMonthDate <> T.CenterLeaseControlEndMonthDate
                                       OR S.CenterLeaseExpirationDate <> T.CenterLeaseExpirationDate
                                       OR S.CenterLeaseExtensionOptionNoticeDate <> T.CenterLeaseExtensionOptionNoticeDate
                                       OR S.CenterLeaseExtensionOptionsRemainingCount <> T.CenterLeaseExtensionOptionsRemainingCount
                                       OR S.CenterLeaseExtensionOptionRemainingYears <> T.CenterLeaseExtensionOptionRemainingYears
                                       OR S.CenterLeaseStatus <> T.CenterLeaseStatus
                                       OR S.CenterLatitude <> T.CenterLatitude
                                       OR S.CenterLongitude <> T.CenterLongitude
                                       OR S.CenterCurrentHumanSigmaScore <> T.CenterCurrentHumanSigmaScore
                                       OR S.CenterPreviousHumanSigmaScore <> T.CenterPreviousHumanSigmaScore
                                       OR T.Deleted IS NOT NULL)
                 THEN UPDATE SET
                                 T.EDWEndDate = S.EDWEffectiveDate -- Updates the EDWEndDate from NULL (current) to the current date	
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(EDWEndDate,
                          CostCenterNumber,
                          CostCenterName,
                          CompanyID,
                          CostCenterTypeID,
                          CCHierarchyLevel1Name,
                          CCHierarchyLevel2Name,
                          CCHierarchyLevel3Name,
                          CCHierarchyLevel4Name,
                          CCHierarchyLevel5Name,
                          CCHierarchyLevel6Name,
                          CCHierarchyLevel7Name,
                          CCHierarchyLevel8Name,
                          CCHierarchyLevel9Name,
                          CCHierarchyLevel10Name,
                          CCHierarchyLevel11Name,
                          CCOpenDate,
                          CCClosedDate,
                          CCReopenDate,
                          CCReopenDateType,
                          CCClassification,
                          CCStatus,
                          CCConsolidation,
                          CCFlexAttribute1,
                          CCFlexAttribute2,
                          CCFlexAttribute3,
                          CCFlexAttribute4,
                          CCFlexAttribute5,
                          CenterCMSID,
                          CenterCSSID,
                          SiteHorizonID,
                          CenterEnrollmentSourceSystem,
                          CenterCMSMigrationDate,
                          CenterCMSMigrationStatus,
                          CenterLicensedCapacity,
                          CenterBackupCareFlag,
                          CenterChildCareSelectFlag,
                          CenterPublicAllowedFlag,
                          CenterOpenTime,
                          CenterCloseTime,
                          CenterStudentMinimumAge,
                          CenterStudentMaximumAge,
                          CenterOpenSunFlag,
                          CenterOpenMonFlag,
                          CenterOpenTueFlag,
                          CenterOpenWedFlag,
                          CenterOpenThuFlag,
                          CenterOpenFriFlag,
                          CenterOpenSatFlag,
                          CenterFoodProgramStartDate,
                          CenterFoodProgramEndDate,
                          CenterRegistrationType,
                          SiteSchoolDistrict,
                          SiteClassYear,
                          CenterMenuURL,
                          CenterHasBreakfastFlag,
                          CenterHasMorningSlackFlag,
                          CenterHasLunchFlag,
                          CenterHasAfternoonSnackFlag,
                          CenterSpeaksASLFlag,
                          CenterSpeaksArabicFlag,
                          CenterSpeaksFrenchFlag,
                          CenterSpeaksGermanFlag,
                          CenterSpeaksHindiFlag,
                          CenterSpeaksMandarinFlag,
                          CenterSpeaksPunjabiFlag,
                          CenterSpeaksSpanishFlag,
                          CenterSpeaksOtherLanguages,
                          CenterAccreditationAgencyCode,
                          CenterAccreditationStartDate,
                          CenterAccreditationExpirationDate,
                          CenterAccreditationNextActivity,
                          CenterAccreditationNextActivityDueDate,
                          CenterAccreditationPrimaryStatus,
                          CenterAccreditationProgramID,
                          CenterQRISRating,
                          CenterQRISRatingStartDate,
                          CenterQRISRatingExpirationDate,
                          CenterMaintenanceSupervisorName,
                          CenterPreventativeTechnicianName,
                          CenterRegionalFacilitiesCoordinatorName,
                          CenterRegionalFacilitiesManagerName,
                          CenterNutritionAndWellnessAdministratorName,
                          CenterNutritionAndWellnessAdministratorEmail,
                          CenterNutritionAndWellnessAdministratorPhone,
                          CenterSubsidyCoordinatorName,
                          CenterSubsidyCoordinatorEmail,
                          CenterSubsidyCoordinatorPhone,
                          CenterSubsidyManagerName,
                          CenterSubsidyManagerEmail,
                          CenterSubsidyManagerPhone,
                          CenterSubsidySupervisorName,
                          CenterSubsidySupervisorEmail,
                          CenterSubsidySupervisorPhone,
                          CenterBuildingSquareFootage,
                          CenterLandSquareFootage,
                          CenterCoreBasedStatisticalAreaName,
                          CenterLandlordName,
                          CenterLeaseControlEndMonthDate,
                          CenterLeaseExpirationDate,
                          CenterLeaseExtensionOptionNoticeDate,
                          CenterLeaseExtensionOptionsRemainingCount,
                          CenterLeaseExtensionOptionRemainingYears,
                          CenterLeaseStatus,
                          CenterLatitude,
                          CenterLongitude,
                          CenterCurrentHumanSigmaScore,
                          CenterPreviousHumanSigmaScore,
                          EDWEffectiveDate,
                          EDWCreatedDate,
                          EDWCreatedBy,
                          Deleted)
                   VALUES
             (NULL, -- Updates EDWEndDate so it is the current record
              CostCenterNumber,
              CostCenterName,
              CompanyID,
              CostCenterTypeID,
              CCHierarchyLevel1Name,
              CCHierarchyLevel2Name,
              CCHierarchyLevel3Name,
              CCHierarchyLevel4Name,
              CCHierarchyLevel5Name,
              CCHierarchyLevel6Name,
              CCHierarchyLevel7Name,
              CCHierarchyLevel8Name,
              CCHierarchyLevel9Name,
              CCHierarchyLevel10Name,
              CCHierarchyLevel11Name,
              CCOpenDate,
              CCClosedDate,
              CCReopenDate,
              CCReopenDateType,
              CCClassification,
              CCStatus,
              CCConsolidation,
              CCFlexAttribute1,
              CCFlexAttribute2,
              CCFlexAttribute3,
              CCFlexAttribute4,
              CCFlexAttribute5,
              CenterCMSID,
              CenterCSSID,
              SiteHorizonID,
              CenterEnrollmentSourceSystem,
              CenterCMSMigrationDate,
              CenterCMSMigrationStatus,
              CenterLicensedCapacity,
              CenterBackupCareFlag,
              CenterChildCareSelectFlag,
              CenterPublicAllowedFlag,
              CenterOpenTime,
              CenterCloseTime,
              CenterStudentMinimumAge,
              CenterStudentMaximumAge,
              CenterOpenSunFlag,
              CenterOpenMonFlag,
              CenterOpenTueFlag,
              CenterOpenWedFlag,
              CenterOpenThuFlag,
              CenterOpenFriFlag,
              CenterOpenSatFlag,
              CenterFoodProgramStartDate,
              CenterFoodProgramEndDate,
              CenterRegistrationType,
              SiteSchoolDistrict,
              SiteClassYear,
              CenterMenuURL,
              CenterHasBreakfastFlag,
              CenterHasMorningSlackFlag,
              CenterHasLunchFlag,
              CenterHasAfternoonSnackFlag,
              CenterSpeaksASLFlag,
              CenterSpeaksArabicFlag,
              CenterSpeaksFrenchFlag,
              CenterSpeaksGermanFlag,
              CenterSpeaksHindiFlag,
              CenterSpeaksMandarinFlag,
              CenterSpeaksPunjabiFlag,
              CenterSpeaksSpanishFlag,
              CenterSpeaksOtherLanguages,
              CenterAccreditationAgencyCode,
              CenterAccreditationStartDate,
              CenterAccreditationExpirationDate,
              CenterAccreditationNextActivity,
              CenterAccreditationNextActivityDueDate,
              CenterAccreditationPrimaryStatus,
              CenterAccreditationProgramID,
              CenterQRISRating,
              CenterQRISRatingStartDate,
              CenterQRISRatingExpirationDate,
              CenterMaintenanceSupervisorName,
              CenterPreventativeTechnicianName,
              CenterRegionalFacilitiesCoordinatorName,
              CenterRegionalFacilitiesManagerName,
              CenterNutritionAndWellnessAdministratorName,
              CenterNutritionAndWellnessAdministratorEmail,
              CenterNutritionAndWellnessAdministratorPhone,
              CenterSubsidyCoordinatorName,
              CenterSubsidyCoordinatorEmail,
              CenterSubsidyCoordinatorPhone,
              CenterSubsidyManagerName,
              CenterSubsidyManagerEmail,
              CenterSubsidyManagerPhone,
              CenterSubsidySupervisorName,
              CenterSubsidySupervisorEmail,
              CenterSubsidySupervisorPhone,
              CenterBuildingSquareFootage,
              CenterLandSquareFootage,
              CenterCoreBasedStatisticalAreaName,
              CenterLandlordName,
              CenterLeaseControlEndMonthDate,
              CenterLeaseExpirationDate,
              CenterLeaseExtensionOptionNoticeDate,
              CenterLeaseExtensionOptionsRemainingCount,
              CenterLeaseExtensionOptionRemainingYears,
              CenterLeaseStatus,
              CenterLatitude,
              CenterLongitude,
              CenterCurrentHumanSigmaScore,
              CenterPreviousHumanSigmaScore,
              EDWEffectiveDate,
              EDWCreatedDate,
              EDWCreatedBy,
              Deleted
             )
             -- We need to get the details of the records we updated, so we can insert a further row for them as the current row.
             OUTPUT $action,
                    S.CostCenterNumber,
                    S.EDWEffectiveDate
                    INTO @tblMrgeActions_SCD2;
	         --

             SELECT @InsertCount = SUM(Inserted),
                    @UpdateCount = SUM(Updated)
             FROM
             ( 
		         -- Count the number of inserts 

                 SELECT COUNT(*) AS Inserted,
                        0 AS Updated
                 FROM @tblMrgeActions_SCD2
                 WHERE MergeAction = 'INSERT'
                 UNION ALL 
			     -- Count the number of updates

                 SELECT 0 AS Inserted,
                        COUNT(*) AS Updated
                 FROM @tblMrgeActions_SCD2
                 WHERE MergeAction = 'UPDATE'
             ) merge_actions;
             --
		   
		   
		     -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' rows into Target.';
                     PRINT @DebugMsg;
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Updated [Closed-out previous version] '+CONVERT(NVARCHAR(20), @UpdateCount)+' rows into Target.';
                     PRINT @DebugMsg;
             END;             
		   
		   --
		   -- Perform the Insert for new updated records for Type 2 SCD
		   --
             INSERT INTO BING_EDW.dbo.DimCostCenter
             (EDWEndDate,
              CostCenterNumber,
              CostCenterName,
              CompanyID,
              CostCenterTypeID,
              CCHierarchyLevel1Name,
              CCHierarchyLevel2Name,
              CCHierarchyLevel3Name,
              CCHierarchyLevel4Name,
              CCHierarchyLevel5Name,
              CCHierarchyLevel6Name,
              CCHierarchyLevel7Name,
              CCHierarchyLevel8Name,
              CCHierarchyLevel9Name,
              CCHierarchyLevel10Name,
              CCHierarchyLevel11Name,
              CCOpenDate,
              CCClosedDate,
              CCReopenDate,
              CCReopenDateType,
              CCClassification,
              CCStatus,
              CCConsolidation,
              CCFlexAttribute1,
              CCFlexAttribute2,
              CCFlexAttribute3,
              CCFlexAttribute4,
              CCFlexAttribute5,
              CenterCMSID,
              CenterCSSID,
              SiteHorizonID,
              CenterEnrollmentSourceSystem,
              CenterCMSMigrationDate,
              CenterCMSMigrationStatus,
              CenterLicensedCapacity,
              CenterBackupCareFlag,
              CenterChildCareSelectFlag,
              CenterPublicAllowedFlag,
              CenterOpenTime,
              CenterCloseTime,
              CenterStudentMinimumAge,
              CenterStudentMaximumAge,
              CenterOpenSunFlag,
              CenterOpenMonFlag,
              CenterOpenTueFlag,
              CenterOpenWedFlag,
              CenterOpenThuFlag,
              CenterOpenFriFlag,
              CenterOpenSatFlag,
              CenterFoodProgramStartDate,
              CenterFoodProgramEndDate,
              CenterRegistrationType,
              SiteSchoolDistrict,
              SiteClassYear,
              CenterMenuURL,
              CenterHasBreakfastFlag,
              CenterHasMorningSlackFlag,
              CenterHasLunchFlag,
              CenterHasAfternoonSnackFlag,
              CenterSpeaksASLFlag,
              CenterSpeaksArabicFlag,
              CenterSpeaksFrenchFlag,
              CenterSpeaksGermanFlag,
              CenterSpeaksHindiFlag,
              CenterSpeaksMandarinFlag,
              CenterSpeaksPunjabiFlag,
              CenterSpeaksSpanishFlag,
              CenterSpeaksOtherLanguages,
              CenterAccreditationAgencyCode,
              CenterAccreditationStartDate,
              CenterAccreditationExpirationDate,
              CenterAccreditationNextActivity,
              CenterAccreditationNextActivityDueDate,
              CenterAccreditationPrimaryStatus,
              CenterAccreditationProgramID,
              CenterQRISRating,
              CenterQRISRatingStartDate,
              CenterQRISRatingExpirationDate,
              CenterMaintenanceSupervisorName,
              CenterPreventativeTechnicianName,
              CenterRegionalFacilitiesCoordinatorName,
              CenterRegionalFacilitiesManagerName,
              CenterNutritionAndWellnessAdministratorName,
              CenterNutritionAndWellnessAdministratorEmail,
              CenterNutritionAndWellnessAdministratorPhone,
              CenterSubsidyCoordinatorName,
              CenterSubsidyCoordinatorEmail,
              CenterSubsidyCoordinatorPhone,
              CenterSubsidyManagerName,
              CenterSubsidyManagerEmail,
              CenterSubsidyManagerPhone,
              CenterSubsidySupervisorName,
              CenterSubsidySupervisorEmail,
              CenterSubsidySupervisorPhone,
              CenterBuildingSquareFootage,
              CenterLandSquareFootage,
              CenterCoreBasedStatisticalAreaName,
              CenterLandlordName,
              CenterLeaseControlEndMonthDate,
              CenterLeaseExpirationDate,
              CenterLeaseExtensionOptionNoticeDate,
              CenterLeaseExtensionOptionsRemainingCount,
              CenterLeaseExtensionOptionRemainingYears,
              CenterLeaseStatus,
              CenterLatitude,
              CenterLongitude,
              CenterCurrentHumanSigmaScore,
              CenterPreviousHumanSigmaScore,
              EDWEffectiveDate,
              EDWCreatedDate,
              EDWCreatedBy,
              Deleted
             )
                    SELECT NULL, -- [EDWEndDate]
                           S.CostCenterNumber,
                           S.CostCenterName,
                           S.CompanyID,
                           S.CostCenterTypeID,
                           S.CCHierarchyLevel1Name,
                           S.CCHierarchyLevel2Name,
                           S.CCHierarchyLevel3Name,
                           S.CCHierarchyLevel4Name,
                           S.CCHierarchyLevel5Name,
                           S.CCHierarchyLevel6Name,
                           S.CCHierarchyLevel7Name,
                           S.CCHierarchyLevel8Name,
                           S.CCHierarchyLevel9Name,
                           S.CCHierarchyLevel10Name,
                           S.CCHierarchyLevel11Name,
                           S.CCOpenDate,
                           S.CCClosedDate,
                           S.CCReopenDate,
                           S.CCReopenDateType,
                           S.CCClassification,
                           S.CCStatus,
                           S.CCConsolidation,
                           S.CCFlexAttribute1,
                           S.CCFlexAttribute2,
                           S.CCFlexAttribute3,
                           S.CCFlexAttribute4,
                           S.CCFlexAttribute5,
                           S.CenterCMSID,
                           S.CenterCSSID,
                           S.SiteHorizonID,
                           S.CenterEnrollmentSourceSystem,
                           S.CenterCMSMigrationDate,
                           S.CenterCMSMigrationStatus,
                           S.CenterLicensedCapacity,
                           S.CenterBackupCareFlag,
                           S.CenterChildCareSelectFlag,
                           S.CenterPublicAllowedFlag,
                           S.CenterOpenTime,
                           S.CenterCloseTime,
                           S.CenterStudentMinimumAge,
                           S.CenterStudentMaximumAge,
                           S.CenterOpenSunFlag,
                           S.CenterOpenMonFlag,
                           S.CenterOpenTueFlag,
                           S.CenterOpenWedFlag,
                           S.CenterOpenThuFlag,
                           S.CenterOpenFriFlag,
                           S.CenterOpenSatFlag,
                           S.CenterFoodProgramStartDate,
                           S.CenterFoodProgramEndDate,
                           S.CenterRegistrationType,
                           S.SiteSchoolDistrict,
                           S.SiteClassYear,
                           S.CenterMenuURL,
                           S.CenterHasBreakfastFlag,
                           S.CenterHasMorningSlackFlag,
                           S.CenterHasLunchFlag,
                           S.CenterHasAfternoonSnackFlag,
                           S.CenterSpeaksASLFlag,
                           S.CenterSpeaksArabicFlag,
                           S.CenterSpeaksFrenchFlag,
                           S.CenterSpeaksGermanFlag,
                           S.CenterSpeaksHindiFlag,
                           S.CenterSpeaksMandarinFlag,
                           S.CenterSpeaksPunjabiFlag,
                           S.CenterSpeaksSpanishFlag,
                           S.CenterSpeaksOtherLanguages,
                           S.CenterAccreditationAgencyCode,
                           S.CenterAccreditationStartDate,
                           S.CenterAccreditationExpirationDate,
                           S.CenterAccreditationNextActivity,
                           S.CenterAccreditationNextActivityDueDate,
                           S.CenterAccreditationPrimaryStatus,
                           S.CenterAccreditationProgramID,
                           S.CenterQRISRating,
                           S.CenterQRISRatingStartDate,
                           S.CenterQRISRatingExpirationDate,
                           S.CenterMaintenanceSupervisorName,
                           S.CenterPreventativeTechnicianName,
                           S.CenterRegionalFacilitiesCoordinatorName,
                           S.CenterRegionalFacilitiesManagerName,
                           S.CenterNutritionAndWellnessAdministratorName,
                           S.CenterNutritionAndWellnessAdministratorEmail,
                           S.CenterNutritionAndWellnessAdministratorPhone,
                           S.CenterSubsidyCoordinatorName,
                           S.CenterSubsidyCoordinatorEmail,
                           S.CenterSubsidyCoordinatorPhone,
                           S.CenterSubsidyManagerName,
                           S.CenterSubsidyManagerEmail,
                           S.CenterSubsidyManagerPhone,
                           S.CenterSubsidySupervisorName,
                           S.CenterSubsidySupervisorEmail,
                           S.CenterSubsidySupervisorPhone,
                           S.CenterBuildingSquareFootage,
                           S.CenterLandSquareFootage,
                           S.CenterCoreBasedStatisticalAreaName,
                           S.CenterLandlordName,
                           S.CenterLeaseControlEndMonthDate,
                           S.CenterLeaseExpirationDate,
                           S.CenterLeaseExtensionOptionNoticeDate,
                           S.CenterLeaseExtensionOptionsRemainingCount,
                           S.CenterLeaseExtensionOptionRemainingYears,
                           S.CenterLeaseStatus,
                           S.CenterLatitude,
                           S.CenterLongitude,
                           S.CenterCurrentHumanSigmaScore,
                           S.CenterPreviousHumanSigmaScore,
                           S.EDWEffectiveDate,
                           S.EDWCreatedDate,
                           S.EDWCreatedBy,
                           S.Deleted
                    FROM #DimCostCenterUpsert S
                         INNER JOIN @tblMrgeActions_SCD2 scd2 ON S.CostCenterNumber = scd2.CostCenterNumber
                                                                 AND s.EDWEffectiveDate = scd2.EDWEffectiveDate
                    WHERE scd2.MergeAction = 'UPDATE';
             SELECT @UpdateCount = @@ROWCOUNT;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Updated [Inserted new current SCD2 row] '+CONVERT(NVARCHAR(20), @UpdateCount)+' from into Target.';
                     PRINT @DebugMsg;
             END;

		   -- ================================================================================
		   --
		   -- S T E P   4.
		   --
		   -- Execute any automated tests associated with this EDW table load
		   --
		   -- ================================================================================


		   -- ================================================================================
		   --
		   -- S T E P   5.
		   --
		   -- Output Source / Insert / Update / Delete counts to caller, commit the transaction,
		   --	and tidy tup.
		   --
		   -- ================================================================================		   
		  
		  -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Committing transaction.';
             PRINT @DebugMsg;
		   
		   --
		   -- Commit the successful transaction 
		   --
             COMMIT TRANSACTION;

		   --
		   -- Drop the temp table
		   --
             DROP TABLE #DimCostCenterUpsert;

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
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Completing successfully.';
             PRINT @DebugMsg;
         END TRY
         BEGIN CATCH
	    	  -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Rolling back transaction.';
             PRINT @DebugMsg;
		   -- Rollback the transaction
             ROLLBACK TRANSACTION;
		   --
		   -- Write our failed run to the EDW AuditLog 
		   --
             EXEC [BING_EDW].[dbo].[spEDWErrorAuditLog]
                  @AuditId = @AuditId;
		   --
		   -- Raise error
		   --	
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
GO