
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingToEDW_DimOrganization'
)
    DROP PROCEDURE dbo.spHR_StagingToEDW_DimOrganization;
GO
*/
CREATE PROCEDURE [dbo].[spHR_StagingToEDW_DimOrganization]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimOrganization
    --
    -- Purpose:            Performs the Insert / Update (Type 2 SCD) ETL process for
    --                         the DimOrganization table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimOrganization, 
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimOrganization @EDWRunDateTime = GETDATE(), @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 10/11/17    sburke              Initial version of proc
	-- 02/08/17    hhebbalu            Added Step 3 block
	--								   Soft delete the records in DimOrganization which 
	--                                 are deleted in source by updating EDWEndDate
	--								   and Deleted columns
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimOrganization';
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
	    -- Column(s) that make up the unique business key for tha table
          [OrgID]            [INT] NOT NULL,
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
             CREATE TABLE #DimOrganizationUpsert
             ([OrgID]                             [INT] NOT NULL,
              [OrgEffectiveDate]                  [DATE] NOT NULL,
              [OrgEndDate]                        [DATE] NOT NULL,
              [ParentOrgID]                       [INT] NULL,
              [DefaultLocationID]                 [INT] NOT NULL,
              [CostCenterNumber]                  [VARCHAR](6) NOT NULL,
              [OrgNumber]                         [VARCHAR](6) NOT NULL,
              [OrgName]                           [VARCHAR](250) NOT NULL,
              [OrgHierarchyLevel1Name]            [VARCHAR](250) NULL,
              [OrgHierarchyLevel2Name]            [VARCHAR](250) NULL,
              [OrgHierarchyLevel3Name]            [VARCHAR](250) NULL,
              [OrgHierarchyLevel4Name]            [VARCHAR](250) NULL,
              [OrgHierarchyLevel5Name]            [VARCHAR](250) NULL,
              [OrgHierarchyLevel6Name]            [VARCHAR](250) NULL,
              [OrgHierarchyLevel7Name]            [VARCHAR](250) NULL,
              [OrgHierarchyLevel8Name]            [VARCHAR](250) NULL,
              [OrgHierarchyLevel9Name]            [VARCHAR](250) NULL,
              [OrgHierarchyLevel10Name]           [VARCHAR](250) NULL,
              [OrgHierarchyLevel11Name]           [VARCHAR](250) NULL,
              [OrgAllName]                        [VARCHAR](250) NOT NULL,
              [OrgExecutiveFunctionName]          [VARCHAR](250) NOT NULL,
              [OrgExecutiveFunctionLeaderName]    [VARCHAR](250) NOT NULL,
              [OrgExecutiveSubFunctionName]       [VARCHAR](250) NOT NULL,
              [OrgExecutiveSubFunctionLeaderName] [VARCHAR](250) NOT NULL,
              [OrgCorporateFunctionName]          [VARCHAR](250) NOT NULL,
              [OrgCorporateSubFunctionName]       [VARCHAR](250) NOT NULL,
              [OrgDivisionName]                   [VARCHAR](250) NOT NULL,
              [OrgDivisionLeaderName]             [VARCHAR](250) NOT NULL,
              [OrgRegionNumber]                   [VARCHAR](10) NOT NULL,
              [OrgRegionName]                     [VARCHAR](250) NOT NULL,
              [OrgRegionLeaderName]               [VARCHAR](250) NOT NULL,
              [OrgMarketNumber]                   [VARCHAR](10) NOT NULL,
              [OrgMarketName]                     [VARCHAR](250) NOT NULL,
              [OrgMarketLeaderName]               [VARCHAR](250) NOT NULL,
              [OrgSubMarketNumber]                [VARCHAR](10) NOT NULL,
              [OrgSubMarketName]                  [VARCHAR](250) NOT NULL,
              [OrgSubMarketLeaderName]            [VARCHAR](250) NOT NULL,
              [OrgDistrictNumber]                 [VARCHAR](10) NOT NULL,
              [OrgDistrictName]                   [VARCHAR](250) NOT NULL,
              [OrgInterimDistrictNumber]          [VARCHAR](10) NOT NULL,
              [OrgInterimDistrictName]            [VARCHAR](250) NOT NULL,
              [OrgDistrictLeaderName]             [VARCHAR](250) NOT NULL,
              [OrgActingDistrictLeaderName]       [VARCHAR](250) NOT NULL,
              [OrgInterimDistrictLeaderName]      [VARCHAR](250) NOT NULL,
              [OrgGroupNumber]                    [VARCHAR](10) NOT NULL,
              [OrgGroupName]                      [VARCHAR](250) NOT NULL,
              [OrgGroupLeaderName]                [VARCHAR](250) NOT NULL,
              [OrgSubgroupNumber]                 [VARCHAR](10) NOT NULL,
              [OrgSubGroupName]                   [VARCHAR](250) NOT NULL,
              [OrgSubGroupLeaderName]             [VARCHAR](250) NOT NULL,
              [OrgCampusNumber]                   [VARCHAR](10) NOT NULL,
              [OrgCampusName]                     [VARCHAR](250) NOT NULL,
              [OrgCampusLeaderName]               [VARCHAR](250) NOT NULL,
              [OrgCenterLeaderName]               [VARCHAR](250) NOT NULL,
              [OrgActingCenterLeaderName]         [VARCHAR](250) NOT NULL,
              [OrgCategoryName]                   [VARCHAR](250) NOT NULL,
              [OrgTypeCode]                       [VARCHAR](250) NOT NULL,
              [OrgTypeName]                       [VARCHAR](250) NOT NULL,
              [OrgPartnerGroupCode]               [VARCHAR](250) NOT NULL,
              [OrgPartnerGroupName]               [VARCHAR](250) NOT NULL,
              [OrgCenterGroupCode]                [VARCHAR](250) NOT NULL,
              [OrgCenterGroupName]                [VARCHAR](250) NOT NULL,
              [OrgDivisionLegacyName]             [VARCHAR](250) NOT NULL,
              [OrgLineOfBusinessCode]             [VARCHAR](250) NOT NULL,
              [OrgBrandCode]                      [VARCHAR](250) NOT NULL,
              [OrgBrandName]                      [VARCHAR](250) NOT NULL,
              [OrgFlexAttribute1]                 [VARCHAR](250) NULL,
              [OrgFlexAttribute2]                 [VARCHAR](250) NULL,
              [OrgFlexAttribute3]                 [VARCHAR](250) NULL,
              [OrgFlexAttribute4]                 [VARCHAR](250) NULL,
              [OrgFlexAttribute5]                 [VARCHAR](250) NULL,
              [OrgCreatedUser]                    [INT] NOT NULL,
              [OrgCreatedDate]                    [DATETIME2](7) NOT NULL,
              [OrgModifiedUser]                   [INT] NOT NULL,
              [OrgModifiedDate]                   [DATETIME2](7) NOT NULL,
              [EDWEffectiveDate]                  [DATETIME2](7) NOT NULL,
              [EDWEndDate]                        [DATETIME2](7) NULL,
              [EDWCreatedDate]                    [DATETIME2](7) NOT NULL,
              [EDWCreatedBy]                      [VARCHAR](50) NOT NULL,
              [Deleted]                           [DATETIME2](7) NULL
             );
          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimOrganizationUpsert
             EXEC dbo.spHR_StagingTransform_DimOrganization
                  @EDWRunDateTime;

		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimOrganizationUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimOrganizationUpsert ON #DimOrganizationUpsert
             ([OrgID] ASC, [EDWEffectiveDate] ASC
             );

		   -- ================================================================================	
		   --
		   -- S T E P   3.
		   --
		   -- Soft delete the records in DimOrganization which are deleted in source
		   --
		   -- ================================================================================

		   UPDATE tgt
				SET Deleted = GETDATE()
				   ,EDWEndDate = GETDATE()
				FROM BING_EDW.dbo.DimOrganization AS tgt 
					LEFT JOIN #DimOrganizationUpsert AS src ON src.OrgID = tgt.OrgID 
				WHERE tgt.EDWEndDate IS NULL 
				AND tgt.OrgKey > 0
				AND src.OrgID IS NULL

		   -- ================================================================================	
		   --
		   -- S T E P   4.
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
             MERGE INTO BING_EDW.dbo.DimOrganization T
             USING #DimOrganizationUpsert S
             ON(S.OrgID = T.OrgID)
                 WHEN MATCHED AND S.Deleted IS NULL
						    -- We also check on the OrgID being -1 (meaning unknown).  If it is there already there is no need to update
                                  AND S.OrgID <> -1
                                  AND T.EDWEndDate IS NULL -- The 'current' record in target
                                  AND (S.OrgEndDate <> T.OrgEndDate
                                       OR S.ParentOrgID <> T.ParentOrgID
                                       OR S.DefaultLocationID <> T.DefaultLocationID
                                       OR S.CostCenterNumber <> T.CostCenterNumber
                                       OR S.OrgNumber <> T.OrgNumber
                                       OR S.OrgName <> T.OrgName
                                       OR S.OrgHierarchyLevel1Name <> T.OrgHierarchyLevel1Name
                                       OR S.OrgHierarchyLevel2Name <> T.OrgHierarchyLevel2Name
                                       OR S.OrgHierarchyLevel3Name <> T.OrgHierarchyLevel3Name
                                       OR S.OrgHierarchyLevel4Name <> T.OrgHierarchyLevel4Name
                                       OR S.OrgHierarchyLevel5Name <> T.OrgHierarchyLevel5Name
                                       OR S.OrgHierarchyLevel6Name <> T.OrgHierarchyLevel6Name
                                       OR S.OrgHierarchyLevel7Name <> T.OrgHierarchyLevel7Name
                                       OR S.OrgHierarchyLevel8Name <> T.OrgHierarchyLevel8Name
                                       OR S.OrgHierarchyLevel9Name <> T.OrgHierarchyLevel9Name
                                       OR S.OrgHierarchyLevel10Name <> T.OrgHierarchyLevel10Name
                                       OR S.OrgHierarchyLevel11Name <> T.OrgHierarchyLevel11Name
                                       OR S.OrgAllName <> T.OrgAllName
                                       OR S.OrgExecutiveFunctionName <> T.OrgExecutiveFunctionName
                                       OR S.OrgExecutiveFunctionLeaderName <> T.OrgExecutiveFunctionLeaderName
                                       OR S.OrgExecutiveSubFunctionName <> T.OrgExecutiveSubFunctionName
                                       OR S.OrgExecutiveSubFunctionLeaderName <> T.OrgExecutiveSubFunctionLeaderName
                                       OR S.OrgCorporateFunctionName <> T.OrgCorporateFunctionName
                                       OR S.OrgCorporateSubFunctionName <> T.OrgCorporateSubFunctionName
                                       OR S.OrgDivisionName <> T.OrgDivisionName
                                       OR S.OrgDivisionLeaderName <> T.OrgDivisionLeaderName
                                       OR S.OrgRegionNumber <> T.OrgRegionNumber
                                       OR S.OrgRegionName <> T.OrgRegionName
                                       OR S.OrgRegionLeaderName <> T.OrgRegionLeaderName
                                       OR S.OrgMarketNumber <> T.OrgMarketNumber
                                       OR S.OrgMarketName <> T.OrgMarketName
                                       OR S.OrgMarketLeaderName <> T.OrgMarketLeaderName
                                       OR S.OrgSubMarketNumber <> T.OrgSubMarketNumber
                                       OR S.OrgSubMarketName <> T.OrgSubMarketName
                                       OR S.OrgSubMarketLeaderName <> T.OrgSubMarketLeaderName
                                       OR S.OrgDistrictNumber <> T.OrgDistrictNumber
                                       OR S.OrgDistrictName <> T.OrgDistrictName
                                       OR S.OrgInterimDistrictNumber <> T.OrgInterimDistrictNumber
                                       OR S.OrgInterimDistrictName <> T.OrgInterimDistrictName
                                       OR S.OrgDistrictLeaderName <> T.OrgDistrictLeaderName
                                       OR S.OrgActingDistrictLeaderName <> T.OrgActingDistrictLeaderName
                                       OR S.OrgInterimDistrictLeaderName <> T.OrgInterimDistrictLeaderName
                                       OR S.OrgGroupNumber <> T.OrgGroupNumber
                                       OR S.OrgGroupName <> T.OrgGroupName
                                       OR S.OrgGroupLeaderName <> T.OrgGroupLeaderName
                                       OR S.OrgSubgroupNumber <> T.OrgSubgroupNumber
                                       OR S.OrgSubGroupName <> T.OrgSubGroupName
                                       OR S.OrgSubGroupLeaderName <> T.OrgSubGroupLeaderName
                                       OR S.OrgCampusNumber <> T.OrgCampusNumber
                                       OR S.OrgCampusName <> T.OrgCampusName
                                       OR S.OrgCampusLeaderName <> T.OrgCampusLeaderName
                                       OR S.OrgCenterLeaderName <> T.OrgCenterLeaderName
                                       OR S.OrgActingCenterLeaderName <> T.OrgActingCenterLeaderName
                                       OR S.OrgCategoryName <> T.OrgCategoryName
                                       OR S.OrgTypeCode <> T.OrgTypeCode
                                       OR S.OrgTypeName <> T.OrgTypeName
                                       OR S.OrgPartnerGroupCode <> T.OrgPartnerGroupCode
                                       OR S.OrgPartnerGroupName <> T.OrgPartnerGroupName
                                       OR S.OrgCenterGroupCode <> T.OrgCenterGroupCode
                                       OR S.OrgCenterGroupName <> T.OrgCenterGroupName
                                       OR S.OrgDivisionLegacyName <> T.OrgDivisionLegacyName
                                       OR S.OrgLineOfBusinessCode <> T.OrgLineOfBusinessCode
                                       OR S.OrgBrandCode <> T.OrgBrandCode
                                       OR S.OrgBrandName <> T.OrgBrandName
                                       OR S.OrgFlexAttribute1 <> T.OrgFlexAttribute1
                                       OR S.OrgFlexAttribute2 <> T.OrgFlexAttribute2
                                       OR S.OrgFlexAttribute3 <> T.OrgFlexAttribute3
                                       OR S.OrgFlexAttribute4 <> T.OrgFlexAttribute4
                                       OR S.OrgFlexAttribute5 <> T.OrgFlexAttribute5
                                       OR S.OrgCreatedUser <> T.OrgCreatedUser
                                       OR S.OrgCreatedDate <> T.OrgCreatedDate
                                       OR S.OrgModifiedUser <> T.OrgModifiedUser
                                       OR S.OrgModifiedDate <> T.OrgModifiedDate
                                       OR T.Deleted IS NOT NULL)
                 THEN UPDATE SET
                                 T.EDWEndDate = S.EDWEffectiveDate -- Updates the EDWEndDate from NULL (current) to the current date						                    
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(EDWEndDate,
                          OrgID,
                          OrgEffectiveDate,
                          OrgEndDate,
                          ParentOrgID,
                          DefaultLocationID,
                          CostCenterNumber,
                          OrgNumber,
                          OrgName,
                          OrgHierarchyLevel1Name,
                          OrgHierarchyLevel2Name,
                          OrgHierarchyLevel3Name,
                          OrgHierarchyLevel4Name,
                          OrgHierarchyLevel5Name,
                          OrgHierarchyLevel6Name,
                          OrgHierarchyLevel7Name,
                          OrgHierarchyLevel8Name,
                          OrgHierarchyLevel9Name,
                          OrgHierarchyLevel10Name,
                          OrgHierarchyLevel11Name,
                          OrgAllName,
                          OrgExecutiveFunctionName,
                          OrgExecutiveFunctionLeaderName,
                          OrgExecutiveSubFunctionName,
                          OrgExecutiveSubFunctionLeaderName,
                          OrgCorporateFunctionName,
                          OrgCorporateSubFunctionName,
                          OrgDivisionName,
                          OrgDivisionLeaderName,
                          OrgRegionNumber,
                          OrgRegionName,
                          OrgRegionLeaderName,
                          OrgMarketNumber,
                          OrgMarketName,
                          OrgMarketLeaderName,
                          OrgSubMarketNumber,
                          OrgSubMarketName,
                          OrgSubMarketLeaderName,
                          OrgDistrictNumber,
                          OrgDistrictName,
                          OrgInterimDistrictNumber,
                          OrgInterimDistrictName,
                          OrgDistrictLeaderName,
                          OrgActingDistrictLeaderName,
                          OrgInterimDistrictLeaderName,
                          OrgGroupNumber,
                          OrgGroupName,
                          OrgGroupLeaderName,
                          OrgSubgroupNumber,
                          OrgSubGroupName,
                          OrgSubGroupLeaderName,
                          OrgCampusNumber,
                          OrgCampusName,
                          OrgCampusLeaderName,
                          OrgCenterLeaderName,
                          OrgActingCenterLeaderName,
                          OrgCategoryName,
                          OrgTypeCode,
                          OrgTypeName,
                          OrgPartnerGroupCode,
                          OrgPartnerGroupName,
                          OrgCenterGroupCode,
                          OrgCenterGroupName,
                          OrgDivisionLegacyName,
                          OrgLineOfBusinessCode,
                          OrgBrandCode,
                          OrgBrandName,
                          OrgFlexAttribute1,
                          OrgFlexAttribute2,
                          OrgFlexAttribute3,
                          OrgFlexAttribute4,
                          OrgFlexAttribute5,
                          OrgCreatedUser,
                          OrgCreatedDate,
                          OrgModifiedUser,
                          OrgModifiedDate,
                          EDWEffectiveDate,
                          EDWCreatedDate,
                          EDWCreatedBy,
                          Deleted)
                   VALUES
             (NULL, -- Updates EDWEndDate so it is the current record
              OrgID,
              OrgEffectiveDate,
              OrgEndDate,
              ParentOrgID,
              DefaultLocationID,
              CostCenterNumber,
              OrgNumber,
              OrgName,
              OrgHierarchyLevel1Name,
              OrgHierarchyLevel2Name,
              OrgHierarchyLevel3Name,
              OrgHierarchyLevel4Name,
              OrgHierarchyLevel5Name,
              OrgHierarchyLevel6Name,
              OrgHierarchyLevel7Name,
              OrgHierarchyLevel8Name,
              OrgHierarchyLevel9Name,
              OrgHierarchyLevel10Name,
              OrgHierarchyLevel11Name,
              OrgAllName,
              OrgExecutiveFunctionName,
              OrgExecutiveFunctionLeaderName,
              OrgExecutiveSubFunctionName,
              OrgExecutiveSubFunctionLeaderName,
              OrgCorporateFunctionName,
              OrgCorporateSubFunctionName,
              OrgDivisionName,
              OrgDivisionLeaderName,
              OrgRegionNumber,
              OrgRegionName,
              OrgRegionLeaderName,
              OrgMarketNumber,
              OrgMarketName,
              OrgMarketLeaderName,
              OrgSubMarketNumber,
              OrgSubMarketName,
              OrgSubMarketLeaderName,
              OrgDistrictNumber,
              OrgDistrictName,
              OrgInterimDistrictNumber,
              OrgInterimDistrictName,
              OrgDistrictLeaderName,
              OrgActingDistrictLeaderName,
              OrgInterimDistrictLeaderName,
              OrgGroupNumber,
              OrgGroupName,
              OrgGroupLeaderName,
              OrgSubgroupNumber,
              OrgSubGroupName,
              OrgSubGroupLeaderName,
              OrgCampusNumber,
              OrgCampusName,
              OrgCampusLeaderName,
              OrgCenterLeaderName,
              OrgActingCenterLeaderName,
              OrgCategoryName,
              OrgTypeCode,
              OrgTypeName,
              OrgPartnerGroupCode,
              OrgPartnerGroupName,
              OrgCenterGroupCode,
              OrgCenterGroupName,
              OrgDivisionLegacyName,
              OrgLineOfBusinessCode,
              OrgBrandCode,
              OrgBrandName,
              OrgFlexAttribute1,
              OrgFlexAttribute2,
              OrgFlexAttribute3,
              OrgFlexAttribute4,
              OrgFlexAttribute5,
              OrgCreatedUser,
              OrgCreatedDate,
              OrgModifiedUser,
              OrgModifiedDate,
              EDWEffectiveDate,
              EDWCreatedDate,
              EDWCreatedBy,
              Deleted
             )
		   -- We need to get the details of the records we updated, so we can insert a further row for them as the current row.
             OUTPUT $action,
                    S.OrgID,
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
             INSERT INTO BING_EDW.dbo.DimOrganization
             (EDWEndDate,
              OrgID,
              OrgEffectiveDate,
              OrgEndDate,
              ParentOrgID,
              DefaultLocationID,
              CostCenterNumber,
              OrgNumber,
              OrgName,
              OrgHierarchyLevel1Name,
              OrgHierarchyLevel2Name,
              OrgHierarchyLevel3Name,
              OrgHierarchyLevel4Name,
              OrgHierarchyLevel5Name,
              OrgHierarchyLevel6Name,
              OrgHierarchyLevel7Name,
              OrgHierarchyLevel8Name,
              OrgHierarchyLevel9Name,
              OrgHierarchyLevel10Name,
              OrgHierarchyLevel11Name,
              OrgAllName,
              OrgExecutiveFunctionName,
              OrgExecutiveFunctionLeaderName,
              OrgExecutiveSubFunctionName,
              OrgExecutiveSubFunctionLeaderName,
              OrgCorporateFunctionName,
              OrgCorporateSubFunctionName,
              OrgDivisionName,
              OrgDivisionLeaderName,
              OrgRegionNumber,
              OrgRegionName,
              OrgRegionLeaderName,
              OrgMarketNumber,
              OrgMarketName,
              OrgMarketLeaderName,
              OrgSubMarketNumber,
              OrgSubMarketName,
              OrgSubMarketLeaderName,
              OrgDistrictNumber,
              OrgDistrictName,
              OrgInterimDistrictNumber,
              OrgInterimDistrictName,
              OrgDistrictLeaderName,
              OrgActingDistrictLeaderName,
              OrgInterimDistrictLeaderName,
              OrgGroupNumber,
              OrgGroupName,
              OrgGroupLeaderName,
              OrgSubgroupNumber,
              OrgSubGroupName,
              OrgSubGroupLeaderName,
              OrgCampusNumber,
              OrgCampusName,
              OrgCampusLeaderName,
              OrgCenterLeaderName,
              OrgActingCenterLeaderName,
              OrgCategoryName,
              OrgTypeCode,
              OrgTypeName,
              OrgPartnerGroupCode,
              OrgPartnerGroupName,
              OrgCenterGroupCode,
              OrgCenterGroupName,
              OrgDivisionLegacyName,
              OrgLineOfBusinessCode,
              OrgBrandCode,
              OrgBrandName,
              OrgFlexAttribute1,
              OrgFlexAttribute2,
              OrgFlexAttribute3,
              OrgFlexAttribute4,
              OrgFlexAttribute5,
              OrgCreatedUser,
              OrgCreatedDate,
              OrgModifiedUser,
              OrgModifiedDate,
              EDWEffectiveDate,
              EDWCreatedDate,
              EDWCreatedBy,
              Deleted
             )
                    SELECT NULL, -- [EDWEndDate]
                           S.OrgID,
                           S.OrgEffectiveDate,
                           S.OrgEndDate,
                           S.ParentOrgID,
                           S.DefaultLocationID,
                           S.CostCenterNumber,
                           S.OrgNumber,
                           S.OrgName,
                           S.OrgHierarchyLevel1Name,
                           S.OrgHierarchyLevel2Name,
                           S.OrgHierarchyLevel3Name,
                           S.OrgHierarchyLevel4Name,
                           S.OrgHierarchyLevel5Name,
                           S.OrgHierarchyLevel6Name,
                           S.OrgHierarchyLevel7Name,
                           S.OrgHierarchyLevel8Name,
                           S.OrgHierarchyLevel9Name,
                           S.OrgHierarchyLevel10Name,
                           S.OrgHierarchyLevel11Name,
                           S.OrgAllName,
                           S.OrgExecutiveFunctionName,
                           S.OrgExecutiveFunctionLeaderName,
                           S.OrgExecutiveSubFunctionName,
                           S.OrgExecutiveSubFunctionLeaderName,
                           S.OrgCorporateFunctionName,
                           S.OrgCorporateSubFunctionName,
                           S.OrgDivisionName,
                           S.OrgDivisionLeaderName,
                           S.OrgRegionNumber,
                           S.OrgRegionName,
                           S.OrgRegionLeaderName,
                           S.OrgMarketNumber,
                           S.OrgMarketName,
                           S.OrgMarketLeaderName,
                           S.OrgSubMarketNumber,
                           S.OrgSubMarketName,
                           S.OrgSubMarketLeaderName,
                           S.OrgDistrictNumber,
                           S.OrgDistrictName,
                           S.OrgInterimDistrictNumber,
                           S.OrgInterimDistrictName,
                           S.OrgDistrictLeaderName,
                           S.OrgActingDistrictLeaderName,
                           S.OrgInterimDistrictLeaderName,
                           S.OrgGroupNumber,
                           S.OrgGroupName,
                           S.OrgGroupLeaderName,
                           S.OrgSubgroupNumber,
                           S.OrgSubGroupName,
                           S.OrgSubGroupLeaderName,
                           S.OrgCampusNumber,
                           S.OrgCampusName,
                           S.OrgCampusLeaderName,
                           S.OrgCenterLeaderName,
                           S.OrgActingCenterLeaderName,
                           S.OrgCategoryName,
                           S.OrgTypeCode,
                           S.OrgTypeName,
                           S.OrgPartnerGroupCode,
                           S.OrgPartnerGroupName,
                           S.OrgCenterGroupCode,
                           S.OrgCenterGroupName,
                           S.OrgDivisionLegacyName,
                           S.OrgLineOfBusinessCode,
                           S.OrgBrandCode,
                           S.OrgBrandName,
                           S.OrgFlexAttribute1,
                           S.OrgFlexAttribute2,
                           S.OrgFlexAttribute3,
                           S.OrgFlexAttribute4,
                           S.OrgFlexAttribute5,
                           S.OrgCreatedUser,
                           S.OrgCreatedDate,
                           S.OrgModifiedUser,
                           S.OrgModifiedDate,
                           S.EDWEffectiveDate,
                           S.EDWCreatedDate,
                           S.EDWCreatedBy,
                           S.Deleted
                    FROM #DimOrganizationUpsert S
                         INNER JOIN @tblMrgeActions_SCD2 scd2 ON S.OrgID = scd2.OrgID
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
		   -- S T E P   5.
		   --
		   -- Execute any automated tests associated with this EDW table load
		   --
		   -- ================================================================================


		   -- ================================================================================
		   --
		   -- S T E P   6.
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
             DROP TABLE #DimOrganizationUpsert;

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