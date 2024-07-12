
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingToEDW_DimLocation'
)
    DROP PROCEDURE dbo.spHR_StagingToEDW_DimLocation;
GO
*/
CREATE PROCEDURE dbo.spHR_StagingToEDW_DimLocation
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimLocation
    --
    -- Purpose:            Performs the Insert / Update (Type 2 SCD) ETL process for
    --                         the DimLocation table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimLocation, 
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimLocation @EDWRunDateTime = GETDATE(), @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 10/20/17    sburke              BNG-255 - Initial version of proc, loading HR Location
    --                                     data from HR_Staging (replacing the CMS, PRO and
    --                                     Horizon loads)
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimLocation';
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
	    -- Column(s) that make up the unique business key for the table
          [LocationID]       [INT] NOT NULL,
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
             CREATE TABLE #DimLocationUpsert
             ([LocationID]                               INT NOT NULL,
              [ShipToLocationID]                         INT NOT NULL,
              [LocationName]                             VARCHAR(100) NOT NULL,
              [LocationAddressLine1]                     VARCHAR(250) NOT NULL,
              [LocationAddressLine2]                     VARCHAR(250) NOT NULL,
              [LocationAddressLine3]                     VARCHAR(250) NOT NULL,
              [LocationCity]                             VARCHAR(50) NOT NULL,
              [LocationState]                            VARCHAR(100) NOT NULL,
              [LocationZip]                              VARCHAR(50) NOT NULL,
              [LocationCounty]                           VARCHAR(100) NOT NULL,
              [LocationCountry]                          VARCHAR(50) NOT NULL,
              [LocationPhone]                            VARCHAR(50) NOT NULL,
              [LocationSecondaryPhone]                   VARCHAR(50) NOT NULL,
              [LocationFax]                              VARCHAR(50) NOT NULL,
              [LocationEmail]                            VARCHAR(250) NOT NULL,
              [LocationADPCode]                          VARCHAR(10) NOT NULL,
              [LocationWarehouseFlag]                    VARCHAR(50) NOT NULL,
              [LocationShipToSiteFlag]                   VARCHAR(50) NOT NULL,
              [LocationReceivingSiteFlag]                VARCHAR(50) NOT NULL,
              [LocationBillToSiteFlag]                   VARCHAR(50) NOT NULL,
              [LocationInOrganizationFlag]               VARCHAR(50) NOT NULL,
              [LocationOfficeSiteFlag]                   VARCHAR(50) NOT NULL,
              [LocationEEOCostCenterName]                VARCHAR(150) NULL,
              [LocationEEOCostCenterNumber]              VARCHAR(150) NULL,
              [LocationEEOApprenticesEmployedFlag]       VARCHAR(150) NULL,
              [LocationEEOGovernmentContractorFlag]      VARCHAR(150) NULL,
              [LocationEEOMainActivitiesLine1]            VARCHAR(150) NULL,
              [LocationEEOMainActivitiesLine2]           VARCHAR(150) NULL,
              [LocationEEOReportedPreviouslyFlag]        VARCHAR(150) NULL,
              [LocationEEOHeadquartersEstablishmentFlag] VARCHAR(150) NULL,
              [LocationDUNSNumber]                       VARCHAR(150) NULL,
              [LocationSICNumber]                        VARCHAR(150) NULL,
              [LocationNAICSNumber]                      VARCHAR(150) NULL,
              [LocationFEINNumber]                       VARCHAR(150) NULL,
              [LocationCreatedUser]                      INT NOT NULL,
              [LocationCreatedDate]                      DATETIME NOT NULL,
              [LocationModifiedUser]                     INT NOT NULL,
              [LocationModifiedDate]                     DATETIME NOT NULL,
              [EDWEffectiveDate]                         DATETIME2 NOT NULL,
              [EDWEndDate]                               DATETIME2 NULL,
              [EDWCreatedDate]                           DATETIME2 NOT NULL,
              [EDWCreatedBy]                             VARCHAR(50) NOT NULL,
              [Deleted]                                  DATETIME2 NULL
             );
          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimLocationUpsert
             EXEC dbo.spHR_StagingTransform_DimLocation
                  @EDWRunDateTime;

		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimLocationUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimLocationUpsert ON #DimLocationUpsert
             ([LocationID] ASC, [EDWEffectiveDate] ASC
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
             MERGE [BING_EDW].[dbo].[DimLocation] T
             USING #DimLocationUpsert S
             ON(S.LocationID = T.LocationID)
                 WHEN MATCHED AND S.Deleted IS NULL
                                  AND T.EDWEndDate IS NULL -- The 'current' record in target
                                  AND (S.ShipToLocationID <> T.ShipToLocationID
                                       OR S.LocationName <> T.LocationName
                                       OR S.LocationAddressLine1 <> T.LocationAddressLine1
                                       OR S.LocationAddressLine2 <> T.LocationAddressLine2
                                       OR S.LocationAddressLine3 <> T.LocationAddressLine3
                                       OR S.LocationCity <> T.LocationCity
                                       OR S.LocationState <> T.LocationState
                                       OR S.LocationZip <> T.LocationZip
                                       OR S.LocationCounty <> T.LocationCounty
                                       OR S.LocationCountry <> T.LocationCountry
                                       OR S.LocationPhone <> T.LocationPhone
                                       OR S.LocationSecondaryPhone <> T.LocationSecondaryPhone
                                       OR S.LocationFax <> T.LocationFax
                                       OR S.LocationEmail <> T.LocationEmail
                                       OR S.LocationADPCode <> T.LocationADPCode
                                       OR S.LocationWarehouseFlag <> T.LocationWarehouseFlag
                                       OR S.LocationShipToSiteFlag <> T.LocationShipToSiteFlag
                                       OR S.LocationReceivingSiteFlag <> T.LocationReceivingSiteFlag
                                       OR S.LocationBillToSiteFlag <> T.LocationBillToSiteFlag
                                       OR S.LocationInOrganizationFlag <> T.LocationInOrganizationFlag
                                       OR S.LocationOfficeSiteFlag <> T.LocationOfficeSiteFlag
                                       OR S.LocationEEOCostCenterName <> T.LocationEEOCostCenterName
                                       OR S.LocationEEOCostCenterNumber <> T.LocationEEOCostCenterNumber
                                       OR S.LocationEEOApprenticesEmployedFlag <> T.LocationEEOApprenticesEmployedFlag
                                       OR S.LocationEEOGovernmentContractorFlag <> T.LocationEEOGovernmentContractorFlag
                                       OR S.LocationEEOMainActivitiesLine1 <> T.LocationEEOMainActivitiesLine1
                                       OR S.LocationEEOMainActivitiesLine2 <> T.LocationEEOMainActivitiesLine2
                                       OR S.LocationEEOReportedPreviouslyFlag <> T.LocationEEOReportedPreviouslyFlag
                                       OR S.LocationEEOHeadquartersEstablishmentFlag <> T.LocationEEOHeadquartersEstablishmentFlag
                                       OR S.LocationDUNSNumber <> T.LocationDUNSNumber
                                       OR S.LocationSICNumber <> T.LocationSICNumber
                                       OR S.LocationNAICSNumber <> T.LocationNAICSNumber
                                       OR S.LocationFEINNumber <> T.LocationFEINNumber
                                       OR S.LocationCreatedUser <> T.LocationCreatedUser
                                       OR S.LocationCreatedDate <> T.LocationCreatedDate
                                       OR S.LocationModifiedUser <> T.LocationModifiedUser
                                       OR S.LocationModifiedDate <> T.LocationModifiedDate
                                       OR T.Deleted IS NOT NULL)
                 THEN UPDATE SET
                                 T.EDWEndDate = S.EDWEffectiveDate -- Updates the EDWEndDate from NULL (current) to the current date
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(EDWEndDate,
                          LocationID,
                          ShipToLocationID,
                          LocationName,
                          LocationAddressLine1,
                          LocationAddressLine2,
                          LocationAddressLine3,
                          LocationCity,
                          LocationState,
                          LocationZip,
                          LocationCounty,
                          LocationCountry,
                          LocationPhone,
                          LocationSecondaryPhone,
                          LocationFax,
                          LocationEmail,
                          LocationADPCode,
                          LocationWarehouseFlag,
                          LocationShipToSiteFlag,
                          LocationReceivingSiteFlag,
                          LocationBillToSiteFlag,
                          LocationInOrganizationFlag,
                          LocationOfficeSiteFlag,
                          LocationEEOCostCenterName,
                          LocationEEOCostCenterNumber,
                          LocationEEOApprenticesEmployedFlag,
                          LocationEEOGovernmentContractorFlag,
                          LocationEEOMainActivitiesLine1,
                          LocationEEOMainActivitiesLine2,
                          LocationEEOReportedPreviouslyFlag,
                          LocationEEOHeadquartersEstablishmentFlag,
                          LocationDUNSNumber,
                          LocationSICNumber,
                          LocationNAICSNumber,
                          LocationFEINNumber,
                          LocationCreatedUser,
                          LocationCreatedDate,
                          LocationModifiedUser,
                          LocationModifiedDate,
                          EDWEffectiveDate,
                          EDWCreatedDate,
                          EDWCreatedBy,
                          Deleted)
                   VALUES
             (NULL, -- Updates EDWEndDate so it is the current record
              LocationID,
              ShipToLocationID,
              LocationName,
              LocationAddressLine1,
              LocationAddressLine2,
              LocationAddressLine3,
              LocationCity,
              LocationState,
              LocationZip,
              LocationCounty,
              LocationCountry,
              LocationPhone,
              LocationSecondaryPhone,
              LocationFax,
              LocationEmail,
              LocationADPCode,
              LocationWarehouseFlag,
              LocationShipToSiteFlag,
              LocationReceivingSiteFlag,
              LocationBillToSiteFlag,
              LocationInOrganizationFlag,
              LocationOfficeSiteFlag,
              LocationEEOCostCenterName,
              LocationEEOCostCenterNumber,
              LocationEEOApprenticesEmployedFlag,
              LocationEEOGovernmentContractorFlag,
              LocationEEOMainActivitiesLine1,
              LocationEEOMainActivitiesLine2,
              LocationEEOReportedPreviouslyFlag,
              LocationEEOHeadquartersEstablishmentFlag,
              LocationDUNSNumber,
              LocationSICNumber,
              LocationNAICSNumber,
              LocationFEINNumber,
              LocationCreatedUser,
              LocationCreatedDate,
              LocationModifiedUser,
              LocationModifiedDate,
              EDWEffectiveDate,
              EDWCreatedDate,
              EDWCreatedBy,
              Deleted
             )
		   -- We need to get the details of the records we updated, so we can insert a further row for them as the current row.
             OUTPUT $action,
                    S.LocationID,
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
             INSERT INTO BING_EDW.dbo.DimLocation
             (EDWEndDate,
              LocationID,
              ShipToLocationID,
              LocationName,
              LocationAddressLine1,
              LocationAddressLine2,
              LocationAddressLine3,
              LocationCity,
              LocationState,
              LocationZip,
              LocationCounty,
              LocationCountry,
              LocationPhone,
              LocationSecondaryPhone,
              LocationFax,
              LocationEmail,
              LocationADPCode,
              LocationWarehouseFlag,
              LocationShipToSiteFlag,
              LocationReceivingSiteFlag,
              LocationBillToSiteFlag,
              LocationInOrganizationFlag,
              LocationOfficeSiteFlag,
              LocationEEOCostCenterName,
              LocationEEOCostCenterNumber,
              LocationEEOApprenticesEmployedFlag,
              LocationEEOGovernmentContractorFlag,
              LocationEEOMainActivitiesLine1,
              LocationEEOMainActivitiesLine2,
              LocationEEOReportedPreviouslyFlag,
              LocationEEOHeadquartersEstablishmentFlag,
              LocationDUNSNumber,
              LocationSICNumber,
              LocationNAICSNumber,
              LocationFEINNumber,
              LocationCreatedUser,
              LocationCreatedDate,
              LocationModifiedUser,
              LocationModifiedDate,
              EDWEffectiveDate,
              EDWCreatedDate,
              EDWCreatedBy,
              Deleted
             )
                    SELECT NULL, -- [EDWEndDate]
                           S.LocationID,
                           S.ShipToLocationID,
                           S.LocationName,
                           S.LocationAddressLine1,
                           S.LocationAddressLine2,
                           S.LocationAddressLine3,
                           S.LocationCity,
                           S.LocationState,
                           S.LocationZip,
                           S.LocationCounty,
                           S.LocationCountry,
                           S.LocationPhone,
                           S.LocationSecondaryPhone,
                           S.LocationFax,
                           S.LocationEmail,
                           S.LocationADPCode,
                           S.LocationWarehouseFlag,
                           S.LocationShipToSiteFlag,
                           S.LocationReceivingSiteFlag,
                           S.LocationBillToSiteFlag,
                           S.LocationInOrganizationFlag,
                           S.LocationOfficeSiteFlag,
                           S.LocationEEOCostCenterName,
                           S.LocationEEOCostCenterNumber,
                           S.LocationEEOApprenticesEmployedFlag,
                           S.LocationEEOGovernmentContractorFlag,
                           S.LocationEEOMainActivitiesLine1,
                           S.LocationEEOMainActivitiesLine2,
                           S.LocationEEOReportedPreviouslyFlag,
                           S.LocationEEOHeadquartersEstablishmentFlag,
                           S.LocationDUNSNumber,
                           S.LocationSICNumber,
                           S.LocationNAICSNumber,
                           S.LocationFEINNumber,
                           S.LocationCreatedUser,
                           S.LocationCreatedDate,
                           S.LocationModifiedUser,
                           S.LocationModifiedDate,
                           S.EDWEffectiveDate,
                           S.EDWCreatedDate,
                           S.EDWCreatedBy,
                           S.Deleted
                    FROM #DimLocationUpsert S
                         INNER JOIN @tblMrgeActions_SCD2 scd2 ON S.LocationID = scd2.LocationID
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
             DROP TABLE #DimLocationUpsert;

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