

/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spSalesForce_StagingToEDW_FactLeadEvent'
)
    DROP PROCEDURE dbo.spSalesForce_StagingToEDW_FactLeadEvent;
GO
*/ 

CREATE PROCEDURE [dbo].[spSalesForce_StagingToEDW_FactLeadEvent]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spSalesForce_StagingToEDW_FactLeadEvent
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the FactLeadEvent table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimOrganization, 
    --                                 and create any helper indexes
    --                         Step 3: Perform the Insert / Update (Merge) / deletes required 
    --                                 for this EDW table load			 
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
    -- Usage:              EXEC dbo.spSalesForce_StagingToEDW_FactLeadEvent @DebugMode = 1	
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
    -- 01/13/18     anmorales          BNG-253 - EDW FactLeadEvent: Created procedure
    -- 10/4/18      valimineti         BNG-3803- Adding two columns; LeadCreatedByName and LeadCreatedById			 
    -- 01/28/19     Adevabhakthuni     Deleting the tours which are completed or Canceled but still showing as Scheduled
    -- 04/18/19     Adevabhakthuni    Selecting Highest LeadKey to avoid duplicates
	--10/17/2021    Adevabhakthuni    BI-5108 Removed the dimension key from update logic 
	--06/08/2022   Adevabhakthuni     BI-5807 Instead of deleting source deleted records updating leadkey to -3 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'FactLeadEvent';
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
	    -- Merge statement action table variables
	    --
         DECLARE @tblMergeActions TABLE(MergeAction VARCHAR(20));
       --  DECLARE @tblDeleteActions TABLE(MergeAction VARCHAR(20));
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
	    -- Extract FROM Source, Upserts and Deletes contained in a single transaction.  
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
             CREATE TABLE #FactLeadEventUpsert
             ([LeadEventTypeKey]  INT NOT NULL,
              [LeadEventID]       VARCHAR(18) NOT NULL,
              [LeadKey]           INT NOT NULL,
              [OrgKey]            INT NOT NULL,
              [LocationKey]       INT NOT NULL,
              [CompanyKey]        INT NOT NULL,
              [CostCenterTypeKey] INT NOT NULL,
              [CostCenterKey]     INT NOT NULL,
              [WebCampaignKey]    INT NOT NULL,
              [DateKey]           INT NOT NULL,
              [EventDate]         DATETIMEOFFSET(0) NOT NULL,
              [LeadCreatedbyName] [VARCHAR](1300) NOT NULL,
              [LeadCreatedbyId]   [VARCHAR](18) NOT NULL,
              [EDWCreatedDate]    DATETIME2(7) NOT NULL,
             );         

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table FROM Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #FactLeadEventUpsert
             EXEC dbo.spSalesforce_StagingTransform_FactLeadEvent
                  @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

             SELECT @SourceCount = COUNT(1)
             FROM #FactLeadEventUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index: use the same index as the target to prevent duplicate inserts
		   --
             CREATE UNIQUE NONCLUSTERED INDEX XAK1FactLeadEventUpsert ON #FactLeadEventUpsert
             ([LeadEventTypeKey] ASC, [LeadEventID] ASC, [LeadKey] ASC
             );
             WITH CTE
                  AS (
                  SELECT LeadKey,
                         LeadEventID,
                         ROW_NUMBER() OVER(PARTITION BY LeadEventID ORDER BY LeadKey DESC) AS RN
                  FROM #FactLeadEventUpsert)
                  DELETE #FactLeadEventUpsert
                  FROM CTE A
                       INNER JOIN #FactLeadEventUpsert B ON A.LeadEventID = B.LeadEventID
                                                            AND A.LeadKey = B.LeadKey
                  WHERE RN <> 1; 

		   -- ================================================================================
		   --
		   -- S T E P   3.
		   --
		   -- Perform the Insert / Update (Merge) / deletes required for this EDW table load
		   --
		   -- ================================================================================

		   --
		   -- Perform the Merge statement for insert / updates
		   --
             MERGE [BING_EDW].[dbo].[FactLeadEvent] T 
             USING #FactLeadEventUpsert S
             ON(S.LeadEventTypeKey = T.LeadEventTypeKey
                AND S.LeadEventID = T.LeadEventID
                )
                 WHEN MATCHED AND(  S.EventDate<>T.EventDate OR 
                                 S.LeadCreatedbyName <> T.LeadCreatedbyName
                                  OR S.LeadCreatedbyId <> T.LeadCreatedbyId )
                 THEN UPDATE SET
                                 T.OrgKey = S.OrgKey,
                                 T.LocationKey = S.LocationKey,
                                 T.CompanyKey = S.CompanyKey,
                                 T.CostCenterTypeKey = S.CostCenterTypeKey,
                                 T.CostCenterKey = S.CostCenterKey,
                                 T.WebCampaignKey = S.WebCampaignKey,
                                 T.DateKey = S.DateKey,
                                 T.EventDate = S.EventDate,
                                 T.LeadCreatedbyName = S.LeadCreatedbyName,
                                 T.LeadCreatedbyId = S.LeadCreatedbyId,
                                 T.EDWCreatedDate = S.EDWCreatedDate
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(LeadEventTypeKey,
                          LeadEventID,
                          LeadKey,
                          OrgKey,
                          LocationKey,
                          CompanyKey,
                          CostCenterTypeKey,
                          CostCenterKey,
                          WebCampaignKey,
                          DateKey,
                          EventDate,
                          LeadCreatedbyName,
                          LeadCreatedbyId,
                          EDWCreatedDate)
                   VALUES
             (LeadEventTypeKey,
              LeadEventID,
              LeadKey,
              OrgKey,
              LocationKey,
              CompanyKey,
              CostCenterTypeKey,
              CostCenterKey,
              WebCampaignKey,
              DateKey,
              EventDate,
              LeadCreatedbyName,
              LeadCreatedbyId,
              EDWCreatedDate
             )
             OUTPUT $action
                    INTO @tblMergeActions;
             SELECT @InsertCount = SUM(Inserted),
                    @UpdateCount = SUM(Updated)
             FROM
             ( 
		   -- Count the number of inserts

                 SELECT COUNT(*) AS Inserted,
                        0 AS Updated
                 FROM @tblMergeActions
                 WHERE MergeAction = 'INSERT'
                 UNION ALL 
			  
			  -- Count the number of updates 

                 SELECT 0 AS Inserted,
                        COUNT(*) AS Updated
                 FROM @tblMergeActions
                 WHERE MergeAction = 'UPDATE'
             ) merge_actions;
		   
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' rows into Target.';
                     PRINT @DebugMsg;
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Updated '+CONVERT(NVARCHAR(20), @UpdateCount)+' rows into Target.';
                     PRINT @DebugMsg;
             END;             
		   
		   --
		   -- Perform the Merge statement for soft deletes

		   -- Debug output progress

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
             DELETE T     FROM (SELECT * FROM BING_EDW.dbo.FactLeadEvent WHERE LeadEventTypeKey='3') T
                  LEFT JOIN (SELECT * FROM vLeadEvent WHERE LeadEventTypeKey =3) S ON S.LeadEventID = T.LeadEventID
             WHERE
                  S.LeadEventID IS NULL ;
			              UPDATE FLE 
			 SET FLE.EDWCreatedDate= GETDATE(), FLE.LeadKey='-3'	
             FROM (SELECT *  FROM BING_EDW.dbo.FactLeadEvent WHERE LeadKey<>'-3') FLE
                  LEFT JOIN BING_EDW.dbo.DimLead LE ON LE.LeadKey = FLE.LeadKey
             WHERE LE.LeadKey IS NULL;
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
             DROP TABLE #FactLeadEventUpsert;

		   --
		   -- Write our successful run to the EDW AuditLog 
		   --
             EXEC [BING_EDW].[dbo].[spEDWEndAuditLog]
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @SourceCount = @SourceCount,
                  @AuditId = @AuditId;

		   -- Also write the successful load to EDWETLBatchControl, so we know how far back in history
		   --     we have to go in the next ETL run
             EXEC dbo.spSalesForce_StagingEDWETLBatchControl
                  @TaskName = @SourceName;

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
		   -- Raiserror
		   --
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;