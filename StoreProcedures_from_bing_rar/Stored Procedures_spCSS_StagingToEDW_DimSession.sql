CREATE PROCEDURE dbo.spCSS_StagingToEDW_DimSession
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_StagingToEDW_DimSession
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the DimSession table from Staging to BING_EDW.
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
    -- Usage:              EXEC dbo.spCSS_StagingToEDW_DimSession @DebugMode = 1	
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- --------    -----------         --------
    --
    --  2/15/18     sburke          BNG-1xxx - Convert from SSIS DFT to stored proc
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimSession - CSS';
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
         DECLARE @tblDeleteActions TABLE(MergeAction VARCHAR(20));
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

         -- ================================================================================
         -- Create temporary landing #table
         -- ================================================================================
         CREATE TABLE #DimSessionUpsert
         ([SessionID]        INT NOT NULL,
          [SessionName]      VARCHAR(50) NOT NULL,
          [SessionCategory]  VARCHAR(25) NOT NULL,
          [SessionFTE]       NUMERIC(5, 3) NOT NULL,
          [SourceSystem]     VARCHAR(3) NOT NULL,
          [EDWEffectiveDate] DATETIME2(7) NOT NULL,
          [EDWEndDate]       DATETIME2(7) NULL,
          [EDWCreatedDate]   DATETIME2(7) NOT NULL,
          [EDWCreatedBy]     VARCHAR(50) NOT NULL,
          [Deleted]          DATETIME2(7) NULL
         );          

         -- ================================================================================
         -- Populate the Landing table FROM Source, and create any helper indexes
         -- ================================================================================
         INSERT INTO #DimSessionUpsert
         EXEC dbo.spCSS_StagingTransform_DimSession;
		   
         -- Get how many rows were extracted from source 

         SELECT @SourceCount = COUNT(1)
         FROM #DimSessionUpsert;
		   
         -- Debug output progress
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
         PRINT @DebugMsg;
         --
         -- Create helper index
         --
         CREATE NONCLUSTERED INDEX XAK1DimSessionUpsert ON #DimSessionUpsert
         ([SourceSystem] ASC, [SessionName] ASC
         );

	    -- --------------------------------------------------------------------------------
	    -- Upserts and Deletes contained in a single transaction.  
	    --	 Rollback on error
	    -- --------------------------------------------------------------------------------
         BEGIN TRY
             BEGIN TRANSACTION;
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Beginning transaction.';
             PRINT @DebugMsg;		   

		   -- ================================================================================
		   -- Perform the Insert / Update (Merge) / deletes required for this EDW table load
		   -- ================================================================================

		   --
		   -- Perform the Merge statement for insert / updates
		   --
             MERGE [BING_EDW].[dbo].[DimSession] T
             USING #DimSessionUpsert S
             ON(S.SourceSystem = T.SourceSystem
                AND S.SessionName = T.SessionName)
                 WHEN MATCHED AND(S.SessionID <> T.SessionID
                                  OR S.SessionCategory <> T.SessionCategory
                                  OR S.SessionFTE <> T.SessionFTE
                                  OR S.EDWEffectiveDate <> T.EDWEffectiveDate)
                 THEN UPDATE SET
                                 T.SessionID = S.SessionID,
                                 T.SessionCategory = S.SessionCategory,
                                 T.SessionFTE = S.SessionFTE,
                                 T.EDWEffectiveDate = S.EDWEffectiveDate,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWCreatedBy = S.EDWCreatedBy
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(SessionID,
                          SessionName,
                          SessionCategory,
                          SessionFTE,
                          SourceSystem,
                          EDWEffectiveDate,
                          EDWCreatedDate,
                          EDWCreatedBy)
                   VALUES
             (SessionID,
              SessionName,
              SessionCategory,
              SessionFTE,
              SourceSystem,
              EDWEffectiveDate,
              EDWCreatedDate,
              EDWCreatedBy
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
		   
		   -- ================================================================================
		   -- This is step was already in the old SSIS code, which we will retain for now 
		   --	(though I am not sure if we really need it).  It performs a soft delete
             --     when we have an existing record in DimSession but no corresponding record
             --     coming in from CSS.
		   -- ================================================================================

             UPDATE [BING_EDW].[dbo].[DimSession]
               SET
                   Deleted = @EDWRunDateTime
             WHERE SourceSystem = 'CSS'
                   AND Deleted IS NULL
                   AND SessionName NOT IN
             (
                 SELECT SessionName
                 FROM #DimSessionUpsert
             );
             SELECT @DeleteCount = @@ROWCOUNT;

		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Soft Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' rows into Target.';
                     PRINT @DebugMsg;
             END; 

		  -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Committing transaction.';
             PRINT @DebugMsg;
		   
		   --
		   -- Commit the successful transaction 
		   --
             COMMIT TRANSACTION;

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
             EXEC dbo.spCSS_StagingEDWETLBatchControl
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