CREATE PROCEDURE [dbo].[spCMS_StagingToEDW_DimPaymentType]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingToEDW_DimPaymentType
    --
    -- Purpose:            Performs the Insert / Update ETL process for
    --                         the DimPaymentType table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spCMS_StagingTransform_DimPaymentType, 
    --                                 and create any helper indexes
    --                         Step 3: Perform the Insert / Update  required for this EDW
    --                                 table load
    --                             (a) Perform a Merge that inserts new rows, and updates any existing 
    --                                 current rows to be a previous version
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
    -- Usage:              EXEC dbo.spCMS_StagingToEDW_DimPaymentType @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 2/19/18     Adevabhakthuni             BNG-1238 - Refactor DimPaymentType ETL so it uses 
	--											Stored Proc over DFTs creating 
	--											temporary DB Objects
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimPaymentType';
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
             CREATE TABLE #DimPaymentType
			 (
	[PaymentTypeID] [int] NOT NULL,
	[PaymentTypeName] [varchar](100) NOT NULL,
	[CSSTransactionCode] [varchar](8) NOT NULL,
	[SourceSystem] [varchar](3) NOT NULL,
	[EDWCreatedDate] [datetime2](7) NOT NULL,
	[EDWCreatedBy] [varchar](50) NOT NULL,
	[EDWModifiedDate] [datetime2](7) NOT NULL,
	[EDWModifiedBy] [varchar](50) NOT NULL,
	[Deleted] [datetime2](7) NULL,

             );
          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimPaymentType
             EXEC dbo.spCMS_StagingTransform_DimPaymentType
                  @EDWRunDateTime;

		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimPaymentType;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimFeeType ON #DimPaymentType
             ([PaymentTypeID] ASC
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
             MERGE [BING_EDW].[dbo].[DimPaymentType] T
             USING #DimPaymentType S
             ON(S.PaymentTypeID = T.PaymentTypeID)
                 WHEN MATCHED AND S.Deleted IS NULL
                                  AND T.SourceSystem = 'CMS'
                                  AND ( 
									  S.PaymentTypeName <> T.PaymentTypeName
									 OR S.CSSTransactionCode <> T.CSSTransactionCode
									 OR S.SourceSystem <> T.SourceSystem 
									 OR T.Deleted IS NOT NULL
                                      )
                 THEN UPDATE SET
								 T.PaymentTypeName = S.PaymentTypeName,
								 T.CSSTransactionCode = S.CSSTransactionCode,
								 T.SourceSystem = S.SourceSystem,
								 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWCreatedBy = S.EDWCreatedBy,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.EDWModifiedBy = S.EDWModifiedBy,
                                 T.Deleted = S.Deleted
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(PaymentTypeID,
						  PaymentTypeName,
						  CSSTransactionCode,
						  SourceSystem,
						  EDWCreatedDate,
						  EDWCreatedBy,
						  EDWModifiedDate,
						  EDWModifiedBy,
						  Deleted
						 )
                   VALUES
						(
						  S.PaymentTypeID,
						  S.PaymentTypeName,
						  S.CSSTransactionCode,
						  S.SourceSystem,
						  S.EDWCreatedDate,
						  S.EDWCreatedBy,
						  S.EDWModifiedDate,
						  S.EDWModifiedBy,
						  S.Deleted
						)
             -- We need to get the details of the records we updated, so we can insert a further row for them as the current row.
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
		   -- Perform the Merge statement for soft deletes
		   --
             MERGE [BING_EDW].[dbo].[DimPaymentType] T
             USING #DimPaymentType S
             ON(S.PaymentTypeID = T.PaymentTypeID
                AND S.SourceSystem = 'CMS')
                 WHEN MATCHED AND(S.Deleted IS NOT NULL
                                  AND T.Deleted IS NULL)
                 THEN UPDATE SET
								 T.PaymentTypeName = S.PaymentTypeName,
								 T.CSSTransactionCode = S.CSSTransactionCode,            
								 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWCreatedBy = S.EDWCreatedBy,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.EDWModifiedBy = S.EDWModifiedBy,
                                 T.Deleted = S.Deleted
             OUTPUT $action
                    INTO @tblDeleteActions;
             SELECT @DeleteCount = SUM(Deleted)
             FROM
             (
		   -- Count the number of updates

                 SELECT COUNT(*) AS Deleted
                 FROM @tblDeleteActions
                 WHERE MergeAction = 'UPDATE' -- It is a 'soft' delete, so shows up as an update in $action
             ) merge_actions;
             --SELECT @UpdateCount = @@ROWCOUNT;
		   
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
		   --	and tidy up.
		   --
		   -- ================================================================================		   
		  --
		   -- Output ETL Rowcounts to the calling process
		   --
		   SELECT @SourceCount AS SourceCount,
                    @InsertCount AS InsertCount,
                    @UpdateCount AS UpdateCount,
                    @DeleteCount AS DeleteCount;

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
             DROP TABLE #DimPaymentType;

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
             EXEC dbo.spCMS_StagingEDWETLBatchControl
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
		   -- Raise error
		   --	
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;