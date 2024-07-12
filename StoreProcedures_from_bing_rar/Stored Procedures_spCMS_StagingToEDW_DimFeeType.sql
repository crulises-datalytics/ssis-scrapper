CREATE PROCEDURE [dbo].[spCMS_StagingToEDW_DimFeeType]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingToEDW_DimStudent
    --
    -- Purpose:            Performs the Insert / Update (Type 2 SCD) ETL process for
    --                         the DimStudent table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spCMS_StagingTransform_DimFeeType, 
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
    -- Usage:              EXEC dbo.spCMS_StagingToEDW_DimFeeType @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 2/16/18    valimineti              BNG-1236 - Refactor DimFeeType ETL so it uses 
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
         DECLARE @SourceName VARCHAR(100)= 'DimFeeType';
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
             CREATE TABLE #DimFeeType
			 (
				[FeeTypeID] [int] NOT NULL,
				[FeeTypeName] [varchar](100) NOT NULL,
				[FeeTypeDescription] [varchar](500) NOT NULL,
				[FeeCategory] [varchar](100) NOT NULL,
				[FeeUnitOfMeasure] [varchar](100) NOT NULL,
				[FeeFTE] [numeric](5, 3) NOT NULL,
				[CSSTransactionCode] [varchar](8) NOT NULL,
				[CSSTransactionType] [varchar](2) NOT NULL,
				[SourceSystem] [varchar](50) NOT NULL,
				[EDWCreatedDate] [datetime2](7) NOT NULL,
				[EDWCreatedBy] [varchar](50) NOT NULL,
				[EDWModifiedDate] [datetime2](7) NOT NULL,
				[EDWModifiedBy] [varchar](50) NOT NULL,
				[Deleted] [datetime2](7) NULL
             );
          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimFeeType
             EXEC dbo.spCMS_StagingTransform_DimFeeType
                  @EDWRunDateTime;

		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimFeeType;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimFeeType ON #DimFeeType
             ([FeeTypeID] ASC
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
             MERGE [BING_EDW].[dbo].[DimFeeType] T
             USING #DimFeeType S
             ON(S.FeeTypeID = T.FeeTypeID)
                 WHEN MATCHED AND S.Deleted IS NULL
                                  AND T.SourceSystem = 'CMS'
                                  AND (   S.FeeTypeName <> T.FeeTypeName
                                       OR S.FeeTypeDescription <> T.FeeTypeDescription
                                       OR S.FeeCategory <> T.FeeCategory
                                       OR S.FeeUnitOfMeasure <> T.FeeUnitOfMeasure
                                       OR S.FeeFTE <> T.FeeFTE
                                       OR S.CSSTransactionCode <> T.CSSTransactionCode
                                       OR S.CSSTransactionType <> T.CSSTransactionType
                                       OR S.SourceSystem <> T.SourceSystem
									   OR T.Deleted IS NOT NULL
                                      )
                 THEN UPDATE SET
                                 T.FeeTypeName = S.FeeTypeName, 	
								 T.FeeTypeDescription = S.FeeTypeDescription,
								 T.FeeCategory = S.FeeCategory,
								 T.FeeUnitOfMeasure = S.FeeUnitOfMeasure,
								 T.FeeFTE = S.FeeFTE,
								 T.CSSTransactionCode = S.CSSTransactionCode,
								 T.CSSTransactionType = S.CSSTransactionType,
								 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWCreatedBy = S.EDWCreatedBy,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.EDWModifiedBy = S.EDWModifiedBy,
                                 T.Deleted = S.Deleted
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(FeeTypeID,
						  FeeTypeName,
						  FeeTypeDescription,
						  FeeCategory,
						  FeeUnitOfMeasure,
						  FeeFTE,
						  CSSTransactionCode,
						  CSSTransactionType,
						  SourceSystem,
						  EDWCreatedDate,
						  EDWCreatedBy,
						  EDWModifiedDate,
						  EDWModifiedBy,
						  Deleted
						 )
                   VALUES
						(
						  S.FeeTypeID,
						  S.FeeTypeName,
						  S.FeeTypeDescription,
						  S.FeeCategory,
						  S.FeeUnitOfMeasure,
						  S.FeeFTE,
						  S.CSSTransactionCode,
						  S.CSSTransactionType,
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
             MERGE [BING_EDW].[dbo].[DimFeeType] T
             USING #DimFeeType S
             ON(S.FeeTypeID = T.FeeTypeID
                AND S.SourceSystem = 'CMS')
                 WHEN MATCHED AND(S.Deleted IS NOT NULL
                                  AND T.Deleted IS NULL)
                 THEN UPDATE SET
                                 T.FeeTypeName = S.FeeTypeName, 	
								 T.FeeTypeDescription = S.FeeTypeDescription,
								 T.FeeCategory = S.FeeCategory,
								 T.FeeUnitOfMeasure = S.FeeUnitOfMeasure,
								 T.FeeFTE = S.FeeFTE,
								 T.CSSTransactionCode = S.CSSTransactionCode,
								 T.CSSTransactionType = S.CSSTransactionType,
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
             DROP TABLE #DimFeeType;

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