


CREATE PROCEDURE [dbo].[spDimPartnerLoad]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL,
 @ExecutionID VARCHAR(100),
 @AuditId		   BIGINT OUTPUT
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingToEDW_DimSession
    --
    -- Purpose:            Performs the Insert / Update (Type 2 SCD) ETL process for
    --                         the DimPartner table from Staging to DW_mart.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spCMS_StagingTransform_DimSession, 
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
    -- Usage:              EXEC dbo.spDimPartnerLoad @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 08/19/2022   hhebbalu            BI-5204     
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimPartner';
     

	    --
	    -- ETL status Variables
	    --
         DECLARE @RowCount INT;
         DECLARE @Error INT;
         DECLARE @SourceCount INT= 0;
         DECLARE @InsertCount INT= 0;
         DECLARE @UpdateCount INT= 0;
         DECLARE @DeleteCount INT= 0;
         DECLARE @EffectiveTo Datetime2(3) = '9999-12-31 23:59:59.999'

	    --
	    -- Merge statement action table variable - for SCD2 we add the unique key columns inaddition to the action
	    --
         DECLARE @tblMrgeActions_SCD2 TABLE
         ([MergeAction]      VARCHAR(250) NOT NULL,
	    -- Column(s) that make up the unique business key for the table we are loading
          [PartnerID]        INT NOT NULL,
          [EffectiveFrom] DATETIME2(3) NOT NULL
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
         EXEC [dbo].[spBeginAuditLog]
              @AuditId = @AuditId OUTPUT,
			  @SourceName = @SourceName,
              @ExecutionID = @ExecutionID; 

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


		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM DW_Landing.dbo.DimPartnerLanding;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   ----
     --        CREATE NONCLUSTERED INDEX XAK1DimPartnerUpsert ON DW_Landing.dbo.DimPartnerLanding
     --        ([StudentID] ASC, [EffectiveFrom] ASC
     --        );

		   
		   


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
             MERGE [DimPartner] T
             USING DW_Landing.dbo.DimPartnerLanding S
             ON(S.PartnerID = T.PartnerID)
                 WHEN MATCHED AND T.EffectiveTo = '9999-12-31 23:59:59.999' -- The 'current' record in target
								  AND T.RowStatus = 'A'
								  AND T.DeletedDate IS NULL
                                  AND LTRIM(RTRIM( UPPER(S.PartnerName))) <> LTRIM(RTRIM(UPPER(T.PartnerName)))
                 THEN UPDATE SET
                                 T.EffectiveTo = S.EffectiveFrom -- Updates the EffectiveTo from '9999-12-31 23:59:59.999' (current) to the current date	
								 , RowStatus = 'H'
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT( [PartnerID]
                          ,[PartnerName]
                          ,[EffectiveFrom]
                          ,[EffectiveTo]
                          ,[RowStatus]
                          ,[ETLJobID])
                   VALUES( [PartnerID]
                          ,[PartnerName]
                          ,'1900-01-01 00:00:00.000'
                          ,@EffectiveTo
                          ,'A'
                          ,@AuditId)
             -- We need to get the details of the records we updated, so we can insert a further row for them as the current row.
             OUTPUT $action,
                    S.PartnerID,
                    S.EffectiveFrom
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
             INSERT INTO DimPartner( [PartnerID]
                          ,[PartnerName]
                          ,[EffectiveFrom]
                          ,[EffectiveTo]
                          ,[RowStatus]
                          ,[ETLJobID])
                   SELECT s.[PartnerID]
                          ,s.[PartnerName]
                          ,s.[EffectiveFrom]
                          ,@EffectiveTo
                          ,'A'
                          ,@AuditId
                    FROM DW_Landing.dbo.DimPartnerLanding S
                         INNER JOIN @tblMrgeActions_SCD2 scd2 ON S.PartnerID = scd2.PartnerID
                                                                 AND s.EffectiveFrom = scd2.EffectiveFrom
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
		   -- Write our successful run to the EDW AuditLog 
		   --
             EXEC [dbo].[spEndAuditLog]
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
             EXEC [dbo].[spErrorAuditLog]
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