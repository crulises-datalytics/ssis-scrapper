
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCSS_StagingToEDW_FactLifeCycleStatusSnapShot'
)
    DROP PROCEDURE dbo.spCSS_StagingToEDW_FactLifeCycleStatusSnapShot;
GO
*/

CREATE PROCEDURE [dbo].[spCSS_StagingToEDW_FactLifeCycleStatusSnapShot]
(@EDWRunDateTime DATETIME2 = NULL,
 @AuditId        BIGINT,
 @FiscalWeek     INT,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_StagingToEDW_FactLifeCycleStatusSnapShot
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the FactLifeCycleStatusSnapShot table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure [spCSS_StagingTransform_FactLifecycleStatusSnapshot], 
    --                                 and create any helper indexes
    --                         Step 3: Perform the Insert / Update (Merge) / deletes required 
    --                                 for this EDW table load           
    --                         Step 4: Execute any automated tests associated with this EDW table load
    --                         Step 5: Output Source / Insert / Update / Delete counts to caller, 
    --                                 commit the transaction, and tidy-up
    --
    -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than making numerous GETDATE() calls  
    --                     @AuditId - This is the Audit tracking ID used both in the EDWAuditLog and EDWBatchLoadLog tables 
    --				  @FiscalWeek - Integer value representing the Fiscal Week 
    --					  that you want to pull
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
    -- Usage:              EXEC dbo.spCSS_StagingToEDW_FactLifeCycleStatusSnapShot @DebugMode = 1  
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:         
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    --  07/27/18      Adevabhakthuni             BNG-3435 - StagingToEDW for LifeCycleStatusSnapShot
    --  8/17/18      sburke             BNG-3582 - Performance improvements, plus adding of Enrollment and Withdrawal details
    --           
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

        --
        -- Housekeeping Variables
        --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @TaskName VARCHAR(100)= 'FactLifeCycleStatusSnapShot - CSS';

	    --
	    -- For larger Fact (and Dimension) loads we will split the loads
	    --
         DECLARE @BatchSplitByName VARCHAR(50)= 'FiscalWeekNumber';
         DECLARE @BatchSplitByValue INT= @FiscalWeek;


        --
        -- ETL status Variables
        --
         DECLARE @RowCount INT;
         DECLARE @Error INT;
         DECLARE @SourceCount INT= 0;
         DECLARE @InsertCount INT= 0;
         DECLARE @UpdateCount INT= 0;
         DECLARE @DeleteCount INT= 0;

        -- If we do not get an @EDWRunDateTime input, set to current date
        --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
        --
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting.';
         PRINT @DebugMsg;

        -- --------------------------------------------------------------------------------
        -- Extract FROM Source, Upserts and Deletes contained in a single transaction.  
        --   Rollback on error
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
             CREATE TABLE #FactLifeCycleStatusSnapShotUpsert
             ([DateKey]                                  [INT] NOT NULL,
              [CurrentAcademicYearNumber]                [INT] NOT NULL,
              [StudentKey]                               [INT] NOT NULL,
              [CostCenterKey]                            [INT] NOT NULL,
              [CostCenterTypeKey]                        [INT] NOT NULL,
              [OrgKey]                                   [INT] NOT NULL,
              [LocationKey]                              [INT] NOT NULL,
              [SponsorKey]                               [INT] NOT NULL,
              [TransactionCodeKey]                       [INT] NOT NULL,
              [LifecycleStatusKey]                       [INT] NOT NULL,
              [StudentFirstEnrollmentDateKey]            [INT] NOT NULL,
              [StudentFirstEnrollmentAcademicYearNumber] [INT] NOT NULL,
              [StudentEnrolledCurrentAcademicYear]       [INT] NOT NULL,
              [StudentWithdrewCurrentAcademicYear]       [INT] NOT NULL,
              [StudentWithdrewCurrentBTSYear]            [INT] NOT NULL,
              [StudentEnrolledPreviousAcademicYear]      [INT] NOT NULL,
              [SourceSystem]                             [VARCHAR](3) NOT NULL,
              [EDWCreatedDate]                           [DATETIME2](7) NOT NULL
             );

           -- ================================================================================
           --
           -- S T E P   2.
           --
           -- Populate the Landing table FROM Source, and create any helper indexes
           --
           -- ================================================================================
             INSERT INTO #FactLifeCycleStatusSnapShotUpsert
             EXEC dbo.spCSS_StagingTransform_FactLifeCycleStatusSnapShot
                  @EDWRunDateTime = @EDWRunDateTime,
                  @FiscalWeek = @FiscalWeek;

           -- Get how many rows were extracted FROM source 

             SELECT @SourceCount = COUNT(1)
             FROM #FactLifeCycleStatusSnapShotUpsert;
           
           -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
             PRINT @DebugMsg;
           --
           -- Create helper index: use the same keys that we're deleting by
           --

             CREATE NONCLUSTERED INDEX XAK2FactLifeCycleStatusSnapShotUpsert ON #FactLifeCycleStatusSnapShotUpsert
             ([DateKey] ASC, [StudentKey] ASC, [SourceSystem] ASC, [CostCenterKey] ASC
             );

           -- ================================================================================
           --
           -- S T E P   3.
           --
           -- Perform the Insert / Update (Merge) / deletes required for this EDW table load
           --
           -- For this Fact load, we do not update or merge - instead we delete and reload
           --     the data for a given Fiscal Week, hence why we are so careful about batching
           --     up the ETL.
           -- ================================================================================
       
           --
           -- Delete data for the given Fiscal Week
           --
             DELETE BING_EDW.dbo.FactLifeCycleStatusSnapShot
             FROM BING_EDW.dbo.FactLifeCycleStatusSnapShot T
                  INNER JOIN
             (
                 SELECT DateKey,
                        SourceSystem
                 FROM #FactLifeCycleStatusSnapShotUpsert
                 GROUP BY DateKey,
                          SourceSystem
             ) S ON S.DateKey = T.DateKey
                    AND S.SourceSystem = T.SourceSystem;
             SELECT @DeleteCount = @@ROWCOUNT;

           -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' rows from Target.';
                     PRINT @DebugMsg;
             END;

           --
           -- Insert the data for the Fiscal Week
           --
             INSERT INTO BING_EDW.dbo.FactLifeCycleStatusSnapShot
             ([DateKey],
              [CurrentAcademicYearNumber],
              [StudentKey],
              [CostCenterKey],
              [CostCenterTypeKey],
              [OrgKey],
              [LocationKey],
              [SponsorKey],
              [TransactionCodeKey],
              [LifecycleStatusKey],
              [StudentFirstEnrollmentDateKey],
              [StudentFirstEnrollmentAcademicYearNumber],
              [StudentEnrolledCurrentAcademicYear],
              [StudentWithdrewCurrentAcademicYear],
              [StudentWithdrewCurrentBTSYear],
              [StudentEnrolledPreviousAcademicYear],
              [SourceSystem],
              [EDWCreatedDate]
             )
                    SELECT [DateKey],
                           [CurrentAcademicYearNumber],
                           [StudentKey],
                           [CostCenterKey],
                           [CostCenterTypeKey],
                           [OrgKey],
                           [LocationKey],
                           [SponsorKey],
                           [TransactionCodeKey],
                           [LifecycleStatusKey],
                           [StudentFirstEnrollmentDateKey],
                           [StudentFirstEnrollmentAcademicYearNumber],
                           [StudentEnrolledCurrentAcademicYear],
                           [StudentWithdrewCurrentAcademicYear],
                           [StudentWithdrewCurrentBTSYear],
                           [StudentEnrolledPreviousAcademicYear],
                           [SourceSystem],
                           [EDWCreatedDate]
                    FROM #FactLifeCycleStatusSnapShotUpsert;
             SELECT @InsertCount = @@ROWCOUNT;
       
           -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' rows into Target.';
                     PRINT @DebugMsg;
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Updated '+CONVERT(NVARCHAR(20), @UpdateCount)+' rows into Target.';
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
           --   and tidy tup.
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
             DROP TABLE #FactLifeCycleStatusSnapShotUpsert;

           --
           -- Write our successful run to the EDW AuditLog 
           --
             EXEC BING_EDW.dbo.spInsertEDWBatchLoadLog
                  @AuditId = @AuditId,
                  @TaskName = @TaskName,
                  @BatchSplitByName = @BatchSplitByName,
                  @BatchSplitByValue = @BatchSplitByValue,
                  @SourceCount = @SourceCount,
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @StartTime = @EDWRunDateTime;

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