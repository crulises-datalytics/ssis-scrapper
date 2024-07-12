/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCMS_StagingToEDW_FactFTESnapshot'
)
    DROP PROCEDURE dbo.spCMS_StagingToEDW_FactFTESnapshot;
GO
*/
CREATE PROCEDURE dbo.spCMS_StagingToEDW_FactFTESnapshot
(
    @EDWRunDateTime DATETIME2 = NULL,
    @AuditId BIGINT,
    @FiscalDate DATE,
    @DebugMode INT = NULL
)
AS
-- ================================================================================
-- 
-- Stored Procedure:   spCMS_StagingToEDW_FactFTESnapshot
--
-- Purpose:            Performs the Insert / Update / Delete ETL process for
--                         the FactFTESnapshot table from Staging to BING_EDW.
--
--                     Step 1: Create temporary landing #table
--                     Step 2: Populate the Landing table from Source by calling
--                         sub-procedure spCMS_StagingTransfrom_FactFTESnapshot, 
--                         and create any helper indexes.
--                     Step 3: Perform the Insert / Deletes required 
--                         for this EDW table load
--                     Step 4: Execute any automated tests associated with this EDW table load
--                     Step 5: Output Source / Insert / Update / Delete counts to caller, 
--                         commit the transaction, and tidy-up
--
-- Parameters:         @EDWRunDateTime
--                     @AuditId - This is the Audit tracking ID used both in the EDWAuditLog and EDWBatchLoadLog tables 
--                     @FiscalDate - Fiscal Date.  This proc has the option of only returning data for a given Fiscal Date, which can 
--                         be leveraged as part of a batched process (so we don't extract and load a huge dataset in one go).
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
-- Usage:              EXEC dbo.spCMS_StagingToEDW_FactAdjustment @FiscalDate = '20171231'
--
-- --------------------------------------------------------------------------------
--
-- Change Log:		   
-- ----------
--
-- Date         Modified By     Comments
-- ----         -----------     --------
--
--  1/01/29     sburke          BNG-294 - Refactored EDW FactFTESnapshot (CMS Source) load
--  03/07/22    adevabhakthuni BI-3789 - Removed group by logic 
-- ================================================================================
BEGIN
    SET NOCOUNT ON;

    --
    -- Housekeeping Variables
    --
    DECLARE @ProcName NVARCHAR(500) = OBJECT_NAME(@@PROCID);
    DECLARE @DebugMsg NVARCHAR(500);
    DECLARE @TaskName VARCHAR(100) = 'FactFTESnapshot - CMS';

    --
    -- For larger Fact (and Dimension) loads we will split the loads
    --
    DECLARE @BatchSplitByName VARCHAR(50) = 'FiscalDate';
    DECLARE @BatchSplitByValue INT
        = CONVERT(
                     INT,
                     CAST(DATEPART(YYYY, @FiscalDate) AS [CHAR](4))
                     + RIGHT('0' + CAST(DATEPART(M, @FiscalDate) AS [VARCHAR](2)), 2)
                     + RIGHT('0' + CAST(DATEPART(D, @FiscalDate) AS [VARCHAR](2)), 2)
                 );

    --
    -- ETL status Variables
    --
    DECLARE @RowCount INT;
    DECLARE @Error INT;
    DECLARE @SourceCount INT = 0;
    DECLARE @InsertCount INT = 0;
    DECLARE @UpdateCount INT = 0;
    DECLARE @DeleteCount INT = 0;

    --
    -- If we do not get an @EDWRunDateTime input, set to current date
    --
    IF @EDWRunDateTime IS NULL
        SET @EDWRunDateTime = GETDATE();
    --
    IF @DebugMode = 1
        SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Starting.';
    PRINT @DebugMsg;

    -- --------------------------------------------------------------------------------
    -- Extract from Source, Upserts and Deletes contained in a single transaction.  
    --	 Rollback on error
    -- --------------------------------------------------------------------------------
    BEGIN TRY
        BEGIN TRANSACTION;
        -- Debug output progress
        IF @DebugMode = 1
            SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Beginning transaction.';
        PRINT @DebugMsg;
        -- ================================================================================
        --
        -- S T E P   1.
        --
        -- Create temporary landing #table
        --
        -- ================================================================================
        CREATE TABLE #FactFTESnapshotUpsert
        (
            [DateKey] [INT] NOT NULL,
            [OrgKey] [INT] NOT NULL,
            [LocationKey] [INT] NOT NULL,
            [StudentKey] [INT] NOT NULL,
            [SponsorKey] [INT] NOT NULL,
            [TuitionAssistanceProviderKey] [INT] NOT NULL,
            [CompanyKey] [INT] NOT NULL,
            [CostCenterTypeKey] [INT] NOT NULL,
            [CostCenterKey] [INT] NOT NULL,
            [AccountSubaccountKey] [INT] NOT NULL,
            [TransactionCodeKey] [INT] NOT NULL,
            [TierKey] [INT] NOT NULL,
            [ProgramKey] [INT] NOT NULL,
            [SessionKey] [INT] NOT NULL,
            [ScheduleWeekKey] [INT] NOT NULL,
            [ClassroomKey] [INT] NOT NULL,
            [FeeTypeKey] [INT] NOT NULL,
            [LifecycleStatusKey] [INT] NOT NULL,
            [ReferenceID] [VARCHAR](50) NOT NULL,
            [FTE] [NUMERIC](9, 4) NOT NULL,
            [SourceSystem] [VARCHAR](3) NOT NULL,
            [EDWCreatedDate] [DATETIME2](7) NOT NULL
        );

        -- ================================================================================
        --
        -- S T E P   2.
        --
        -- Populate the Landing table from Source, and create any helper indexes
        --
        -- ================================================================================
        INSERT INTO #FactFTESnapshotUpsert
        EXEC dbo.spCMS_StagingTransform_FactFTESnapshot @EDWRunDateTime = @EDWRunDateTime,
                                                        @FiscalDate = @FiscalDate; -- @FiscalWeekEndDate needs to be provided

        -- Get how many rows were extracted from source 

        SELECT @SourceCount = COUNT(1)
        FROM #FactFTESnapshotUpsert;

        -- Debug output progress
        IF @DebugMode = 1
            SELECT @DebugMsg
                = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Extracted '
                  + CONVERT(NVARCHAR(20), @SourceCount) + N' rows from Source.';
        PRINT @DebugMsg;
        --
        -- Create helper index: use the same keys that we're deleting by
        --
        CREATE NONCLUSTERED INDEX XAK1FactFTESnapshotUpsert
        ON #FactFTESnapshotUpsert (
                                      [DateKey] ASC,
                                      [SourceSystem] ASC
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
        DELETE BING_EDW.dbo.FactFTESnapshot
        FROM BING_EDW.dbo.FactFTESnapshot T
            INNER JOIN #FactFTESnapshotUpsert S
                ON S.DateKey = T.DateKey
                   AND S.SourceSystem = T.SourceSystem;
        SELECT @DeleteCount = @@ROWCOUNT;

        -- Debug output progress
        IF @DebugMode = 1
        BEGIN
            SELECT @DebugMsg
                = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Deleted '
                  + CONVERT(NVARCHAR(20), @DeleteCount) + N' rows from Target.';
            PRINT @DebugMsg;
        END;

        --
        -- Insert the data for the Fiscal Week
        --
        INSERT INTO BING_EDW.dbo.FactFTESnapshot
        (
            [DateKey],
            [OrgKey],
            [LocationKey],
            [StudentKey],
            [SponsorKey],
            [TuitionAssistanceProviderKey],
            [CompanyKey],
            [CostCenterTypeKey],
            [CostCenterKey],
            [AccountSubaccountKey],
            [TransactionCodeKey],
            [TierKey],
            [ProgramKey],
            [SessionKey],
            [ScheduleWeekKey],
            [ClassroomKey],
            [FeeTypeKey],
            [LifecycleStatusKey],
            [ReferenceID],
            [FTE],
            [SourceSystem],
            [EDWCreatedDate]
        )
        SELECT [DateKey],
               [OrgKey],
               [LocationKey],
               [StudentKey],
               [SponsorKey],
               [TuitionAssistanceProviderKey],
               [CompanyKey],
               [CostCenterTypeKey],
               [CostCenterKey],
               [AccountSubaccountKey],
               [TransactionCodeKey],
               [TierKey],
               [ProgramKey],
               [SessionKey],
               [ScheduleWeekKey],
               [ClassroomKey],
               [FeeTypeKey],
               [LifecycleStatusKey],
               [ReferenceID],
               [FTE],
               [SourceSystem],
               [EDWCreatedDate]
        FROM #FactFTESnapshotUpsert;
        SELECT @InsertCount = @@ROWCOUNT;

        -- Debug output progress
        IF @DebugMode = 1
        BEGIN
            SELECT @DebugMsg
                = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Inserted '
                  + CONVERT(NVARCHAR(20), @InsertCount) + N' rows into Target.';
            PRINT @DebugMsg;
            SELECT @DebugMsg
                = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Updated '
                  + CONVERT(NVARCHAR(20), @UpdateCount) + N' rows into Target.';
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
            SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Committing transaction.';
        PRINT @DebugMsg;

        --
        -- Commit the successful transaction 
        --
        COMMIT TRANSACTION;

        --
        -- Drop the temp table
        --
        DROP TABLE #FactFTESnapshotUpsert;

        --
        -- Write our successful run to the EDW AuditLog 
        --
        EXEC BING_EDW.dbo.spInsertEDWBatchLoadLog @AuditId = @AuditId,
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
            SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Completing successfully.';
        PRINT @DebugMsg;
    END TRY
    BEGIN CATCH
        -- Debug output progress
        IF @DebugMode = 1
            SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Rolling back transaction.';
        PRINT @DebugMsg;
        -- Rollback the transaction
        ROLLBACK TRANSACTION;
        --
        -- Write our failed run to the EDW AuditLog 
        --
        EXEC [BING_EDW].[dbo].[spEDWErrorAuditLog] @AuditId = @AuditId;
        --
        -- Raiserror
        --	
        DECLARE @ErrMsg NVARCHAR(4000),
                @ErrSeverity INT;
        SELECT @ErrMsg = ERROR_MESSAGE(),
               @ErrSeverity = ERROR_SEVERITY();
        RAISERROR(@ErrMsg, @ErrSeverity, 1);
    END CATCH;
END;
GO