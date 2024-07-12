

CREATE PROCEDURE [dbo].[spMISC_StagingToEDW_FactLaborHourTarget]
(@EDWRunDateTime    DATETIME2 = NULL, 
 @AuditId           BIGINT, 
 @FiscalWeekEndDate DATE, 
 @DebugMode         INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spMISC_StagingToEDW_FactLaborHourTarget
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the FactLaborHour table from Staging to BING_EDW.
    --
    --                     Step 1: Create temporary landing #table
    --                     Step 2: Populate the Landing table from Source by calling
    --                         sub-procedure spMISC_StagingTransfrom_FactLaborHour, 
    --                         and create any helper indexes.
    --                     Step 3: Perform the Insert / Deletes required 
    --                         for this EDW table load
    --                     Step 4: Execute any automated tests associated with this EDW table load
    --                     Step 5: Output Source / Insert / Update / Delete counts to caller, 
    --                         commit the transaction, and tidy-up
    --
    -- Parameters:         @EDWRunDateTime
    --                     @AuditId - This is the Audit tracking ID used both in the EDWAuditLog and EDWBatchLoadLog tables 
    --                     @FiscalWeekEndDate - Fiscal Week End Date.  This proc has the option
    --                         of only returning data for a given Fiscal Year, which can 
    --                         be leveraged as part of a batched process (so we don't
    --                         extract and load a huge dataset in one go).
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --
    --
    -- Usage:              EXEC dbo.spMISC_StagingToEDW_FactAdjustment @FiscalWeekEndDate = '20171231'
    --
    -- -----------------------------------------------------------------------------------------------------------------------------------------------
    -- Change Log:		   
    -- -----------------------------------------------------------------------------------------------------------------------------------------------
    -- Date         Modified By     Comments
    -- -----------------------------------------------------------------------------------------------------------------------------------------------
    -- 07/12/18			Adevabhakthuni      BNG-3274 - FactLaborhour
    -- 08/13/18			valimineti			BNG-3503 - Add TSEF column in Fact Labor Hour
    -- 12/10/2018		schwenger			bng-4488 -  updated taskname from cms to adp
    -- 10/18/2019		Puneet				Added SourceSystem in Delete before inserting data to FactLaborHour (BI-2609).	 
	-- 03/03/20     Adevabhakthuni  BI-3569 added two new fields to load to fact table	 
    -- ================================================================================
    BEGIN
        SET NOCOUNT ON;

        --
        -- Housekeeping Variables
        --
        DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
        DECLARE @DebugMsg NVARCHAR(500);
        DECLARE @TaskName VARCHAR(100)= 'FactLaborHourTargetADP';

        --
        -- For larger Fact (and Dimension) loads we will split the loads
        --
        DECLARE @BatchSplitByName VARCHAR(50)= 'FiscalWeekEndDate';
        DECLARE @BatchSplitByValue INT= CONVERT(INT, CAST(DATEPART(YYYY, @FiscalWeekEndDate) AS [CHAR](4)) + RIGHT('0' + CAST(DATEPART(M, @FiscalWeekEndDate) AS [VARCHAR](2)), 2) + RIGHT('0' + CAST(DATEPART(D, @FiscalWeekEndDate) AS [VARCHAR](2)), 2));

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
            SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Starting.';
        PRINT @DebugMsg;

        -- --------------------------------------------------------------------------------
        -- Extract from Source, Upserts and Deletes contained in a single transaction.  
        --	 Rollback on error
        -- --------------------------------------------------------------------------------
        BEGIN TRY
            BEGIN TRANSACTION;
            -- Debug output progress
            IF @DebugMode = 1
                SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Beginning transaction.';
            PRINT @DebugMsg;
            -- ================================================================================
            --
            -- S T E P   1.
            --
            -- Create temporary landing #table
            --
            -- ================================================================================
            CREATE TABLE #FactLaborHourUpsert
            ([EmployeeNumber]       VARCHAR(50) NOT NULL, 
             [CostCenterKey]        [INT] NOT NULL, 
             [OrgKey]               [INT] NOT NULL, 
             [DateKey]              [INT] NOT NULL, 
             [AccountSubaccountKey] [INT] NOT NULL, 
             [IsTSEF]               [VARCHAR](10) NOT NULL, 
             [PayBasisKey]          [INT] NOT NULL, 
             [DataScenarioKey]      [INT] NOT NULL, 
             [Hours]                [DECIMAL](15, 6) NOT NULL, 
             [AdjustmentFactor]     [NUMERIC](19, 4) NOT NULL, 
             [DollarExtension]      [DECIMAL](15, 6) NOT NULL, 
             [SourceSystem]         [VARCHAR](3) NULL, 
             [EDWCreatedDate]       [DATETIME2](7) NOT NULL
            );

            -- ================================================================================
            --
            -- S T E P   2.
            --
            -- Populate the Landing table from Source, and create any helper indexes
            --
            -- ================================================================================
            INSERT INTO #FactLaborHourUpsert
            EXEC dbo.spMISC_StagingTransform_FactLaborHourTarget 
                 @EDWRunDateTime = @EDWRunDateTime, 
                 @FiscalWeekEndDate = @FiscalWeekEndDate; -- @FiscalWeekEndDate needs to be provided
            -- Get how many rows were extracted from source 

            SELECT @SourceCount = COUNT(1)
            FROM #FactLaborHourUpsert;

            -- Debug output progress
            IF @DebugMode = 1
                SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Extracted ' + CONVERT(NVARCHAR(20), @SourceCount) + ' rows from Source.';
            PRINT @DebugMsg;

            -- ================================================================================
            --
            -- S T E P   3.
            --
            -- Perform the Insert / Update (Merge) / deletes required for this EDW table load
            --
            -- For this Fact load, we do not update or merge - instead we delete and reload
            --     the data for a given Fiscal Week, hence why we are so careful about batching
            --     up the ETL.
            --
            -- ================================================================================
            --
            -- Delete data for the given Fiscal Week
            --
            DELETE BING_EDW.dbo.FactLaborHour
            FROM BING_EDW.dbo.FactLaborHour T
                 INNER JOIN #FactLaborHourUpsert S ON S.DateKey = T.DateKey
                                                      AND S.DataScenarioKey = T.DataScenarioKey
                                                      AND S.SourceSystem = T.SourceSystem;
            SELECT @DeleteCount = @@ROWCOUNT;

            -- Debug output progress
            IF @DebugMode = 1
                BEGIN
                    SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Deleted ' + CONVERT(NVARCHAR(20), @DeleteCount) + ' rows from Target.';
                    PRINT @DebugMsg;
            END;
            --
            -- Insert the data for the Fiscal Week
            --
            INSERT INTO BING_EDW.dbo.FactLaborHour
            ([EmployeeNumber], 
             [CostCenterKey], 
             [OrgKey], 
             [DateKey], 
             [AccountSubaccountKey], 
             [IsTSEF], 
             [PayBasisKey], 
             [DataScenarioKey], 
             [Hours], 
			 [AdjustmentFactor],
             [DollarExtension] ,
             [SourceSystem], 
             [EDWCreatedDate]
            )
                   SELECT *
                   FROM #FactLaborHourUpsert;
            SELECT @InsertCount = @@ROWCOUNT;

            -- Debug output progress
            IF @DebugMode = 1
                BEGIN
                    SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Inserted ' + CONVERT(NVARCHAR(20), @InsertCount) + ' rows into Target.';
                    PRINT @DebugMsg;
                    SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Updated ' + CONVERT(NVARCHAR(20), @UpdateCount) + ' rows into Target.';
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
                SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Committing transaction.';
            PRINT @DebugMsg;

            --
            -- Commit the successful transaction 
            --
            COMMIT TRANSACTION;

            --
            -- Drop the temp table
            --
            DROP TABLE #FactLaborHourUpsert;

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
                SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Completing successfully.';
            PRINT @DebugMsg;
        END TRY
        BEGIN CATCH
            -- Debug output progress
            IF @DebugMode = 1
                SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Rolling back transaction.';
            PRINT @DebugMsg;
            -- Rollback the transaction
            ROLLBACK TRANSACTION;
            --
            -- Raiserror
            --	
            DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
            SELECT @ErrMsg = ERROR_MESSAGE(), 
                   @ErrSeverity = ERROR_SEVERITY();
            RAISERROR(@ErrMsg, @ErrSeverity, 1);
        END CATCH;
    END;