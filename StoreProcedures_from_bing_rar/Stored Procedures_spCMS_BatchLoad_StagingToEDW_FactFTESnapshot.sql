CREATE PROCEDURE [dbo].[spCMS_BatchLoad_StagingToEDW_FactFTESnapshot]
    @EDWRunDateTime DATETIME2 = NULL,
    @DebugMode INT = NULL
AS
-- ================================================================================
-- 
-- Stored Procedure:   spCMS_BatchLoad_StagingToEDW_FactFTESnapshot
--
-- Purpose:         Performs the Insert / Update / Delete ETL process for he FactFTESnapshot table from Staging to BING_EDW, loading data in manageable batch sizes for larger datasets.  This
--                  is to ensure we do not fill the log when performing inserts / updates on our larger tables
--
-- Parameters:		@EDWRunDateTime
--					@DebugMode - Used just for development & debug purposes, outputting helpful info back to the caller.  Not required for Production, and does not affect any core logic.
--					@DaysBack - For FactFTESnapshot load we have to go back and potentially reporcess data we have already ingested that might have changed in the source.  As it
--                  is not possible to uniquely identify records to do a Merge, we are forced to delete and re-insert.  This parameter controls how far back we go to reprocess
--                  (as of Nov 2017 we are going back 2 weeks from the LastProcessedDate)
--Usage:            EXEC dbo.spCMS_BatchLoad_StagingToEDW_FactFTESnapshot
--------------------------------------------------------------------------------------------------------------------------------------------------
-- Change Log:
--------------------------------------------------------------------------------------------------------------------------------------------------
-- Date          Modified By         Comments
--------------------------------------------------------------------------------------------------------------------------------------------------
--
-- 01/29/18		 sburke              BNG-294 - Refactored EDW FactFTESnapshot (CMS Source) load
-- 05/2/18		 tmorales			 BNG-1677 - Add filter in CMS FTE Snapshot for Adjustment Time Period
-- 11/15/18		 sburke              BNG-4423 - Add logic to look for records updated beyond the previous 2 weeks and include in the batch load
-- 05/24/19		 hhebbalu			 Removed the 14 days delete and reload logic and added the logic to load all that is changed in the staging table since last run
-- 09/02/19		 banandesi   		 BI-2248 FTE - BING EDW fix CDC Deleted Record handling
-- 01/21/2020	 Puneet				 Added code to insert section to populate all the dates in @BatchByFiscalDate (BI-2849).
-- 02/19/2020    hhebbalu            BI-3505 FTE Incremental Changes bug from UAT
-- 03/07/2022    Adevabhakthuni      BI-3798 updated logic not to load additional weeks
-- 03/11/2022    Adevabhakthuni      BI-3798 Updated the logic for inserting the changed dates in temp table 
-- 08/09/2023	 Suhas.De			 BI-8306 - Changes for Target Reload
-- ================================================================================
BEGIN
    SET NOCOUNT ON;

    --
    -- Housekeeping Variables
    --
    DECLARE @ProcName NVARCHAR(500) = OBJECT_NAME(@@PROCID);
    DECLARE @DebugMsg NVARCHAR(500);
    DECLARE @TaskName VARCHAR(100) = 'FactFTESnapshot - CMS';
	DECLARE @SourceName SYSNAME = DB_NAME();
	DECLARE @IsOverrideBatch BIT;

    --
    -- ETL status Variables
    --
    DECLARE @RowCount INT;
    DECLARE @Error INT;

    --
    -- ETL variables specific to this load
    --
    DECLARE @AuditId BIGINT;
    DECLARE @FiscalDate DATE;
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

    -- Write to AuditLog that we are starting, and get the AuditId we use for each batch we load		  
    /*
	EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog] @SourceName = @TaskName,
                                               @AuditId = @AuditId OUTPUT;
	*/

    --
    -- Determine how far back in history we have extract data for
    --
    DECLARE @LastProcessedDate DATETIME, @MinProcessingDate DATE;
	
	SELECT
		@LastProcessedDate = LastProcessedDate, @MinProcessingDate = [MinProcessingDate]
	FROM dbo.EDWETLBatchControl (NOLOCK)
    WHERE EventName = @TaskName;

    IF @LastProcessedDate IS NULL
        SET @LastProcessedDate = '19000101'; -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

    BEGIN TRY

        -- ================================================================================
        -- STEP 1.
        -- 
        -- Ascertain by what criteria we are splitting this batch load by, and build a loop
        --
        -- We build our list or dates to process data for by:
        -- 1.) Looking at the LastProcessedDate to see when we last loaded data from CMS
        -- 2.) Looking at finStudentInvoice table for any updated records (for Fees)
        -- 3.) Looking at enrlSession table for any updated records (enrollment)
        -- ================================================================================
        /*
		DECLARE @BatchByFiscalDate TABLE
        (
            FiscalDate DATE
        );
		*/
		DROP TABLE IF EXISTS #OriginalProcessingRange;
		CREATE TABLE #OriginalProcessingRange ( [BatchIdentifier] SQL_VARIANT );

        DECLARE @CMSStartDate DATE =
			(
				SELECT MIN(CAST(CreatedDate AS DATE))
				FROM dbo.enrlSession
				WHERE Deleted IS NULL
			); -- Use @CMSStartDate as the earliest dated transaction in CMS.  No point going further back than this

        INSERT INTO #OriginalProcessingRange
        (
            [BatchIdentifier]
        )
        SELECT DISTINCT
               CAST(a.BillingStartDate AS DATE) AS FiscalDate
        FROM dbo.finStudentInvoice (NOLOCK) a
            JOIN dbo.srvcatFees (NOLOCK) b
                ON a.idFees = b.idFees
                   AND a.Deleted IS NULL
                   AND b.Deleted IS NULL
        WHERE FTE > 0
              AND a.StgModifiedDate > @LastProcessedDate
			  AND CAST(a.BillingStartDate AS DATE) <= @EDWRunDateTime;


        INSERT INTO #OriginalProcessingRange
        (
            [BatchIdentifier]
        )
        SELECT FullDate
        FROM BING_EDW.dbo.DimDate (NOLOCK) dd
        WHERE FiscalWeekNumber IN (
			SELECT DISTINCT
					d.FiscalWeekNumber
			FROM dbo.enrlSession (NOLOCK) ss
			INNER JOIN dbo.enrlScheduleDay (NOLOCK) sd
				ON ss.idSessionEnrollment = sd.idSessionEnrollment
					AND ss.Deleted IS NULL
					AND sd.Deleted IS NULL
			JOIN BING_EDW.dbo.DimDate (NOLOCK) d
				ON CAST(sd.EffectiveDate AS DATE) = d.FullDate
			WHERE ss.StgModifiedDate > @LastProcessedDate

				---Added this step not to process ifthere is change in the idclassroom to avoid processing history records)
			AND ss.idSessionEnrollment NOT IN (
				SELECT DISTINCT
						A.idSessionEnrollment
				FROM dbo.enrlSession(NOLOCK) A
				INNER JOIN dbo.enrlSessionSupportFTEInc(NOLOCK) B
					ON A.idSessionEnrollment = B.idSessionEnrollment
					AND A.idProgramEnrollment = B.idProgramEnrollment
					AND A.idSiteProgramSession = A.idSiteProgramSession
					AND A.idClassroom <> B.idClassroom
			)
			AND CAST(sd.EffectiveDate AS DATE) >= @CMSStartDate
			AND sd.EndDate IS NULL
			AND sd.idScheduleStatus IN ( 2, 15 )
		)
		AND FullDate <= @EDWRunDateTime;

        INSERT INTO #OriginalProcessingRange
        (
            [BatchIdentifier]
        )
        SELECT DISTINCT
               FullDate
        FROM BING_EDW..DimDate
        WHERE FullDate
        BETWEEN @LastProcessedDate AND CONVERT(DATE, GETDATE())
		AND FullDate <= @EDWRunDateTime;

		DROP TABLE IF EXISTS #FinalProcessingRange;
		CREATE TABLE #FinalProcessingRange ( [FullDate] DATE );
		EXEC [BING_EDW].[dbo].[spEDW_StagingToEDW_GetProcessingRange]
			@SourceName = @SourceName,
			@TaskName = @TaskName,
			@MinProcessingDate = @MinProcessingDate,
			@LastProcessedDate = @LastProcessedDate,
			@DimDateColumnName = 'FullDate',
			@IsOverrideBatch = @IsOverrideBatch OUTPUT,
			@AuditID = @AuditId OUTPUT,
			@Debug = 0;

        -- ================================================================================
        -- STEP 2.
        -- 
        -- Loop through each Fiscal Year, and execute the ETL Upsert Stored Proc
        -- ================================================================================

        DECLARE cms_fact_ftesnapshot_fisc_date CURSOR FOR
        SELECT DISTINCT
               FullDate
        FROM #FinalProcessingRange --@BatchByFiscalDate
        -- WHERE FiscalDate <= @EDWRunDateTime
        ORDER BY FullDate;
        --
        -- Use cursor to loop through the values we have chosen to split this batch by
        --
        OPEN cms_fact_ftesnapshot_fisc_date;
        FETCH NEXT FROM cms_fact_ftesnapshot_fisc_date
        INTO @FiscalDate;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SELECT @DebugMsg
                = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Processing data for Fiscal Date '
                  + CONVERT(VARCHAR(10), @FiscalDate);
            PRINT @DebugMsg;
            -- ================================================================================
            -- Execute the main StagingToEDW stored proc to load data for the given batch
            -- ================================================================================
            EXEC dbo.spCMS_StagingToEDW_FactFTESnapshot @AuditId = @AuditId,
                                                        @FiscalDate = @FiscalDate,
                                                        @DebugMode = @DebugMode;
            FETCH NEXT FROM cms_fact_ftesnapshot_fisc_date
            INTO @FiscalDate;
        END;
        CLOSE cms_fact_ftesnapshot_fisc_date;
        DEALLOCATE cms_fact_ftesnapshot_fisc_date;

        -- ================================================================================
        -- STEP 3.
        --
        -- Once we have successfully ran all the batch loads, collect all the Source / Insert 
        --     / Update / Delete numbers from all the batch loads for this table, and use them
        --     to pupulate EDWEndAuditLog
        -- ================================================================================

        SELECT @SourceCount = SUM(SourceCount)
        FROM BING_EDW.dbo.EDWBatchLoadLog
        WHERE AuditId = @AuditId;
        SELECT @InsertCount = SUM(InsertCount)
        FROM BING_EDW.dbo.EDWBatchLoadLog
        WHERE AuditId = @AuditId;
        SELECT @UpdateCount = SUM(UpdateCount)
        FROM BING_EDW.dbo.EDWBatchLoadLog
        WHERE AuditId = @AuditId;
        SELECT @DeleteCount = SUM(DeleteCount)
        FROM BING_EDW.dbo.EDWBatchLoadLog
        WHERE AuditId = @AuditId;

        -- Write the successful load to EDWAuditLog
        EXEC [BING_EDW].[dbo].[spEDWEndAuditLog]
			@InsertCount = @InsertCount,
            @UpdateCount = @UpdateCount,
            @DeleteCount = @DeleteCount,
            @SourceCount = @SourceCount,
            @AuditId = @AuditId;

        -- Also write the successful load to EDWETLBatchControl, so we know how far back in history
        --     we have to go in the next ETL run
		IF (@IsOverrideBatch = 1)
				BEGIN
					UPDATE [BING_EDW].[dbo].[EDWBatchOverride]
						SET [IsActive] = 0
					WHERE [SourceName] = @SourceName
					AND [TaskName] = @TaskName
					AND [IsActive] = 1;
				END;
			ELSE
				BEGIN
					EXEC dbo.spCMS_StagingEDWETLBatchControl @TaskName;
				END;

        ----------------------------------------------------------------------------------------------------
        -- Step 4 Truncate and reload the support table 
        ------------------------------------------------------------------------------------------------------
        TRUNCATE TABLE dbo.enrlSessionSupportFTEInc;

        INSERT INTO dbo.enrlSessionSupportFTEInc
        SELECT idSessionEnrollment,
               idProgramEnrollment,
               idSiteProgramSession,
               idClassroom,
               idDisenrollmentReason,
               CreatedDate,
               CreatedBy,
               ModifiedDate,
               ModifiedBy,
               isDelete,
               DisEnrollmentNote,
               StgCreatedDate,
               StgCreatedBy,
               StgModifiedDate,
               StgModifiedBy,
               Deleted
        FROM CMS_Staging..enrlSession;
    END TRY
    BEGIN CATCH
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
