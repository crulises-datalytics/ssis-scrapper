
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spGL_StagingToEDW_DimDate'
)
    DROP PROCEDURE dbo.spGL_StagingToEDW_DimDate;
GO
*/
CREATE PROCEDURE dbo.spGL_StagingToEDW_DimDate
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingToEDW_DimDate
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the DimDate table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                             sub-procedure spGL_StagingTransform_DimDate, 
    --                             and create any helper indexes
    --                         Step 3: Perform the Insert / Update (Merge) / deletes required 
    --                             for this EDW table load
    --                         Step 4: Execute any automated tests associated with this EDW table load
    --                         Step 5: Output Source / Insert / Update / Delete counts to caller, 
    --                             commit the transaction, and tidy-up
    --
    -- Parameters:             @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                             making numerous GETDATE() calls  
    --                         @DebugMode - Used just for development & debug purposes,
    --                             outputting helpful info back to the caller.  Not
    --                             required for Production, and does not affect any
    --                             core logic.
    --
    -- Returns:                Single-row results set containing the following columns:
    --                             SourceCount - Number of rows extracted from source
    --                             InsertCount - Number or rows inserted to target table
    --                             UpdateCount - Number or rows updated in target table
    --                             DeleteCount - Number or rows deleted in target table
    --
    -- Usage:                  EXEC dbo.spGL_StagingToEDW_DimDate @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    --
    -- 10/20/17     sburke          BNG-708 - Extend dates returned to pre-2011
    --  8/14/18     sburke          BNG-727 - Addition of Payroll Calendar dates	
    --  8/15/18     sburke          BNG-3582 - Addition of Academic and Back-to-School Dates
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimDate';
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
             CREATE TABLE #DimDateUpsert
             ([DateKey]                      [INT] NOT NULL,
              [FullDate]                     [DATE] NOT NULL,
              [FullDateName]                 [VARCHAR](10) NOT NULL,
              [WeekdayWeekend]               [VARCHAR](10) NOT NULL,
              [HolidayName]                  [VARCHAR](50) NOT NULL,
              [HolidayFlag]                  [VARCHAR](50) NOT NULL,
              [HolidayFiscalWeekFlag]        [VARCHAR](50) NOT NULL,
              [CalendarFirstDayOfMonthFlag]  [VARCHAR](50) NOT NULL,
              [CalendarLastDayOfMonthFlag]   [VARCHAR](50) NOT NULL,
              [CalendarFirstWeekOfMonthFlag] [VARCHAR](50) NOT NULL,
              [CalendarLastWeekOfMonthFlag]  [VARCHAR](50) NOT NULL,
              [CalendarDaySequenceNumber]    [INT] NOT NULL,
              [CalendarDayOfWeekNumber]      [INT] NOT NULL,
              [CalendarDayOfWeekName]        [VARCHAR](10) NOT NULL,
              [CalendarDayOfWeekNameShort]   [VARCHAR](10) NOT NULL,
              [CalendarDayOfMonthNumber]     [INT] NOT NULL,
              [CalendarDayOfQuarterNumber]   [INT] NOT NULL,
              [CalendarDayOfYearNumber]      [INT] NOT NULL,
              [CalendarWeekNumber]           [INT] NOT NULL,
              [CalendarWeekName]             [VARCHAR](50) NOT NULL,
              [CalendarWeekOfMonthNumber]    [INT] NOT NULL,
              [CalendarWeekOfMonthName]      [VARCHAR](50) NOT NULL,
              [CalendarWeekOfYearNumber]     [INT] NOT NULL,
              [CalendarWeekOfYearName]       [VARCHAR](50) NOT NULL,
              [CalendarWeekStartDate]        [DATE] NOT NULL,
              [CalendarWeekEndDate]          [DATE] NOT NULL,
              [CalendarMonthNumber]          [INT] NOT NULL,
              [CalendarMonthName]            [VARCHAR](50) NOT NULL,
              [CalendarMonthOfYearNumber]    [INT] NOT NULL,
              [CalendarMonthOfYearName]      [VARCHAR](50) NOT NULL,
              [CalendarMonthOfYearNameShort] [VARCHAR](10) NOT NULL,
              [CalendarMonthStartDate]       [DATE] NOT NULL,
              [CalendarMonthEndDate]         [DATE] NOT NULL,
              [CalendarQuarterNumber]        [INT] NOT NULL,
              [CalendarQuarterName]          [VARCHAR](50) NOT NULL,
              [CalendarQuarterOfYearNumber]  [INT] NOT NULL,
              [CalendarQuarterOfYearName]    [VARCHAR](50) NOT NULL,
              [CalendarQuarterStartDate]     [DATE] NOT NULL,
              [CalendarQuarterEndDate]       [DATE] NOT NULL,
              [CalendarYearNumber]           [INT] NOT NULL,
              [CalendarYearName]             [VARCHAR](50) NOT NULL,
              [CalendarYearStartDate]        [DATE] NOT NULL,
              [CalendarYearEndDate]          [DATE] NOT NULL,
              [FiscalDayOfWeekNumber]        [INT] NOT NULL,
              [FiscalDayOfPeriodNumber]      [INT] NOT NULL,
              [FiscalDayOfQuarterNumber]     [INT] NOT NULL,
              [FiscalDayOfYearNumber]        [INT] NOT NULL,
              [FiscalWeekNumber]             [INT] NOT NULL,
              [FiscalWeekName]               [VARCHAR](50) NOT NULL,
              [FiscalWeekOfPeriodNumber]     [INT] NOT NULL,
              [FiscalWeekOfPeriodName]       [VARCHAR](50) NOT NULL,
              [FiscalWeekOfQuarterNumber]    [INT] NOT NULL,
              [FiscalWeekOfQuarterName]      [VARCHAR](50) NOT NULL,
              [FiscalWeekOfYearNumber]       [INT] NOT NULL,
              [FiscalWeekOfYearName]         [VARCHAR](50) NOT NULL,
              [FiscalWeekSequenceNumber]     [INT] NOT NULL,
              [FiscalWeekStartDate]          [DATE] NOT NULL,
              [FiscalWeekEndDate]            [DATE] NOT NULL,
              [FiscalPeriodNumber]           [INT] NOT NULL,
              [FiscalPeriodName]             [VARCHAR](50) NOT NULL,
              [FiscalPeriodType]             [VARCHAR](10) NOT NULL,
              [FiscalPeriodOfYearNumber]     [INT] NOT NULL,
              [FiscalPeriodOfYearName]       [VARCHAR](50) NOT NULL,
              [FiscalPeriodSequenceNumber]   [INT] NOT NULL,
              [FiscalPeriodStartDate]        [DATE] NOT NULL,
              [FiscalPeriodEndDate]          [DATE] NOT NULL,
              [FiscalQuarterNumber]          [INT] NOT NULL,
              [FiscalQuarterName]            [VARCHAR](50) NOT NULL,
              [FiscalQuarterOfYearNumber]    [INT] NOT NULL,
              [FiscalQuarterOfYearName]      [VARCHAR](50) NOT NULL,
              [FiscalQuarterSequenceNumber]  [INT] NOT NULL,
              [FiscalQuarterStartDate]       [DATE] NOT NULL,
              [FiscalQuarterEndDate]         [DATE] NOT NULL,
              [FiscalYearNumber]             [INT] NOT NULL,
              [FiscalYearName]               [VARCHAR](50) NOT NULL,
              [FiscalYearStartDate]          [DATE] NOT NULL,
              [FiscalYearEndDate]            [DATE] NOT NULL,
              [PayrollStartDate]             [DATE] NOT NULL,
              [PayrollEndDate]               [DATE] NOT NULL,
              [PayrollCheckDate]             [DATE] NOT NULL,
              [BTSPeriodFlag]                [VARCHAR](20) NOT NULL,
              [AcademicYearNumber]          [INT] NOT NULL,
              [BTSYearNumber]                [INT] NOT NULL,
              [EDWCreatedDate]               [DATETIME2](7) NOT NULL,
              [EDWCreatedBy]                 [VARCHAR](50) NOT NULL,
              [EDWModifiedDate]              [DATETIME2](7) NOT NULL,
              [EDWModifiedBy]                [VARCHAR](50) NOT NULL,
              [Deleted]                      [DATETIME2](7) NULL
             );
          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimDateUpsert
             EXEC dbo.spGL_StagingTransform_DimDate
                  @EDWRunDateTime;


		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimDateUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimDateUpsert ON #DimDateUpsert
             ([DateKey] ASC
             );

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
             MERGE [BING_EDW].[dbo].[DimDate] T
             USING #DimDateUpsert S
             ON(S.DateKey = T.DateKey)
                 WHEN MATCHED AND S.Deleted IS NULL
                                  AND (S.FullDate <> T.FullDate
                                       OR S.FullDateName <> T.FullDateName
                                       OR S.WeekdayWeekend <> T.WeekdayWeekend
                                       OR S.HolidayName <> T.HolidayName
                                       OR S.HolidayFlag <> T.HolidayFlag
                                       OR S.HolidayFiscalWeekFlag <> T.HolidayFiscalWeekFlag
                                       OR S.CalendarFirstDayOfMonthFlag <> T.CalendarFirstDayOfMonthFlag
                                       OR S.CalendarLastDayOfMonthFlag <> T.CalendarLastDayOfMonthFlag
                                       OR S.CalendarFirstWeekOfMonthFlag <> T.CalendarFirstWeekOfMonthFlag
                                       OR S.CalendarLastWeekOfMonthFlag <> T.CalendarLastWeekOfMonthFlag
                                       OR S.CalendarDaySequenceNumber <> T.CalendarDaySequenceNumber
                                       OR S.CalendarDayOfWeekNumber <> T.CalendarDayOfWeekNumber
                                       OR S.CalendarDayOfWeekName <> T.CalendarDayOfWeekName
                                       OR S.CalendarDayOfWeekNameShort <> T.CalendarDayOfWeekNameShort
                                       OR S.CalendarDayOfMonthNumber <> T.CalendarDayOfMonthNumber
                                       OR S.CalendarDayOfQuarterNumber <> T.CalendarDayOfQuarterNumber
                                       OR S.CalendarDayOfYearNumber <> T.CalendarDayOfYearNumber
                                       OR S.CalendarWeekNumber <> T.CalendarWeekNumber
                                       OR S.CalendarWeekName <> T.CalendarWeekName
                                       OR S.CalendarWeekOfMonthNumber <> T.CalendarWeekOfMonthNumber
                                       OR S.CalendarWeekOfMonthName <> T.CalendarWeekOfMonthName
                                       OR S.CalendarWeekOfYearNumber <> T.CalendarWeekOfYearNumber
                                       OR S.CalendarWeekOfYearName <> T.CalendarWeekOfYearName
                                       OR S.CalendarWeekStartDate <> T.CalendarWeekStartDate
                                       OR S.CalendarWeekEndDate <> T.CalendarWeekEndDate
                                       OR S.CalendarMonthNumber <> T.CalendarMonthNumber
                                       OR S.CalendarMonthName <> T.CalendarMonthName
                                       OR S.CalendarMonthOfYearNumber <> T.CalendarMonthOfYearNumber
                                       OR S.CalendarMonthOfYearName <> T.CalendarMonthOfYearName
                                       OR S.CalendarMonthOfYearNameShort <> T.CalendarMonthOfYearNameShort
                                       OR S.CalendarMonthStartDate <> T.CalendarMonthStartDate
                                       OR S.CalendarMonthEndDate <> T.CalendarMonthEndDate
                                       OR S.CalendarQuarterNumber <> T.CalendarQuarterNumber
                                       OR S.CalendarQuarterName <> T.CalendarQuarterName
                                       OR S.CalendarQuarterOfYearNumber <> T.CalendarQuarterOfYearNumber
                                       OR S.CalendarQuarterOfYearName <> T.CalendarQuarterOfYearName
                                       OR S.CalendarQuarterStartDate <> T.CalendarQuarterStartDate
                                       OR S.CalendarQuarterEndDate <> T.CalendarQuarterEndDate
                                       OR S.CalendarYearNumber <> T.CalendarYearNumber
                                       OR S.CalendarYearName <> T.CalendarYearName
                                       OR S.CalendarYearStartDate <> T.CalendarYearStartDate
                                       OR S.CalendarYearEndDate <> T.CalendarYearEndDate
                                       OR S.FiscalDayOfWeekNumber <> T.FiscalDayOfWeekNumber
                                       OR S.FiscalDayOfPeriodNumber <> T.FiscalDayOfPeriodNumber
                                       OR S.FiscalDayOfQuarterNumber <> T.FiscalDayOfQuarterNumber
                                       OR S.FiscalDayOfYearNumber <> T.FiscalDayOfYearNumber
                                       OR S.FiscalWeekNumber <> T.FiscalWeekNumber
                                       OR S.FiscalWeekName <> T.FiscalWeekName
                                       OR S.FiscalWeekOfPeriodNumber <> T.FiscalWeekOfPeriodNumber
                                       OR S.FiscalWeekOfPeriodName <> T.FiscalWeekOfPeriodName
                                       OR S.FiscalWeekOfQuarterNumber <> T.FiscalWeekOfQuarterNumber
                                       OR S.FiscalWeekOfQuarterName <> T.FiscalWeekOfQuarterName
                                       OR S.FiscalWeekOfYearNumber <> T.FiscalWeekOfYearNumber
                                       OR S.FiscalWeekOfYearName <> T.FiscalWeekOfYearName
                                       OR S.FiscalWeekSequenceNumber <> T.FiscalWeekSequenceNumber
                                       OR S.FiscalWeekStartDate <> T.FiscalWeekStartDate
                                       OR S.FiscalWeekEndDate <> T.FiscalWeekEndDate
                                       OR S.FiscalPeriodNumber <> T.FiscalPeriodNumber
                                       OR S.FiscalPeriodName <> T.FiscalPeriodName
                                       OR S.FiscalPeriodType <> T.FiscalPeriodType
                                       OR S.FiscalPeriodOfYearNumber <> T.FiscalPeriodOfYearNumber
                                       OR S.FiscalPeriodOfYearName <> T.FiscalPeriodOfYearName
                                       OR S.FiscalPeriodSequenceNumber <> T.FiscalPeriodSequenceNumber
                                       OR S.FiscalPeriodStartDate <> T.FiscalPeriodStartDate
                                       OR S.FiscalPeriodEndDate <> T.FiscalPeriodEndDate
                                       OR S.FiscalQuarterNumber <> T.FiscalQuarterNumber
                                       OR S.FiscalQuarterName <> T.FiscalQuarterName
                                       OR S.FiscalQuarterOfYearNumber <> T.FiscalQuarterOfYearNumber
                                       OR S.FiscalQuarterOfYearName <> T.FiscalQuarterOfYearName
                                       OR S.FiscalQuarterSequenceNumber <> T.FiscalQuarterSequenceNumber
                                       OR S.FiscalQuarterStartDate <> T.FiscalQuarterStartDate
                                       OR S.FiscalQuarterEndDate <> T.FiscalQuarterEndDate
                                       OR S.FiscalYearNumber <> T.FiscalYearNumber
                                       OR S.FiscalYearName <> T.FiscalYearName
                                       OR S.FiscalYearStartDate <> T.FiscalYearStartDate
                                       OR S.FiscalYearEndDate <> T.FiscalYearEndDate
                                       OR S.PayrollStartDate <> T.PayrollStartDate
                                       OR S.PayrollEndDate <> T.PayrollEndDate
                                       OR S.PayrollCheckDate <> T.PayrollCheckDate
                                       OR S.BTSPeriodFlag <> T.BTSPeriodFlag
                                       OR S.AcademicYearNumber <> T.AcademicYearNumber
                                       OR S.BTSYearNumber <> T.BTSYearNumber
                                       OR T.Deleted IS NOT NULL)
                 THEN UPDATE SET
                                 T.FullDate = S.FullDate,
                                 T.FullDateName = S.FullDateName,
                                 T.WeekdayWeekend = S.WeekdayWeekend,
                                 T.HolidayName = S.HolidayName,
                                 T.HolidayFlag = S.HolidayFlag,
                                 T.HolidayFiscalWeekFlag = S.HolidayFiscalWeekFlag,
                                 T.CalendarFirstDayOfMonthFlag = S.CalendarFirstDayOfMonthFlag,
                                 T.CalendarLastDayOfMonthFlag = S.CalendarLastDayOfMonthFlag,
                                 T.CalendarFirstWeekOfMonthFlag = S.CalendarFirstWeekOfMonthFlag,
                                 T.CalendarLastWeekOfMonthFlag = S.CalendarLastWeekOfMonthFlag,
                                 T.CalendarDaySequenceNumber = S.CalendarDaySequenceNumber,
                                 T.CalendarDayOfWeekNumber = S.CalendarDayOfWeekNumber,
                                 T.CalendarDayOfWeekName = S.CalendarDayOfWeekName,
                                 T.CalendarDayOfWeekNameShort = S.CalendarDayOfWeekNameShort,
                                 T.CalendarDayOfMonthNumber = S.CalendarDayOfMonthNumber,
                                 T.CalendarDayOfQuarterNumber = S.CalendarDayOfQuarterNumber,
                                 T.CalendarDayOfYearNumber = S.CalendarDayOfYearNumber,
                                 T.CalendarWeekNumber = S.CalendarWeekNumber,
                                 T.CalendarWeekName = S.CalendarWeekName,
                                 T.CalendarWeekOfMonthNumber = S.CalendarWeekOfMonthNumber,
                                 T.CalendarWeekOfMonthName = S.CalendarWeekOfMonthName,
                                 T.CalendarWeekOfYearNumber = S.CalendarWeekOfYearNumber,
                                 T.CalendarWeekOfYearName = S.CalendarWeekOfYearName,
                                 T.CalendarWeekStartDate = S.CalendarWeekStartDate,
                                 T.CalendarWeekEndDate = S.CalendarWeekEndDate,
                                 T.CalendarMonthNumber = S.CalendarMonthNumber,
                                 T.CalendarMonthName = S.CalendarMonthName,
                                 T.CalendarMonthOfYearNumber = S.CalendarMonthOfYearNumber,
                                 T.CalendarMonthOfYearName = S.CalendarMonthOfYearName,
                                 T.CalendarMonthOfYearNameShort = S.CalendarMonthOfYearNameShort,
                                 T.CalendarMonthStartDate = S.CalendarMonthStartDate,
                                 T.CalendarMonthEndDate = S.CalendarMonthEndDate,
                                 T.CalendarQuarterNumber = S.CalendarQuarterNumber,
                                 T.CalendarQuarterName = S.CalendarQuarterName,
                                 T.CalendarQuarterOfYearNumber = S.CalendarQuarterOfYearNumber,
                                 T.CalendarQuarterOfYearName = S.CalendarQuarterOfYearName,
                                 T.CalendarQuarterStartDate = S.CalendarQuarterStartDate,
                                 T.CalendarQuarterEndDate = S.CalendarQuarterEndDate,
                                 T.CalendarYearNumber = S.CalendarYearNumber,
                                 T.CalendarYearName = S.CalendarYearName,
                                 T.CalendarYearStartDate = S.CalendarYearStartDate,
                                 T.CalendarYearEndDate = S.CalendarYearEndDate,
                                 T.FiscalDayOfWeekNumber = S.FiscalDayOfWeekNumber,
                                 T.FiscalDayOfPeriodNumber = S.FiscalDayOfPeriodNumber,
                                 T.FiscalDayOfQuarterNumber = S.FiscalDayOfQuarterNumber,
                                 T.FiscalDayOfYearNumber = S.FiscalDayOfYearNumber,
                                 T.FiscalWeekNumber = S.FiscalWeekNumber,
                                 T.FiscalWeekName = S.FiscalWeekName,
                                 T.FiscalWeekOfPeriodNumber = S.FiscalWeekOfPeriodNumber,
                                 T.FiscalWeekOfPeriodName = S.FiscalWeekOfPeriodName,
                                 T.FiscalWeekOfQuarterNumber = S.FiscalWeekOfQuarterNumber,
                                 T.FiscalWeekOfQuarterName = S.FiscalWeekOfQuarterName,
                                 T.FiscalWeekOfYearNumber = S.FiscalWeekOfYearNumber,
                                 T.FiscalWeekOfYearName = S.FiscalWeekOfYearName,
                                 T.FiscalWeekSequenceNumber = S.FiscalWeekSequenceNumber,
                                 T.FiscalWeekStartDate = S.FiscalWeekStartDate,
                                 T.FiscalWeekEndDate = S.FiscalWeekEndDate,
                                 T.FiscalPeriodNumber = S.FiscalPeriodNumber,
                                 T.FiscalPeriodName = S.FiscalPeriodName,
                                 T.FiscalPeriodType = S.FiscalPeriodType,
                                 T.FiscalPeriodOfYearNumber = S.FiscalPeriodOfYearNumber,
                                 T.FiscalPeriodOfYearName = S.FiscalPeriodOfYearName,
                                 T.FiscalPeriodSequenceNumber = S.FiscalPeriodSequenceNumber,
                                 T.FiscalPeriodStartDate = S.FiscalPeriodStartDate,
                                 T.FiscalPeriodEndDate = S.FiscalPeriodEndDate,
                                 T.FiscalQuarterNumber = S.FiscalQuarterNumber,
                                 T.FiscalQuarterName = S.FiscalQuarterName,
                                 T.FiscalQuarterOfYearNumber = S.FiscalQuarterOfYearNumber,
                                 T.FiscalQuarterOfYearName = S.FiscalQuarterOfYearName,
                                 T.FiscalQuarterSequenceNumber = S.FiscalQuarterSequenceNumber,
                                 T.FiscalQuarterStartDate = S.FiscalQuarterStartDate,
                                 T.FiscalQuarterEndDate = S.FiscalQuarterEndDate,
                                 T.FiscalYearNumber = S.FiscalYearNumber,
                                 T.FiscalYearName = S.FiscalYearName,
                                 T.FiscalYearStartDate = S.FiscalYearStartDate,
                                 T.FiscalYearEndDate = S.FiscalYearEndDate,
                                 T.PayrollStartDate = S.PayrollStartDate,
                                 T.PayrollEndDate = S.PayrollEndDate,
                                 T.PayrollCheckDate = S.PayrollCheckDate,
                                 T.BTSPeriodFlag = S.BTSPeriodFlag,
                                 T.AcademicYearNumber = S.AcademicYearNumber,
                                 T.BTSYearNumber = S.BTSYearNumber,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWCreatedBy = S.EDWCreatedBy,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.EDWModifiedBy = S.EDWModifiedBy,
                                 T.Deleted = S.Deleted
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(DateKey,
                          FullDate,
                          FullDateName,
                          WeekdayWeekend,
                          HolidayName,
                          HolidayFlag,
                          HolidayFiscalWeekFlag,
                          CalendarFirstDayOfMonthFlag,
                          CalendarLastDayOfMonthFlag,
                          CalendarFirstWeekOfMonthFlag,
                          CalendarLastWeekOfMonthFlag,
                          CalendarDaySequenceNumber,
                          CalendarDayOfWeekNumber,
                          CalendarDayOfWeekName,
                          CalendarDayOfWeekNameShort,
                          CalendarDayOfMonthNumber,
                          CalendarDayOfQuarterNumber,
                          CalendarDayOfYearNumber,
                          CalendarWeekNumber,
                          CalendarWeekName,
                          CalendarWeekOfMonthNumber,
                          CalendarWeekOfMonthName,
                          CalendarWeekOfYearNumber,
                          CalendarWeekOfYearName,
                          CalendarWeekStartDate,
                          CalendarWeekEndDate,
                          CalendarMonthNumber,
                          CalendarMonthName,
                          CalendarMonthOfYearNumber,
                          CalendarMonthOfYearName,
                          CalendarMonthOfYearNameShort,
                          CalendarMonthStartDate,
                          CalendarMonthEndDate,
                          CalendarQuarterNumber,
                          CalendarQuarterName,
                          CalendarQuarterOfYearNumber,
                          CalendarQuarterOfYearName,
                          CalendarQuarterStartDate,
                          CalendarQuarterEndDate,
                          CalendarYearNumber,
                          CalendarYearName,
                          CalendarYearStartDate,
                          CalendarYearEndDate,
                          FiscalDayOfWeekNumber,
                          FiscalDayOfPeriodNumber,
                          FiscalDayOfQuarterNumber,
                          FiscalDayOfYearNumber,
                          FiscalWeekNumber,
                          FiscalWeekName,
                          FiscalWeekOfPeriodNumber,
                          FiscalWeekOfPeriodName,
                          FiscalWeekOfQuarterNumber,
                          FiscalWeekOfQuarterName,
                          FiscalWeekOfYearNumber,
                          FiscalWeekOfYearName,
                          FiscalWeekSequenceNumber,
                          FiscalWeekStartDate,
                          FiscalWeekEndDate,
                          FiscalPeriodNumber,
                          FiscalPeriodName,
                          FiscalPeriodType,
                          FiscalPeriodOfYearNumber,
                          FiscalPeriodOfYearName,
                          FiscalPeriodSequenceNumber,
                          FiscalPeriodStartDate,
                          FiscalPeriodEndDate,
                          FiscalQuarterNumber,
                          FiscalQuarterName,
                          FiscalQuarterOfYearNumber,
                          FiscalQuarterOfYearName,
                          FiscalQuarterSequenceNumber,
                          FiscalQuarterStartDate,
                          FiscalQuarterEndDate,
                          FiscalYearNumber,
                          FiscalYearName,
                          FiscalYearStartDate,
                          FiscalYearEndDate,
                          PayrollStartDate,
                          PayrollEndDate,
                          PayrollCheckDate,
                          BTSPeriodFlag,
                          AcademicYearNumber,
                          BTSYearNumber,
                          EDWCreatedDate,
                          EDWCreatedBy,
                          EDWModifiedDate,
                          EDWModifiedBy,
                          Deleted)
                   VALUES
             (DateKey,
              FullDate,
              FullDateName,
              WeekdayWeekend,
              HolidayName,
              HolidayFlag,
              HolidayFiscalWeekFlag,
              CalendarFirstDayOfMonthFlag,
              CalendarLastDayOfMonthFlag,
              CalendarFirstWeekOfMonthFlag,
              CalendarLastWeekOfMonthFlag,
              CalendarDaySequenceNumber,
              CalendarDayOfWeekNumber,
              CalendarDayOfWeekName,
              CalendarDayOfWeekNameShort,
              CalendarDayOfMonthNumber,
              CalendarDayOfQuarterNumber,
              CalendarDayOfYearNumber,
              CalendarWeekNumber,
              CalendarWeekName,
              CalendarWeekOfMonthNumber,
              CalendarWeekOfMonthName,
              CalendarWeekOfYearNumber,
              CalendarWeekOfYearName,
              CalendarWeekStartDate,
              CalendarWeekEndDate,
              CalendarMonthNumber,
              CalendarMonthName,
              CalendarMonthOfYearNumber,
              CalendarMonthOfYearName,
              CalendarMonthOfYearNameShort,
              CalendarMonthStartDate,
              CalendarMonthEndDate,
              CalendarQuarterNumber,
              CalendarQuarterName,
              CalendarQuarterOfYearNumber,
              CalendarQuarterOfYearName,
              CalendarQuarterStartDate,
              CalendarQuarterEndDate,
              CalendarYearNumber,
              CalendarYearName,
              CalendarYearStartDate,
              CalendarYearEndDate,
              FiscalDayOfWeekNumber,
              FiscalDayOfPeriodNumber,
              FiscalDayOfQuarterNumber,
              FiscalDayOfYearNumber,
              FiscalWeekNumber,
              FiscalWeekName,
              FiscalWeekOfPeriodNumber,
              FiscalWeekOfPeriodName,
              FiscalWeekOfQuarterNumber,
              FiscalWeekOfQuarterName,
              FiscalWeekOfYearNumber,
              FiscalWeekOfYearName,
              FiscalWeekSequenceNumber,
              FiscalWeekStartDate,
              FiscalWeekEndDate,
              FiscalPeriodNumber,
              FiscalPeriodName,
              FiscalPeriodType,
              FiscalPeriodOfYearNumber,
              FiscalPeriodOfYearName,
              FiscalPeriodSequenceNumber,
              FiscalPeriodStartDate,
              FiscalPeriodEndDate,
              FiscalQuarterNumber,
              FiscalQuarterName,
              FiscalQuarterOfYearNumber,
              FiscalQuarterOfYearName,
              FiscalQuarterSequenceNumber,
              FiscalQuarterStartDate,
              FiscalQuarterEndDate,
              FiscalYearNumber,
              FiscalYearName,
              FiscalYearStartDate,
              FiscalYearEndDate,
              PayrollStartDate,
              PayrollEndDate,
              PayrollCheckDate,
              BTSPeriodFlag,
              AcademicYearNumber,
              BTSYearNumber,
              EDWCreatedDate,
              EDWCreatedBy,
              EDWModifiedDate,
              EDWModifiedBy,
              Deleted
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
           
		   
		   --
		   -- Perform the Merge statement for soft deletes
		   --
             MERGE [BING_EDW].[dbo].[DimDate] T
             USING #DimDateUpsert S
             ON(S.DateKey = T.DateKey)
                 WHEN MATCHED AND(S.Deleted IS NOT NULL
                                  AND T.Deleted IS NULL)
                 THEN UPDATE SET
                                 T.FullDate = S.FullDate,
                                 T.FullDateName = S.FullDateName,
                                 T.WeekdayWeekend = S.WeekdayWeekend,
                                 T.HolidayName = S.HolidayName,
                                 T.HolidayFlag = S.HolidayFlag,
                                 T.HolidayFiscalWeekFlag = S.HolidayFiscalWeekFlag,
                                 T.CalendarFirstDayOfMonthFlag = S.CalendarFirstDayOfMonthFlag,
                                 T.CalendarLastDayOfMonthFlag = S.CalendarLastDayOfMonthFlag,
                                 T.CalendarFirstWeekOfMonthFlag = S.CalendarFirstWeekOfMonthFlag,
                                 T.CalendarLastWeekOfMonthFlag = S.CalendarLastWeekOfMonthFlag,
                                 T.CalendarDaySequenceNumber = S.CalendarDaySequenceNumber,
                                 T.CalendarDayOfWeekNumber = S.CalendarDayOfWeekNumber,
                                 T.CalendarDayOfWeekName = S.CalendarDayOfWeekName,
                                 T.CalendarDayOfWeekNameShort = S.CalendarDayOfWeekNameShort,
                                 T.CalendarDayOfMonthNumber = S.CalendarDayOfMonthNumber,
                                 T.CalendarDayOfQuarterNumber = S.CalendarDayOfQuarterNumber,
                                 T.CalendarDayOfYearNumber = S.CalendarDayOfYearNumber,
                                 T.CalendarWeekNumber = S.CalendarWeekNumber,
                                 T.CalendarWeekName = S.CalendarWeekName,
                                 T.CalendarWeekOfMonthNumber = S.CalendarWeekOfMonthNumber,
                                 T.CalendarWeekOfMonthName = S.CalendarWeekOfMonthName,
                                 T.CalendarWeekOfYearNumber = S.CalendarWeekOfYearNumber,
                                 T.CalendarWeekOfYearName = S.CalendarWeekOfYearName,
                                 T.CalendarWeekStartDate = S.CalendarWeekStartDate,
                                 T.CalendarWeekEndDate = S.CalendarWeekEndDate,
                                 T.CalendarMonthNumber = S.CalendarMonthNumber,
                                 T.CalendarMonthName = S.CalendarMonthName,
                                 T.CalendarMonthOfYearNumber = S.CalendarMonthOfYearNumber,
                                 T.CalendarMonthOfYearName = S.CalendarMonthOfYearName,
                                 T.CalendarMonthOfYearNameShort = S.CalendarMonthOfYearNameShort,
                                 T.CalendarMonthStartDate = S.CalendarMonthStartDate,
                                 T.CalendarMonthEndDate = S.CalendarMonthEndDate,
                                 T.CalendarQuarterNumber = S.CalendarQuarterNumber,
                                 T.CalendarQuarterName = S.CalendarQuarterName,
                                 T.CalendarQuarterOfYearNumber = S.CalendarQuarterOfYearNumber,
                                 T.CalendarQuarterOfYearName = S.CalendarQuarterOfYearName,
                                 T.CalendarQuarterStartDate = S.CalendarQuarterStartDate,
                                 T.CalendarQuarterEndDate = S.CalendarQuarterEndDate,
                                 T.CalendarYearNumber = S.CalendarYearNumber,
                                 T.CalendarYearName = S.CalendarYearName,
                                 T.CalendarYearStartDate = S.CalendarYearStartDate,
                                 T.CalendarYearEndDate = S.CalendarYearEndDate,
                                 T.FiscalDayOfWeekNumber = S.FiscalDayOfWeekNumber,
                                 T.FiscalDayOfPeriodNumber = S.FiscalDayOfPeriodNumber,
                                 T.FiscalDayOfQuarterNumber = S.FiscalDayOfQuarterNumber,
                                 T.FiscalDayOfYearNumber = S.FiscalDayOfYearNumber,
                                 T.FiscalWeekNumber = S.FiscalWeekNumber,
                                 T.FiscalWeekName = S.FiscalWeekName,
                                 T.FiscalWeekOfPeriodNumber = S.FiscalWeekOfPeriodNumber,
                                 T.FiscalWeekOfPeriodName = S.FiscalWeekOfPeriodName,
                                 T.FiscalWeekOfQuarterNumber = S.FiscalWeekOfQuarterNumber,
                                 T.FiscalWeekOfQuarterName = S.FiscalWeekOfQuarterName,
                                 T.FiscalWeekOfYearNumber = S.FiscalWeekOfYearNumber,
                                 T.FiscalWeekOfYearName = S.FiscalWeekOfYearName,
                                 T.FiscalWeekSequenceNumber = S.FiscalWeekSequenceNumber,
                                 T.FiscalWeekStartDate = S.FiscalWeekStartDate,
                                 T.FiscalWeekEndDate = S.FiscalWeekEndDate,
                                 T.FiscalPeriodNumber = S.FiscalPeriodNumber,
                                 T.FiscalPeriodName = S.FiscalPeriodName,
                                 T.FiscalPeriodType = S.FiscalPeriodType,
                                 T.FiscalPeriodOfYearNumber = S.FiscalPeriodOfYearNumber,
                                 T.FiscalPeriodOfYearName = S.FiscalPeriodOfYearName,
                                 T.FiscalPeriodSequenceNumber = S.FiscalPeriodSequenceNumber,
                                 T.FiscalPeriodStartDate = S.FiscalPeriodStartDate,
                                 T.FiscalPeriodEndDate = S.FiscalPeriodEndDate,
                                 T.FiscalQuarterNumber = S.FiscalQuarterNumber,
                                 T.FiscalQuarterName = S.FiscalQuarterName,
                                 T.FiscalQuarterOfYearNumber = S.FiscalQuarterOfYearNumber,
                                 T.FiscalQuarterOfYearName = S.FiscalQuarterOfYearName,
                                 T.FiscalQuarterSequenceNumber = S.FiscalQuarterSequenceNumber,
                                 T.FiscalQuarterStartDate = S.FiscalQuarterStartDate,
                                 T.FiscalQuarterEndDate = S.FiscalQuarterEndDate,
                                 T.FiscalYearNumber = S.FiscalYearNumber,
                                 T.FiscalYearName = S.FiscalYearName,
                                 T.FiscalYearStartDate = S.FiscalYearStartDate,
                                 T.FiscalYearEndDate = S.FiscalYearEndDate,
                                 T.PayrollStartDate = S.PayrollStartDate,
                                 T.PayrollEndDate = S.PayrollEndDate,
                                 T.PayrollCheckDate = S.PayrollCheckDate,
						   T.BTSPeriodFlag = S.BTSPeriodFlag,
                                 T.AcademicYearNumber = S.AcademicYearNumber,
                                 T.BTSYearNumber = S.BTSYearNumber,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWCreatedBy = S.EDWCreatedBy,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.EDWModifiedBy = S.EDWModifiedBy,
                                 T.Deleted = S.Deleted
             OUTPUT $action
                    INTO @tblDeleteActions;
             SELECT @DeleteCount = SUM(Updated)
             FROM
             ( 
		   -- Count the number of updates

                 SELECT 0 AS Inserted,
                        COUNT(*) AS Updated
                 FROM @tblDeleteActions
                 WHERE MergeAction = 'UPDATE' -- It is a 'soft' delete, so shows up as an update in $action
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
             DROP TABLE #DimDateUpsert;

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
		   -- Raiserror
		   --	
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
GO