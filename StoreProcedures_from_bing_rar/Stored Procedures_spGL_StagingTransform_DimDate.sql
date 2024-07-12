/*
IF EXISTS 
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spGL_StagingTransform_DimDate'
)
    DROP PROCEDURE dbo.spGL_StagingTransform_DimDate;
GO
*/
CREATE PROCEDURE [dbo].[spGL_StagingTransform_DimDate] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_DimDate
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                         making numerous GETDATE() calls  
    --				   
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #TemplateUpsert -- (Temporary table)
    --                     EXEC dbo.spGL_StagingTransform_DimDate @EDWRunDateTime
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
    -- 10/20/17    sburke              BNG-708 - Extend dates returned to pre-2011
    --  8/14/18    sburke              BNG-727 - Addition of Payroll Calendar dates	
    --  8/15/18     sburke          BNG-3582 - Addition of Academic and Back-to-School Dates
    --                                  (corrected logic of Academic & BTS Years, switching from using Calendar to Fiscal Years)
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @UnknownDate DATE= '19001001';
	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             SELECT udf_dt.DateKey,
                    udf_dt.FullDate,
                    udf_dt.FullDateName,
                    udf_dt.WeekdayWeekend,
                    udf_dt.HolidayName,
                    udf_dt.HolidayFlag,
                    udf_dt.HolidayFiscalWeekFlag,
                    udf_dt.CalendarFirstDayOfMonthFlag,
                    udf_dt.CalendarLastDayOfMonthFlag,
                    udf_dt.CalendarFirstWeekOfMonthFlag,
                    udf_dt.CalendarLastWeekOfMonthFlag,
                    udf_dt.CalendarDaySequenceNumber,
                    udf_dt.CalendarDayOfWeekNumber,
                    udf_dt.CalendarDayOfWeekName,
                    udf_dt.CalendarDayOfWeekNameShort,
                    udf_dt.CalendarDayOfMonthNumber,
                    udf_dt.CalendarDayOfQuarterNumber,
                    udf_dt.CalendarDayOfYearNumber,
                    udf_dt.CalendarWeekNumber,
                    udf_dt.CalendarWeekName,
                    udf_dt.CalendarWeekOfMonthNumber,
                    udf_dt.CalendarWeekOfMonthName,
                    udf_dt.CalendarWeekOfYearNumber,
                    udf_dt.CalendarWeekOfYearName,
                    udf_dt.CalendarWeekStartDate,
                    udf_dt.CalendarWeekEndDate,
                    udf_dt.CalendarMonthNumber,
                    udf_dt.CalendarMonthName,
                    udf_dt.CalendarMonthOfYearNumber,
                    udf_dt.CalendarMonthOfYearName,
                    udf_dt.CalendarMonthOfYearNameShort,
                    udf_dt.CalendarMonthStartDate,
                    udf_dt.CalendarMonthEndDate,
                    udf_dt.CalendarQuarterNumber,
                    udf_dt.CalendarQuarterName,
                    udf_dt.CalendarQuarterOfYearNumber,
                    udf_dt.CalendarQuarterOfYearName,
                    udf_dt.CalendarQuarterStartDate,
                    udf_dt.CalendarQuarterEndDate,
                    udf_dt.CalendarYearNumber,
                    udf_dt.CalendarYearName,
                    udf_dt.CalendarYearStartDate,
                    udf_dt.CalendarYearEndDate,
                    udf_dt.FiscalDayOfWeekNumber,
                    udf_dt.FiscalDayOfPeriodNumber,
                    udf_dt.FiscalDayOfQuarterNumber,
                    udf_dt.FiscalDayOfYearNumber,
                    udf_dt.FiscalWeekNumber,
                    udf_dt.FiscalWeekName,
                    udf_dt.FiscalWeekOfPeriodNumber,
                    udf_dt.FiscalWeekOfPeriodName,
                    udf_dt.FiscalWeekOfQuarterNumber,
                    udf_dt.FiscalWeekOfQuarterName,
                    udf_dt.FiscalWeekOfYearNumber,
                    udf_dt.FiscalWeekOfYearName,
                    udf_dt.FiscalWeekSequenceNumber,
                    udf_dt.FiscalWeekStartDate,
                    udf_dt.FiscalWeekEndDate,
                    udf_dt.FiscalPeriodNumber,
                    udf_dt.FiscalPeriodName,
                    udf_dt.FiscalPeriodType,
                    udf_dt.FiscalPeriodOfYearNumber,
                    udf_dt.FiscalPeriodOfYearName,
                    udf_dt.FiscalPeriodSequenceNumber,
                    udf_dt.FiscalPeriodStartDate,
                    udf_dt.FiscalPeriodEndDate,
                    udf_dt.FiscalQuarterNumber,
                    udf_dt.FiscalQuarterName,
                    udf_dt.FiscalQuarterOfYearNumber,
                    udf_dt.FiscalQuarterOfYearName,
                    udf_dt.FiscalQuarterSequenceNumber,
                    udf_dt.FiscalQuarterStartDate,
                    udf_dt.FiscalQuarterEndDate,
                    udf_dt.FiscalYearNumber,
                    udf_dt.FiscalYearName,
                    udf_dt.FiscalYearStartDate,
                    udf_dt.FiscalYearEndDate,
                    COALESCE(vPyrl.PayrollStartDate, @UnknownDate) AS PayrollStartDate,
                    COALESCE(vPyrl.PayrollEndDate, @UnknownDate) AS PayrollEndDate,
                    COALESCE(vPyrl.PayrollCheckDate, @UnknownDate) AS PayrollCheckDate,
				-- BTS (Back To School) Flag.  This is Aug & Sep every year
                    CASE
                        WHEN FiscalPeriodOfYearNumber IN(8, 9)
                        THEN 'BTS Period'
                        ELSE 'Not BTS Period'
                    END AS BTSPeriodFlag,
				-- Academic Year.  For now we have this starting in Aug every year (each Center has different schedules, but these are not stored anywhere in CMS/CSS/GL/HR)
                    CASE
                        WHEN FiscalPeriodOfYearNumber >= 8
                        THEN FiscalYearNumber + 1
                        ELSE FiscalYearNumber
                    END AS AcademicYearNumber,
				-- BTS Year (Back to School Year).  This is shifted forward one month after the Academic Year
                    CASE
                        WHEN FiscalPeriodOfYearNumber >= 9
                        THEN FiscalYearNumber + 1
                        ELSE FiscalYearNumber
                    END AS BTSYearNumber,
                    @EDWRunDateTime AS EDWCreatedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                    @EDWRunDateTime AS EDWModifiedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy,
                    NULL AS Deleted
             FROM tfnGL_StagingGenerate_Dates_DimDate() udf_dt
                  LEFT JOIN dbo.vPayrollCalendar vPyrl ON udf_dt.FullDate BETWEEN vPyrl.PayrollStartDate AND vPyrl.PayrollEndDate; 
         END TRY
	    --
	    -- Catch, and throw the error back to the calling procedure or client
	    --
         BEGIN CATCH
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = 'Sub-procedure '+@ProcName+' - '+ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
GO