

CREATE PROCEDURE [dbo].[spMISC_StagingTransform_DimPayrollType] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spMISC_StagingTransform_DimPayrollType
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given  table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime - Not required, and defaults to GETDATE()
    --
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT INTO #DimPayrollTypeUpsert -- (Temporary table)
    --                     EXEC dbo.spMISC_StagingTransform_DimPayrollType
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date           Modified By       Comments
    -- ----           -----------       --------
    -- 7/30/2018      Banandesi         Initial version
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);

	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             SELECT DISTINCT
                    COALESCE(Code, 'Unknown Payroll code') AS PayrollCode,
                    COALESCE(PayElement, 'Unknown Pay Element')  AS PayrollElement,
                    COALESCE(JournalDesc, 'Unknown Journal Desc')  AS PayrollDescriptionShortName,
                    COALESCE([Description], 'Unknown Desc') AS PayrollDescriptionLongName,
				@EDWRunDateTime AS EDWCreatedDate
             FROM dbo.vLaborHoursCosting;
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