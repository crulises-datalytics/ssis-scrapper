CREATE PROCEDURE [dbo].[spCSS_StagingTransform_DimTransactionCode]
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_StagingTransform_DimTransactionCode
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
    --                     EXEC dbo.spCSS_StagingTransform_DimTransactionCode 
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    --
    --  2/01/18     sburke          BNG-1074 - Convert from SSIS DFT to stored proc
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
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             SELECT COALESCE(NULLIF(a.tx_code, ''), 'Unknown') AS TransactionCode,
                    COALESCE(NULLIF(a.tx_code_desc, ''), 'Unknown Transaction Code') AS TransactionCodeName,
                    COALESCE(NULLIF(a.tx_type, ''), 'XX') AS TransactionTypeCode,
                    COALESCE(NULLIF(b.TransactionTypeName, ''), 'Unknown Transaction Type') AS TransactionTypeName,
                    COALESCE(c.per_of_full, 0) AS TransactionCodeFTE,
                    '1900-01-01' AS EDWEffectiveDate,
                    NULL AS EDWEndDate,
                    GETDATE() AS EDWCreatedDate,
                    SUSER_SNAME() AS EDWCreatedBy
             FROM dbo.cspratec a
                  LEFT JOIN CSS_Staging.dbo.LookupTransactionType b ON a.tx_type = b.TransactionType
                  LEFT JOIN CSS_Staging.dbo.CSOTXPRR c ON a.tx_code = c.tx_code
             WHERE a.tx_code IS NOT NULL;
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