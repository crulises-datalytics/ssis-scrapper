CREATE PROCEDURE [dbo].[spCSS_StagingTransform_DimFeeType]
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_StagingTransform_DimFeeType
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
    --                     EXEC dbo.spCSS_StagingTransform_DimFeeType 
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
    --  1/31/18     sburke          BNG-1074 - Convert from SSIS DFT to stored proc
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
             SELECT-2 AS FeeTypeID,
                   COALESCE(TransactionCodeName, 'Unknown Fee Type') AS FeeTypeName,
                   COALESCE(TransactionCodeName, 'Unknown Fee Type Description') AS FeeTypeDescription,
                   COALESCE(GLAccountGroup, 'Unknown Fee Category') AS FeeCategory,
                   'Unknown Fee Unit of Measure' AS FeeUnitOfMeasure,
                   COALESCE(FTE, 0) AS FeeFTE,
                   COALESCE(TransactionCode, '-1') AS CSSTransactionCode,
                   COALESCE(TransactionType, '-1') AS CSSTransactionType,
                   'CSS' AS SourceSystem,
                   GETDATE() AS EDWCreatedDate,
                   CAST(SYSTEM_USER AS VARCHAR(50)) EDWCreatedBy,
                   GETDATE() AS EDWModifiedDate,
                   CAST(SYSTEM_USER AS VARCHAR(50)) EDWModifiedBy
             FROM dbo.LookupTransactionCode
             WHERE GLAccountGroup = 'Other Revenue';
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