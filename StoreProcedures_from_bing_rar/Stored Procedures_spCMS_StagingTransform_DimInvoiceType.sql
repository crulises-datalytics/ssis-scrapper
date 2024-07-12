CREATE PROCEDURE [dbo].[spCMS_StagingTransform_DimInvoiceType]
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_DimInvoiceType
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
    --                     EXEC dbo.spCMS_StagingTransform_DimInvoiceType 
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
    --  1/25/18     sburke          BNG-1006 - Converting SSIS source logic to the 
    --                                  sp_CMS_StagingTransform pattern
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
             DECLARE @LastProcessedDate DATETIME;
             SET @LastProcessedDate =
             (
                 SELECT LastProcessedDate
                 FROM EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'DimInvoiceType'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

             SELECT COALESCE(NULLIF(f.idInvoiceType, ''), -1) InvoiceTypeID,
                    COALESCE(NULLIF(f.InvoiceType, ''), 'Unknown Invoice Type') InvoiceTypeName,
                    COALESCE(NULLIF(lkp.InvoiceTypeUnaryOperator, ''), '~') AS InvoicePostingType,
                    GETDATE() AS EDWCreatedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                    GETDATE() AS EDWModifiedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy,
                    f.Deleted
             FROM dbo.finInvoiceType(NOLOCK) f
                  LEFT JOIN LookupInvoiceTypeUnaryOperator(NOLOCK) lkp ON f.idInvoiceType = lkp.InvoiceTypeID
             WHERE f.StgModifiedDate >= @LastProcessedDate
                   OR lkp.StgModifiedDate >= @LastProcessedDate;
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