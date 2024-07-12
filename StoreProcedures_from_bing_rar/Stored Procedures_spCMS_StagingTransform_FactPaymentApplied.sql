/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCMS_StagingTransform_FactPaymentApplied'
)
    DROP PROCEDURE dbo.spCMS_StagingTransform_FactPaymentApplied;
GO
*/
CREATE PROCEDURE [dbo].[spCMS_StagingTransform_FactPaymentApplied](@EDWRunDateTime DATETIME2 = NULL)
AS
    -- ================================================================================
    -- 
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime
    --                     @FiscalYearNumber - Stored proc runs for just a single FiscalYear 
    --                          (so we run in batches for multiple years)
    --
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #FactPaymentUpsert -- (Temporary table)
    --                     EXEC dbo.spCMS_StagingTransform_FactPaymentApplied @FiscalYearNumber = 2017
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By        Comments
    -- ----          -----------        --------
    --
    -- 10/03/17      sburke             Initial version of proc, converted from SSIS logic
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
             --Get the last time that the Fact Invoice Event table was processed.
             DECLARE @LastProcessedDate DATETIME;
             SET @LastProcessedDate =
             (
                 SELECT LastProcessedDate
                 FROM CMS_Staging..EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'FactPaymentApplied'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

             SELECT sip.idStudentInvoicePayment AS PaymentAppliedID,
                    ISNULL(sp.PaymentNumber, 'No Payment Nmber') AS PaymentID,
                    ISNULL(s.InvoiceNumber, 'No Invoice NUmber') AS InvoiceID,
                    COALESCE(dd.DateKey, -1) AS PaymentAppliedDateKey,
                    COALESCE(sip.PaidAmount, 0) AS PaymentAppliedAmount,
                    @EDWRunDateTime AS EDWCreatedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                    @EDWRunDateTime AS EDWModifiedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy,
                    sip.Deleted
             FROM CMS_Staging..finStudentInvoicePayment sip
                  LEFT JOIN CMS_Staging..finStudentPayment sp ON sp.idStudentPayment = sip.idStudentPayment
                  LEFT JOIN CMS_Staging..finStudentInvoice s ON sip.idStudentInvoice = s.idStudentInvoice
                  LEFT JOIN BING_EDW.dbo.DimDate dd ON CAST(sip.CreatedDate AS DATE) = dd.FullDate
             WHERE(sip.StgModifiedDate >= @LastProcessedDate);
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


