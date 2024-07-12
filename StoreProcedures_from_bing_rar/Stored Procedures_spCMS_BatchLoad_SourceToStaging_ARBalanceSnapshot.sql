

CREATE PROCEDURE [dbo].[spCMS_BatchLoad_SourceToStaging_ARBalanceSnapshot]
(@RunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_BatchLoad_SourceToStaging_ARBalanceSnapshot
    --
    -- Purpose:            Performs the Insert / Delete ETL process for
    --                         the ARBalanceSnapshot table from Source to Staging, loading
    --                         data in manageable batch sizes for larger datasets.  This
    --                         is to ensure we do not fill the log when performing 
    --                         inserts / updates on our larger tables
    --
    -- Parameters:         @RunDateTime
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --
    --Usage:              EXEC dbo.spCMS_BatchLoad_SourceToStaging_ARBalanceSnapshot
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By       Comments
    -- ----          -----------       --------
    --
    -- -7/26/19      hhebbalu          BI-1653 /BI-1654/BI-1655 - Created this procedure so 
	--								   the AR in staging gets updated when AR in source is changed
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
	    -- ETL status Variables
	    --
         DECLARE @RowCount INT;
         DECLARE @Error INT;

	    --
	    -- ETL variables specific to this load
	    --
         DECLARE @AuditId BIGINT;
         DECLARE @FiscalDate DATE;
         DECLARE @SourceCount INT= 0;
         DECLARE @InsertCount INT= 0;
         DECLARE @UpdateCount INT= 0;
         DECLARE @DeleteCount INT= 0;

	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @RunDateTime IS NULL
             SET @RunDateTime = GETDATE(); 
	    --
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting.';
         PRINT @DebugMsg;

	    --
	    -- Determine how far back in history we have extract data for
	    --
         DECLARE @LastProcessedDate DATETIME =
         (
             SELECT LastProcessedDate
             FROM dbo.StagingETLBatchControl(NOLOCK)
         );
         IF @LastProcessedDate IS NULL
             SET @LastProcessedDate = '2010-12-25';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

		   -- ================================================================================
		   -- STEP 1.
		   -- 
		   -- Ascertain by what criteria we are splitting this batch load by, and build a loop
		   --
		   -- We build our list or dates to process data for by:
		   -- 1.) Looking at the LastProcessedDate to see when we last loaded data from CMS
		   -- 2.) Looking at finStudentInvoice,finCreditMemo, finAccountAdjustmentQueue, finStudentPayment 
		   --     finStudentPayment, finStudentInvoicePayment table for any updated records
		   -- ================================================================================
           
		     DECLARE @BatchByFiscalDate TABLE(FiscalDate DATE);
			 DECLARE @CMSMinFiscalDate DATE;

             SET @CMSMinFiscalDate =
                (
				 SELECT MIN(FiscalDate) FROM

				 (SELECT DISTINCT CAST(i.InvoiceDate AS DATE) FiscalDate
                 FROM dbo.finStudentInvoice(NOLOCK) i
				 WHERE i.StgModifiedDate > @LastProcessedDate

				UNION

                 SELECT DISTINCT CAST(cm.CreditMemoDate AS DATE)
                 FROM dbo.finCreditMemo(NOLOCK) cm
				 WHERE cm.StgModifiedDate > @LastProcessedDate

				UNION

                 SELECT DISTINCT CAST(aaq.CreatedDate AS DATE)
                 FROM [dbo].[finAccountAdjustmentQueue-Subset](NOLOCK) aaq
				 WHERE aaq.StgModifiedDate > @LastProcessedDate

				UNION

                 SELECT DISTINCT CAST(p.PaymentDate AS DATE)
                 FROM dbo.finStudentPayment(NOLOCK) p
				 WHERE p.StgModifiedDate > @LastProcessedDate

				UNION

                 SELECT DISTINCT CAST(sip.CreatedDate AS DATE)
                 FROM dbo.finStudentInvoicePayment(NOLOCK) sip
				 WHERE sip.StgModifiedDate > @LastProcessedDate

				 )a
				 WHERE a.FiscalDate >= '2010-12-25' --no need to load data beyond 2011
				 )

             INSERT INTO @BatchByFiscalDate(FiscalDate)
                    SELECT DISTINCT
                           FiscalWeekEndDate 
                    FROM BING_EDW.dbo.DimDate(NOLOCK) d
                    WHERE FiscalWeekEndDate >= @CMSMinFiscalDate
						AND FiscalWeekStartDate <= @RunDateTime
					    AND FiscalPeriodOfYearName <> 'ADJ'

				--SELECT * FROM @BatchByFiscalDate ORDER BY FiscalDate

		   -- ================================================================================
		   -- STEP 2.
		   -- 
		   -- Loop through each Fiscal Year, and execute the ETL Upsert Stored Proc
		   -- ================================================================================
             DECLARE cms_stg_arbalancesnapshot_fisc_date CURSOR
             FOR
                 SELECT FiscalDate
                 FROM @BatchByFiscalDate
                 ORDER BY FiscalDate;             
		   --
		   -- Use cursor to loop through the values we have chosen to split this batch by
		   --
             OPEN cms_stg_arbalancesnapshot_fisc_date;
             FETCH NEXT FROM cms_stg_arbalancesnapshot_fisc_date INTO @FiscalDate;
             WHILE @@FETCH_STATUS = 0
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Processing data for Fiscal Date '+CONVERT(VARCHAR(10), @FiscalDate);
                     PRINT @DebugMsg;

				 -- ================================================================================
				 -- Execute the main spCMS_SourceToStaging_ARBalanceSnapshot stored proc to load data 
				 --for the given batch
				 -- ================================================================================

                     EXEC dbo.spCMS_SourceToStaging_ARBalanceSnapshot
                          @FiscalWeekEndDate = @FiscalDate,
                          @DebugMode = @DebugMode;
                     FETCH NEXT FROM cms_stg_arbalancesnapshot_fisc_date INTO @FiscalDate;
                 END;
             CLOSE cms_stg_arbalancesnapshot_fisc_date;
             DEALLOCATE cms_stg_arbalancesnapshot_fisc_date;
			  
              BEGIN
                     UPDATE dbo.StagingETLBatchControl
                       SET
                           LastProcessedDate = GETDATE()
                     WHERE Id = 1;
              END;    	 
   END