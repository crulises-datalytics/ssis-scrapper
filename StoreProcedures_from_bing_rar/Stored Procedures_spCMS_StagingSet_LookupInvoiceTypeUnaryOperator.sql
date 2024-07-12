CREATE PROCEDURE [dbo].[spCMS_StagingSet_LookupInvoiceTypeUnaryOperator]
AS
     -- ================================================================================
     -- 
     -- Stored Procedure:   spCMS_StagingSet_LookupInvoiceTypeUnaryOperator
     --
     -- Purpose:            Updates the LookupInvoiceTypeUnaryOperator helper table
     --                         in CMS_Staging for InvoiceTypeName.
     -- 
     -- --------------------------------------------------------------------------------
     --
     -- Change Log:		   
     -- ----------
     --
     -- Date        Modified By         Comments
     -- ----        -----------         --------
     --
     --  5/22/18    sburke              BNG-1750 Add in-line SQL from CMS Incremental SSIS
     --                                     package to its dedicated process (that can be
     --                                     called from Historical and Incremental loads.		
     --	07/26/19    hhebbalu            BI-1653/BI-1654/BI-1655 - Added the new invoice 
	 --									id 11-Migrated Balance Invoice in the list to 
	 --									update unary operator as '+'
     -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	     --
	     -- Housekeeping Variables
	     --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'CMS - LookupInvoiceTypeUnaryOperator';
         DECLARE @AuditId BIGINT;

	     --
	     -- ETL status Variables
	     --
         DECLARE @DeleteCount INT;
         DECLARE @InsertCount INT;
         DECLARE @UpdateCount INT;
         DECLARE @Error INT;

	     --
	     -- Write to EDW AuditLog we are starting
	     --
         EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
              @SourceName = @SourceName,
              @AuditId = @AuditId OUTPUT;
         BEGIN TRY
             -- ----------------------------------------------------------------------
             -- Initial Insert of LookupInvoiceTypeUnaryOperator from CMS
             -- ----------------------------------------------------------------------	
		   --
		   -- This was originally executed as a seperate SSIS process, with the update
		   -- SQL (below) being in a seperate process.  Will probably merge the two at 
		   -- a later date, but for now we lift and shift
		   --

             SELECT @DeleteCount = COUNT(1)
             FROM LookupInvoiceTypeUnaryOperator;
             --
             TRUNCATE TABLE LookupInvoiceTypeUnaryOperator;             
             --
             INSERT INTO LookupInvoiceTypeUnaryOperator
                    SELECT CAST(idInvoiceType AS INT) AS InvoiceTypeID,
                           InvoiceType AS InvoiceTypeName,
                           '~' AS InvoiceTypeUnaryoperator,
                           GETDATE() AS STGCreatedDate,
                           CAST(SYSTEM_USER AS VARCHAR(50)) AS STGCreatedBy,
                           GETDATE() AS STGModifiedDate,
                           CAST(SYSTEM_USER AS VARCHAR(50)) AS STGModifiedBy
                    FROM dbo.finInvoiceType; 
             --

             SELECT @InsertCount = @@ROWCOUNT;

             -- ----------------------------------------------------------------------
             -- Update LookupInvoiceTypeUnaryOperator for Subsidy
             -- ----------------------------------------------------------------------			 
             UPDATE LookupInvoiceTypeUnaryOperator
               SET
                   InvoiceTypeUnaryOperator = '+',
                   STGModifiedBy = SUSER_NAME(),
                   STGModifiedDate = GETDATE()
             WHERE InvoiceTypeID IN(1, 2, 4, 5, 8, 9, 11);
		     --
             SET @UpdateCount = @@ROWCOUNT;
             -- ----------------------------------------------------------------------			 
             -- Update LookupInvoiceTypeUnaryOperator for Provate Pay
             -- ----------------------------------------------------------------------			 
             UPDATE LookupInvoiceTypeUnaryOperator
               SET
                   InvoiceTypeUnaryOperator = '-',
                   STGModifiedBy = SUSER_NAME(),
                   STGModifiedDate = GETDATE()
             WHERE InvoiceTypeID IN(3, 6, 7, 10);
		     --
             SET @UpdateCount = @UpdateCount + @@ROWCOUNT;

		     --
		     -- Write our successful run to the EDW AuditLog 
		     --
             EXEC [BING_EDW].[dbo].[spEDWEndAuditLog]
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @SourceCount = @InsertCount,
                  @AuditId = @AuditId;
         END TRY
         BEGIN CATCH
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


