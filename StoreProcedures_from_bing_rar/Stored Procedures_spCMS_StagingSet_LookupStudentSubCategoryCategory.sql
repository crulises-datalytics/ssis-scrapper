CREATE PROCEDURE [dbo].[spCMS_StagingSet_LookupStudentSubCategoryCategory]
AS
     -- ================================================================================
     -- 
     -- Stored Procedure:   spCMS_StagingSet_LookupStudentSubCategoryCategory
     --
     -- Purpose:            Updates the LookupStudentSubCategoryCategory helper table
     --                         in CMS_Staging for StudentCategory.
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
     --			 
     -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	     --
	     -- Housekeeping Variables
	     --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'CMS - LookupStudentSubCategoryCategory';
         DECLARE @AuditId BIGINT;

	     --
	     -- ETL status Variables
	     --
         DECLARE @RowCount INT;
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
             -- Initial Insert of LookupStudentSubCategoryCategory from CMS
             -- ----------------------------------------------------------------------	
		   --
		   -- This was originally executed as a seperate SSIS process, with the update
		   -- SQL (below) being in a seperate process.  Will probably merge the two at 
		   -- a later date, but for now we lift and shift
		   --
             SELECT @DeleteCount = COUNT(1)
             FROM LookupStudentSubCategoryCategory;
             --
             TRUNCATE TABLE LookupStudentSubCategoryCategory;             
             --
             INSERT INTO LookupStudentSubCategoryCategory
                    SELECT idStudentCategory AS StudentSubCategoryID,
                           CAST(Name AS VARCHAR) AS StudentSubCategory,
                           'Unknown Student Category' AS StudentCategory,
                           GETDATE() AS STGCreatedDate,
                           CAST(SYSTEM_USER AS VARCHAR(50)) AS STGCreatedBy,
                           GETDATE() AS STGModifiedDate,
                           CAST(SYSTEM_USER AS VARCHAR(50)) AS STGModifiedBy
                    FROM sbsdyStudentCategory;
             SELECT @InsertCount = @@ROWCOUNT;
             -- ----------------------------------------------------------------------
             -- Update LookupStudentSubCategoryCategory for Subsidy
             -- ----------------------------------------------------------------------			 
             UPDATE LookupStudentSubCategoryCategory
               SET
                   StudentCategory = 'Subsidy',
                   STGModifiedBy = SUSER_NAME(),
                   STGModifiedDate = GETDATE()
             WHERE StudentSubCategoryID BETWEEN 1 AND 4;
             --
             SET @UpdateCount = @@ROWCOUNT;
             -- ----------------------------------------------------------------------			 
             -- Update LookupStudentSubCategoryCategory for Provate Pay
             -- ----------------------------------------------------------------------			 
             UPDATE LookupStudentSubCategoryCategory
               SET
                   StudentCategory = 'Private Pay',
                   STGModifiedBy = SUSER_NAME(),
                   STGModifiedDate = GETDATE()
             WHERE StudentSubCategoryID BETWEEN 5 AND 8;
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


