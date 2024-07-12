CREATE  PROCEDURE [dbo].[SpPopulateNSESupport]
	@DebugMode INT = NULL
AS
	 -- ================================================================================
	 -- 
	 -- Stored Procedure:   [SpPopulateNSESupport]
	 --
	 -- Purpose:             Process to hold  Data from View [dbo].[vNSESupport] to Table [dbo].[NSESupport]
 	 
	 -- Date          Modified By         Comments
	 -- --------      -----------         --------
	 -- 04/12/2023 	  Aniket	          Created - BI 8010 HOTFIX - PROD - BING Daily Master Workflow extending into the afternoon
	 -- 05/11/2023    Suhas De			  BI-8371 - HOT FIX - PROD - Unexplainable increase in NSE-Actual measure starting in FW202201
	 -- ================================================================================
 
     BEGIN
         
         SET NOCOUNT ON;

         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'NSESupport';
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
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting.';
         PRINT @DebugMsg;

         --
         -- Write to EDW AuditLog we are starting
         --
         EXEC [dbo].[spEDWBeginAuditLog]
              @SourceName = @SourceName,
              @AuditId = @AuditId OUTPUT; 		 	 
         --
         BEGIN TRY
		  
			 TRUNCATE TABLE dbo.NSESupport;

			 -- Drop Index cl_NSESupport on NSESupport
			 DROP INDEX  IF EXISTS  cl_NSESupport ON dbo.NSESupport 
		
			 --Insert Into Table NSESupport
			 INSERT INTO dbo.NSESupport(
				[StudentID] ,
				[CostCenterNumber] ,
				[FiscalWeekEndDate] ,
				[FiscalWeekNumber] ,
				[FTE] ,
				[RW]
			 )
			 SELECT
				E.StudentID ,
				C.CostCenterNumber,
				B.FiscalWeekEndDate,
				B.FiscalWeekNumber,
				SUM(A.FTE) AS FTE,
				ROW_NUMBER() OVER (PARTITION BY E.StudentID ORDER BY B.FiscalWeekNumber) AS RW 
			 FROM dbo.FactFTESnapshot A
			 INNER JOIN dbo.DimDate B
				ON A.DateKey = B.DateKey
			 INNER JOIN dbo.DimCostCenter C
				ON A.CostCenterKey = C.CostCenterKey
			 INNER JOIN dbo.DimProgram D
				ON A.ProgramKey = D.ProgramKey
			 INNER JOIN dbo.DimStudent E
				ON A.StudentKey = E.StudentKey
			 INNER JOIN dbo.DimSession si 
				ON A.SessionKey=si.SessionKey
			 WHERE D.ProgramName NOT IN ( 'BUCC', 'Drop In','Dedicated Back Up Care Infants','Dedicated Back Up Care Preschool','Dedicated Back Up Care Toddlers','Dedicated Back Up Care Twos' )
				 AND  si.SessionName NOT IN ('BUCC','Care.com Dedicated Back Up Care','Drop-In')
				 AND A.FTE > 0
				 AND A.ReferenceID NOT LIKE 'INV%'
			 GROUP BY E.StudentID,
				C.CostCenterNumber,
				B.FiscalWeekEndDate,
				B.FiscalWeekNumber
			  
				 

			 SELECT @InsertCount = @@ROWCOUNT ;
		
			 ---Create Clustered Index in table NSESupport
			 CREATE CLUSTERED INDEX [cl_NSESupport] ON dbo.NSESupport (RW, FiscalWeekNumber);

			 IF @DebugMode = 1
				BEGIN
					SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' into Target.';
					PRINT @DebugMsg;
				END;

			   
			 --
			 -- Write our successful run to the EDW AuditLog 
			 --
			 EXEC [dbo].[spEDWEndAuditLog]
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
         --
         -- Catch, and throw the error back to the calling procedure or client
         --
         BEGIN CATCH
			--
			-- Write our failed run to the EDW AuditLog 
			--
			EXEC [dbo].[spEDWErrorAuditLog]
				@AuditId = @AuditId;
			DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
			SELECT @ErrMsg = 'Sub-procedure '+@ProcName+' - '+ERROR_MESSAGE(),
				@ErrSeverity = ERROR_SEVERITY();
			RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;