CREATE PROCEDURE [dbo].[SpPopulateNSESupportSD]
	@DebugMode INT = NULL
AS
	 -- ================================================================================
	 -- 
	 -- Stored Procedure:   [SpPopulateNSESupportSD]
	 --
	 -- Purpose:             Process to hold  Data from View [dbo].[vNSESupportSD] to Table [dbo].[NSESupportSD]
 	 
	 -- Date          Modified By         Comments
	 -- --------      -----------         --------
	 -- 04/12/2023 	Aniket	            Created - BI 8010 HOTFIX - PROD - BING Daily Master Workflow extending into the afternoon
	 -- 05/11/2023    Suhas De			BI-8371 - HOT FIX - PROD - Unexplainable increase in NSE-Actual measure starting in FW202201
	 -- ================================================================================
 
     BEGIN
         
         SET NOCOUNT ON;
      
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'NSESupportSD';
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

         BEGIN TRY

			TRUNCATE TABLE dbo.NSESupportSD;

			-- Drop Index cl_NSESupportSD on NSESupportSD
			DROP INDEX  IF EXISTS  cl_NSESupportSD ON dbo.NSESupportSD  
		
			--Insert Into Table NSESupportSD
			INSERT INTO dbo.NSESupportSD (
				[StudentID],
				[CostCenterNumber],
				[Fulldate],
				[FiscalWeekEndDate],
				[FiscalWeekNumber] ,
				[ACCountSubAccountID]  ,
				[LifecycleStatusName]  ,
				[ScheduleWeekName] ,
				[SponsorID] ,
				[PartnerID] ,
				[programid] ,
				[SessionID] ,
				[RW]
			)
			SELECT E.StudentID,
				C.CostCenterNumber,
				B.Fulldate,
				B.FiscalWeekEndDate,
				B.FiscalWeekNumber,
				ASA.ACCountSubAccountID,
				LifecycleStatusName,
				ScheduleWeekName,
				spn.SponsorID,
				spn.PartnerID,
				d.programid,
				si.SessionID,	   
				ROW_NUMBER() OVER (PARTITION BY E.StudentID ORDER BY B.FiscalWeekNumber) AS RW 
			FROM dbo.DimSession si 
			INNER JOIN dbo.FactFTESnapshot A
				ON A.SessionKey=si.SessionKey
			INNER JOIN dbo.DimDate B
				ON A.DateKey = B.DateKey
			INNER JOIN dbo.DimCostCenter C
				ON A.CostCenterKey = C.CostCenterKey
			INNER JOIN dbo.DimProgram D
				ON A.ProgramKey = D.ProgramKey
			INNER JOIN dbo.DimStudent E
				ON A.StudentKey = E.StudentKey	
			INNER JOIN dbo.DimAccountSubaccount ASA
				ON a.AccountSubaccountKey=ASA.AccountSubaccountKey
			INNER JOIN dbo.DimSponsor spn
				ON a.SponsorKey=spn.SponsorKey 
			INNER JOIN dbo.Dimscheduleweek Schd
				ON a.scheduleweekkey = Schd.scheduleweekkey
			INNER JOIN dbo.dimlifecyclestatus Lcs
				ON a.lifecyclestatuskey = Lcs.lifecyclestatuskey 
			WHERE D.ProgramName NOT IN ( 'BUCC', 'Drop In','Dedicated Back Up Care Infants','Dedicated Back Up Care Preschool','Dedicated Back Up Care Toddlers','Dedicated Back Up Care Twos' )
				AND  si.SessionName NOT IN ('BUCC','Care.com Dedicated Back Up Care','Drop-In')
				AND A.FTE > 0
				AND A.ReferenceID NOT LIKE 'INV%';
				-- AND E.EDWEndDate IS NULL
				-- AND spn.EDWEndDate IS NULL

			SELECT @InsertCount = @@ROWCOUNT ;
		
			---Create Clustered Index in table NSESupportSD
            CREATE CLUSTERED INDEX [cl_NSESupportSD] ON dbo.NSESupportSD (RW, FiscalWeekNumber)

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