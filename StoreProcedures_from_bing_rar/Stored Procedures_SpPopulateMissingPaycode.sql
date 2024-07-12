CREATE PROCEDURE [dbo].[SpPopulateMissingPaycode] ( @DebugMode      INT       = NULL)
AS
   -- ================================================================================
   -- 
   -- Stored Procedure:   [spHoldPlaceHolder]
   --
   -- Purpose:             Process to hold Missing Paycodes FROM [AdpLaborHoursActuals] to [LaborHoursBILookup]
   --                        Hold Missing Paycodes into PlaceHolder table
   --
    
   --
   -- Date        Modified By         Comments
   -- --------    -----------         --------
   --	06/16/2022 	Vishal	          Created -BI 3495
   -- ================================================================================
 
     BEGIN
         
         SET NOCOUNT ON;
         --
       
         -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'AdpLaborHoursActuals';
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
              ;WITH CTE_Paycode as
			  (
				SELECT   DISTINCT adp.PayCode , Adp.PayGroup -- ,Getdate() as MissingDate 
				FROM [MISC_Staging].[dbo].[vLaborHoursActuals] Adp
				LEFT JOIN  [MISC_Staging].[dbo].[LaborHoursBILookup] lp  on adp.paycode=lp.name
				WHERE lp.name IS NULL 
				EXCEPT 
				SELECT PayCode, PayGroup FROM dbo.MissingPaycodes
				
			  )
			  INSERT INTO dbo.MissingPaycodes(PayCode,PayGroup , MissingDate)
				SELECT    PayCode ,PayGroup, Getdate() as MissingDate 
				FROM CTE_Paycode
				order by 1 ,2 

				SELECT @InsertCount = @@ROWCOUNT ;

            
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' into Target.';
                     PRINT @DebugMsg;
             END;


					Update P set P.FoundDate=Getdate()
					FROM dbo.MissingPaycodes P
					JOIN [MISC_Staging].[dbo].[LaborHoursBILookup] lkp on P.PayCode=lkp.name 
					WHERE p.FoundDate IS NULL


					SELECT @UpdateCount = @@ROWCOUNT ;

          
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Updated '+CONVERT(NVARCHAR(20), @UpdateCount)+' into Target.';
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