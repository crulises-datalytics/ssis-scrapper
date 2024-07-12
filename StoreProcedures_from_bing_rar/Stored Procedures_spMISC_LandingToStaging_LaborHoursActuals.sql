

CREATE PROCEDURE [dbo].[spMISC_LandingToStaging_LaborHoursActuals] (
  @DeleteCount int OUT 
  ,@InsertCount int OUT
  ,@RunDateTime datetime2 = NULL
  ,@DebugMode      int       = NULL
)
AS
   -- ================================================================================
   -- 
   -- Stored Procedure:   spMISC_LandingToStaging_LaborHoursActuals
   --
   -- Purpose:            Performs the Insert / Update / Delete ETL process for
   --                     the staging table ADPLaborHoursActuals table from the landing table  
   --					  ADPLaborHoursActualsLanding
   --
   --
   -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
   --                         making numerous GETDATE() calls  
   --                     @DebugMode - Used just for development & debug purposes,
   --                         outputting helpful info back to the caller.  Not
   --                         required for Production, and does not affect any
   --                         core logic.			   
   --
   -- Usage:              EXEC dbo.spMISC_LandingToStaging_LaborHoursActuals @DebugMode = 1	
   -- 
   --
   -- --------------------------------------------------------------------------------
   --
   -- Change Log:		   
   -- ----------
   --
   -- Date        Modified By         Comments
   -- --------    -----------         --------
   --
   --  10/19/18   hhebbalu            BNG-4293 - Staging - Fix Labor Hours logic in Staging
   --  02/01/22   hhebbalu            BI-5437 - Fixed the code to load all the weeks for the
   --                                 weekend file and load only the latest week for a weekday file
   --			 
   -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

         --
         -- Housekeeping Variables
         --
         DECLARE @ProcName nvarchar(500) = OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg nvarchar(500);
		 DECLARE @RefDate DATE;

         --
         -- ETL status Variables
         --
         DECLARE @AuditId BIGINT;
         DECLARE @Error int;

         -- If we do not get an @EDWRunDateTime input, set to current date
         --
         IF @RunDateTime IS NULL
             SET @RunDateTime = GETDATE();

         IF @DebugMode = 1
             SELECT
                    @DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20),GETDATE())+' - Starting.';
         PRINT @DebugMsg;
         
         --
         -- Write to AuditLog we are starting
         --
         --EXEC [dbo].[spStagingBeginAuditLog]
         --       @SourceName = @SourceName
         --      ,@AuditId = @AuditId OUTPUT;

     BEGIN TRY
         BEGIN TRANSACTION;
         -- 

	SELECT @RefDate = MAX(CAST(DateWorked AS DATE)) FROM dbo.AdpLaborHoursActualsLanding;


 IF DateName(WEEKDAY, @RefDate) = 'Saturday'
                BEGIN

		--Delete data
		--
			DELETE FROM dbo.AdpLaborHoursActuals 
			WHERE WeekEndingDate IN (SELECT DISTINCT WeekEndingDate FROM dbo.AdpLaborHoursActualsLanding);
	
			SET @DeleteCount = @@ROWCOUNT;

		 --
         -- Insert data     
         -- 
				    INSERT INTO dbo.AdpLaborHoursActuals(
						EmployeeNumber
						,PayCode
						,PayGroup
						,JobCode
						,CostCenter
						,TotalHrs
						,HourlyRate
						,DollarExtension
						,WeekEndingDate
						,PayBasis
						,DateWorked
						,FileName
						,SourceLoadDate
						)
                    SELECT 
						EmployeeNumber
						,PayCode
						,PayGroup
						,JobCode
						,CostCenter
						,TotalHrs
						,HourlyRate
						,DollarExtension
						,WeekEndingDate
						,PayBasis
						,DateWorked
						,FileName
						,COALESCE(SourceLoadDate, @RunDateTime)
					FROM dbo.AdpLaborHoursActualsLanding

         SET
		 @InsertCount  = @@ROWCOUNT;

				END

					ELSE 

				BEGIN

		--Delete data
		--
			DELETE FROM dbo.AdpLaborHoursActuals 
			WHERE WeekEndingDate IN 
			(
				SELECT DISTINCT WeekEndingDate FROM dbo.AdpLaborHoursActualsLanding
				WHERE DateWorked = @RefDate
			);
	
			SET @DeleteCount = @@ROWCOUNT;

		 --
         -- Insert data     
         -- 
					INSERT INTO dbo.AdpLaborHoursActuals(
						EmployeeNumber
						,PayCode
						,PayGroup
						,JobCode
						,CostCenter
						,TotalHrs
						,HourlyRate
						,DollarExtension
						,WeekEndingDate
						,PayBasis
						,DateWorked
						,FileName
						,SourceLoadDate
						)
                    SELECT 
						EmployeeNumber
                        , PayCode
                        , PayGroup
                        , JobCode
                        , CostCenter
                        , TotalHrs
                        , HourlyRate
                        , DollarExtension
                        , WeekEndingDate
                        , PayBasis
                        , DateWorked
                        , FileName
                        , SourceLoadDate
					FROM dbo.AdpLaborHoursActualsLanding
					WHERE WeekEndingDate IN
						(
						    SELECT WeekEndingDate FROM dbo.AdpLaborHoursActualsLanding
							  WHERE DateWorked = @RefDate
						);

         SET
		 @InsertCount  = @@ROWCOUNT;

				 END;

         IF @DebugMode = 1
         BEGIN
             SELECT
                    @DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20),GETDATE())+' - Inserted '+CONVERT(nvarchar(20),@InsertCount)+' rows into Target.';
             PRINT @DebugMsg;
         END;     

		  
             -- Debug output progress
         IF @DebugMode = 1
             SELECT
                    @DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20),GETDATE())+' - Committing transaction.';
         PRINT @DebugMsg;
		   
		   --
		   -- Commit the successful transaction 
		   --
         COMMIT TRANSACTION;


		   -- Debug output progress
         IF @DebugMode = 1
             SELECT
                    @DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20),GETDATE())+' - Completing successfully.';
         PRINT @DebugMsg;
     END TRY
     BEGIN CATCH
	    	  -- Debug output progress
         IF @DebugMode = 1
         BEGIN
             SELECT
                    @DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20),GETDATE())+' - Inserted '+CONVERT(nvarchar(20),@InsertCount)+' rows into Target.';
             PRINT @DebugMsg;
         END;

		   -- Rollback the transaction
         ROLLBACK TRANSACTION;
		   --
		   -- Write our failed run to the EDW AuditLog 
		   --
         EXEC [dbo].[spStagingErrorAuditLog]
                @AuditId = @AuditId;
		   --
		   -- Raiserror
		   --
         DECLARE @ErrMsg      nvarchar(4000)
                ,@ErrSeverity int;
         SELECT
                @ErrMsg = ERROR_MESSAGE()
               ,@ErrSeverity = ERROR_SEVERITY();
         RAISERROR(@ErrMsg,@ErrSeverity,1);
     END CATCH;
     END;