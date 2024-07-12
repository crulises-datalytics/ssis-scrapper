CREATE PROCEDURE [dbo].[spBING_EDW_Build_DimensionStaticData]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
     BEGIN
         -- ================================================================================
         -- 
         -- Stored Procedure:   spBING_EDW_Build_DimensionStaticData
         --
         -- Purpose:            Populates the StaticDimensions tables in BING_EDW.
         --                     The tables in question are almost static - that is, we don't
         --                         expect the data to change often.  However, we have
         --                         the population process encapsulated in a proc if we  
         --                         need to update or [re]deploy the entire database solution
         --                         from scratch.	  
         --
         --                     The logic for this was in an SSIS Project called PostDeploymentExecution,
         --                         which was lost and forgotten in our Source Repository.  Putting it here
         --                         makes it easier to locate what's actually populating the table	   	    	    	      	    
         --
         --
         -- Populates:          Truncates and [re]loads BING_EDW..StaticDimensions
         --
         -- Usage:              EXEC dbo.spBING_EDW_Build_DimensionStaticData @DebugMode = 1
         --
         -- --------------------------------------------------------------------------------
         --
         -- Change Log:		   
         -- ----------
         --
         -- Date         Modified By     Comments
         -- ----         -----------     --------
         --
         -- 12/01/17     sburke    
         --  2/13/18     sburke          BNG-259 - Addition of DimLeadEventType         
         --	 2/13/19     aquitta         BNG-4522 - Addition of rep.ReportFilter		 
         -- ================================================================================
         SET NOCOUNT ON;
         --
         -- Housekeeping Variables
         -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimensionStaticData';
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
         -- If we do not get an @EDWRunDateTime input, set to current date
         --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
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
             -- DimARAgingBucket		 
             EXEC spBING_EDW_Generate_DimARAgingBucket
                  @DebugMode = @DebugMode;
             -- DimARBalanceType				  
             EXEC dbo.spBING_EDW_Generate_DimARBalanceType
                  @DebugMode = @DebugMode;
             -- DimDataScenario				  
             EXEC dbo.spBING_EDW_Generate_DimDataScenario
                  @DebugMode = @DebugMode;
             -- DimGLMetricType				  
             EXEC spBING_EDW_Generate_DimGLMetricType
                  @DebugMode = @DebugMode;
             -- DimLeadEventType
             EXEC dbo.spBING_EDW_Generate_DimLeadEventType
                  @DebugMode = @DebugMode;
             -- DimLifecycleStatus 				  
             EXEC spBING_EDW_Generate_DimLifecycleStatus
                  @DebugMode = @DebugMode;
             -- DimScheduleWeek				  
             EXEC spBING_EDW_Generate_DimScheduleWeek
                  @DebugMode = @DebugMode;
             -- DimTimeCalculation				  
             EXEC spBING_EDW_Generate_DimTimeCalculation
                  @DebugMode = @DebugMode;
		       -- rep.ReportFilter
			    EXEC rep.spBING_EDW_Generate_repReportFilter
                  @DebugMode = @DebugMode;

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
GO
