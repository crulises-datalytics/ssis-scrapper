CREATE PROCEDURE [dbo].[spBING_EDW_Generate_DimDataScenario]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
     BEGIN
         -- ================================================================================
         -- 
         -- Stored Procedure:   spBING_EDW_Generate_DimDataScenario
         --
         -- Purpose:            Populates the DimDataScenario table in BING_EDW.
         --                     The table in question is almost static - that is, we don't
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
         -- Populates:          Truncates and [re]loads BING_EDW..DimDataScenario
         --
         -- Usage:              EXEC dbo.spBING_EDW_Generate_DimDataScenario @DebugMode = 1
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
         --  1/09/17     sburke          BNG-998 - Add -2 'Not Applicable' records for Dimension
         --                                 tables		        	          
         --	09/20/18	 hhebbalu		 BNG-3762 - Add DataScenarioType and DataScenarioSubtype columns to DimDataScenario 
		 --								( Added 2 new columns to improve the report performance as suggested by Tony)
         -- ================================================================================
         SET NOCOUNT ON;
         --
         -- Housekeeping Variables
         -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimDataScenario';
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
         EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
              @SourceName = @SourceName,
              @AuditId = @AuditId OUTPUT; 		 	 
         --
         BEGIN TRY
             SELECT @DeleteCount = COUNT(1)
             FROM dbo.DimDataScenario;
             TRUNCATE TABLE dbo.DimDataScenario;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' from Target.';
                     PRINT @DebugMsg;
             END;
             --
             -- Add Seed row
             --
             DBCC CHECKIDENT('[DimDataScenario]', RESEED, 1);
             SET IDENTITY_INSERT dbo.DimDataScenario ON;
             INSERT INTO [dbo].[DimDataScenario]
             ([DataScenarioKey],
              [DataScenarioName],
			  [DataScenarioType],
			  [DataScenarioSubtype],
              [GLActualFlag],
              [GLBudgetVersionID],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1,
                          'Unknown Scenario',
						  'Unknown Scenario Type',
						  'Unknown Scenario Subtype',
                          NULL,
                          NULL,
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          'Not Applicable Scenario',
						  'Not Applicable Scenario Type',
						  'Not Applicable Scenario Subtype',
                          NULL,
                          NULL,
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET IDENTITY_INSERT dbo.DimDataScenario OFF;

             -- ================================================================================
             -- Insert into dbo.DimDataScenario
             -- ================================================================================
             WITH DataScenario
                  AS (
                  SELECT 1 AS Id,
                         'Actual' AS DataScenarioName,
						 'Actual' AS DataScenarioType,
						 'Scenario' AS DataScenarioSubtype,
                         'A' AS GLActualFlag,
                         NULL AS GLBudgetVersionID
                  UNION
                  SELECT 2 AS Id,
                         'Plan' AS DataScenarioName,
						 'Plan' AS DataScenarioType,
						 'Scenario' AS DataScenarioSubtype,
                         'B' AS GLActualFlag,
                         1101 AS GLBudgetVersionID
                  UNION
                  SELECT 3 AS Id,
                         'Plan Var' AS DataScenarioName,
						 'Plan' AS DataScenarioType,
						 'Scenario v Actual' AS DataScenarioSubtype,
                         NULL AS GLActualFlag,
                         NULL AS GLBudgetVersionID
                  UNION
                  SELECT 4 AS Id,
                         'Plan Var %' AS DataScenarioName,
						 'Plan' AS DataScenarioType,
						 'Scenario v Actual %' AS DataScenarioSubtype,
                         NULL AS GLActualFlag,
                         NULL AS GLBudgetVersionID
                  UNION
                  SELECT 5 AS Id,
                         'Forecast' AS DataScenarioName,
						 'Forecast' AS DataScenarioType,
						 'Scenario' AS DataScenarioSubtype,
                         'B' AS GLActualFlag,
                         1181 AS GLBudgetVersionID
                  UNION
                  SELECT 6 AS Id,
                         'Forecast Var' AS DataScenarioName,
						 'Forecast' AS DataScenarioType,
						 'Scenario v Actual' AS DataScenarioSubtype,
                         NULL AS GLActualFlag,
                         NULL AS GLBudgetVersionID
                  UNION
                  SELECT 7 AS Id,
                         'Forecast Var %' AS DataScenarioName,
						 'Forecast' AS DataScenarioType,
						 'Scenario v Actual %' AS DataScenarioSubtype,
                         NULL AS GLActualFlag,
                         NULL AS GLBudgetVersionID
                  UNION
                  SELECT 8 AS Id,
                         'Target' AS DataScenarioName,
						 'Target' AS DataScenarioType,
						 'Scenario' AS DataScenarioSubtype,
                         NULL AS GLActualFlag,
                         NULL AS GLBudgetVersionID
                  UNION
                  SELECT 9 AS Id,
                         'Target Var' AS DataScenarioName,
						 'Target' AS DataScenarioType,
						 'Scenario v Actual' AS DataScenarioSubtype,
                         NULL AS GLActualFlag,
                         NULL AS GLBudgetVersionID
                  UNION
                  SELECT 10 AS Id,
                         'Target Var %' AS DataScenarioName,
						 'Target' AS DataScenarioType,
						 'Scenario v Actual %' AS DataScenarioSubtype,
                         NULL AS GLActualFlag,
                         NULL AS GLBudgetVersionIDUNION
				  UNION	
                  SELECT 11 AS Id,
                         'LY' AS DataScenarioName,
						 'Last Year' AS DataScenarioType,
						 'Scenario' AS DataScenarioSubtype,
                         NULL AS GLActualFlag,
                         NULL AS GLBudgetVersionID
                  UNION
                  SELECT 12 AS Id,
                         'LY Var' AS DataScenarioName,
						 'Last Year' AS DataScenarioType,
						 'Scenario v Actual' AS DataScenarioSubtype,
                         NULL AS GLActualFlag,
                         NULL AS GLBudgetVersionID
                  UNION
                  SELECT 13 AS Id,
                         'LY Var %' AS DataScenarioName,
						 'Last Year' AS DataScenarioType,
						 'Scenario v Actual %' AS DataScenarioSubtype,
                         NULL AS GLActualFlag,
                         NULL AS GLBudgetVersionID)
                  INSERT INTO [dbo].[DimDataScenario]
                  ([DataScenarioName],
				   [DataScenarioType],
				   [DataScenarioSubtype],
                   [GLActualFlag],
                   [GLBudgetVersionID],
                   [EDWCreatedDate],
                   [EDWCreatedBy],
                   [EDWModifiedDate],
                   [EDWModifiedBy]
                  )
                         SELECT COALESCE(DataScenarioName, 'Unknown Scenario') AS DataScenarioName,
								COALESCE(DataScenarioType, 'Unknown Scenario Type') AS DataScenarioType,
								COALESCE(DataScenarioSubtype, 'Unknown Scenario Subtype') AS DataScenarioSubtype,
                                GLActualFlag,
                                GLBudgetVersionID,
                                @EDWRunDateTime,
                                CAST(SYSTEM_USER AS VARCHAR(50)),
                                @EDWRunDateTime,
                                CAST(SYSTEM_USER AS VARCHAR(50))
                         FROM DataScenario
                         ORDER BY Id;
             SELECT @SourceCount = @@ROWCOUNT + 1; -- The See Row is the +1

             SELECT @InsertCount = @SourceCount;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' into Target.';
                     PRINT @DebugMsg;
             END;

             --
             -- Write our successful run to the EDW AuditLog 
             --
             EXEC [BING_EDW].[dbo].[spEDWEndAuditLog]
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
             EXEC [BING_EDW].[dbo].[spEDWErrorAuditLog]
                  @AuditId = @AuditId;
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = 'Sub-procedure '+@ProcName+' - '+ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
GO


