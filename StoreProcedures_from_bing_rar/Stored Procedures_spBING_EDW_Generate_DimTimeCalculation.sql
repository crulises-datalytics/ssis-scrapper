CREATE PROCEDURE  [dbo].[spBING_EDW_Generate_DimTimeCalculation]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
     BEGIN
         -- ================================================================================
         -- 
         -- Stored Procedure:   spBING_EDW_Generate_DimTimeCalculation
         --
         -- Purpose:            Populates the DimTimeCalculation table in BING_EDW.
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
         -- Populates:          Truncates and [re]loads BING_EDW..DimTimeCalculation
         --
         -- Usage:              EXEC dbo.spBING_EDW_Generate_DimTimeCalculation @DebugMode = 1
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
         --  1/09/18     sburke          BNG-998 - Add -2 'Not Applicable' records for Dimension
         --                                 tables
         --  1/15/18     sburke          BNG-1015 - Add additional Time Calculation records	    		       
         --  3/26/2018   Banandesi       BNG-1468 - Add WTD time caluclation to DimTimeCalculation	 
	    --  9/10/18     anmorales       Adding TimeCalculationType and TimeCalculationSubtype
	    --  9/20/18     adevabhakthuni  BNG- 3674 Removed Ly ,v Ly and % v Ly attributes
         -- ================================================================================
         SET NOCOUNT ON;
         --
         -- Housekeeping Variables
         -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimTimeCalculation';
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
             SELECT @DeleteCount = COUNT(1)
             FROM dbo.DimTimeCalculation;
             TRUNCATE TABLE dbo.DimTimeCalculation;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' from Target.';
                     PRINT @DebugMsg;
             END;

             -- ================================================================================
             -- Unknown and Not Applicable Seed rows
             -- ================================================================================
             SET IDENTITY_INSERT dbo.DimTimeCalculation ON;
             INSERT INTO dbo.DimTimeCalculation
             (TimeCalculationKey,
              TimeCalculationName,
              TimeCalculationType,
              TimeCalculationSubtype,
              EDWCreatedDate,
              EDWCreatedBy,
              EDWModifiedDate,
              EDWModifiedBy
             )
                    SELECT-1,
                          'Unknown Time Calculation',
                          'Unknown Time Calculation Type',
                          'Unknown Time Calculation Sub-Type',
                          @EDWRunDateTime,
                          CAST(SYSTEM_USER AS VARCHAR(50)),
                          @EDWRunDateTime,
                          CAST(SYSTEM_USER AS VARCHAR(50))
                    UNION
                    SELECT-2,
                          'Not Applicable Time Calculation',
                          'Not Applicable Time Calculation Type',
                          'Not Applicable Time Calculation Sub-Type',
                          @EDWRunDateTime,
                          CAST(SYSTEM_USER AS VARCHAR(50)),
                          @EDWRunDateTime,
                          CAST(SYSTEM_USER AS VARCHAR(50));
             SET IDENTITY_INSERT dbo.DimTimeCalculation OFF;

             -- ================================================================================
             -- Insert into dbo.DimTimeCalculation
             -- ================================================================================
             WITH LookupTimeCalculation
                  AS (
                  SELECT 1 AS Id,
                         'Selection' AS TimeCalculation,
                         'Selection' AS TimeCalculationType,
                         'Time Calculation' AS TimeCalculationSubtype
                  UNION
                  SELECT 2 AS Id,
                         'WTD' AS TimeCalculation,
                         'WTD' AS TimeCalculationType,
                         'Time Calculation' AS TimeCalculationSubtype
                  UNION
                  SELECT 3 AS Id,
                         'PTD' AS TimeCalculation,
                         'PTD' AS TimeCalculationType,
                         'Time Calculation' AS TimeCalculationSubtype
                  UNION
                  SELECT 4 AS Id,
                         'QTD' AS TimeCalculation,
                         'QTD' AS TimeCalculationType,
                         'Time Calculation' AS TimeCalculationSubtype
                  UNION
                  SELECT 5 AS Id,
                         'YTD' AS TimeCalculation,
                         'YTD' AS TimeCalculationType,
                         'Time Calculation' AS TimeCalculationSubtype
                  UNION
                  SELECT 6 AS Id,
                         'Rolling 12 Periods' AS TimeCalculation,
                         'Rolling 12 Periods' AS TimeCalculationType,
                         'Time Calculation' AS TimeCalculationSubtype
                  UNION
                  SELECT 7 AS Id,
                         'Rolling 13 Weeks' AS TimeCalculation,
                         'Rolling 13 Weeks' AS TimeCalculationType,
                         'Time Calculation' AS TimeCalculationSubtype
)                  INSERT INTO dbo.DimTimeCalculation
                  (TimeCalculationName,
                   TimeCalculationType,
                   TimeCalculationSubtype,
                   EDWCreatedDate,
                   EDWCreatedBy,
                   EDWModifiedDate,
                   EDWModifiedBy
                  )
                         SELECT COALESCE(TimeCalculation, 'Unknown Time Calculation'),
                                COALESCE(TimeCalculationType, 'Unknown Time Calculation'),
                                COALESCE(TimeCalculationSubtype, 'Unknown Time Calculation'),
                                @EDWRunDateTime,
                                CAST(SYSTEM_USER AS VARCHAR(50)),
                                @EDWRunDateTime,
                                CAST(SYSTEM_USER AS VARCHAR(50))
                         FROM LookupTimeCalculation
                         ORDER BY Id;
             SELECT @SourceCount = @@ROWCOUNT;
             SELECT @InsertCount = @SourceCount + 2; -- PLus the two seeds rows
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
GO


