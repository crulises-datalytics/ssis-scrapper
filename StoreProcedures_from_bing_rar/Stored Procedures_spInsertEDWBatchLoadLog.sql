
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spInsertEDWBatchLoadLog'
)
    DROP PROCEDURE dbo.spInsertEDWBatchLoadLog;
GO
*/
CREATE PROCEDURE dbo.spInsertEDWBatchLoadLog
(@AuditId           BIGINT,
 @TaskName          VARCHAR(100),
 @BatchSplitByName  VARCHAR(50),
 @BatchSplitByValue INT,
 @SourceCount       INT,
 @InsertCount       INT,
 @UpdateCount       INT,
 @DeleteCount       INT,
 @StartTime         DATETIME2,
 @EndTime           DATETIME2 = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spInsertEDWBatchLoadLog
    --
    -- Purpose:            Inserts status information for Fact (and Dimension) loads
    --                         that we have chosen to split into manageable batches,
    --                         rather than loading in one go.
    --
    --                     The EDWBatchLoadLog maintains records of all the individual
    --                         loads, and is leveraged by the main ETL process to provide
    --                         total extracts, inserts, updates and deletes for the specified
    --                         load.
    --
    -- Parameters:         @AuditId - THe AuditId from the main EDWAuditLog table.
	--                         There is only a single AuditId in the main EDWAuditLog, but in our
	--                         EDWBatchLoadLog there will be multiple batch loads for a single AuditId.	
    --                     @TaskName - The name of the ETL Task (e.g. FactNetRevenue)
	--                     @BatchSplitByName - What we are using to split up batch loads 
	--                         (e.g. FiscalYear)
	--                     @BatchSplitByValue - The value of current splitter we are using 
	--                         (e.g. Fiscal Year 2017)
	--                     @SourceCount
	--                     @InsertCount
	--                     @UpdateCount
	--                     @DeleteCount
    --
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By        Comments
    -- ----        -----------        --------
    --
    -- 10/25/17    sburke             INitial version of proc
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
         BEGIN TRY
             INSERT INTO dbo.EDWBatchLoadLog
             ([AuditId],
              [TaskName],
              [BatchSplitByName],
              [BatchSplitByValue],
              [SourceCount],
              [InsertCount],
              [UpdateCount],
              [DeleteCount],
              [StartTime],
              [EndTime]
             )
             VALUES
             (@AuditId,
              @TaskName,
              @BatchSplitByName,
              @BatchSplitByValue,
              @SourceCount,
              @InsertCount,
              @UpdateCount,
              @DeleteCount,
              @StartTime,
              GETDATE()
             );
         END TRY
         BEGIN CATCH	    	 
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


