
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spUpdateStatistics'
)
    DROP PROCEDURE dbo.spUpdateStatistics;
GO
--*/

CREATE PROCEDURE [dbo].[spUpdateStatistics]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spUpdateStatistics
    --
    -- Parameters:         @EDWRunDateTime
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --
    --Usage:              EXEC dbo.spUpdateStatistics @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By         Comments
    -- ----          -----------         --------
    --
    -- 11/19/18      sburke              Initial version
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

         --
         -- Housekeeping Variables
         --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @TaskName VARCHAR(100)= 'UpdateStatistics - '+DB_NAME();

         --
         -- ETL status Variables
         --
         DECLARE @RowCount INT;
         DECLARE @Error INT;

         --
         -- ETL variables specific to this load
         --
         DECLARE @AuditId BIGINT;
         DECLARE @FiscalWeekNumber INT;
         DECLARE @SourceCount INT= 0;
         DECLARE @InsertCount INT= 0;
         DECLARE @UpdateCount INT= 0;
         DECLARE @DeleteCount INT= 0;

         --
         -- If we do not get an @EDWRunDateTime input, set to current date
         --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting.';
         PRINT @DebugMsg;
	    
         -- Write to AuditLog that we are starting	  
         EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
              @SourceName = @TaskName,
              @AuditId = @AuditId OUTPUT;
         BEGIN TRY
             DECLARE @UpdateStatsSQLCommand VARCHAR(500);
             DECLARE @UpdateStatsTable TABLE(UpdateStatsSQL VARCHAR(500) NOT NULL);
		   -- Build a list of SQL commands to Update Stats for each table in the database
             INSERT INTO @UpdateStatsTable
                    SELECT 'UPDATE STATISTICS [dbo].['+[name]+'];'
                    FROM sysobjects
                    WHERE type = 'U';

		   --
		   -- Cursor to loop through the list of SQL statements we built
		   --
             DECLARE csr_update_stats CURSOR
             FOR
                 SELECT UpdateStatsSQL
                 FROM @UpdateStatsTable
                 ORDER BY UpdateStatsSQL;             
		   --
		   -- Use cursor to loop through the values we have chosen to split this batch by
		   --
             OPEN csr_update_stats;
             FETCH NEXT FROM csr_update_stats INTO @UpdateStatsSQLCommand;
             WHILE @@FETCH_STATUS = 0
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Executing SQL command... '+@UpdateStatsSQLCommand;
                     EXEC (@UpdateStatsSQLCommand);
				 -- Execute the Update Stats command
                     PRINT @DebugMsg;
                     FETCH NEXT FROM csr_update_stats INTO @UpdateStatsSQLCommand;
                 END;
             CLOSE csr_update_stats;
             DEALLOCATE csr_update_stats;

             -- Write the successful load to EDWAuditLog
             EXEC [BING_EDW].[dbo].[spEDWEndAuditLog]
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @SourceCount = @SourceCount,
                  @AuditId = @AuditId;
         END TRY
         BEGIN CATCH
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


