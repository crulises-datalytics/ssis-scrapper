/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spBackupTranLog'
)
    DROP PROCEDURE dbo.spBackupTranLog;
GO
*/
CREATE PROCEDURE [dbo].[spBackupTranLog](@DatabaseName NVARCHAR(100))
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spBackupTranLog
    --
    -- Purpose:            Performs a Transaction Log Backup for Database in Full Recovery 
    --                         Mode.  Used after a large ETL insert/delete/upload
    --
    -- Parameters:         @DatabaseName - The Database for which we want to backup the Tran Log for.
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
    -- 10/25/17    sburke             Initial version of proc
    --			 
    -- ================================================================================
     BEGIN
         DECLARE @BING_EDW_TrnLog_Path NVARCHAR(255);
         DECLARE @Timestamp NVARCHAR(255)= CONVERT(CHAR(8), GETDATE(), 112)+'_'+REPLACE(CONVERT(CHAR(8), GETDATE(), 108), ':', '')+'.trn';
	    --
	    -- Build the LOG BACKUP COmmand
	    --
         SET @BING_EDW_TrnLog_Path = N'L:\Logs\'+@DatabaseName+'_log'+@Timestamp;
         PRINT @BING_EDW_TrnLog_Path;
         BACKUP LOG @DatabaseName TO DISK = @BING_EDW_TrnLog_Path;
     END;
GO
