

CREATE PROCEDURE [dbo].[spGL_StagingEDWETLBatchControl](@TaskName VARCHAR(100))
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingEDWETLBatchControl
    --
    -- Purpose:            Inserts status and last run date information for Fact 
    --                         (and Dimension) loads into the dbo.EDWETLBatchControl
    --                         table.  We use this to drive ETL runs, as the most recent
    --                         LastProcessedDate in EDWETLBatchControl is used as the
    --                         starting date for how far back any historical load should go.
    --
    -- Parameters:          @TaskName - The name of the ETL Task (e.g. FactGLBalance)
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By        Comments
    -- ----        -----------        --------
    --
    -- 11/07/17    Banandesi          Initial version of proc
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
         BEGIN TRY
             IF EXISTS
             (
                 SELECT 1
                 FROM dbo.EDWETLBatchControl
                 WHERE EventName = @TaskName
             )
                 BEGIN
                     UPDATE dbo.EDWETLBatchControl
                       SET
                           LastProcessedDate = GETDATE(),
                           Status = 'Success'
                     WHERE EventName = @TaskName;
             END;
                 ELSE
                 BEGIN
                     INSERT INTO dbo.EDWETLBatchControl
                     (EventName,
                      LastProcessedDate,
                      Status
                     )
                     VALUES
                     (@TaskName,
                      GETDATE(),
                      'Success'
                     );
             END;
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