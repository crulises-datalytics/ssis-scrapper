CREATE PROCEDURE [dbo].[spGL_StagingTransform_TruncateEDWInfAccountSubaccount]
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingTransform_TruncateEDWInfAccountSubaccount
    --
    -- Purpose:            Truncates [GL_Staging].[dbo].[InfAccountSubaccount]
    --
    -- Usage:              
    --                     EXEC dbo.spGL_StagingTransform_TruncateEDWInfAccountSubaccount
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:                                
    -- ----------
    --
    -- Date        Modified By      Comments
    -- ----        -----------      --------
    --
    -- ================================================================================
	BEGIN
		SET NOCOUNT ON;
                   --
                   -- Housekeeping Variables
                   -- 
		DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
		DECLARE @DebugMsg NVARCHAR(500);

        BEGIN TRY

			TRUNCATE TABLE [dbo].[InfAccountSubaccount]
        
		END TRY
                   --
                   -- Catch, and throw the error back to the calling procedure or client
                   --
         BEGIN CATCH
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = 'Sub-procedure ' + @ProcName + ' - ' + ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;