CREATE PROCEDURE [dbo].[spCMS_StagingTransform_DimProgram] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_DimProgram
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                         making numerous GETDATE() calls  
    --				   
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #TemplateUpsert -- (Temporary table)
    --                     EXEC dbo.spCMS_StagingTransform_DimProgram 
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    --
    --  01/25/18     sburke          BNG-1006 - Converting SSIS source logic to the 
    --                                  sp_CMS_StagingTransform pattern
	--  02/21/18	hhebbalu		BNG-1239 - Refactor DimProgram ETL so it uses Stored Proc over DFTs creating temporary DB Objects
	--								Added parameter @EDWRunDateTime parameter.
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);

		--
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
             IF @EDWRunDateTime IS NULL
                 SET @EDWRunDateTime = GETDATE();
		--
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             DECLARE @LastProcessedDate DATETIME;
             SET @LastProcessedDate =
             (
                 SELECT LastProcessedDate
                 FROM EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'DimProgram'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

             SELECT COALESCE(NULLIF(sp.idProgram, ''), -1) ProgramID,
                    COALESCE(NULLIF(sp.ProgramName, ''), 'Unknown Program') ProgramName,
                    COALESCE(NULLIF(sp.Description, ''), 'Unknown Program Description') ProgramDescription,
                    'CMS' AS SourceSystem,
                    @LastProcessedDate AS EDWCreatedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                    @LastProcessedDate AS EDWModifiedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy,
                    sp.Deleted
             FROM [dbo].[srvcatProgram](NOLOCK) sp
             WHERE sp.StgModifiedDate >= @LastProcessedDate;
         END TRY
	    --
	    -- Catch, and throw the error back to the calling procedure or client
	    --
         BEGIN CATCH
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = 'Sub-procedure '+@ProcName+' - '+ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
GO