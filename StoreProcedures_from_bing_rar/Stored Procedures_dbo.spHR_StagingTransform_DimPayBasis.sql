CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimPayBasis] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimPayBasis
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
    --                     EXEC dbo.spHR_StagingTransform_DimPayBasis 
    --                     (Note: 12/18/17 - Proc currently takes approx. 7 mins to process 1.5m rows into #temp table)
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
    -- 12/28/17     sburke          BNG-269 - Initial version
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
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             SELECT COALESCE(PayBasisID, -1) AS PayBasisID,
                    COALESCE(PayBasisName, 'Unknown Pay Basis') AS PayBasisName,
                    COALESCE(PayBasisAnnualizationFactor, 0) AS PayBasisAnnualizationFactor,
                    PayBasisFlexAttribute1,
                    PayBasisFlexAttribute2,
                    PayBasisFlexAttribute3,
                    PayBasisFlexAttribute4,
                    PayBasisFlexAttribute5,
                    COALESCE(PayBasisCreatedDate, '19000101') AS PayBasisCreatedDate,
                    COALESCE(PayBasisCreatedUser, -1) AS PayBasisCreatedUser,
                    COALESCE(PayBasisModifiedDate, '19000101') AS PayBasisModifiedDate,
                    COALESCE(PayBasisModifiedUser, -1) AS PayBasisModifiedUser,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate
             FROM dbo.vPayBases;
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