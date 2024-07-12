CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimPayRateChangeReason] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimPayRateChangeReason
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
    -- Usage:              DECLARE @EDWRunDateTime DATETIME2 = GETDATE();              
    --                     INSERT #DimPayRateChangeReasonUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_DimPayRateChangeReason @EDWRunDateTime
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    --  2/27/2018  valimineti              BNG-270 Initial version of proc
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
             SELECT COALESCE(PayRateChangeReasonCode, '-1') AS PayRateChangeReasonCode,
                    COALESCE(PayRateChangeReasonName, 'Unknown Pay Rate Change Reason') AS PayRateChangeReasonName,
                    COALESCE(NULLIF(PayRateChangeReasonFlexAttribute1, ''),NULL) AS PayRateChangeReasonFlexAttribute1,
                    COALESCE(NULLIF(PayRateChangeReasonFlexAttribute2, ''),NULL) AS PayRateChangeReasonFlexAttribute2,
                    COALESCE(NULLIF(PayRateChangeReasonFlexAttribute3, ''),NULL) AS PayRateChangeReasonFlexAttribute3,
                    COALESCE(NULLIF(PayRateChangeReasonFlexAttribute4, ''),NULL) AS PayRateChangeReasonFlexAttribute4,
                    COALESCE(NULLIF(PayRateChangeReasonFlexAttribute5, ''),NULL) AS PayRateChangeReasonFlexAttribute5,
                    COALESCE(PayRateChangeReasonCreatedDate, '1/1/1900') AS PayRateChangeReasonCreatedDate,
                    COALESCE(PayRateChangeReasonCreatedUser, -1) AS PayRateChangeReasonCreatedUser,
                    COALESCE(PayRateChangeReasonModifiedDate, '1/1/1900') AS PayRateChangeReasonModifiedDate,
                    COALESCE(PayRateChangeReasonModifiedUser, -1) AS PayRateChangeReasonModifiedUser,
                    @EDWRunDateTime AS EDWCreatedDate,
					@EDWRunDateTime AS EDWModifiedDate
             FROM dbo.vPayRateChangeReasons;
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