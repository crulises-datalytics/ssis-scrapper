CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimSpecialInfo] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimSpecialInfo
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
    --                     EXEC dbo.spHR_StagingTransform_DimSpecialInfo 
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
    -- 03/01/18     ADevabhakthuni         BNG-262 - Initial version
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
             SELECT COALESCE(SpecialInfoID, -1) AS SpecialInfoID,
                    COALESCE(SpecialInfoTypeID, -1) AS SpecialInfoTypeID,
                    COALESCE(SpecialInfoTypeName, 'Unknown Special Info Type') AS SpecialInfoTypeName,
                    SpecialInfoAttribute1,
                    SpecialInfoAttribute2,
                    SpecialInfoAttribute3,
                    SpecialInfoAttribute4,
                    SpecialInfoAttribute5,
                    SpecialInfoAttribute6,
                    SpecialInfoAttribute7,
                    SpecialInfoAttribute8,
                    SpecialInfoAttribute9,
                    SpecialInfoAttribute10,
                    SpecialInfoAttribute11,
                    SpecialInfoAttribute12,
                    SpecialInfoAttribute13,
                    SpecialInfoAttribute14,
                    SpecialInfoAttribute15,
                    SpecialInfoAttribute16,
                    SpecialInfoAttribute17,
                    SpecialInfoAttribute18,
                    SpecialInfoAttribute19,
                    SpecialInfoAttribute20,
                    SpecialInfoAttribute21,
                    SpecialInfoAttribute22,
                    SpecialInfoAttribute23,
                    SpecialInfoAttribute24,
                    SpecialInfoAttribute25,
                    SpecialInfoAttribute26,
                    SpecialInfoAttribute27,
                    SpecialInfoAttribute28,
                    SpecialInfoAttribute29,
                    SpecialInfoAttribute30,
                    COALESCE(SpecialInfoSummaryFlag, 'Unknown Summary') AS SpecialInfoSummaryFlag,
                    COALESCE(SpecialInfoEnabledFlag, 'Unknown Enabled') AS SpecialInfoEnabledFlag,
                    COALESCE(SpecialInfoCreatedUser, -1) AS SpecialInfoCreatedUser,
                    COALESCE(SpecialInfoCreatedDate, '19000101') AS SpecialInfoCreatedDate,
                    COALESCE(SpecialInfoModifiedUser, -1) AS SpecialInfoModifiedUser,
                    COALESCE(SpecialInfoModifiedDate, '19000101') AS SpecialInfoModifiedDate,
                    @EDWRunDateTime AS EDWCreatedDate,
				@EDWRunDateTime AS EDWModifiedDate
             FROM dbo.vSpecialInfo;
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