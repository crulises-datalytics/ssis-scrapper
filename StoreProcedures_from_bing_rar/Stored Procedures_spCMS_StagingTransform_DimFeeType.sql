CREATE PROCEDURE [dbo].[spCMS_StagingTransform_DimFeeType]
(@EDWRunDateTime   DATETIME2 = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_DimFeeType
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
    --                     EXEC dbo.spCMS_StagingTransform_DimFeeType 
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
    --  1/25/18     sburke          BNG-1006 - Converting SSIS source logic to the 
    --                                  sp_CMS_StagingTransform pattern
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
             DECLARE @LastProcessedDate DATETIME;
             SET @LastProcessedDate =
             (
                 SELECT LastProcessedDate
                 FROM EDWETLBatchControl
                 WHERE EventName = 'DimFeeType'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

             SELECT COALESCE(f.idFees, -1) AS FeeTypeID,
                    COALESCE(NULLIF(f.FeesName, ''), 'Unknown Fee Type') AS FeeTypeName,
                    COALESCE(NULLIF(f.FeesDescription, ''), 'Unknown Fee Type Description') AS FeeTypeDescription,
                    COALESCE(NULLIF(ft.FeesType, ''), 'Unknown Fee Category') AS FeeCategory,
                    COALESCE(NULLIF(fu.Unit, ''), 'Unknown Fee Unit of Measure') AS FeeUnitOfMeasure,
                    COALESCE(f.FTE, 0) AS FeeFTE,
                    -2 AS CSSTransactionCode,
                    -2 AS CSSTransactionType,
                    'CMS' AS SourceSystem,
                    @EDWRunDateTime AS EDWCreatedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                    @EDWRunDateTime AS EDWModifiedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy,
                    f.Deleted
             FROM srvcatFees f
                  LEFT JOIN dbo.srvcatFeesType ft ON f.idFeesType = ft.idFeesType
                  LEFT JOIN dbo.srvcatFeesUnit fu ON f.idFeesUnit = fu.idFeesUnit
             WHERE f.StgModifiedDate >= @LastProcessedDate
                   OR ft.StgModifiedDate >= @LastProcessedDate
                   OR fu.StgModifiedDate >= @LastProcessedDate;
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