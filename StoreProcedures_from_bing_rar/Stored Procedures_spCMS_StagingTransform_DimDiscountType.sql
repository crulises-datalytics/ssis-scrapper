CREATE PROCEDURE [dbo].[spCMS_StagingTransform_DimDiscountType] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_DimDiscountType
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
    --                     EXEC dbo.spCMS_StagingTransform_DimDiscountType 
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
    -- 02/14/18    anmorales        BNG-1235 - Refactor DimDiscountType ETL so it 
    --                                         uses Stored Proc over DFTs creating 
    --                                         temporary DB Objects: Added Parameter to make consistent with other procedures.
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
                 FROM EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'DimDiscountType'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

             SELECT d.idDiscount AS DiscountTypeID,
                    COALESCE(NULLIF(d.DiscountName, ''), 'Unknown Discount') DiscountTypeName,
                    COALESCE(NULLIF(d.DiscountDescription, ''), 'Unknown Discount Description') DiscountTypeDescription,
                    COALESCE(NULLIF(dt.DiscountType, ''), 'Unknown Discount Category') DiscountCategory,
                    (CASE
                         WHEN d.isRecurringDiscount = 0
                         THEN 'Discount Not Recurring'
                         WHEN d.isRecurringDiscount = 1
                         THEN 'Discount Recurring'
                         ELSE 'Unknown Discount Recurring'
                     END) AS DiscountRecurring,
                    COALESCE(NULLIF(d.DiscountPriority, ''), '999999') DiscountPriority,
                    (CASE
                         WHEN d.isNet = 0
                         THEN 'Gross Discount'
                         WHEN d.isNet = 1
                         THEN 'Net Discount'
                         ELSE 'Unknown Net Discount'
                     END) AS DiscountNet,
                    '-2' AS CSSTransactionCode,
                    'XX' AS CSSTransactionType,
                    'CMS' AS SourceSystem,
                    @EDWRunDateTime AS EDWCreatedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) EDWCreatedBy,
                    @EDWRunDateTime AS EDWModifiedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) EDWModifiedBy,
                    d.Deleted
             FROM CMS_Staging.dbo.srvcatDiscount d(NOLOCK)
                  LEFT JOIN CMS_Staging.dbo.srvcatDiscountType dt(NOLOCK) ON d.idDiscountType = dt.idDiscountType
             WHERE d.StgModifiedDate >= @LastProcessedDate
                   OR dt.StgModifiedDate >= @LastProcessedDate;
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