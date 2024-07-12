CREATE PROCEDURE [dbo].[spSalesForce_StagingTransform_DimLeadType] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spSalesForce_StagingTransform_DimLeadType
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
    --                     EXEC dbo.spSalesForce_StagingTransform_DimLeadType 
    --                     
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
    -- 2/2/18        Banandesi          BNG-250 - Initial version
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
             SELECT DISTINCT
                    COALESCE(LeadStatus, 'Unknown Status') AS LeadStatus,
                    COALESCE(InquiryBrand, 'Unknown Brand') AS InquiryBrand,
                    COALESCE(InquirySourceType, 'Unknown Source Type') AS InquirySourceType,
                    COALESCE(InquirySource, 'Unknown Source') AS InquirySource,
                    COALESCE(InquiryType, 'Unknown Inquiry Type') AS InquiryType,
                    COALESCE(MethodOfContact, 'Unknown Method of Contact') AS MethodOfContact,
                    COALESCE(ContactPreference, 'Unknown Contact Preference') AS ContactPreference,
                    @EDWRunDateTime AS EDWCreatedDate
             FROM dbo.vLeads;
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