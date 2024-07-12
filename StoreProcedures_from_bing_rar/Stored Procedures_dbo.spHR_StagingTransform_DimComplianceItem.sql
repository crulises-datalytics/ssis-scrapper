Create Procedure [dbo].[spHR_StagingTransform_DimComplianceItem] 
(@EDWRunDateTime DateTime2=Null) 
As
 
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimComplianceItem
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
    --                     EXEC dbo.spHR_StagingTransform_DimComplianceItem 
    -- 
    --
    -- --------------------------------------------------------------------------------
	-- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    --
    -- 03/12/18    Adevabhakthuni          BNG-272 - Initial version
    --			 
    -- ================================================================================
	Begin 
	 SET NOCOUNT ON;
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
		 SELECT COALESCE(ComplianceItemID, -1) AS ComplianceItemID,
                    COALESCE(ComplianceItemName, 'Unknown Compliance Item') AS ComplianceItemName,
                    COALESCE(ComplianceItemDescription, 'Unknown Compliance Item') AS ComplianceItemDescription,
                    COALESCE(ComplianceItemEvaluationMethodCode, '-1') AS ComplianceItemEvaluationMethodCode,
					COALESCE(ComplianceItemEvaluationMethodName, 'Unknown Evalution Method') AS ComplianceItemEvaluationMethodName,
					ComplianceItemFlexAttribute1,
					ComplianceItemFlexAttribute2,
					ComplianceItemFlexAttribute3,
					ComplianceItemFlexAttribute4,
					ComplianceItemFlexAttribute5,
                    COALESCE(ComplianceItemCreatedDate, '19000101') AS ComplianceItemCreatedDate,
                    COALESCE(ComplianceItemCreatedUser, -1) AS ComplianceItemCreatedUser,
                    COALESCE(ComplianceItemModifiedDate, '19000101') AS ComplianceItemModifiedDate,
                    COALESCE(ComplianceItemModifiedUser, -1) AS ComplianceItemModifiedUser,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate
					From dbo.vComplianceItems;
			End Try 
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