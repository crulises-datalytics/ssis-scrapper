CREATE PROCEDURE [dbo].[spCSS_StagingTransform_DimTuitionAssistanceProvider]
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_StagingTransform_DimTuitionAssistanceProvider
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
    --                     EXEC dbo.spCSS_StagingTransform_DimTuitionAssistanceProvider 
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
    --  1/24/18     sburke          BNG-655 - Correcting proc logic to correctly record to AuditLog,
    --                                  and while we are at it, convert from SSIS DFT to stored proc
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
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             WITH TAPCustomers
                  AS (
                  SELECT *
                  FROM dbo.cspcustr
                  WHERE cust_code LIKE '%[A-z]%'),
                  -- --------------------------------------------------------------------------------
                  -- There are more than one record for a center and custumer code combination. 
                  -- For example ctr_no = '4617' and cust_code = 'moac'. 
                  -- This code helps to choose one record among the multiple records
                  -- --------------------------------------------------------------------------------
                  DistinctTAP
                  AS (
                  SELECT ctr_no,
                         cust_code,
                         acct_no = MAX(acct_no)
                  FROM TAPCustomers
                  GROUP BY ctr_no,
                           cust_code),
                  TAP
                  AS (
                  SELECT a.*
                  FROM TAPCustomers a
                       INNER JOIN DistinctTAP b ON a.ctr_no = b.ctr_no
                                                   AND a.cust_code = b.cust_code
                                                   AND a.acct_no = b.acct_no)
                  SELECT-2 AS TuitionAssistanceProviderID,
                        COALESCE(cust_name, 'Unknown Tuition Assistance Provider') AS TuitionAssistanceProviderName,
                        CASE
                            WHEN cust_type = 'CC'
                            THEN 'Child Care Assistance'
                            WHEN cust_type = 'HS'
                            THEN 'Head Start'
                            WHEN cust_type = 'P'
                            THEN 'Parent'
                            WHEN cust_type = 'MP'
                            THEN 'Military'
                            WHEN cust_type = 'K'
                            THEN 'Kindustry Transfer'
                            WHEN cust_type = 'KD'
                            THEN 'Kindustry Discount'
                            WHEN cust_type = 'KB'
                            THEN 'Kindustry Both'
                            WHEN cust_type = 'KC'
                            THEN 'KC'
                            WHEN cust_type = 'KT'
                            THEN 'KT'
                            WHEN cust_type = 'LC'
                            THEN 'Local Corp'
                            WHEN cust_type = 'OT'
                            THEN 'Other'
                            ELSE 'Unknown Tuition Assistance Provider Type'
                        END AS TuitionAssistanceProviderType,
                        COALESCE(address1, 'Unknown Address') AS TuitionAssistanceProviderAddress1,
                        COALESCE(address2, 'Unknown Address') AS TuitionAssistanceProviderAddress2,
                        COALESCE(city, 'Unknown City') AS TuitionAssistanceProviderCity,
                        COALESCE(state, 'XX') AS TuitionAssistanceProviderState,
                        COALESCE(zip, 'Unknown') AS TuitionAssistanceProviderZIP,
                        COALESCE(contact, 'Unknown Tuition Assistance Provider Contact') AS TuitionAssistanceProviderContact,
                        'Unknown Subsidy Provider' AS TuitionAssistanceProviderProvidesSubsidy,
                        'Unknown Backup Care Provider' AS TuitionAssistanceProviderBackupCare,
                        'Unknown Care Select Discount' AS TuitionAssistanceProviderCareSelectDiscount,
                        NULL AS TuitionAssistanceProviderFirstContractDate,
                        ctr_no AS CSSCenterNumber,
                        cust_code AS CSSCustomerCode,
                        'CSS' AS SourceSystem,
                        GETDATE() AS EDWCreatedDate,
                        CAST(SYSTEM_USER AS VARCHAR(50)) EDWCreatedBy,
                        GETDATE() AS EDWModifiedDate,
                        CAST(SYSTEM_USER AS VARCHAR(50)) EDWModifiedBy
                  FROM TAP;
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