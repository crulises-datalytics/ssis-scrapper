CREATE PROCEDURE [dbo].[spCMS_StagingTransform_DimTuitionAssistanceProvider]
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_DimTuitionAssistanceProvider
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
    --                     EXEC dbo.spCMS_StagingTransform_DimTuitionAssistanceProvider 
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
    --  1/24/18     sburke          BNG-1006 - Converting SSIS source logic to the 
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
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             DECLARE @LastProcessedDate DATETIME;
             SET @LastProcessedDate =
             (
                 SELECT LastProcessedDate
                 FROM EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'DimTuitionAssistanceProvider'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

             WITH sbsdySubsidyContractCTE
                  AS (
                  SELECT idSubsidyAgency,
                         MIN(StartDate) AS ContractStartDate
                  FROM sbsdySubsidyContract
                  GROUP BY idSubsidyAgency)
                  SELECT sa.idSubsidyAgency AS TuitionAssistanceProviderID,
                         COALESCE(NULLIF(sa.SubsidyAgencyName, ''), 'Unknown Tuition Assistance Provider') AS TuitionAssistanceProviderName,
                         COALESCE(NULLIF(sat.SubsidyAgencyTypeName, ''), 'Unknown Tuition Assistance Provider Type') AS TuitionAssistanceProviderType,
                         COALESCE(NULLIF(a.Address1, ''), 'Unknown Address') AS TuitionAssistanceProviderAddress1,
                         COALESCE(NULLIF(a.Address2, ''), 'Unknown Address') AS TuitionAssistanceProviderAddress2,
                         COALESCE(NULLIF(a.City, ''), 'Unknown City') AS TuitionAssistanceProviderCity,
                         COALESCE(NULLIF(a.idState, ''), 'XX') AS TuitionAssistanceProviderState,
                         COALESCE(NULLIF(a.ZipCode, ''), 'Unknown') AS TuitionAssistanceProviderZIP,
                         COALESCE(NULLIF(pep.ContactName, ''), 'Unknown Tuition Assistance Provider Contact') AS TuitionAssistanceProviderContact,
                         CASE
                             WHEN pep.ProvidesSubsidy = 0
                             THEN 'Provides Subsidy'
                             WHEN pep.ProvidesSubsidy = 1
                             THEN 'Does Not Provide Subsidy'
                             ELSE 'Unknown Subsidy Provider'
                         END AS TuitionAssistanceProviderProvidesSubsidy,
                         CASE
                             WHEN pep.IsBackupCare = 0
                             THEN 'Provides Backup Care'
                             WHEN pep.IsBackupCare = 1
                             THEN 'Does Not Provide Backup Care'
                             ELSE 'Unknown Backup Care Provider'
                         END AS TuitionAssistanceProviderBackupCare,
                         CASE
                             WHEN pep.KindustryDiscountFlag = 0
                             THEN 'Provides Care Select'
                             WHEN pep.KindustryDiscountFlag = 1
                             THEN 'Does Not Provide Care Select'
                             ELSE 'Unknown Care Select Discount'
                         END AS TuitionAssistanceProviderCareSelectDiscount,
                         sccte.ContractStartDate AS TuitionAssistanceProviderFirstContractDate,
                         '-2' AS CSSCenterNumber,
                         '-2' AS CSSCustomerCode,
                         'CMS' AS SourceSystem,
                         GETDATE() AS EDWCreatedDate,
                         CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                         GETDATE() AS EDWModifiedDate,
                         CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy,
                         sa.Deleted AS Deleted
                  FROM dbo.orgSubsidyAgency sa(NOLOCK)
                       LEFT JOIN dbo.orgSubsidyAgencyType sat(NOLOCK) ON sa.idSubsidyAgencyType = sat.idSubsidyAgencyType
                       LEFT JOIN dbo.Address a(NOLOCK) ON sa.idAddress = a.idAddress
                       LEFT JOIN dbo.orgPartnerExtendedProperties pep(NOLOCK) ON sa.idSubsidyAgency = pep.idSubsidyAgency
                       LEFT JOIN sbsdySubsidyContractCTE sccte ON sccte.idSubsidyAgency = sa.idSubsidyAgency
                  WHERE sa.StgModifiedDate >= @LastProcessedDate
                        OR sat.StgModifiedDate >= @LastProcessedDate
                        OR a.StgModifiedDate >= @LastProcessedDate
                        OR pep.StgModifiedDate >= @LastProcessedDate;
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