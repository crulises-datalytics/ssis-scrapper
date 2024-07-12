Create PROCEDURE [dbo].[spCMS_StagingTransform_DimTier]
(@EDWRunDateTime   DATETIME2 = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_DimTier
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
    --                     EXEC dbo.spCMS_StagingTransform_DimTier 
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By				Comments
    -- ----         -----------				--------
    --
    --  02/27/18     Adevabhakthuni      BNG-1242 - Converting SSIS source logic to the 
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
                 WHERE EventName = 'DimTier'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

             WITH TierCTE
                  AS (
                  SELECT idSiteTier AS TierID,
                         TierName,
                         obc.BillingCycleName AS TierBillingFrequency,
                         CASE
                             WHEN ShowToSponsor = 0
                             THEN 'Hide Tier'
                             WHEN ShowToSponsor = 1
                             THEN 'Show Tier'
                             ELSE 'Unknown Show Tier'
                         END AS TierShowToSponsor,
                         TierDesc AS TierFriendlyName,
                         SUBSTRING(TierName, 0, (((PATINDEX('%[a-z]%', SUBSTRING(TierName, PATINDEX('%[0-9]%', TierName), LEN(TierName))))-1)+PATINDEX('%[0-9]%', TierName))) AS TierAssignment,
                         CASE
                             WHEN CHARINDEX('Weekly', TierName) != 0
                             THEN COALESCE(NULLIF(SUBSTRING(tiername, (CHARINDEX('Weekly', tiername)+6), LEN(tiername)), ''), 'No Student Tier Label')
                             WHEN CHARINDEX('Monthly', TierName) != 0
                             THEN COALESCE(NULLIF(SUBSTRING(tiername, (CHARINDEX('Monthly', tiername)+7), LEN(tiername)), ''), 'No Student Tier Label')
                             WHEN CHARINDEX('Weekly', TierName) = 0
                                  OR CHARINDEX('Monthly', TierName) = 0
                             THEN COALESCE(NULLIF(SUBSTRING(tiername, (((PATINDEX('%[a-z]%', SUBSTRING(TierName, PATINDEX('%[0-9]%', TierName), LEN(TierName))))-1)+PATINDEX('%[0-9]%', TierName)), LEN(tiername)), ''), 'No Student Tier Label')
                             ELSE 'Unknown Student Tier Label'
                         END AS TierLabel,
                         lst.Deleted
                  FROM locSiteTier lst
                       LEFT JOIN orgBillingCycle obc ON lst.idBillingCycle = obc.idBillingCycle
                  WHERE lst.StgModifiedDate >= @LastProcessedDate
                        OR obc.StgModifiedDate >= @LastProcessedDate)
                  SELECT TierID,
                         COALESCE(NULLIF(TierName, ''), 'Unknown Tier Name') AS TierName,
                         COALESCE(NULLIF(TierFriendlyName, ''), 'Unknown Tier Friendly Name') AS TierFriendlyName,
                         CAST(COALESCE(NULLIF(REPLACE(TierAssignment, 'T', 'Tier '), ''), 'Unknown') AS VARCHAR(10)) AS TierAssignment,
                         COALESCE(NULLIF(TierBillingFrequency, ''), 'Unknown Billing Frequency') AS TierBillingFrequency,
                         COALESCE(NULLIF(TierLabel, ''), 'Unknown Tier Label') AS TierLabel,
                         COALESCE(NULLIF(TierShowToSponsor, ''), 'Unknown Show To Sponsor') AS TierShowToSponsor,
                         -2 AS CSSTierNumber,
                         'CMS' AS SourceSystem,
                         GETDATE() AS EDWCreatedDate,
                         CAST(SYSTEM_USER AS VARCHAR(50)) EDWCreatedBy,
                         GETDATE() AS EDWModifiedDate,
                         CAST(SYSTEM_USER AS VARCHAR(50)) EDWModifiedBy,
                         Deleted
                  FROM TierCTE;
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