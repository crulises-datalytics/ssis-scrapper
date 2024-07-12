

/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spSalesforce_StagingTransform_DimLead'
)
    DROP PROCEDURE dbo.spSalesforce_StagingTransform_DimLead;
GO
*/
CREATE   PROCEDURE [dbo].[spSalesforce_StagingTransform_DimLead] @EDWRunDateTime DATETIME2 = NULL
AS
-- ================================================================================
-- 
-- Stored Procedure:   spSalesforce_StagingTransform_DimLead
--
-- Purpose:            Performs the Transformation logic with the source database
--                         for a given Fact or Dimension table, and returns the
--                         results set to the caller (usually for populating a
--                         temporary table).
--
-- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
--                         making numerous GETDATE() calls  
--				   
-- Returns:            Results set containing the Transformed data ready for
--                         consumption by the ETL process for load into BING_EDW
--
-- Usage:              INSERT #TemplateUpsert -- (Temporary table)
--                     EXEC dbo.spSalesforce_StagingTransform_DimLead @EDWRunDateTime
-- 
--
-- --------------------------------------------------------------------------------
--
-- Change Log:		   
-- ----------
--
-- Date			Modified By         Comments
-- ----			-----------         --------
--
-- 02/02/18		hhebbalu            BNG-257 - EDW DimLead
-- 03/14/18		hhebbalu			BNG-1380 - Correct DimLead load merge failures when running in Incremental loads
--									The code is failing after we loaded SponsorManagementLeadID column in DimSponsor from Family Builder.
--									In CSS, there are cases where a family has multiple famno and all those families have the same accountID from family builder.
--									This creates duplicates in DimLead when joined with DimSponsor to pull the SponsorKey. This is fixed to not fail. But if the source 
--									has duplicates, we're going to show as it is.
-- 12/10/18     hhebbalu			BNG-4502 Fix DimLead table logic in BING EDW							
-- 11/23/2021   Banandesi           BI-5140 Added new attaributes 		
-- 01/25/2022   Adevabhakthuni      BI-5389 Updated the query to remove the Nullif logic for the BIT type columns  
-- ================================================================================
BEGIN
    SET NOCOUNT ON;
    --
    -- Housekeeping Variables
    -- 
    DECLARE @ProcName NVARCHAR(500) = OBJECT_NAME(@@PROCID);
    DECLARE @DebugMsg NVARCHAR(500);
    --
    -- If we do not get an @EDWRunDateTime input, set to current date
    --
    IF @EDWRunDateTime IS NULL
        SET @EDWRunDateTime = GETDATE();
    --
    -- Execute the extract / Transform from the Staging database source
    --
    BEGIN TRY
        DECLARE @LastProcessedDate DATETIME;
        SET @LastProcessedDate =
        (
            SELECT LastProcessedDate
            FROM Salesforce_Staging.dbo.EDWETLBatchControl (NOLOCK)
            WHERE EventName = 'DimLead'
        );
        IF @LastProcessedDate IS NULL
            SET @LastProcessedDate = '19000101'; -- If no previous load logged in EDWETLBatchControl, assume we bring in everything


        SELECT COALESCE(NULLIF(s.SponsorKey, ''), -1) SponsorKey,
               COALESCE(NULLIF(l.LeadID, ''), '-1') LeadID,
               COALESCE(NULLIF(l.LeadName, ''), 'Unknown Lead') LeadName,
               COALESCE(NULLIF(l.LeadContact, ''), 'Unknown Lead Contact') LeadContact,
               COALESCE(NULLIF(l.LeadAddress, ''), 'Unknown Address') LeadAddress,
               COALESCE(NULLIF(l.LeadCity, ''), 'Unknown City') LeadCity,
               COALESCE(NULLIF(l.LeadState, ''), 'Unknown State') LeadState,
               COALESCE(NULLIF(l.LeadZIP, ''), 'Unknown ZIP') LeadZIP,
               COALESCE(NULLIF(l.LeadPhone, ''), 'Unknown Phone') LeadPhone,
               COALESCE(NULLIF(l.LeadMobilePhone, ''), 'Unknown MobilePhone') LeadMobilePhone,
               COALESCE(NULLIF(l.LeadEmail, ''), 'Unknown Email') LeadEmail,
               COALESCE(NULLIF(l.LeadStatus, ''), 'Unknown Status') LeadStatus,
               COALESCE(NULLIF(l.InquiryBrand, ''), 'Unknown Brand') InquiryBrand,
               COALESCE(NULLIF(l.InquirySourceType, ''), 'Unknown Source Type') InquirySourceType,
               COALESCE(NULLIF(l.InquirySource, ''), 'Unknown Source') InquirySource,
               COALESCE(NULLIF(l.InquiryType, ''), 'Unknown Inquiry Type') InquiryType,
               l.IsWebInquiryC AS IsWebInquiry,
               l.IsContactedWithin24HoursC AS IsContactedWithin24Hours,
               l.IsCreatedMondayThursdayLocalC AS IsCreatedMondayThursdayLocal,
               COALESCE(NULLIF(l.MethodOfContact, ''), 'Unknown Method Of Contact') MethodOfContact,
               COALESCE(NULLIF(l.ContactPreference, ''), 'Unknown Contact Preference') ContactPreference,
               COALESCE(ps.SponsorKey, -1) AS PreviousSponsorKey,
               @EDWRunDateTime AS EDWCreatedDate,
               @EDWRunDateTime AS EDWModifiedDate
        FROM Salesforce_Staging.dbo.vLeads l
            LEFT JOIN CSS_Staging..CenterCSSMigrations CS
                ON l.LocationNumber = CS.CostCenterNumber
            LEFT JOIN BING_EDW.dbo.DimSponsor s
                ON l.AccountID = s.SponsorLeadManagementID
                   AND s.EDWEndDate IS NULL
                   AND (CASE
                            WHEN CAST(InquiryDate AS DATE) <= MigrationDate THEN
                                'CSS'
                            WHEN InquiryBrand = 'Champions' THEN
                                'PRO'
                            ELSE
                                'CMS'
                        END = s.SourceSystem
                       )
            --BNG-4502 Fix DimLead table logic in BING EDW/ Added the condition to join on the AccountID from CSS source until the migration and fetch Acount ID from CMS after the Migration date.
            LEFT JOIN BING_EDW.dbo.DimSponsor ps
                ON s.SponsorID = ps.SponsorID
                   AND s.CSSCenterNumber = ps.CSSCenterNumber
                   AND s.CSSFamilyNumber = ps.CSSFamilyNumber
                   AND ps.EDWEndDate = s.EDWEffectiveDate
        WHERE l.StgModifiedDate >= @LastProcessedDate;
    END TRY
    --
    -- Catch, and throw the error back to the calling procedure or client
    --
    BEGIN CATCH
        DECLARE @ErrMsg NVARCHAR(4000),
                @ErrSeverity INT;
        SELECT @ErrMsg = N'Sub-procedure ' + @ProcName + N' - ' + ERROR_MESSAGE(),
               @ErrSeverity = ERROR_SEVERITY();
        RAISERROR(@ErrMsg, @ErrSeverity, 1);
    END CATCH;
END;