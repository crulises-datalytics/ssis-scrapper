


/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCMS_StagingTransform_DimSponsor'
)
    DROP PROCEDURE dbo.spCMS_StagingTransform_DimSponsor;
GO
*/
CREATE PROCEDURE [dbo].[spCMS_StagingTransform_DimSponsor]
(@EDWRunDateTime   DATETIME2 = NULL
)
AS
-- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_DimSponsor
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
    --                     EXEC dbo.spCMS_StagingTransform_DimSponsor @EDWRunDateTime
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By        Comments
    -- ----          -----------        --------
    --
    -- 12/12/17      Adevabhakthuni     Initial version of proc, converted from SSIS logic
	-- 08/29/22      hhebbalu           Added PartnerID BI-6478
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
            DECLARE @LastProcessedDate DATETIME
						SET @LastProcessedDate = 
												(SELECT LastProcessedDate 
												FROM EDWETLBatchControl (NOLOCK) WHERE EventName = 'DimSponsor');
								     IF @LastProcessedDate IS NULL
										 SET @LastProcessedDate = '19000101'; 
						
						
						SELECT 	s.idSponsor AS SponsorID,
								COALESCE(NULLIF(s.idPartner, ''),-1) AS PartnerID,
								COALESCE(NULLIF(s.FirstName, ''), 'Unknown Sponsor Name') AS SponsorFirstName,
								COALESCE(NULLIF(s.MiddleName,''), 'Unknown Sponsor Name') AS SponsorMiddleName,
								COALESCE(NULLIF(s.LastName, ''), 'Unknown Sponsor Name') AS SponsorLastName,
								COALESCE(NULLIF(s.FirstName + ' ' + s.LastName, ''), 'Unknown Sponsor Name') AS SponsorFullName,
								COALESCE(NULLIF(s.PrimaryPhoneNumber,''), 'Unknown Phone Number') AS SponsorPhonePrimary,
								COALESCE(NULLIF(s.SecondaryPhoneNumber,''), 'Unknown Phone Number') AS SponsorPhoneSecondary,
								COALESCE(NULLIF(s.OtherPhoneNumber,''), 'Unknown Phone Number') AS SponsorPhoneTertiary,
								COALESCE(NULLIF(s.EmailAddress,''), 'Unknown Email') AS SponsorEmailPrimary,
								COALESCE(NULLIF(s.AlternateEmailAddress,''), 'Unknown Email') AS SponsorEmailSecondary,
								COALESCE(NULLIF(a.Address1,''), 'Unknown Address') AS SponsorAddress1,
								COALESCE(NULLIF(a.Address2,''), 'Unknown Address') AS SponsorAddress2,
								COALESCE(NULLIF(a.City,''), 'Unknown City') AS SponsorCity,
								COALESCE(NULLIF(a.idState,''), 'XX') AS SponsorState,
								COALESCE(NULLIF(a.ZipCode,''), 'Unknown') AS SponsorZIP,
								COALESCE(NULLIF(sr.relation,''), 'Unknown Relationship') AS SponsorStudentRelationship,
								CASE
									WHEN s.Gender = 'M' THEN 'Male'
									WHEN s.Gender = 'F' THEN 'Female'
										ELSE  'Unknown'
					END AS SponsorGender,
								CASE
									WHEN s.isKLCEmployee = 0 THEN 'Internal Employee'
									WHEN s.isKLCEmployee = 1 THEN 'External Employee'
											ELSE  'Unknown Employee Type'
					END AS SponsorInternalEmployee,
								COALESCE(NULLIF(ss.SponsorStatus,''), 'Unknown Sponsor Status') AS SponsorStatus,
									CASE
										WHEN s.DoNotEmail = 0 THEN 'Can Email Sponsor'
										WHEN s.DoNotEmail = 1 THEN 'Cannot Email Sponsor'
												ELSE  'Unknown Email Sponsor'
								END AS SponsorDoNotEmail,
								COALESCE(NULLIF(s.LeadMgmtAccountID,''), '-1') AS SponsorLeadManagementID,
								-2 AS CSSCenterNumber,
								-2 AS CSSFamilyNumber,
								'CMS' AS SourceSystem,
						 @EDWRunDateTime AS EDWEffectiveDate,
                         NULL AS EDWEndDate,
                         GETDATE() AS EDWCreatedDate,
                         CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
								COALESCE(NULLIF(s.Deleted,''), NULL) AS Deleted
								FROM CMS_Staging.dbo.sponSponsor s (NOLOCK)
								LEFT JOIN CMS_Staging.dbo.Address a (NOLOCK) ON s.idAddress = a.idAddress
								LEFT JOIN CMS_Staging.dbo.sponSponsorStatus ss (NOLOCK) ON s.idSponsorStatus = ss.idSponsorStatus
								Outer Apply(
												SELECT top 1 sr.Relation FROM  CMS_Staging.dbo.stdStudentSponsor sp (NOLOCK) 
												LEFT JOIN CMS_Staging.dbo.stdRelationship sr (NOLOCK) on sr.idRelationship = sp.idRelationship
							WHERE sp.idSponsor = s.idSponsor
											Order by Sp.ModifiedDate 
									)sr
									WHERE  s.StgModifiedDate>= @LastProcessedDate OR
									 a.StgModifiedDate>= @LastProcessedDate OR
										 ss.StgModifiedDate>=@LastProcessedDate
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