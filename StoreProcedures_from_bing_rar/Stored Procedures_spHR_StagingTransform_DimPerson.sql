
CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimPerson] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimPerson
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
    --                     EXEC dbo.spHR_StagingTransform_DimPerson 
    --                     (Note: 12/18/17 - Proc currently takes approx. 7 mins to process 1.5m rows into #temp table)
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
    -- 12/18/17     sburke          BNG-263 - Initial version
	-- 07/11/18     hhebbalu        BNG-3362
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
             SELECT COALESCE(PersonEffectiveDate, '19000101') AS PersonEffectiveDate, -- This should never be NULL, but in the source it is so we have to handle it
                    COALESCE(PersonEndDate, '19000101') AS PersonEndDate, -- This should never be NULL, but in the source it is so we have to handle it
                    PersonCurrentRecordFlag,
                    COALESCE(PersonID, -1) AS PersonID,
                    COALESCE(PersonTypeID, -1) AS PersonTypeID,
                    COALESCE(PersonTypeName, 'Unknown Person Type') AS PersonTypeName,
                    COALESCE(PersonFirstName, 'Unknown Person') AS PersonFirstName,
                    COALESCE(PersonMiddleName, 'Unknown Person') AS PersonMiddleName,
                    COALESCE(PersonLastName, 'Unknown Person') AS PersonLastName,
                    COALESCE(PersonFullName, 'Unknown Person') AS PersonFullName,
                    COALESCE(PersonBirthDate, '19000101') AS PersonBirthDate,
                    COALESCE(PersonBirthMonthDay, '1/1') AS PersonBirthMonthDay,
                    PersonDeathDate,
                    COALESCE(PersonGenderCode, 'Unknown Gender') AS PersonGenderCode,
                    COALESCE(PersonGenderName, 'Unknown Gender') AS PersonGenderName,
                    COALESCE(PersonMaritalStatusCode, 'Unknown Marital Status') AS PersonMaritalStatusCode,
                    COALESCE(PersonMaritalStatusName, 'Unknown Marital Status') AS PersonMaritalStatusName,
                    COALESCE(PersonEthnicityLegacyCode, 'Unknown Ethnicity') AS PersonEthnicityLegacyCode,
                    COALESCE(PersonEthnicityLegacyName, 'Unknown Ethnicity') AS PersonEthnicityLegacyName,
                    COALESCE(PersonEthnicityAIANFlag, 'Unknown American Indian / Alaska Native') AS PersonEthnicityAIANFlag,
                    COALESCE(PersonEthnicityAsianFlag, 'Unknown Asian') AS PersonEthnicityAsianFlag,
                    COALESCE(PersonEthnicityBlackFlag, 'Unknown Black') AS PersonEthnicityBlackFlag,
                    COALESCE(PersonEthnicityHispanicLatinoFlag, 'Unknown Hispanic / Latino') AS PersonEthnicityHispanicLatinoFlag,
                    COALESCE(PersonEthnicityNHOPIFlag, 'Unknown Native Hawaiian / Other Pacific Islander') AS PersonEthnicityNHOPIFlag,
                    COALESCE(PersonEthnicityWhiteFlag, 'Unknown White') AS PersonEthnicityWhiteFlag,
                    COALESCE(PersonEthnicityMultipleFlag, 'Unknown Multiple Ethnicity') AS PersonEthnicityMultipleFlag,
                    COALESCE(PersonVeteranStatusCode, 'Unknown Veteran Status') AS PersonVeteranStatusCode,
                    COALESCE(PersonVeteranStatusName, 'Unknown Veteran Status') AS PersonVeteranStatusName,
                    COALESCE(PersonSSN, '000-00-0000') AS PersonSSN,
                    COALESCE(PersonSSNLast4Digits, '0000') AS PersonSSNLast4Digits,
                    COALESCE(PersonHomePhone, 'Unknown Phone') AS PersonHomePhone,
                    COALESCE(PersonMobilePhone, 'Unknown Phone') AS PersonMobilePhone,
                    COALESCE(PersonWorkPhone, 'Unknown Phone') AS PersonWorkPhone,
                    COALESCE(PersonOtherPhone, 'Unknown Phone') AS PersonOtherPhone,
                    COALESCE(PersonEmail, 'Unknown Email') AS PersonEmail,
                    COALESCE(PersonAddress1, 'Unknown Address') AS PersonAddress1,
                    COALESCE(PersonAddress2, 'Unknown Address') AS PersonAddress2,
                    COALESCE(PersonCity, 'Unknown City') AS PersonCity,
                    COALESCE(PersonState, 'Unknown State') AS PersonState,
                    COALESCE(PersonZIP, 'Unknown ZIP') AS PersonZIP,
                    COALESCE(PersonCounty, 'Unknown County') AS PersonCounty,
                    COALESCE(PersonCountry, 'Unknown Country') AS PersonCountry,
                    PersonEnteredSystemDate,
                    COALESCE(PersonMealPeriodWaivedFlag, 'Unknown Meal Period Waived') AS PersonMealPeriodWaivedFlag,
                    COALESCE(PersonAlaskaWaiveDailyOTFlag, 'Unknown Alaska Waive Daily OT') AS PersonAlaskaWaiveDailyOTFlag,
                    Person401kVestingDate,
                    ApplicantNumber,
                    EmployeeNumber,
                    EmployeeLegacyNumber,
                    COALESCE(EmployeeOriginalStartDate, '19000101') AS EmployeeOriginalStartDate, -- This should never be NULL, but in the source it is so we have to handle it
                    COALESCE(EmployeeCurrentFlag, 'Unknown Employee') AS EmployeeCurrentFlag,
                    COALESCE(EmployeeOrApplicantCurrentFlag, 'Unknown Employee Or Applicant') AS EmployeeOrApplicantCurrentFlag,
                    EmployeeEmailAddress,
                    EmployeeActiveDirectoryUserNumber,
                    EmployeeActiveDirectoryUserName,
					COALESCE(DomainName, 'Unknown Employee Active Directory Domain Name')AS EmployeeActiveDirectoryDomainName, --BNG-3362
					COALESCE(DomainName + '\' + EmployeeActiveDirectoryUserName, 'Unknown Employee Active Directory Domain And UserName') AS EmployeeActiveDirectoryDomainAndUserName, --BNG-3362
                    ContingentWorkerNumber,
                    COALESCE(ContingentWorkerCurrentFlag, 'Unknown Contingent Worker') AS ContingentWorkerCurrentFlag,
                    PersonFlexValue1,
                    PersonFlexValue2,
                    PersonFlexValue3,
                    PersonFlexValue4,
                    PersonFlexValue5,
                    COALESCE(PersonCreatedDate, '19000101') AS PersonCreatedDate,
                    COALESCE(PersonCreatedUser, -1) AS PersonCreatedUser,
                    COALESCE(PersonModifiedDate, '19000101') AS PersonModifiedDate,
                    COALESCE(PersonModifiedUser, -1) AS PersonModifiedUser,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate
             FROM dbo.vPeople;
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