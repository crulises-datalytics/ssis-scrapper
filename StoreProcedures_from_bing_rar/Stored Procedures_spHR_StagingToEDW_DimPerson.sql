CREATE PROCEDURE [dbo].[spHR_StagingToEDW_DimPerson]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimPerson
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the DimCreditMemoType table from Staging to BING_EDW.
    --
    --
    -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                         making numerous GETDATE() calls  
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --				   
    -- Returns:            Single-row results set containing the following columns:
    --                         SourceCount - Number of rows extracted from source
    --                         InsertCount - Number or rows inserted to target table
    --                         DeleteCount - Number or rows deleted in target table
    --
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimPerson @DebugMode = 1	
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By         Comments
    -- ----         -----------         --------
    --
    -- 12/19/17     sburke              BNG-263 - Initial version
    -- 07/11/18     hhebbalu            BNG-3362 - Added 2 new columns EmployeeActiveDirectoryDomainName, EmployeeActiveDirectoryDomainAndUserName		 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimPerson';
         DECLARE @AuditId BIGINT;

	    --
	    -- ETL status Variables
	    --
         DECLARE @RowCount INT;
         DECLARE @Error INT;
         DECLARE @SourceCount INT= 0;
         DECLARE @InsertCount INT= 0;
         DECLARE @UpdateCount INT= 0;
         DECLARE @DeleteCount INT= 0;

	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
	    --
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting.';
         PRINT @DebugMsg;

	    --
	    -- Write to EDW AuditLog we are starting
	    --
         EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
              @SourceName = @SourceName,
              @AuditId = @AuditId OUTPUT; 

	    -- --------------------------------------------------------------------------------
	    -- Extract from Source, Upserts and Deletes contained in a single transaction.  
	    --	 Rollback on error
	    -- --------------------------------------------------------------------------------
         BEGIN TRY
             BEGIN TRANSACTION;
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Beginning transaction.';
             PRINT @DebugMsg;

           -- ================================================================================
           -- For the DimPerson load we deviate slightly from theb usual pattern of using
           --     a MERGE statement.  This is because the Source deals with all the history
           --     and SCD for us, and to do a MERGE again here is quite costly for no real
           --     benefit (we would have to compare on the entire record).
           -- Therefore, we just do a kill & fill as it is easier logically (and on the 
           --     optimizer
           -- ================================================================================

		   -- --------------------------------------------------------------------------------
		   -- Get @SourceCount & @DeleteCount (which is the EDW DimPerson rowcount pre-truncate)		   
		   -- --------------------------------------------------------------------------------

             SELECT @SourceCount = COUNT(1)
             FROM dbo.vPeople;
             SELECT @DeleteCount = COUNT(1)
             FROM [BING_EDW].[dbo].[DimPerson];

		   -- --------------------------------------------------------------------------------
		   -- Clear-down EDW DimPerson		   
		   -- --------------------------------------------------------------------------------

             TRUNCATE TABLE [BING_EDW].[dbo].[DimPerson];
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' rows from Target.';
             PRINT @DebugMsg;

		   -- --------------------------------------------------------------------------------
		   -- [Re]Insert Seed Rows into EDW DimPerson	   
		   -- --------------------------------------------------------------------------------
             SET IDENTITY_INSERT [BING_EDW].[dbo].[DimPerson] ON;
             INSERT INTO [BING_EDW].[dbo].[DimPerson]
             (PersonKey,
              PersonEffectiveDate,
              PersonEndDate,
              PersonCurrentRecordFlag,
              PersonID,
              PersonTypeID,
              PersonTypeName,
              PersonFirstName,
              PersonMiddleName,
              PersonLastName,
              PersonFullName,
              PersonBirthDate,
              PersonBirthMonthDay,
              PersonDeathDate,
              PersonGenderCode,
              PersonGenderName,
              PersonMaritalStatusCode,
              PersonMaritalStatusName,
              PersonEthnicityLegacyCode,
              PersonEthnicityLegacyName,
              PersonEthnicityAIANFlag,
              PersonEthnicityAsianFlag,
              PersonEthnicityBlackFlag,
              PersonEthnicityHispanicLatinoFlag,
              PersonEthnicityNHOPIFlag,
              PersonEthnicityWhiteFlag,
              PersonEthnicityMultipleFlag,
              PersonVeteranStatusCode,
              PersonVeteranStatusName,
              PersonSSN,
              PersonSSNLast4Digits,
              PersonHomePhone,
              PersonMobilePhone,
              PersonWorkPhone,
              PersonOtherPhone,
              PersonEmail,
              PersonAddress1,
              PersonAddress2,
              PersonCity,
              PersonState,
              PersonZIP,
              PersonCounty,
              PersonCountry,
              PersonEnteredSystemDate,
              PersonMealPeriodWaivedFlag,
              PersonAlaskaWaiveDailyOTFlag,
              Person401kVestingDate,
              ApplicantNumber,
              EmployeeNumber,
              EmployeeLegacyNumber,
              EmployeeOriginalStartDate,
              EmployeeCurrentFlag,
              EmployeeOrApplicantCurrentFlag,
              EmployeeEmailAddress,
              EmployeeActiveDirectoryUserNumber,
              EmployeeActiveDirectoryUserName,
			  EmployeeActiveDirectoryDomainName,
			  EmployeeActiveDirectoryDomainAndUserName,
              ContingentWorkerNumber,
              ContingentWorkerCurrentFlag,
              PersonFlexValue1,
              PersonFlexValue2,
              PersonFlexValue3,
              PersonFlexValue4,
              PersonFlexValue5,
              PersonCreatedDate,
              PersonCreatedUser,
              PersonModifiedDate,
              PersonModifiedUser,
              EDWCreatedDate,
              EDWModifiedDate
             )
             VALUES
             (-1,
              '19000101',
              '99991231',
              'Y',
              -1,
              -1,
              'Unknown Person Type',
              'Unknown Person',
              'Unknown Person',
              'Unknown Person',
              'Unknown Person',
              '19000101',
              '1/1',
              NULL,
              'Unknown Gender',
              'Unknown Gender',
              'Unknown Marital Status',
              'Unknown Marital Status',
              'Unknown Ethnicity',
              'Unknown Ethnicity',
              'Unknown American Indian / Alaska Native',
              'Unknown Asian',
              'Unknown Black',
              'Unknown Hispanic / Latino',
              'Unknown Native Hawaiian / Other Pacific Islander',
              'Unknown White',
              'Unknown Multiple Ethnicity',
              'Unknown Veteran Status',
              'Unknown Veteran Status',
              '000-00-0000',
              '0000',
              'Unknown Phone',
              'Unknown Phone',
              'Unknown Phone',
              'Unknown Phone',
              'Unknown Email',
              'Unknown Address',
              'Unknown Address',
              'Unknown City',
              'Unknown State',
              'Unknown ZIP',
              'Unknown County',
              'Unknown Country',
              '19000101',
              'Unknown Meal Period Waived',
              'Unknown Alaska Waive Daily OT',
              NULL,
              NULL,
              NULL,
              NULL,
              '19000101',
              'Unknown Employee',
              'Unknown Employee Or Applicant',
              NULL,
              NULL,
              NULL,
			  'Unknown Employee Active Directory Domain Name',
			  'Unknown Employee Active Directory Domain And User Name',
              NULL,
              'Unknown Contingent Worker',
              NULL,
              NULL,
              NULL,
              NULL,
              NULL,
              '19000101',
              -1,
              '19000101',
              -1,
              GETDATE(),
              GETDATE()
             );
             SET IDENTITY_INSERT [BING_EDW].[dbo].[DimPerson] OFF;
		   -- --------------------------------------------------------------------------------
		   -- Insert Rows into EDW DimPerson	   
		   -- --------------------------------------------------------------------------------
             INSERT INTO [BING_EDW].[dbo].[DimPerson]
             EXEC dbo.spHR_StagingTransform_DimPerson
                  @EDWRunDateTime;

		   -- Get how many rows were extracted from source 

             SELECT @InsertCount = COUNT(1)
             FROM [BING_EDW].[dbo].[DimPerson];
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
                     PRINT @DebugMsg;
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' into Target.';
                     PRINT @DebugMsg;
             END;

		  
		  -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Committing transaction.';
             PRINT @DebugMsg;
		   
		   --
		   -- Commit the successful transaction 
		   --
             COMMIT TRANSACTION;
		   --
		   -- Write our successful run to the EDW AuditLog 
		   --
             EXEC [BING_EDW].[dbo].[spEDWEndAuditLog]
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @SourceCount = @SourceCount,
                  @AuditId = @AuditId;

		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Completing successfully.';
             PRINT @DebugMsg;
         END TRY
         BEGIN CATCH
	    	  -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Rolling back transaction.';
             PRINT @DebugMsg;
		   -- Rollback the transaction
             ROLLBACK TRANSACTION;
		   --
		   -- Write our failed run to the EDW AuditLog 
		   --
             EXEC [BING_EDW].[dbo].[spEDWErrorAuditLog]
                  @AuditId = @AuditId;
		   --
		   -- Raiserror
		   --				  
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
GO