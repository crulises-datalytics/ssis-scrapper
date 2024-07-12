CREATE PROCEDURE [dbo].[spBING_EDW_Build_DimensionSeedRows]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
     BEGIN
         -- ================================================================================
         -- 
         -- Stored Procedure:   spBING_EDW_Build_DimensionSeedRows
         --
         -- Purpose:            Populates the DimensionSeedRows table in BING_EDW.
         --                     The table in question is almost static - that is, we don't
         --                         expect the data to change often.  However, we have
         --                         the population process encapsulated in a proc if we  
         --                         need to update or [re]deploy the entire database solution
         --                         from scratch.	  
         --
         --                     The logic for this was in an SSIS Project called PostDeploymentExecution,
         --                         which was lost and forgotten in our Source Repository.  Putting it here
         --                         makes it easier to locate what's actually populating the table	   	    	    	      	    
         --
         --
         -- Populates:          Truncates and [re]loads BING_EDW..DimensionSeedRows
         --
         -- Usage:              EXEC dbo.spBING_EDW_Build_DimensionSeedRows @DebugMode = 1
         --
         -- --------------------------------------------------------------------------------
         --
         -- Change Log:		   
         -- ----------
         --
         -- Date         Modified By     Comments
         -- ----         -----------     --------
         --
         -- 12/01/17     sburke          Taking this prcess out of SSIS and into a stored proc  
         -- 12/07/17     sburke          Amend the seed value for DimSponsor.SponsorDoNotEmail 
         --                                  to 'Unknown Email Sponsor'.
         --                              Also remove DimStudent.StudentLifecycleStatus, as that
         --                                 field is redundant (all LfecycleStatus detail is now
         --                                 held in DimLifecycleStatus
         --  1/09/17     sburke          BNG-998 - Add -2 'Not Applicable' records for Dimension
         --                                 tables
         --  2/02/18     sburke          BNG-250 / BNG-257 - Adding DimLead, DimLeadType & DimLeadEventType
         --  2/09/18     sburke          BNG-260 - Adding DimWebCampaign
         --  3/08/18     sburke          BNG-1270 - Adding 7 new tables added in Sprint 2018-05:
         --                                  DimPayBasis,         DimPayRateChangeReason
         --                                  DimAssignmentType,   DimSpecialInfo
         --                                  DimPosition,         DimLeaveType
         --                                  DimLeaveReason
		 -- 03/14/2018 Adevabhakthuni    BNG-272 Adding DimComplianceItem
         --  4/23/2018   sburke          BNG-1644 - Removing redundant population of seed rows for
         --                                  DimARAgingBucket (this is done by spBING_EDW_Generate_DimARAgingBucket)	     
         --	4/24/2018    anmorales       BNG-1639 - Adding DimARAgencyType		 
         -- ================================================================================
         SET NOCOUNT ON;
         --
         -- Housekeeping Variables
         -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimensionSeedRows';
         DECLARE @AuditId BIGINT;
         --
         -- ETL status Variables
         --
         DECLARE @RowCount INT= 0;
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
         EXEC [dbo].[spEDWBeginAuditLog]
              @SourceName = @SourceName,
              @AuditId = @AuditId OUTPUT; 		 	 
         --
         BEGIN TRY
             BEGIN TRANSACTION;

             -- ====================================================================================================
             -- DimAccountSubaccount
             -- ====================================================================================================
             DELETE FROM [dbo].[DimAccountSubaccount]
             WHERE [AccountSubaccountKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimAccountSubaccount : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimAccountSubaccount ON;
             INSERT INTO [dbo].[DimAccountSubaccount]
             ([AccountSubaccountKey],
              [AccountSubaccountID],
              [AccountSubaccountName],
              [AccountID],
              [AccountName],
              [SubaccountID],
              [SubaccountName],
              [ASATCOUnary],
              [ASATCOSort],
              [ASATCODepth],
              [ASATCOLevel1ID],
              [ASATCOLevel1Name],
              [ASATCOLevel1Unary],
              [ASATCOLevel1Sort],
              [ASATCOLevel2ID],
              [ASATCOLevel2Name],
              [ASATCOLevel2Unary],
              [ASATCOLevel2Sort],
              [ASATCOLevel3ID],
              [ASATCOLevel3Name],
              [ASATCOLevel3Unary],
              [ASATCOLevel3Sort],
              [ASATCOLevel4ID],
              [ASATCOLevel4Name],
              [ASATCOLevel4Unary],
              [ASATCOLevel4Sort],
              [ASATCOLevel5ID],
              [ASATCOLevel5Name],
              [ASATCOLevel5Unary],
              [ASATCOLevel5Sort],
              [ASATCOLevel6ID],
              [ASATCOLevel6Name],
              [ASATCOLevel6Unary],
              [ASATCOLevel6Sort],
              [ASATCOLevel7ID],
              [ASATCOLevel7Name],
              [ASATCOLevel7Unary],
              [ASATCOLevel7Sort],
              [ASATCOLevel8ID],
              [ASATCOLevel8Name],
              [ASATCOLevel8Unary],
              [ASATCOLevel8Sort],
              [ASATCOLevel9ID],
              [ASATCOLevel9Name],
              [ASATCOLevel9Unary],
              [ASATCOLevel9Sort],
              [ASATCOLevel10ID],
              [ASATCOLevel10Name],
              [ASATCOLevel10Unary],
              [ASATCOLevel10Sort],
              [ASATCOLevel11ID],
              [ASATCOLevel11Name],
              [ASATCOLevel11Unary],
              [ASATCOLevel11Sort],
              [ASATCOLevel12ID],
              [ASATCOLevel12Name],
              [ASATCOLevel12Unary],
              [ASATCOLevel12Sort],
              [ASAFieldUnary],
              [ASAFieldSort],
              [ASAFieldDepth],
              [ASAFieldLevel1ID],
              [ASAFieldLevel1Name],
              [ASAFieldLevel1Unary],
              [ASAFieldLevel1Sort],
              [ASAFieldLevel2ID],
              [ASAFieldLevel2Name],
              [ASAFieldLevel2Unary],
              [ASAFieldLevel2Sort],
              [ASAFieldLevel3ID],
              [ASAFieldLevel3Name],
              [ASAFieldLevel3Unary],
              [ASAFieldLevel3Sort],
              [ASAFieldLevel4ID],
              [ASAFieldLevel4Name],
              [ASAFieldLevel4Unary],
              [ASAFieldLevel4Sort],
              [ASAFieldLevel5ID],
              [ASAFieldLevel5Name],
              [ASAFieldLevel5Unary],
              [ASAFieldLevel5Sort],
              [ASAFieldLevel6ID],
              [ASAFieldLevel6Name],
              [ASAFieldLevel6Unary],
              [ASAFieldLevel6Sort],
              [ASAFieldLevel7ID],
              [ASAFieldLevel7Name],
              [ASAFieldLevel7Unary],
              [ASAFieldLevel7Sort],
              [ASAFieldLevel8ID],
              [ASAFieldLevel8Name],
              [ASAFieldLevel8Unary],
              [ASAFieldLevel8Sort],
              [ASAFieldLevel9ID],
              [ASAFieldLevel9Name],
              [ASAFieldLevel9Unary],
              [ASAFieldLevel9Sort],
              [ASAFieldLevel10ID],
              [ASAFieldLevel10Name],
              [ASAFieldLevel10Unary],
              [ASAFieldLevel10Sort],
              [ASAFieldLevel11ID],
              [ASAFieldLevel11Name],
              [ASAFieldLevel11Unary],
              [ASAFieldLevel11Sort],
              [ASAFieldLevel12ID],
              [ASAFieldLevel12Name],
              [ASAFieldLevel12Unary],
              [ASAFieldLevel12Sort],
              [ASAGAUnary],
              [ASAGASort],
              [ASAGADepth],
              [ASAGALevel1ID],
              [ASAGALevel1Name],
              [ASAGALevel1Unary],
              [ASAGALevel1Sort],
              [ASAGALevel2ID],
              [ASAGALevel2Name],
              [ASAGALevel2Unary],
              [ASAGALevel2Sort],
              [ASAGALevel3ID],
              [ASAGALevel3Name],
              [ASAGALevel3Unary],
              [ASAGALevel3Sort],
              [ASAGALevel4ID],
              [ASAGALevel4Name],
              [ASAGALevel4Unary],
              [ASAGALevel4Sort],
              [ASAGALevel5ID],
              [ASAGALevel5Name],
              [ASAGALevel5Unary],
              [ASAGALevel5Sort],
              [ASAGALevel6ID],
              [ASAGALevel6Name],
              [ASAGALevel6Unary],
              [ASAGALevel6Sort],
              [ASAGALevel7ID],
              [ASAGALevel7Name],
              [ASAGALevel7Unary],
              [ASAGALevel7Sort],
              [ASAGALevel8ID],
              [ASAGALevel8Name],
              [ASAGALevel8Unary],
              [ASAGALevel8Sort],
              [ASATuitionType],
              [ASALaborType],
              [ASAEBITDAAddbackFlag],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1, -- [AccountSubaccountKey]
                          '0000.000000', -- [AccountSubaccountID]
                          'Unknown Account Subaccount', -- [AccountSubaccountName]
                          '0000', -- [AccountID]
                          'Unknown Account', -- [AccountName]
                          '000000', -- [SubaccountID] 
                          'Unknown Subaccount', -- [SubaccountName]
                          0, -- [ASATCOUnary]
                          99999, -- [ASATCOSort]
                          -1, -- [ASATCODepth]
                          '0000.000000', -- [ASATCOLevel1ID] 
                          'Unknown Account Subaccount', -- [ASATCOLevel1Name]
                          0, -- [ASATCOLevel1Unary]
                          99999, -- [ASATCOLevel1Sort]
                          '0000.000000', -- [ASATCOLevel2ID]
                          'Unknown Account Subaccount', -- [ASATCOLevel2Name] 
                          0, -- [ASATCOLevel2Unary] 
                          99999, -- [ASATCOLevel2Sort]
                          '0000.000000', -- [ASATCOLevel3ID]
                          'Unknown Account Subaccount', -- [ASATCOLevel3Name]
                          0, -- [ASATCOLevel3Unary]
                          99999, -- [ASATCOLevel3Sort]
                          '0000.000000', -- [ASATCOLevel4ID]
                          'Unknown Account Subaccount', -- [ASATCOLevel4Name]
                          0, -- [ASATCOLevel4Unary]
                          99999, -- [ASATCOLevel4Sort]
                          '0000.000000', -- [ASATCOLevel5ID]
                          'Unknown Account Subaccount', -- [ASATCOLevel5Name]
                          0, -- [ASATCOLevel5Unary]
                          99999, -- [ASATCOLevel5Sort]
                          '0000.000000', -- [ASATCOLevel6ID]
                          'Unknown Account Subaccount', -- [ASATCOLevel6Name]
                          0, -- [ASATCOLevel6Unary]
                          99999, -- [ASATCOLevel6Sort]
                          '0000.000000', -- [ASATCOLevel7ID]
                          'Unknown Account Subaccount', -- [ASATCOLevel7Name]
                          0, -- [ASATCOLevel7Unary]
                          99999, -- [ASATCOLevel7Sort]
                          '0000.000000', -- [ASATCOLevel8ID]
                          'Unknown Account Subaccount', -- [ASATCOLevel8Name]
                          0, -- [ASATCOLevel8Unary]
                          99999, -- [ASATCOLevel8Sort]
                          '0000.000000', -- [ASATCOLevel9ID]
                          'Unknown Account Subaccount', -- [ASATCOLevel9Name]
                          0, -- [ASATCOLevel9Unary]
                          99999, -- [ASATCOLevel9Sort]
                          '0000.000000', -- [ASATCOLevel10ID]
                          'Unknown Account Subaccount', -- [ASATCOLevel10Name]
                          0, -- [ASATCOLevel10Unary]
                          99999, -- [ASATCOLevel10Sort]
                          '0000.000000', -- [ASATCOLevel11ID]
                          'Unknown Account Subaccount', -- [ASATCOLevel11Name]
                          0, -- [ASATCOLevel11Unary]
                          99999, -- [ASATCOLevel11Sort]
                          '0000.000000', -- [ASATCOLevel12ID]
                          'Unknown Account Subaccount', -- [ASATCOLevel12Name]
                          0, -- [ASATCOLevel12Unary]
                          99999, -- [ASATCOLevel12Sort] 
                          1, -- [ASAFieldUnary]
                          99999, -- [ASAFieldSort]
                          -1, -- [ASAFieldDepth]
                          '0000.000000', -- [ASAFieldLevel1ID]	
                          'Unknown Account Subaccount', -- [ASAFieldLevel1Name]	
                          1, -- [ASAFieldLevel1Unary]	
                          99999, -- [ASAFieldLevel1Sort]
                          '0000.000000', -- [ASAFieldLevel2ID]	
                          'Unknown Account Subaccount', -- [ASAFieldLevel2Name]	
                          1, -- [ASAFieldLevel2Unary]	
                          99999, -- [ASAFieldLevel2Sort]
                          '0000.000000', -- [ASAFieldLevel3ID]	
                          'Unknown Account Subaccount', -- [ASAFieldLevel3Name]	
                          1, -- [ASAFieldLevel3Unary]	
                          99999, -- [ASAFieldLevel3Sort]
                          '0000.000000', -- [ASAFieldLevel4ID]	
                          'Unknown Account Subaccount', -- [ASAFieldLevel4Name]	
                          1, -- [ASAFieldLevel4Unary]	
                          99999, -- [ASAFieldLevel4Sort]
                          '0000.000000', -- [ASAFieldLevel5ID]	
                          'Unknown Account Subaccount', -- [ASAFieldLevel5Name]	
                          1, -- [ASAFieldLevel5Unary]	
                          99999, -- [ASAFieldLevel5Sort]
                          '0000.000000', -- [ASAFieldLevel6ID]	
                          'Unknown Account Subaccount', -- [ASAFieldLevel6Name]	
                          1, -- [ASAFieldLevel6Unary]	
                          99999, -- [ASAFieldLevel6Sort]
                          '0000.000000', -- [ASAFieldLevel7ID]	
                          'Unknown Account Subaccount', -- [ASAFieldLevel7Name]	
                          1, -- [ASAFieldLevel7Unary]	
                          99999, -- [ASAFieldLevel7Sort]
                          '0000.000000', -- [ASAFieldLevel8ID]	
                          'Unknown Account Subaccount', -- [ASAFieldLevel8Name]	
                          1, -- [ASAFieldLevel8Unary]	
                          99999, -- [ASAFieldLevel8Sort]
                          '0000.000000', -- [ASAFieldLevel9ID]	
                          'Unknown Account Subaccount', -- [ASAFieldLevel9Name]	
                          1, -- [ASAFieldLevel9Unary]	
                          99999, -- [ASAFieldLevel9Sort]
                          '0000.000000', -- [ASAFieldLevel10ID]	
                          'Unknown Account Subaccount', -- [ASAFieldLevel10Name]	
                          1, -- [ASAFieldLevel10Unary]	
                          99999, -- [ASAFieldLevel10Sort]
                          '0000.000000', -- [ASAFieldLevel11ID]	
                          'Unknown Account Subaccount', -- [ASAFieldLevel11Name]	
                          1, -- [ASAFieldLevel11Unary]	
                          99999, -- [ASAFieldLevel11Sort]
                          '0000.000000', -- [ASAFieldLevel12ID]	
                          'Unknown Account Subaccount', -- [ASAFieldLevel12Name]	
                          1, -- [ASAFieldLevel12Unary]	
                          99999, -- [ASAFieldLevel12Sort]
                          1, -- [ASAGAUnary]
                          99999, -- [ASAGASort]
                          -1, -- [ASAGADepth]
                          '0000.000000', -- [ASAGALevel1ID]	
                          'Unknown Account Subaccount', -- [ASAGALevel1Name]	
                          1, -- [ASAGALevel1Unary]	
                          99999, -- [ASAGALevel1Sort]
                          '0000.000000', -- [ASAGALevel2ID]	
                          'Unknown Account Subaccount', -- [ASAGALevel2Name]	
                          1, -- [ASAGALevel2Unary]	
                          99999, -- [ASAGALevel2Sort]
                          '0000.000000', -- [ASAGALevel3ID]	
                          'Unknown Account Subaccount', -- [ASAGALevel3Name]	
                          1, -- [ASAGALevel3Unary]	
                          99999, -- [ASAGALevel3Sort]
                          '0000.000000', -- [ASAGALevel4ID]	
                          'Unknown Account Subaccount', -- [ASAGALevel4Name]	
                          1, -- [ASAGALevel4Unary]	
                          99999, -- [ASAGALevel4Sort]
                          '0000.000000', -- [ASAGALevel5ID]	
                          'Unknown Account Subaccount', -- [ASAFieldLevel5Name]	
                          1, -- [ASAFieldLevel5Unary]	
                          99999, -- [ASAFieldLevel5Sort]
                          '0000.000000', -- [ASAFieldLevel6ID]	
                          'Unknown Account Subaccount', -- [ASAGALevel6Name]	
                          1, -- [ASAGALevel6Unary]	
                          99999, -- [ASAGALevel6Sort]
                          '0000.000000', -- [ASAGALevel7ID]	
                          'Unknown Account Subaccount', -- [ASAGALevel7Name]	
                          1, -- [ASAGALevel7Unary]	
                          99999, -- [ASAGALevel7Sort]
                          '0000.000000', -- [ASAGALevel8ID]	
                          'Unknown Account Subaccount', -- [ASAGALevel8Name]	
                          1, -- [ASAGALevel8Unary]	
                          99999, -- [ASAGALevel8Sort]
                          'Unknown Tuition Type', -- [ASATuitionType]
                          'Unknown Labor Type', -- [ASALaborType]
                          'Unknown 2015 EBITDA Flag', -- [ASAEBITDAAddbackFlag]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER, -- [EDWCreatedBy]
                          GETDATE(), -- [EDWModifiedDate]
                          SYSTEM_USER, -- [EDWModifiedBy]
                          NULL
                    UNION
                    SELECT-2, -- [AccountSubaccountKey]
                          'xxxx.xxxxxx', -- [AccountSubaccountID]
                          'Not Applicable Account Subaccount', -- [AccountSubaccountName]
                          '0000', -- [AccountID]
                          'Not Applicable Account', -- [AccountName]
                          '000000', -- [SubaccountID] 
                          'Not Applicable Subaccount', -- [SubaccountName]
                          0, -- [ASATCOUnary]
                          99999, -- [ASATCOSort]
                          -2, -- [ASATCODepth]
                          '0000.000000', -- [ASATCOLevel1ID] 
                          'Not Applicable Account Subaccount', -- [ASATCOLevel1Name]
                          0, -- [ASATCOLevel1Unary]
                          99999, -- [ASATCOLevel1Sort]
                          '0000.000000', -- [ASATCOLevel2ID]
                          'Not Applicable Account Subaccount', -- [ASATCOLevel2Name] 
                          0, -- [ASATCOLevel2Unary] 
                          99999, -- [ASATCOLevel2Sort]
                          '0000.000000', -- [ASATCOLevel3ID]
                          'Not Applicable Account Subaccount', -- [ASATCOLevel3Name]
                          0, -- [ASATCOLevel3Unary]
                          99999, -- [ASATCOLevel3Sort]
                          '0000.000000', -- [ASATCOLevel4ID]
                          'Not Applicable Account Subaccount', -- [ASATCOLevel4Name]
                          0, -- [ASATCOLevel4Unary]
                          99999, -- [ASATCOLevel4Sort]
                          '0000.000000', -- [ASATCOLevel5ID]
                          'Not Applicable Account Subaccount', -- [ASATCOLevel5Name]
                          0, -- [ASATCOLevel5Unary]
                          99999, -- [ASATCOLevel5Sort]
                          '0000.000000', -- [ASATCOLevel6ID]
                          'Not Applicable Account Subaccount', -- [ASATCOLevel6Name]
                          0, -- [ASATCOLevel6Unary]
                          99999, -- [ASATCOLevel6Sort]
                          '0000.000000', -- [ASATCOLevel7ID]
                          'Not Applicable Account Subaccount', -- [ASATCOLevel7Name]
                          0, -- [ASATCOLevel7Unary]
                          99999, -- [ASATCOLevel7Sort]
                          '0000.000000', -- [ASATCOLevel8ID]
                          'Not Applicable Account Subaccount', -- [ASATCOLevel8Name]
                          0, -- [ASATCOLevel8Unary]
                          99999, -- [ASATCOLevel8Sort]
                          '0000.000000', -- [ASATCOLevel9ID]
                          'Not Applicable Account Subaccount', -- [ASATCOLevel9Name]
                          0, -- [ASATCOLevel9Unary]
                          99999, -- [ASATCOLevel9Sort]
                          '0000.000000', -- [ASATCOLevel10ID]
                          'Not Applicable Account Subaccount', -- [ASATCOLevel10Name]
                          0, -- [ASATCOLevel10Unary]
                          99999, -- [ASATCOLevel10Sort]
                          '0000.000000', -- [ASATCOLevel11ID]
                          'Not Applicable Account Subaccount', -- [ASATCOLevel11Name]
                          0, -- [ASATCOLevel11Unary]
                          99999, -- [ASATCOLevel11Sort]
                          '0000.000000', -- [ASATCOLevel12ID]
                          'Not Applicable Account Subaccount', -- [ASATCOLevel12Name]
                          0, -- [ASATCOLevel12Unary]
                          99999, -- [ASATCOLevel12Sort] 
                          1, -- [ASAFieldUnary]
                          99999, -- [ASAFieldSort]
                          -1, -- [ASAFieldDepth]
                          '0000.000000', -- [ASAFieldLevel1ID]	
                          'Not Applicable Account Subaccount', -- [ASAFieldLevel1Name]	
                          1, -- [ASAFieldLevel1Unary]	
                          99999, -- [ASAFieldLevel1Sort]
                          '0000.000000', -- [ASAFieldLevel2ID]	
                          'Not Applicable Account Subaccount', -- [ASAFieldLevel2Name]	
                          1, -- [ASAFieldLevel2Unary]	
                          99999, -- [ASAFieldLevel2Sort]
                          '0000.000000', -- [ASAFieldLevel3ID]	
                          'Not Applicable Account Subaccount', -- [ASAFieldLevel3Name]	
                          1, -- [ASAFieldLevel3Unary]	
                          99999, -- [ASAFieldLevel3Sort]
                          '0000.000000', -- [ASAFieldLevel4ID]	
                          'Not Applicable Account Subaccount', -- [ASAFieldLevel4Name]	
                          1, -- [ASAFieldLevel4Unary]	
                          99999, -- [ASAFieldLevel4Sort]
                          '0000.000000', -- [ASAFieldLevel5ID]	
                          'Not Applicable Account Subaccount', -- [ASAFieldLevel5Name]	
                          1, -- [ASAFieldLevel5Unary]	
                          99999, -- [ASAFieldLevel5Sort]
                          '0000.000000', -- [ASAFieldLevel6ID]	
                          'Not Applicable Account Subaccount', -- [ASAFieldLevel6Name]	
                          1, -- [ASAFieldLevel6Unary]	
                          99999, -- [ASAFieldLevel6Sort]
                          '0000.000000', -- [ASAFieldLevel7ID]	
                          'Not Applicable Account Subaccount', -- [ASAFieldLevel7Name]	
                          1, -- [ASAFieldLevel7Unary]	
                          99999, -- [ASAFieldLevel7Sort]
                          '0000.000000', -- [ASAFieldLevel8ID]	
                          'Not Applicable Account Subaccount', -- [ASAFieldLevel8Name]	
                          1, -- [ASAFieldLevel8Unary]	
                          99999, -- [ASAFieldLevel8Sort]
                          '0000.000000', -- [ASAFieldLevel9ID]	
                          'Not Applicable Account Subaccount', -- [ASAFieldLevel9Name]	
                          1, -- [ASAFieldLevel9Unary]	
                          99999, -- [ASAFieldLevel9Sort]
                          '0000.000000', -- [ASAFieldLevel10ID]	
                          'Not Applicable Account Subaccount', -- [ASAFieldLevel10Name]	
                          1, -- [ASAFieldLevel10Unary]	
                          99999, -- [ASAFieldLevel10Sort]
                          '0000.000000', -- [ASAFieldLevel11ID]	
                          'Not Applicable Account Subaccount', -- [ASAFieldLevel11Name]	
                          1, -- [ASAFieldLevel11Unary]	
                          99999, -- [ASAFieldLevel11Sort]
                          '0000.000000', -- [ASAFieldLevel12ID]	
                          'Not Applicable Account Subaccount', -- [ASAFieldLevel12Name]	
                          1, -- [ASAFieldLevel12Unary]	
                          99999, -- [ASAFieldLevel12Sort]
                          1, -- [ASAGAUnary]
                          99999, -- [ASAGASort]
                          -1, -- [ASAGADepth]
                          '0000.000000', -- [ASAGALevel1ID]	
                          'Not Applicable Account Subaccount', -- [ASAGALevel1Name]	
                          1, -- [ASAGALevel1Unary]	
                          99999, -- [ASAGALevel1Sort]
                          '0000.000000', -- [ASAGALevel2ID]	
                          'Not Applicable Account Subaccount', -- [ASAGALevel2Name]	
                          1, -- [ASAGALevel2Unary]	
                          99999, -- [ASAGALevel2Sort]
                          '0000.000000', -- [ASAGALevel3ID]	
                          'Not Applicable Account Subaccount', -- [ASAGALevel3Name]	
                          1, -- [ASAGALevel3Unary]	
                          99999, -- [ASAGALevel3Sort]
                          '0000.000000', -- [ASAGALevel4ID]	
                          'Not Applicable Account Subaccount', -- [ASAGALevel4Name]	
                          1, -- [ASAGALevel4Unary]	
                          99999, -- [ASAGALevel4Sort]
                          '0000.000000', -- [ASAGALevel5ID]	
                          'Not Applicable Account Subaccount', -- [ASAFieldLevel5Name]	
                          1, -- [ASAFieldLevel5Unary]	
                          99999, -- [ASAFieldLevel5Sort]
                          '0000.000000', -- [ASAFieldLevel6ID]	
                          'Not Applicable Account Subaccount', -- [ASAGALevel6Name]	
                          1, -- [ASAGALevel6Unary]	
                          99999, -- [ASAGALevel6Sort]
                          '0000.000000', -- [ASAGALevel7ID]	
                          'Not Applicable Account Subaccount', -- [ASAGALevel7Name]	
                          1, -- [ASAGALevel7Unary]	
                          99999, -- [ASAGALevel7Sort]
                          '0000.000000', -- [ASAGALevel8ID]	
                          'Not Applicable Account Subaccount', -- [ASAGALevel8Name]	
                          1, -- [ASAGALevel8Unary]	
                          99999, -- [ASAGALevel8Sort]
                          'Not Applicable Tuition Type', -- [ASATuitionType]
                          'Not Applicable Labor Type', -- [ASALaborType]
                          'Not Applicable 2015 EBITDA Flag', -- [ASAEBITDAAddbackFlag]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER, -- [EDWCreatedBy]
                          GETDATE(), -- [EDWModifiedDate]
                          SYSTEM_USER, -- [EDWModifiedBy]
                          NULL;
             -- Keep a running total of the Inserts

             SELECT @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimAccountSubaccount : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimAccountSubaccount OFF;

             -- ====================================================================================================
             -- DimAdjustmentReason
             -- ====================================================================================================

             DELETE FROM [dbo].[DimAdjustmentReason]
             WHERE [AdjustmentReasonKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimAdjustmentReason : Deleted seed row[s]';
             END;

--
             SET IDENTITY_INSERT dbo.DimAdjustmentReason ON;
             INSERT INTO [dbo].[DimAdjustmentReason]
             ([AdjustmentReasonKey],
              [AdjustmentReasonID],
              [AdjustmentReasonName],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1, -- [AdjustmentReasonKey]
                          -1, -- [AdjustmentReasonID]
                          'Unknown Adjustment Reason Name', -- [AdjustmentReasonName]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER, -- [EDWCreatedBye]
                          GETDATE(), -- [EDWModifiedDate]
                          SYSTEM_USER, -- [EDWModifiedBy]
                          NULL
                    UNION
                    SELECT-2, -- [AdjustmentReasonKey]
                          -2, -- [AdjustmentReasonID]
                          'Not Applicable Adjustment Reason Name', -- [AdjustmentReasonName]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER, -- [EDWCreatedBye]
                          GETDATE(), -- [EDWModifiedDate]
                          SYSTEM_USER, -- [EDWModifiedBy]
                          NULL;
             -- Keep a running total of the inserts
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimAdjustmentReason : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimAdjustmentReason OFF;

             -- ====================================================================================================
             -- DimARAgingBucket
             -- ====================================================================================================   
		   -- This is now populated by spBING_EDW_Generate_DimARAgingBucket        

             -- ====================================================================================================
             -- DimAssignmentType
             -- ====================================================================================================  
             DELETE FROM [dbo].[DimAssignmentType]
             WHERE [AssignmentTypeKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimAssignmentType : Deleted seed row[s]';
             END;
             --
             SET IDENTITY_INSERT [BING_EDW].[dbo].[DimAssignmentType] ON;
             INSERT INTO [BING_EDW].[dbo].[DimAssignmentType]
             ([AssignmentTypeKey],
              [AssignmentStatusTypeID],
              [AssignmentStatusTypeName],
              [AssignmentNQDCFlag],
              [AssignmentBusinessTitleName],
              [AssignmentWorkAtHomeFlag],
              [AssignmentIVRCode],
              [AssignmentESMStatusChangeReasonName],
              [AssignmentBonusPercent],
              [AssignmentTypeCode],
              [AssignmentTypeName],
              [EmploymentCategoryCode],
              [EmploymentCategoryName],
              [EmploymentEligibleRehireFlag],
              [EmploymentTwoWeeksNoticeFlag],
              [EmploymentTerminationRegrettableFlag],
              [EmploymentLeavingReasonCode],
              [EmploymentLeavingReasonName],
              [EmploymentLeavingReasonDescription],
              [EmploymentLeavingReasonTypeName],
              [EDWCreatedDate]
             )
                    SELECT-1,
                          -1,
                          'Unknown Assignment Status Type',
                          'Unknown NQDC',
                          'Unknown Business Title',
                          'Unknown Work At Home',
                          -1,
                          'Unknown ESM Status Change Reason',
                          0,
                          -1,
                          'Unknown Assignment Type',
                          -1,
                          'Unknown Employment Category',
                          'Unknown Eligible Rehire',
                          'Unknown Two Weeks Notice',
                          'Unknown Termination Regrettable',
                          -1,
                          'Unknown Leaving Reason',
                          'Unknown Leaving Reason',
                          'Unknown Leaving Reason Type',
                          @EDWRunDateTime
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable Assignment Status Type',
                          'Not Applicable NQDC',
                          'Not Applicable Business Title',
                          'Not Applicable Work At Home',
                          -1,
                          'Not Applicable ESM Status Change Reason',
                          0,
                          -1,
                          'Not Applicable Assignment Type',
                          -1,
                          'Not Applicable Employment Category',
                          'Not Applicable Eligible Rehire',
                          'Not Applicable Two Weeks Notice',
                          'Not Applicable Termination Regrettable',
                          -1,
                          'Not Applicable Leaving Reason',
                          'Not Applicable Leaving Reason',
                          'Not Applicable Leaving Reason Type',
                          @EDWRunDateTime;
             -- Keep a running total of the inserts
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimAssignmentType : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT [BING_EDW].[dbo].[DimAssignmentType] OFF;

             -- ====================================================================================================
             -- DimClassroom
             -- ====================================================================================================  
             DELETE FROM [dbo].[DimClassroom]
             WHERE [ClassroomKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimClassroom : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimClassroom ON;
             INSERT INTO [dbo].[DimClassroom]
             ([ClassroomKey],
              [ClassroomID],
              [ClassroomName],
              [ClassroomCapacity],
              [ClassroomType],
              [CSSCenterNumber],
              [SourceSystem],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1, -- [ClassroomKey]
                          -1, -- [ClassroomID]
                          'Unknown Classroom', -- [ClassroomName]
                          0, -- [ClassroomCapacity]
                          'Unknown Classroom Type', -- [ClassroomType]
                          -1, -- [CSSCenterNumber]
                          'UNK', -- [SourceSystem]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER, -- [EDWCreatedBy]
                          GETDATE(), -- [EDWModifiedDate]
                          SYSTEM_USER, -- [EDWModifiedBy]
                          NULL -- [Deleted]
                    UNION
                    SELECT-2, -- [ClassroomKey]
                          -2, -- [ClassroomID]
                          'Not Applicable Classroom', -- [ClassroomName]
                          0, -- [ClassroomCapacity]
                          'Not Applicable Classroom Type', -- [ClassroomType]
                          -2, -- [CSSCenterNumber]
                          'N/A', -- [SourceSystem]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER, -- [EDWCreatedBy]
                          GETDATE(), -- [EDWModifiedDate]
                          SYSTEM_USER, -- [EDWModifiedBy]
                          NULL;
		   -- Keep a running total of the inserts
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimClassroom : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimClassroom OFF;

             -- ====================================================================================================
             -- DimCompany
             -- ==================================================================================================== 
             DELETE FROM [dbo].[DimCompany]
             WHERE [CompanyKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimCompany : Deleted seed row[s]';
             END;

--
             SET IDENTITY_INSERT dbo.DimCompany ON;
             INSERT INTO [dbo].[DimCompany]
             ([CompanyKey],
              [CompanyID],
              [CompanyName],
              [CompanyTaxNumber],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1, -- [CompanyKey]
                          -1, -- [CompanyID]
                          'Unknown Company', -- [CompanyName]
                          'Unknown Tax Number', -- [CompanyTaxNumber]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER, -- [EDWCreatedBy]
                          GETDATE(), -- [EDWModifiedDate]
                          SYSTEM_USER, -- [EDWModifiedBy]
                          NULL -- [Deleted]
                    UNION
                    SELECT-2, -- [CompanyKey]
                          -2, -- [CompanyID]
                          'Not Applicable Company', -- [CompanyName]
                          'Not Applicable Tax Number', -- [CompanyTaxNumber]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER, -- [EDWCreatedBy]
                          GETDATE(), -- [EDWModifiedDate]
                          SYSTEM_USER, -- [EDWModifiedBy]
                          NULL;

		   -- Keep a running total of Inserts
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimCompany : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimCompany OFF;

             -- ====================================================================================================
             -- DimCompanyRollup
             -- ==================================================================================================== 
             DELETE FROM [dbo].[DimCompanyRollup]
             WHERE CompanyRollupKey < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimCompanyRollup : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimCompanyRollup ON;
             INSERT INTO dbo.DimCompanyRollup
             (CompanyRollupKey,
              CompanyRollupID,
              CompanyRollupName,
              EDWCreatedDate,
              EDWCreatedBy,
              EDWModifiedDate,
              EDWModifiedBy
             )
                    SELECT-1,
                          '-1',
                          'Unknown Company Rollup',
                          GETDATE(),
                          USER_NAME(),
                          GETDATE(),
                          USER_NAME()
                    UNION
                    SELECT-2,
                          '-2',
                          'Not Applicable Company Rollup',
                          GETDATE(),
                          USER_NAME(),
                          GETDATE(),
                          USER_NAME();
		   -- Keep a running total of Inserts
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimCompanyRollup : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimCompanyRollup OFF;

             -- ====================================================================================================
             -- DimCostCenter
             -- ==================================================================================================== 
             DELETE FROM [dbo].[DimCostCenter]
             WHERE [CostCenterKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimCostCenter : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimCostCenter ON;
             INSERT INTO [dbo].[DimCostCenter]
             ([CostCenterKey],
              [CostCenterNumber],
              [CostCenterName],
              [CompanyID],
              [CostCenterTypeID],
              [CCHierarchyLevel1Name],
              [CCHierarchyLevel2Name],
              [CCHierarchyLevel3Name],
              [CCHierarchyLevel4Name],
              [CCHierarchyLevel5Name],
              [CCHierarchyLevel6Name],
              [CCHierarchyLevel7Name],
              [CCHierarchyLevel8Name],
              [CCHierarchyLevel9Name],
              [CCHierarchyLevel10Name],
              [CCHierarchyLevel11Name],
              [CCOpenDate],
              [CCClosedDate],
              [CCReopenDate],
              [CCReopenDateType],
              [CCClassification],
              [CCStatus],
              [CCConsolidation],
              [CCFlexAttribute1],
              [CCFlexAttribute2],
              [CCFlexAttribute3],
              [CCFlexAttribute4],
              [CCFlexAttribute5],
              [CenterCMSID],
              [CenterCSSID],
              [SiteHorizonID],
              [CenterEnrollmentSourceSystem],
              [CenterCMSMigrationDate],
              [CenterCMSMigrationStatus],
              [CenterLicensedCapacity],
              [CenterBackupCareFlag],
              [CenterChildCareSelectFlag],
              [CenterPublicAllowedFlag],
              [CenterOpenTime],
              [CenterCloseTime],
              [CenterStudentMinimumAge],
              [CenterStudentMaximumAge],
              [CenterOpenSunFlag],
              [CenterOpenMonFlag],
              [CenterOpenTueFlag],
              [CenterOpenWedFlag],
              [CenterOpenThuFlag],
              [CenterOpenFriFlag],
              [CenterOpenSatFlag],
              [CenterFoodProgramStartDate],
              [CenterFoodProgramEndDate],
              [CenterRegistrationType],
              [SiteSchoolDistrict],
              [SiteClassYear],
              [CenterMenuURL],
              [CenterHasBreakfastFlag],
              [CenterHasMorningSlackFlag],
              [CenterHasLunchFlag],
              [CenterHasAfternoonSnackFlag],
              [CenterSpeaksASLFlag],
              [CenterSpeaksArabicFlag],
              [CenterSpeaksFrenchFlag],
              [CenterSpeaksGermanFlag],
              [CenterSpeaksHindiFlag],
              [CenterSpeaksMandarinFlag],
              [CenterSpeaksPunjabiFlag],
              [CenterSpeaksSpanishFlag],
              [CenterSpeaksOtherLanguages],
              [CenterAccreditationAgencyCode],
              [CenterAccreditationStartDate],
              [CenterAccreditationExpirationDate],
              [CenterAccreditationNextActivity],
              [CenterAccreditationNextActivityDueDate],
              [CenterAccreditationPrimaryStatus],
              [CenterAccreditationProgramID],
              [CenterQRISRating],
              [CenterQRISRatingStartDate],
              [CenterQRISRatingExpirationDate],
              [CenterMaintenanceSupervisorName],
              [CenterPreventativeTechnicianName],
              [CenterRegionalFacilitiesCoordinatorName],
              [CenterRegionalFacilitiesManagerName],
              [CenterNutritionAndWellnessAdministratorName],
              [CenterNutritionAndWellnessAdministratorEmail],
              [CenterNutritionAndWellnessAdministratorPhone],
              [CenterSubsidyCoordinatorName],
              [CenterSubsidyCoordinatorEmail],
              [CenterSubsidyCoordinatorPhone],
              [CenterSubsidyManagerName],
              [CenterSubsidyManagerEmail],
              [CenterSubsidyManagerPhone],
              [CenterSubsidySupervisorName],
              [CenterSubsidySupervisorEmail],
              [CenterSubsidySupervisorPhone],
              [CenterBuildingSquareFootage],
              [CenterLandSquareFootage],
              [CenterCoreBasedStatisticalAreaName],
              [CenterLandlordName],
              [CenterLeaseControlEndMonthDate],
              [CenterLeaseExpirationDate],
              [CenterLeaseExtensionOptionNoticeDate],
              [CenterLeaseExtensionOptionsRemainingCount],
              [CenterLeaseExtensionOptionRemainingYears],
              [CenterLeaseStatus],
              [CenterLatitude],
              [CenterLongitude],
              [CenterCurrentHumanSigmaScore],
              [CenterPreviousHumanSigmaScore],
              [EDWEffectiveDate],
              [EDWEndDate],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [Deleted]
             )
                    SELECT-1, -- [CostCenterKey]
                          '-1', -- [CostCenterNumber]
                          'Unknown Cost Center', -- [CostCenterName ]
                          '-1', -- [CompanyID ]
                          '-1', -- [CostCenterTypeID]
                          'Unknown Cost Center', -- [CCHierarchyLevel1Name ]
                          NULL, -- [CCHierarchyLevel2Name ]
                          NULL, -- [CCHierarchyLevel3Name ]
                          NULL, -- [CCHierarchyLevel4Name ]
                          NULL, -- [CCHierarchyLevel5Name ]
                          NULL, -- [CCHierarchyLevel6Name ]
                          NULL, -- [CCHierarchyLevel7Name ]
                          NULL, -- [CCHierarchyLevel8Name ]
                          NULL, -- [CCHierarchyLevel9Name ]
                          NULL, -- [CCHierarchyLevel10Name ]
                          NULL, -- [CCHierarchyLevel11Name ]
                          '19000101', -- [CCOpenDate]
                          NULL, -- [CCClosedDate]
                          NULL, -- [CCReopenDate]
                          NULL, -- [CCReopenDateType ]
                          'Unknown Classification', -- [CCClassification ]
                          'Unknown Status', -- [CCStatus ]
                          'Unknown Consolidation', -- [CCConsolidation ]
                          NULL, -- [CCFlexAttribute1 ]
                          NULL, -- [CCFlexAttribute2 ]
                          NULL, -- [CCFlexAttribute3 ]
                          NULL, -- [CCFlexAttribute4 ]
                          NULL, -- [CCFlexAttribute5 ]
                          NULL, -- [CenterCMSID]
                          NULL, -- [CenterCSSID]
                          NULL, -- [SiteHorizonID]
                          NULL, -- [CenterEnrollmentSourceSystem]
                          NULL, -- [CenterCMSMigrationDate]
                          NULL, -- [CenterCMSMigrationStatus]
                          NULL, -- [CenterLicensedCapacity]
                          NULL, -- [CenterBackupCareFlag ]
                          NULL, -- [CenterChildCareSelectFlag ]
                          NULL, -- [CenterPublicAllowedFlag ]
                          NULL, -- [CenterOpenTime]
                          NULL, -- [CenterCloseTime]
                          NULL, -- [CenterStudentMinimumAge ]
                          NULL, -- [CenterStudentMaximumAge ]
                          NULL, -- [CenterOpenSunFlag]
                          NULL, -- [CenterOpenMonFlag]
                          NULL, -- [CenterOpenTueFlag]
                          NULL, -- [CenterOpenWedFlag]
                          NULL, -- [CenterOpenThuFlag]
                          NULL, -- [CenterOpenFriFlag]
                          NULL, -- [CenterOpenSatFlag]
                          NULL, -- [CenterFoodProgramStartDate]
                          NULL, -- [CenterFoodProgramEndDate]
                          NULL, -- [CenterRegistrationType]
                          NULL, -- [SiteSchoolDistrict]
                          NULL, -- [SiteClassYear]
                          NULL, -- [CenterMenuURL]
                          NULL, -- [CenterHasBreakfastFlag]
                          NULL, -- [CenterHasMorningSlackFlag]
                          NULL, -- [CenterHasLunchFlag]
                          NULL, -- [CenterHasAfternoonSnackFlag]
                          NULL, -- [CenterSpeaksASLFlag]
                          NULL, -- [CenterSpeaksArabicFlag]
                          NULL, -- [CenterSpeaksFrenchFlag]
                          NULL, -- [CenterSpeaksGermanFlag]
                          NULL, -- [CenterSpeaksHindiFlag]
                          NULL, -- [CenterSpeaksMandarinFlag]
                          NULL, -- [CenterSpeaksPunjabiFlag]
                          NULL, -- [CenterSpeaksSpanishFlag]
                          NULL, -- [CenterSpeaksOtherLanguages]
                          NULL, -- [CenterAccreditationAgencyCode]
                          NULL, -- [CenterAccreditationStartDate]
                          NULL, -- [CenterAccreditationExpirationDate]
                          NULL, -- [CenterAccreditationNextActivity]
                          NULL, -- [CenterAccreditationNextActivityDueDate]
                          NULL, -- [CenterAccreditationPrimaryStatus]
                          NULL, -- [CenterAccreditationProgramID]
                          NULL, -- [CenterQRISRating]
                          NULL, -- [CenterQRISRatingStartDate]
                          NULL, -- [CenterQRISRatingExpirationDate]
                          NULL, -- [CenterMaintenanceSupervisorName ]
                          NULL, -- [CenterPreventativeTechnicianName ]
                          NULL, -- [CenterRegionalFacilitiesCoordinatorName ]
                          NULL, -- [CenterRegionalFacilitiesManagerName ]
                          NULL, -- [CenterNutritionAndWellnessAdministratorName ]
                          NULL, -- [CenterNutritionAndWellnessAdministratorEmail ]
                          NULL, -- [CenterNutritionAndWellnessAdministratorPhone ]
                          NULL, -- [CenterSubsidyCoordinatorName ]
                          NULL, -- [CenterSubsidyCoordinatorEmail ]
                          NULL, -- [CenterSubsidyCoordinatorPhone ]
                          NULL, -- [CenterSubsidyManagerName ]
                          NULL, -- [CenterSubsidyManagerEmail ]
                          NULL, -- [CenterSubsidyManagerPhone ]
                          NULL, -- [CenterSubsidySupervisorName ]
                          NULL, -- [CenterSubsidySupervisorEmail ]
                          NULL, -- [CenterSubsidySupervisorPhone ]
                          NULL, -- [CenterBuildingSquareFootage]
                          NULL, -- [CenterLandSquareFootage]
                          NULL, -- [CenterCoreBasedStatisticalAreaName ]
                          NULL, -- [CenterLandlordName ]
                          NULL, -- [CenterLeaseControlEndMonthDate]
                          NULL, -- [CenterLeaseExpirationDate]
                          NULL, -- [CenterLeaseExtensionOptionNoticeDate]
                          NULL, -- [CenterLeaseExtensionOptionsRemainingCount]
                          NULL, -- [CenterLeaseExtensionOptionRemainingYears]
                          NULL, -- [CenterLeaseStatus ]
                          NULL, -- [CenterLatitude ]
                          NULL, -- [CenterLongitude]
                          NULL, -- [CenterCurrentHumanSigmaScore]
                          NULL, -- [CenterPreviousHumanSigmaScore ]
                          '19000101', -- [EDWEffectiveDate]
                          NULL, -- [EDWEndDate]
                          GETDATE(), -- [EDWCreatedDate]
                          USER_NAME(), -- [EDWCreatedBy]
                          NULL   -- [Deleted]
                    UNION
                    SELECT-2, -- [CostCenterKey]
                          '-2', -- [CostCenterNumber]
                          'Not Applicable Cost Center', -- [CostCenterName ]
                          '-2', -- [CompanyID ]
                          '-2', -- [CostCenterTypeID]
                          'Not Applicable Cost Center', -- [CCHierarchyLevel1Name ]
                          NULL, -- [CCHierarchyLevel2Name ]
                          NULL, -- [CCHierarchyLevel3Name ]
                          NULL, -- [CCHierarchyLevel4Name ]
                          NULL, -- [CCHierarchyLevel5Name ]
                          NULL, -- [CCHierarchyLevel6Name ]
                          NULL, -- [CCHierarchyLevel7Name ]
                          NULL, -- [CCHierarchyLevel8Name ]
                          NULL, -- [CCHierarchyLevel9Name ]
                          NULL, -- [CCHierarchyLevel10Name ]
                          NULL, -- [CCHierarchyLevel11Name ]
                          '19000101', -- [CCOpenDate]
                          NULL, -- [CCClosedDate]
                          NULL, -- [CCReopenDate]
                          NULL, -- [CCReopenDateType ]
                          'Not Applicable Classification', -- [CCClassification ]
                          'Not Applicable Status', -- [CCStatus ]
                          'Not Applicable Consolidation', -- [CCConsolidation ]
                          NULL, -- [CCFlexAttribute1 ]
                          NULL, -- [CCFlexAttribute2 ]
                          NULL, -- [CCFlexAttribute3 ]
                          NULL, -- [CCFlexAttribute4 ]
                          NULL, -- [CCFlexAttribute5 ]
                          NULL, -- [CenterCMSID]
                          NULL, -- [CenterCSSID]
                          NULL, -- [SiteHorizonID]
                          NULL, -- [CenterEnrollmentSourceSystem]
                          NULL, -- [CenterCMSMigrationDate]
                          NULL, -- [CenterCMSMigrationStatus]
                          NULL, -- [CenterLicensedCapacity]
                          NULL, -- [CenterBackupCareFlag ]
                          NULL, -- [CenterChildCareSelectFlag ]
                          NULL, -- [CenterPublicAllowedFlag ]
                          NULL, -- [CenterOpenTime]
                          NULL, -- [CenterCloseTime]
                          NULL, -- [CenterStudentMinimumAge ]
                          NULL, -- [CenterStudentMaximumAge ]
                          NULL, -- [CenterOpenSunFlag]
                          NULL, -- [CenterOpenMonFlag]
                          NULL, -- [CenterOpenTueFlag]
                          NULL, -- [CenterOpenWedFlag]
                          NULL, -- [CenterOpenThuFlag]
                          NULL, -- [CenterOpenFriFlag]
                          NULL, -- [CenterOpenSatFlag]
                          NULL, -- [CenterFoodProgramStartDate]
                          NULL, -- [CenterFoodProgramEndDate]
                          NULL, -- [CenterRegistrationType]
                          NULL, -- [SiteSchoolDistrict]
                          NULL, -- [SiteClassYear]
                          NULL, -- [CenterMenuURL]
                          NULL, -- [CenterHasBreakfastFlag]
                          NULL, -- [CenterHasMorningSlackFlag]
                          NULL, -- [CenterHasLunchFlag]
                          NULL, -- [CenterHasAfternoonSnackFlag]
                          NULL, -- [CenterSpeaksASLFlag]
                          NULL, -- [CenterSpeaksArabicFlag]
                          NULL, -- [CenterSpeaksFrenchFlag]
                          NULL, -- [CenterSpeaksGermanFlag]
                          NULL, -- [CenterSpeaksHindiFlag]
                          NULL, -- [CenterSpeaksMandarinFlag]
                          NULL, -- [CenterSpeaksPunjabiFlag]
                          NULL, -- [CenterSpeaksSpanishFlag]
                          NULL, -- [CenterSpeaksOtherLanguages]
                          NULL, -- [CenterAccreditationAgencyCode]
                          NULL, -- [CenterAccreditationStartDate]
                          NULL, -- [CenterAccreditationExpirationDate]
                          NULL, -- [CenterAccreditationNextActivity]
                          NULL, -- [CenterAccreditationNextActivityDueDate]
                          NULL, -- [CenterAccreditationPrimaryStatus]
                          NULL, -- [CenterAccreditationProgramID]
                          NULL, -- [CenterQRISRating]
                          NULL, -- [CenterQRISRatingStartDate]
                          NULL, -- [CenterQRISRatingExpirationDate]
                          NULL, -- [CenterMaintenanceSupervisorName ]
                          NULL, -- [CenterPreventativeTechnicianName ]
                          NULL, -- [CenterRegionalFacilitiesCoordinatorName ]
                          NULL, -- [CenterRegionalFacilitiesManagerName ]
                          NULL, -- [CenterNutritionAndWellnessAdministratorName ]
                          NULL, -- [CenterNutritionAndWellnessAdministratorEmail ]
                          NULL, -- [CenterNutritionAndWellnessAdministratorPhone ]
                          NULL, -- [CenterSubsidyCoordinatorName ]
                          NULL, -- [CenterSubsidyCoordinatorEmail ]
                          NULL, -- [CenterSubsidyCoordinatorPhone ]
                          NULL, -- [CenterSubsidyManagerName ]
                          NULL, -- [CenterSubsidyManagerEmail ]
                          NULL, -- [CenterSubsidyManagerPhone ]
                          NULL, -- [CenterSubsidySupervisorName ]
                          NULL, -- [CenterSubsidySupervisorEmail ]
                          NULL, -- [CenterSubsidySupervisorPhone ]
                          NULL, -- [CenterBuildingSquareFootage]
                          NULL, -- [CenterLandSquareFootage]
                          NULL, -- [CenterCoreBasedStatisticalAreaName ]
                          NULL, -- [CenterLandlordName ]
                          NULL, -- [CenterLeaseControlEndMonthDate]
                          NULL, -- [CenterLeaseExpirationDate]
                          NULL, -- [CenterLeaseExtensionOptionNoticeDate]
                          NULL, -- [CenterLeaseExtensionOptionsRemainingCount]
                          NULL, -- [CenterLeaseExtensionOptionRemainingYears]
                          NULL, -- [CenterLeaseStatus ]
                          NULL, -- [CenterLatitude ]
                          NULL, -- [CenterLongitude]
                          NULL, -- [CenterCurrentHumanSigmaScore]
                          NULL, -- [CenterPreviousHumanSigmaScore ]
                          '19000101', -- [EDWEffectiveDate]
                          NULL, -- [EDWEndDate]
                          GETDATE(), -- [EDWCreatedDate]
                          USER_NAME(), -- [EDWCreatedBy]
                          NULL;
		   -- Keep a running total of Inserts
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimCostCenter : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimCostCenter OFF;

             -- ====================================================================================================
             -- DimCostCenterType
             -- ==================================================================================================== 
             DELETE FROM [dbo].[DimCostCenterType]
             WHERE [CostCenterTypeKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimCostCenterType : Deleted seed row[s]';
             END;

--
             SET IDENTITY_INSERT dbo.DimCostCenterType ON;
             INSERT INTO [dbo].[DimCostCenterType]
             ([CostCenterTypeKey],
              [CostCenterTypeID],
              [CostCenterTypeName],
              [CCTBusinessUnitCode],
              [CCTBusinessUnitName],
              [CCTLineOfBusinessCode],
              [CCTLineOfBusinessName],
              [CCTLineOfBusinessSubcategoryCode],
              [CCTLineOfBusinessSubcategoryName],
              [CCTLineOfBusinessCategoryCode],
              [CCTLineOfBusinessCategoryName],
              [CCTOrganizationLevelCode],
              [CCTOrganizationLevelName],
              [CCTFunctionCode],
              [CCTFunctionName],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1, -- [CostCenterTypeKey]
                          '-1', -- [CostCenterTypeID]
                          'Unknown Cost Center Type', -- [CostCenterTypeName]
                          'Unknown Business Unit', -- [CCTBusinessUnitCode]
                          'Unknown Business Unit', -- [CCTBusinessUnitName]
                          'Unknown Line of Business', -- [CCTLineOfBusinessCode]
                          'Unknown Line of Business', -- [CCTLineOfBusinessName]
                          'Unknown Line of Business Subcategory', -- [CCTLineOfBusinessSubcategoryCode]
                          'Unknown Line of Business Subcategory', -- [CCTLineOfBusinessSubcategoryName]
                          'Unknown Line of Business Category', -- [CCTLineOfBusinessCategoryCode]
                          'Unknown Line of Business Category', -- [CCTLineOfBusinessCategoryName]
                          'Unknown Organizational Level', -- [CCTOrganizationLevelCode]
                          'Unknown Organizational Level', -- [CCTOrganizationLevelName]
                          'Unknown Function', -- [CCTFunctionCode]
                          'Unknown Function', -- [CCTFunctionName]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER, -- [EDWCreatedBy]
                          GETDATE(), -- [EDWModifiedDate]
                          SYSTEM_USER, -- [EDWModifiedBy]
                          NULL -- [Deleted]
                    UNION
                    SELECT-2, -- [CostCenterTypeKey]
                          '-2', -- [CostCenterTypeID]
                          'Not Applicable Cost Center Type', -- [CostCenterTypeName]
                          'Not Applicable Business Unit', -- [CCTBusinessUnitCode]
                          'Not Applicable Business Unit', -- [CCTBusinessUnitName]
                          'Not Applicable Line of Business', -- [CCTLineOfBusinessCode]
                          'Not Applicable Line of Business', -- [CCTLineOfBusinessName]
                          'Not Applicable Line of Business Subcategory', -- [CCTLineOfBusinessSubcategoryCode]
                          'Not Applicable Line of Business Subcategory', -- [CCTLineOfBusinessSubcategoryName]
                          'Not Applicable Line of Business Category', -- [CCTLineOfBusinessCategoryCode]
                          'Not Applicable Line of Business Category', -- [CCTLineOfBusinessCategoryName]
                          'Not Applicable Organizational Level', -- [CCTOrganizationLevelCode]
                          'Not Applicable Organizational Level', -- [CCTOrganizationLevelName]
                          'Not Applicable Function', -- [CCTFunctionCode]
                          'Not Applicable Function', -- [CCTFunctionName]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER, -- [EDWCreatedBy]
                          GETDATE(), -- [EDWModifiedDate]
                          SYSTEM_USER, -- [EDWModifiedBy]
                          NULL;
		    -- Keep a running total of Inserts
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimCostCenterType : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimCostCenterType OFF;

             -- ====================================================================================================
             -- DimCreditMemoType
             -- ==================================================================================================== 
             DELETE FROM [dbo].[DimCreditMemoType]
             WHERE [CreditMemoTypeKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimCreditMemoType : Deleted seed row[s]';
             END;

--
             SET IDENTITY_INSERT dbo.DimCreditMemoType ON;
             INSERT INTO [dbo].[DimCreditMemoType]
             ([CreditMemoTypeKey],
              [CreditMemoTypeID],
              [CreditMemoTypeName],
              [CreditMemoCategory],
              [CreditMemoPostingType],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1, -- [CreditMemoTypeKey]
                          -1, -- [CreditMemoTypeID]
                          'Unknown Credit Memo Type', -- [CreditMemoTypeName]
                          'Unknown Category', -- [CreditMemoCategory]
                          'Unknown', -- [CreditMemoPostingType]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER, -- [EDWCreatedBy]
                          GETDATE(), -- [EDWModifiedDate]
                          SYSTEM_USER, -- [EDWModifiedBy]
                          NULL -- [Deleted]
                    UNION
                    SELECT-2, -- [CreditMemoTypeKey]
                          -2, -- [CreditMemoTypeID]
                          'Not Applicable Credit Memo Type', -- [CreditMemoTypeName]
                          'Not Applicable Category', -- [CreditMemoCategory]
                          'N/A', -- [CreditMemoPostingType]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER, -- [EDWCreatedBy]
                          GETDATE(), -- [EDWModifiedDate]
                          SYSTEM_USER, -- [EDWModifiedBy]
                          NULL;
		   -- Keep a running total of Inserts
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimCreditMemoType : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimCreditMemoType OFF;

             -- ====================================================================================================
             -- DimDataScenario
             -- ==================================================================================================== 
             --
             -- DimDataScenario seed row populated by spBING_EDW_Generate_DimDataScenario
             --

             -- ====================================================================================================
             -- DimDate
             -- ==================================================================================================== 
             DELETE FROM [dbo].[DimDate]
             WHERE [DateKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimDate : Deleted seed row[s]';
             END;
--

             INSERT INTO [dbo].[DimDate]
             ([DateKey],
              [FullDate],
              [FullDateName],
              [WeekdayWeekend],
              [HolidayName],
              [HolidayFlag],
              [HolidayFiscalWeekFlag],
              [CalendarFirstDayOfMonthFlag],
              [CalendarLastDayOfMonthFlag],
              [CalendarFirstWeekOfMonthFlag],
              [CalendarLastWeekOfMonthFlag],
              [CalendarDaySequenceNumber],
              [CalendarDayOfWeekNumber],
              [CalendarDayOfWeekName],
              [CalendarDayOfWeekNameShort],
              [CalendarDayOfMonthNumber],
              [CalendarDayOfQuarterNumber],
              [CalendarDayOfYearNumber],
              [CalendarWeekNumber],
              [CalendarWeekName],
              [CalendarWeekOfMonthNumber],
              [CalendarWeekOfMonthName],
              [CalendarWeekOfYearNumber],
              [CalendarWeekOfYearName],
              [CalendarWeekStartDate],
              [CalendarWeekEndDate],
              [CalendarMonthNumber],
              [CalendarMonthName],
              [CalendarMonthOfYearNumber],
              [CalendarMonthOfYearName],
              [CalendarMonthOfYearNameShort],
              [CalendarMonthStartDate],
              [CalendarMonthEndDate],
              [CalendarQuarterNumber],
              [CalendarQuarterName],
              [CalendarQuarterOfYearNumber],
              [CalendarQuarterOfYearName],
              [CalendarQuarterStartDate],
              [CalendarQuarterEndDate],
              [CalendarYearNumber],
              [CalendarYearName],
              [CalendarYearStartDate],
              [CalendarYearEndDate],
              [FiscalDayOfWeekNumber],
              [FiscalDayOfPeriodNumber],
              [FiscalDayOfQuarterNumber],
              [FiscalDayOfYearNumber],
              [FiscalWeekNumber],
              [FiscalWeekName],
              [FiscalWeekOfPeriodNumber],
              [FiscalWeekOfPeriodName],
              [FiscalWeekOfQuarterNumber],
              [FiscalWeekOfQuarterName],
              [FiscalWeekOfYearNumber],
              [FiscalWeekOfYearName],
              [FiscalWeekSequenceNumber],
              [FiscalWeekStartDate],
              [FiscalWeekEndDate],
              [FiscalPeriodNumber],
              [FiscalPeriodName],
              [FiscalPeriodType],
              [FiscalPeriodOfYearNumber],
              [FiscalPeriodOfYearName],
              [FiscalPeriodSequenceNumber],
              [FiscalPeriodStartDate],
              [FiscalPeriodEndDate],
              [FiscalQuarterNumber],
              [FiscalQuarterName],
              [FiscalQuarterOfYearNumber],
              [FiscalQuarterOfYearName],
              [FiscalQuarterSequenceNumber],
              [FiscalQuarterStartDate],
              [FiscalQuarterEndDate],
              [FiscalYearNumber],
              [FiscalYearName],
              [FiscalYearStartDate],
              [FiscalYearEndDate],
              [PayrollStartDate],
              [PayrollEndDate],
              [PayrollCheckDate],
              [BTSPeriodFlag],
              [AcademicYearNumber],
              [BTSYearNumber],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1, --[DateKey]
                          '19000101', --[FullDate]
                          '1900-01-01', --[FullDateName]
                          'Unknown', --[WeekdayWeekend]
                          'Unknown Holiday', --[HolidayName]
                          'Unknown Holiday', --[HolidayFlag]
                          'Unknown Holiday Fiscal Week', --[HolidayFiscalWeekFlag]
                          'Unknown Day of Month', --[CalendarFirstDayOfMonthFlag]
                          'Unknown Day of Month', --[CalendarLastDayOfMonthFlag]
                          'Unknown Week of Month', --[CalendarFirstWeekOfMonthFlag]
                          'Unknown Week of Month', --[CalendarLastWeekOfMonthFlag]
                          -1, --[CalendarDaySequenceNumber]
                          -1, --[CalendarDayOfWeekNumber]
                          'Unknown', --[CalendarDayOfWeekName]
                          'Unknown', --[CalendarDayOfWeekNameShort]
                          -1, --[CalendarDayOfMonthNumber]
                          -1, --[CalendarDayOfQuarterNumber]
                          -1, --[CalendarDayOfYearNumber]
                          -1, --[CalendarWeekNumber]
                          'Unknown Calendar Week', --[CalendarWeekName]
                          -1, --[CalendarWeekOfMonthNumber]
                          'Unknown Calendar Week of Month', --[CalendarWeekOfMonthName]
                          -1, --[CalendarWeekOfYearNumber]
                          'Unknown Calendar Week of Year', --[CalendarWeekOfYearName]
                          '19000101', --[CalendarWeekStartDate]
                          '19000101', --[CalendarWeekEndDate]
                          -1, --[CalendarMonthNumber]
                          'Unknown Calendar Month', --[CalendarMonthName]
                          -1, --[CalendarMonthOfYearNumber]
                          'Unknown Calendar Month of Year', --[CalendarMonthOfYearName]
                          'Unknown', --[CalendarMonthOfYearNameShort]
                          '19000101', --[CalendarMonthStartDate]
                          '19000101', --[CalendarMonthEndDate]
                          -1, --[CalendarQuarterNumber]
                          'Unknown Calendar Quarter', --[CalendarQuarterName]
                          -1, --[CalendarQuarterOfYearNumber]
                          'Unknown Calendar Quarter of Year', --[CalendarQuarterOfYearName]
                          '19000101', --[CalendarQuarterStartDate]
                          '19000101', --[CalendarQuarterEndDate]
                          -1, --[CalendarYearNumber]
                          'Unknown Calendar Year', --[CalendarYearName]
                          '19000101', --[CalendarYearStartDate]
                          '19000101', --[CalendarYearEndDate]
                          -1, --[FiscalDayOfWeekNumber]
                          -1, --[FiscalDayOfPeriodNumber]
                          -1, --[FiscalDayOfQuarterNumber]
                          -1, --[FiscalDayOfYearNumber]
                          -1, --[FiscalWeekNumber]
                          'Unknown Fiscal Week', --[FiscalWeekName]
                          -1, --[FiscalWeekOfPeriodNumber]
                          'Unknown Fiscal Week of Period', --[FiscalWeekOfPeriodName]
                          -1, --[FiscalWeekOfQuarterNumber]
                          'Unknown Fiscal Week of Quarter', --[FiscalWeekOfQuarterName]
                          -1, --[FiscalWeekOfYearNumber]
                          'Unknown Fiscal Week of Year', --[FiscalWeekOfYearName]
                          -1, --[FiscalWeekSequenceNumber]
                          '19000101', --[FiscalWeekStartDate]
                          '19000101', --[FiscalWeekEndDate]
                          -1, --[FiscalPeriodNumber]
                          'Unknown Fiscal Period', --[FiscalPeriodName]
                          'Unknown', --[FiscalPeriodType]
                          -1, --[FiscalPeriodOfYearNumber]
                          'Unknown Fiscal Period of Year', --[FiscalPeriodOfYearName]
                          -1, --[FiscalPeriodSequenceNumber]
                          '19000101', --[FiscalPeriodStartDate]
                          '19000101', --[FiscalPeriodEndDate]
                          -1, --[FiscalQuarterNumber]
                          'Unknown Fiscal Quarter', --[FiscalQuarterName]
                          -1, --[FiscalQuarterOfYearNumber]
                          'Unknown Fiscal Quarter of Year', --[FiscalQuarterOfYearName]
                          -1, --[FiscalQuarterSequenceNumber]
                          '19000101', --[FiscalQuarterStartDate]
                          '19000101', --[FiscalQuarterEndDate]
                          -1, --[FiscalYearNumber]
                          'Unknown Fiscal Year', --[FiscalYearName]
                          '19000101', --[FIscalYearStartDate]
                          '19000101', --[FiscalYearEndDate]
                          '19000101', --[PayrollStartDate],
                          '19000101', --[PayrollEndDate],
                          '19000101', --[PayrollCheckDate],
                          'Unknown BTS Period', --[BTSPeriodFlag],
                          -1, --[AcademicYearNumber],
                          -1, --[BTSYearNumber],
                          GETDATE(), --[EDWCreatedDate]
                          SUSER_NAME(), --[EDWCreatedBy]
                          GETDATE(), --[EDWModifiedDate]
                          SUSER_NAME(), --[EDWModifiedBy]
                          NULL
                    UNION
                    SELECT-2, --[DateKey]
                          '19000102', --[FullDate]
                          '1900-01-02', --[FullDateName]
                          'N/A', --[WeekdayWeekend]
                          'Not Applicable Holiday', --[HolidayName]
                          'Not Applicable Holiday', --[HolidayFlag]
                          'Not Applicable Holiday Fiscal Week', --[HolidayFiscalWeekFlag]
                          'Not Applicable Day of Month', --[CalendarFirstDayOfMonthFlag]
                          'Not Applicable Day of Month', --[CalendarLastDayOfMonthFlag]
                          'Not Applicable Week of Month', --[CalendarFirstWeekOfMonthFlag]
                          'Not Applicable Week of Month', --[CalendarLastWeekOfMonthFlag]
                          -2, --[CalendarDaySequenceNumber]
                          -2, --[CalendarDayOfWeekNumber]
                          'N/A', --[CalendarDayOfWeekName]
                          'N/A', --[CalendarDayOfWeekNameShort]
                          -2, --[CalendarDayOfMonthNumber]
                          -2, --[CalendarDayOfQuarterNumber]
                          -2, --[CalendarDayOfYearNumber]
                          -2, --[CalendarWeekNumber]
                          'Not Applicable Calendar Week', --[CalendarWeekName]
                          -2, --[CalendarWeekOfMonthNumber]
                          'Not Applicable Calendar Week of Month', --[CalendarWeekOfMonthName]
                          -2, --[CalendarWeekOfYearNumber]
                          'Not Applicable Calendar Week of Year', --[CalendarWeekOfYearName]
                          '19000102', --[CalendarWeekStartDate]
                          '19000102', --[CalendarWeekEndDate]
                          -2, --[CalendarMonthNumber]
                          'Not Applicable Calendar Month', --[CalendarMonthName]
                          -2, --[CalendarMonthOfYearNumber]
                          'Not Applicable Calendar Month of Year', --[CalendarMonthOfYearName]
                          'N/A', --[CalendarMonthOfYearNameShort]
                          '19000102', --[CalendarMonthStartDate]
                          '19000102', --[CalendarMonthEndDate]
                          -2, --[CalendarQuarterNumber]
                          'Not Applicable Calendar Quarter', --[CalendarQuarterName]
                          -2, --[CalendarQuarterOfYearNumber]
                          'Not Applicable Calendar Quarter of Year', --[CalendarQuarterOfYearName]
                          '19000102', --[CalendarQuarterStartDate]
                          '19000102', --[CalendarQuarterEndDate]
                          -2, --[CalendarYearNumber]
                          'Not Applicable Calendar Year', --[CalendarYearName]
                          '19000102', --[CalendarYearStartDate]
                          '19000102', --[CalendarYearEndDate]
                          -2, --[FiscalDayOfWeekNumber]
                          -2, --[FiscalDayOfPeriodNumber]
                          -2, --[FiscalDayOfQuarterNumber]
                          -2, --[FiscalDayOfYearNumber]
                          -2, --[FiscalWeekNumber]
                          'Not Applicable Fiscal Week', --[FiscalWeekName]
                          -2, --[FiscalWeekOfPeriodNumber]
                          'Not Applicable Fiscal Week of Period', --[FiscalWeekOfPeriodName]
                          -2, --[FiscalWeekOfQuarterNumber]
                          'Not Applicable Fiscal Week of Quarter', --[FiscalWeekOfQuarterName]
                          -2, --[FiscalWeekOfYearNumber]
                          'Not Applicable Fiscal Week of Year', --[FiscalWeekOfYearName]
                          -2, --[FiscalWeekSequenceNumber]
                          '19000102', --[FiscalWeekStartDate]
                          '19000102', --[FiscalWeekEndDate]
                          -2, --[FiscalPeriodNumber]
                          'Not Applicable Fiscal Period', --[FiscalPeriodName]
                          'N/A', --[FiscalPeriodType]
                          -2, --[FiscalPeriodOfYearNumber]
                          'Not Applicable Fiscal Period of Year', --[FiscalPeriodOfYearName]
                          -2, --[FiscalPeriodSequenceNumber]
                          '19000102', --[FiscalPeriodStartDate]
                          '19000102', --[FiscalPeriodEndDate]
                          -2, --[FiscalQuarterNumber]
                          'Not Applicable Fiscal Quarter', --[FiscalQuarterName]
                          -2, --[FiscalQuarterOfYearNumber]
                          'Not Applicable Fiscal Quarter of Year', --[FiscalQuarterOfYearName]
                          -2, --[FiscalQuarterSequenceNumber]
                          '19000102', --[FiscalQuarterStartDate]
                          '19000102', --[FiscalQuarterEndDate]
                          -2, --[FiscalYearNumber]
                          'Not Applicable Fiscal Year', --[FiscalYearName]
                          '19000102', --[FIscalYearStartDate]
                          '19000102', --[FiscalYearEndDate]
                          '19000102', --[PayrollStartDate],
                          '19000102', --[PayrollEndDate],
                          '19000102', --[PayrollCheckDate],
                          'N/A BTS Period', --[BTSPeriodFlag],
                          -2, --[AcademicYearNumber],
                          -2, --[BTSYearNumber],
                          GETDATE(), --[EDWCreatedDate]
                          SUSER_NAME(), --[EDWCreatedBy]
                          GETDATE(), --[EDWModifiedDate]
                          SUSER_NAME(), --[EDWModifiedBy]
                          NULL;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimDate : Inserted seed row[s]';
             END;

             -- ====================================================================================================
             -- DimDiscountType
             -- ==================================================================================================== 
             DELETE FROM [dbo].[DimDiscountType]
             WHERE [DiscountTypeKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimDiscountType : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimDiscountType ON;
             INSERT INTO [dbo].[DimDiscountType]
             ([DiscountTypeKey],
              [DiscountTypeID],
              [DiscountTypeName],
              [DiscountTypeDescription],
              [DiscountCategory],
              [DiscountRecurring],
              [DiscountPriority],
              [DiscountNet],
              [CSSTransactionCode],
              [CSSTransactionType],
              [SourceSystem],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1,
                          -1,
                          'Unknown Discount',
                          'Unknown Discount Description',
                          'Unknown Discount Category',
                          'Unknown Discount Recurring',
                          999999,
                          'Unknown Net Discount',
                          -1,
                          'XX',
                          'UNK',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable Discount',
                          'Not Applicable Discount Description',
                          'Not Applicable Discount Category',
                          'Not Applicable Discount Recurring',
                          999999,
                          'N/A Net Discount',
                          -1,
                          'XX',
                          'N/A',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimDiscountType : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimDiscountType OFF;

             -- ====================================================================================================
             -- DimFeeType
             -- ==================================================================================================== 
             DELETE FROM [dbo].[DimFeeType]
             WHERE [FeeTypeKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimFeeType : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimFeeType ON;
             INSERT INTO [dbo].[DimFeeType]
             ([FeeTypeKey],
              [FeeTypeID],
              [FeeTypeName],
              [FeeTypeDescription],
              [FeeCategory],
              [FeeUnitOfMeasure],
              [FeeFTE],
              [CSSTransactionCode],
              [CSSTransactionType],
              [SourceSystem],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1,
                          -1,
                          'Unknown Fee Type',
                          'Unknown Fee Type Description',
                          'Unknown Fee Category',
                          'Unknown Fee Unit of Measure',
                          0,
                          -1,
                          -1,
                          'UNK',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable Fee Type',
                          'Not Applicable Fee Type Description',
                          'Not Applicable Fee Category',
                          'Not Applicable Fee Unit of Measure',
                          0,
                          -1,
                          -1,
                          'N/A',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimFeeType : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimFeeType OFF;

             -- ====================================================================================================
             -- DimGLMetricType
             -- ====================================================================================================
             --
             -- DimGLMetricType seed row populated by spBING_EDW_Generate_DimGLMetricType
             --

             -- ====================================================================================================
             -- DimInvoiceType
             -- ====================================================================================================
             DELETE FROM [dbo].[DimInvoiceType]
             WHERE [InvoiceTypeKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimInvoiceType : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimInvoiceType ON;
             INSERT INTO [dbo].[DimInvoiceType]
             ([InvoiceTypeKey],
              [InvoiceTypeID],
              [InvoiceTypeName],
              [InvoicePostingType],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1,
                          -1,
                          'Unknown InvoiceType',
                          '~',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable InvoiceType',
                          '~',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimInvoiceType : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimInvoiceType OFF;

             -- ====================================================================================================
             -- DimLead
             -- ====================================================================================================
             DELETE FROM [dbo].[DimLead]
             WHERE [LeadKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimLead : Deleted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimLead ON;
             INSERT INTO [dbo].[DimLead]
             ([LeadKey],
              [SponsorKey],
              [LeadID],
              [LeadName],
              [LeadContact],
              [LeadAddress],
              [LeadCity],
              [LeadState],
              [LeadZIP],
              [LeadPhone],
              [LeadMobilePhone],
              [LeadEmail],
              [LeadStatus],
              [InquiryBrand],
              [InquirySourceType],
              [InquirySource],
              [InquiryType],
			  [IsWebInquiry],
			  [IsContactedWithin24Hours],
			  [IsCreatedMondayThursdayLocal],
              [MethodOfContact],
              [ContactPreference],
              [EDWCreatedDate],
              [EDWModifiedDate]
             )
                    SELECT-1, -- LeadKey
                          -1, -- SponsorKey
                          '-1', -- LeadID
                          'Unknown Lead', -- LeadName
                          'Unknown Lead Contact', -- LeadContact
                          'Unknown Address', -- LeadAddress
                          'Unknown City', -- LeadCity
                          'Unknown State', -- LeadState
                          'Unknown ZIP', -- LeadZIP
                          'Unknown Phone', -- LeadPhone
                          'Unknown Mobile Phone', -- LeadMobilePhone
                          'Unknown Email', -- LeadEmail
                          'Unknown Status', -- LeadStatus
                          'Unknown Brand', -- InquiryBrand
                          'Unknown Source Type', -- InquirySourceType
                          'Unknown Source', -- InquirySource
                          'Unknown Inquiry Type', -- InquiryType
						  -1,   --- IsWebInquiry
						  -1,   --- IsContactedWithin24Hours
						  -1,   --- IsCreatedMondayThursdayLocal
                          'Unknown Method of Contact', -- MethodOfContact
                          'Unknown Contact Preference', -- ContactPreference
                          GETDATE(), -- EDWCreatedDate
                          GETDATE() -- EDWModifiedDate
                    UNION
                    SELECT-2, -- LeadKey
                          -2, -- SponsorKey
                          '-2', -- LeadID
                          'Not Applicable Lead', -- LeadName
                          'Not Applicable Lead Contact', -- LeadContact
                          'Not Applicable Address', -- LeadAddress
                          'Not Applicable City', -- LeadCity
                          'Not Applicable State', -- LeadState
                          'Not Applicable ZIP', -- LeadZIP
                          'Not Applicable Phone', -- LeadPhone
                          'Not Applicable Mobile Phone', -- LeadMobilePhone
                          'Not Applicable Email', -- LeadEmail
                          'Not Applicable Status', -- LeadStatus
                          'Not Applicable Brand', -- InquiryBrand
                          'Not Applicable Source Type', -- InquirySourceType
                          'Not Applicable Source', -- InquirySource
                          'Not Applicable Inquiry Type', -- InquiryType
						  -2,   --- IsWebInquiry
						  -2,   --- IsContactedWithin24Hours
						  -2,   --- IsCreatedMondayThursdayLocal
                          'Not Applicable Method of Contact', -- MethodOfContact
                          'Not Applicable Contact Preference', -- ContactPreference
                          GETDATE(), -- EDWCreatedDate
                          GETDATE(); -- EDWModifiedDate

             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimLead : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimLead OFF;

             -- ====================================================================================================
             -- DimLeadEventType
             -- ====================================================================================================
             DELETE FROM [dbo].[DimLeadEventType]
             WHERE [LeadEventTypeKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimLeadEventType : Deleted seed row[s]';
             END;
             --
             INSERT INTO [dbo].[DimLeadEventType]
             ([LeadEventTypeKey],
              [LeadEventTypeName],
              [EDWCreatedDate]
             )
                    SELECT-1,
                          'Unknown Lead Event Type',
                          GETDATE()
                    UNION
                    SELECT-2,
                          'Not Applicable Lead Event Type',
                          GETDATE();
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimLeadEventType : Inserted seed row[s]';
             END;

             -- ====================================================================================================
             -- DimLeadType
             -- ====================================================================================================
             DELETE FROM [dbo].[DimLeadType]
             WHERE [LeadTypeKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimLeadType : Deleted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimLeadType ON;
             INSERT INTO dbo.DimLeadType
             ([LeadTypeKey],
              [LeadStatus],
              [InquiryBrand],
              [InquirySourceType],
              [InquirySource],
              [InquiryType],
              [MethodOfContact],
              [ContactPreference],
              [EDWCreatedDate]
             )
                    SELECT-1,
                          '-1',
                          'Unknown Brand',
                          'Unknown Source Type',
                          'Unknown Source',
                          'Unknown Inquiry Type',
                          'Unknown Method of Contact',
                          'Unknown Contact Preference',
                          GETDATE()
                    UNION
                    SELECT-2,
                          '-2',
                          'Not Applicable Brand',
                          'Not Applicable Source Type',
                          'Not Applicable Source',
                          'Not Applicable Inquiry Type',
                          'Not Applicable Method of Contact',
                          'Not Applicable Contact Preference',
                          GETDATE();
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimLeadType : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimLeadType OFF;

             -- ====================================================================================================
             -- DimLeaveReason
             -- ====================================================================================================
             DELETE FROM [dbo].[DimLeaveReason]
             WHERE [LeaveReasonKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimLeaveReason : Deleted seed row[s]';
             END;
             SET IDENTITY_INSERT [BING_EDW].[dbo].[DimLeaveReason] ON;
             INSERT INTO [BING_EDW].[dbo].[DimLeaveReason]
             ([LeaveReasonKey],
              [LeaveReasonCode],
              [LeaveReasonName],
              [LeaveReasonCreatedDate],
              [LeaveReasonCreatedUser],
              [LeaveReasonModifiedDate],
              [LeaveReasonModifiedUser],
              [EDWCreatedDate],
              [EDWModifiedDate]
             )
                    SELECT-1,
                          '-1',
                          'Unknown Leave Reason',
                          '1/1/1900',
                          -1,
                          '1/1/1900',
                          -1,
                          @EDWRunDateTime,
                          @EDWRunDateTime
                    UNION
                    SELECT-2,
                          '-2',
                          'Not Applicable Leave Reason',
                          '1/1/1900',
                          -1,
                          '1/1/1900',
                          -1,
                          @EDWRunDateTime,
                          @EDWRunDateTime;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimLeaveType : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT [BING_EDW].[dbo].[DimLeaveReason] OFF;
             -- ====================================================================================================
             -- DimLeaveType
             -- ====================================================================================================
             DELETE FROM [dbo].[DimLeaveType]
             WHERE [LeaveTypeKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimLeaveType : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimLeaveType ON;
             INSERT INTO [dbo].[DimLeaveType]
             (LeaveTypeKey,
              LeaveTypeID,
              LeaveTypeName,
              LeaveCategoryCode,
              LeaveCategoryName,
              LeaveTimeframeCode,
              LeaveTimeframeName,
              LeaveTypeFlexAttribute1,
              LeaveTypeFlexAttribute2,
              LeaveTypeFlexAttribute3,
              LeaveTypeFlexAttribute4,
              LeaveTypeFlexAttribute5,
              LeaveTypeCreatedDate,
              LeaveTypeCreatedUser,
              LeaveTypeModifiedDate,
              LeaveTypeModifiedUser,
              EDWCreatedDate,
              EDWModifiedDate
             )
                    SELECT-1,
                          -1,
                          'Unknown Leave Type',
                          -1,
                          'Unknown Leave Category',
                          -1,
                          'Unknown Leave Timeframe',
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
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable Leave Type',
                          -2,
                          'Not Applicable Leave Category',
                          -2,
                          'Not Applicable Leave Timeframe',
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          '19000101',
                          -2,
                          '19000101',
                          -2,
                          GETDATE(),
                          GETDATE();
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimLeaveType : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimLeaveType OFF;
		                
             -- ====================================================================================================
             -- DimLocation
             -- ====================================================================================================
             DELETE FROM [dbo].[DimLocation]
             WHERE [LocationKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimLocation : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimLocation ON;
             INSERT INTO [dbo].[DimLocation]
             ([LocationKey],
              [LocationID],
              [ShipToLocationID],
              [LocationName],
              [LocationAddressLine1],
              [LocationAddressLine2],
              [LocationAddressLine3],
              [LocationCity],
              [LocationState],
              [LocationZip],
              [LocationCounty],
              [LocationCountry],
              [LocationPhone],
              [LocationSecondaryPhone],
              [LocationFax],
              [LocationEmail],
              [LocationADPCode],
              [LocationWarehouseFlag],
              [LocationShipToSiteFlag],
              [LocationReceivingSiteFlag],
              [LocationBillToSiteFlag],
              [LocationInOrganizationFlag],
              [LocationOfficeSiteFlag],
              [LocationEEOCostCenterName],
              [LocationEEOCostCenterNumber],
              [LocationEEOApprenticesEmployedFlag],
              [LocationEEOGovernmentContractorFlag],
              [LocationEEOMainActivitiesLine1],
              [LocationEEOMainActivitiesLine2],
              [LocationEEOReportedPreviouslyFlag],
              [LocationEEOHeadquartersEstablishmentFlag],
              [LocationDUNSNumber],
              [LocationSICNumber],
              [LocationNAICSNumber],
              [LocationFEINNumber],
              [LocationCreatedUser],
              [LocationCreatedDate],
              [LocationModifiedUser],
              [LocationModifiedDate],
              [EDWEffectiveDate],
              [EDWEndDate],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [Deleted]
             )
                    SELECT-1, --[LocationKey]
                          -1, --[LocationID]
                          -1, --[ShipToLocationID]
                          'Unknown Location', --[LocationName]
                          'Unknown Address', --[LocationAddressLine1]
                          'Unknown Address', --[LocationAddressLine2]
                          'Unknown Address', --[LocationAddressLine3]
                          'Unknown City', --[LocationCity]
                          'Unknown Org', --[LocationState]
                          '-1', --[LocationZip]
                          'Unknown County', --[LocationCounty]
                          'Unknown Country', --[LocationCountry]
                          'Unknown Phone', --[LocationPhone]
                          'Unknown Secondary Phone', --[LocationSecondaryPhone]
                          'Unknown Fax', --[LocationFax]
                          'Unknown Email', --[LocationEmail]
                          '-1', --[LocationADPCode]
                          'Unknown Warehouse', --[LocationWarehouseFlag]
                          'Unknown Ship To Site', --[LocationShipToSiteFlag]
                          'Unknown Receiving Site', --[LocationReceivingSiteFlag]
                          'Unknown Bill To Site', --[LocationBillToSiteFlag]
                          'Unknown In Organization', --[LocationInOrganizationFlag]
                          'Unknown Office Site', --[LocationOfficeSiteFlag]
                          NULL, --[LocationEEOCostCenterName ]
                          NULL, --[LocationEEOCostCenterNumber ]
                          NULL, --[LocationEEOApprenticesEmployedFlag ]
                          NULL, --[LocationEEOGovernmentContractorFlag ]
                          NULL, --[LocationEEOMainActivitesLine1 ]
                          NULL, --[LocationEEOMainActivitiesLine2 ]
                          NULL, --[LocationEEOReportedPreviouslyFlag]
                          NULL, --[LocationEEOHeadquartersEstablishmentFlag ]
                          NULL, --[LocationDUNSNumber ]
                          NULL, --[LocationSICNumber ]
                          NULL, --[LocationNAICSNumber ]
                          NULL, --[LocationFEINNumber]
                          -1, --[LocationCreatedUser]
                          '19000101', --[LocationCreatedDate]
                          -1, --[LocationModifiedUser]
                          '19000101', --[LocationModifiedDate]
                          '19000101', --[EDWEffectiveDate]
                          NULL, --[EDWEndDate]
                          GETDATE(), --[EDWCreatedDate]
                          'Service Account', --[EDWCreatedBy]
                          NULL --[Deleted]
                    UNION
                    SELECT-2, --[LocationKey]
                          -2, --[LocationID]
                          -2, --[ShipToLocationID]
                          'Not Applicable Location', --[LocationName]
                          'Not Applicable Address', --[LocationAddressLine1]
                          'Not Applicable Address', --[LocationAddressLine2]
                          'Not Applicable Address', --[LocationAddressLine3]
                          'Not Applicable City', --[LocationCity]
                          'Not Applicable Org', --[LocationState]
                          '-2', --[LocationZip]
                          'Not Applicable County', --[LocationCounty]
                          'Not Applicable Country', --[LocationCountry]
                          'Not Applicable Phone', --[LocationPhone]
                          'Not Applicable Secondary Phone', --[LocationSecondaryPhone]
                          'Not Applicable Fax', --[LocationFax]
                          'Not Applicable Email', --[LocationEmail]
                          '-2', --[LocationADPCode]
                          'Not Applicable Warehouse', --[LocationWarehouseFlag]
                          'Not Applicable Ship To Site', --[LocationShipToSiteFlag]
                          'Not Applicable Receiving Site', --[LocationReceivingSiteFlag]
                          'Not Applicable Bill To Site', --[LocationBillToSiteFlag]
                          'Not Applicable In Organization', --[LocationInOrganizationFlag]
                          'Not Applicable Office Site', --[LocationOfficeSiteFlag]
                          NULL, --[LocationEEOCostCenterName ]
                          NULL, --[LocationEEOCostCenterNumber ]
                          NULL, --[LocationEEOApprenticesEmployedFlag ]
                          NULL, --[LocationEEOGovernmentContractorFlag ]
                          NULL, --[LocationEEOMainActivitesLine1 ]
                          NULL, --[LocationEEOMainActivitiesLine2 ]
                          NULL, --[LocationEEOReportedPreviouslyFlag]
                          NULL, --[LocationEEOHeadquartersEstablishmentFlag ]
                          NULL, --[LocationDUNSNumber ]
                          NULL, --[LocationSICNumber ]
                          NULL, --[LocationNAICSNumber ]
                          NULL, --[LocationFEINNumber]
                          -1, --[LocationCreatedUser]
                          '19000101', --[LocationCreatedDate]
                          -1, --[LocationModifiedUser]
                          '19000101', --[LocationModifiedDate]
                          '19000101', --[EDWEffectiveDate]
                          NULL, --[EDWEndDate]
                          GETDATE(), --[EDWCreatedDate]
                          'Service Account', --[EDWCreatedBy]
                          NULL; --[Deleted]

             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimLocation : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimLocation OFF;

             -- ====================================================================================================
             -- DimOrganization
             -- ====================================================================================================
             DELETE FROM [dbo].[DimOrganization]
             WHERE [OrgKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimOrganization : Deleted seed row[s]';
             END;

--
             SET IDENTITY_INSERT dbo.DimOrganization ON;
             INSERT INTO [dbo].[DimOrganization]
             ([OrgKey],
              [OrgID],
              [OrgEffectiveDate],
              [OrgEndDate],
              [ParentOrgID],
              [DefaultLocationID],
              [CostCenterNumber],
              [OrgNumber],
              [OrgName],
              [OrgHierarchyLevel1Name],
              [OrgHierarchyLevel2Name],
              [OrgHierarchyLevel3Name],
              [OrgHierarchyLevel4Name],
              [OrgHierarchyLevel5Name],
              [OrgHierarchyLevel6Name],
              [OrgHierarchyLevel7Name],
              [OrgHierarchyLevel8Name],
              [OrgHierarchyLevel9Name],
              [OrgHierarchyLevel10Name],
              [OrgHierarchyLevel11Name],
              [OrgAllName],
              [OrgExecutiveFunctionName],
              [OrgExecutiveFunctionLeaderName],
              [OrgExecutiveSubFunctionName],
              [OrgExecutiveSubFunctionLeaderName],
              [OrgCorporateFunctionName],
              [OrgCorporateSubFunctionName],
              [OrgDivisionName],
              [OrgDivisionLeaderName],
              [OrgRegionNumber],
              [OrgRegionName],
              [OrgRegionLeaderName],
              [OrgMarketNumber],
              [OrgMarketName],
              [OrgMarketLeaderName],
              [OrgSubMarketNumber],
              [OrgSubMarketName],
              [OrgSubMarketLeaderName],
              [OrgDistrictNumber],
              [OrgDistrictName],
              [OrgInterimDistrictNumber],
              [OrgInterimDistrictName],
              [OrgDistrictLeaderName],
              [OrgActingDistrictLeaderName],
              [OrgInterimDistrictLeaderName],
              [OrgGroupNumber],
              [OrgGroupName],
              [OrgGroupLeaderName],
              [OrgSubgroupNumber],
              [OrgSubGroupName],
              [OrgSubGroupLeaderName],
              [OrgCampusNumber],
              [OrgCampusName],
              [OrgCampusLeaderName],
              [OrgCenterLeaderName],
              [OrgActingCenterLeaderName],
              [OrgCategoryName],
              [OrgTypeCode],
              [OrgTypeName],
              [OrgPartnerGroupCode],
              [OrgPartnerGroupName],
              [OrgCenterGroupCode],
              [OrgCenterGroupName],
              [OrgDivisionLegacyName],
              [OrgLineOfBusinessCode],
              [OrgBrandCode],
              [OrgBrandName],
              [OrgFlexAttribute1],
              [OrgFlexAttribute2],
              [OrgFlexAttribute3],
              [OrgFlexAttribute4],
              [OrgFlexAttribute5],
              [OrgCreatedUser],
              [OrgCreatedDate],
              [OrgModifiedUser],
              [OrgModifiedDate],
              [EDWEffectiveDate],
              [EDWEndDate],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [Deleted]
             )
                    SELECT-1, -- [OrgKey]
                          -1, -- [OrgID ]
                          '19000101', -- [OrgEffectiveDate ]
                          '99991231', -- [OrgEndDate]
                          NULL, -- [ParentOrgID]
                          -1, -- [DefaultLocationID]
                          '-1', -- [CostCenterNumber ]
                          '-1', -- [OrgNumber ]
                          'Unknown Org', -- [OrgName ]
                          NULL, -- [OrgHierarchyLevel1Name ]
                          NULL, -- [OrgHierarchyLevel2Name ]
                          NULL, -- [OrgHierarchyLevel3Name ]
                          NULL, -- [OrgHierarchyLevel4Name ]
                          NULL, -- [OrgHierarchyLevel5Name ]
                          NULL, -- [OrgHierarchyLevel6Name ]
                          NULL, -- [OrgHierarchyLevel7Name ]
                          NULL, -- [OrgHierarchyLevel8Name ]
                          NULL, -- [OrgHierarchyLevel9Name ]
                          NULL, -- [OrgHierarchyLevel10Name ]
                          NULL, -- [OrgHierarchyLevel11Name ]
                          'Unknown All', -- [OrgAllName ]
                          'Unknown Executive Function', -- [OrgExecutiveFunctionName ]
                          'Unknown Executive Function Leader', -- [OrgExecutiveFunctionLeaderName]
                          'Unknown Executive Subfunction', -- [OrgExecutiveSubFunctionName ]
                          'Unknown Executive Subfunction Leader', -- [OrgExecutiveSubFunctionLeaderName]
                          'Unknown Corporate Function', -- [OrgCorporateFunctionName ]
                          'Unknown Corporate Subfunction', -- [OrgCorporateSubFunctionName ]
                          'Unknown Division', -- [OrgDivisionName ]
                          'Unknown Division Leader', -- [OrgDivisionLeaderName ]
                          '-1', -- [OrgRegionNumber ]
                          'Unknown Region', -- [OrgRegionName ]
                          'Unknown Region Leader', -- [OrgRegionLeaderName]
                          '-1', -- [OrgMarketNumber ]
                          'Unknown Market', -- [OrgMarketName ]
                          'Unknown Market Leader', -- [OrgMarketLeaderName]
                          '-1', -- [OrgSubMarketNumber ]
                          'Unknown Submarket', -- [OrgSubMarketName ]
                          'Unknown Submarket Leader', -- [OrgSubMarketLeaderName]
                          '-1', -- [OrgDistrictNumber ]
                          'Unknown District', -- [OrgDistrictName ]
                          '-1', -- [OrgInterimDistrictNumber ]
                          'Unknown Interim District', -- [OrgInterimDistrictName ]
                          'Unknown District Leader', -- [OrgDistrictLeaderName]
                          'Unknown Acting District Leader', -- [OrgActingDistrictLeaderName]
                          'Unknown Interim District Leader', -- [OrgInterimDistrictLeaderName]
                          '-1', -- [OrgGroupNumber ]
                          'Unknown Group', -- [OrgGroupName ]
                          'Unknown Group Leader', -- [OrgGroupLeaderName]
                          '-1', -- [OrgSubgroupNumber ]
                          'Unknown Subgroup', -- [OrgSubGroupName ]
                          'Unknown Subgroup Leader', -- [OrgSubGroupLeaderName]
                          '-1', -- [OrgCampusNumber ]
                          'Unknown Campus', -- [OrgCampusName ]
                          'Unknown Campus Leader', -- [OrgCampusLeaderName]
                          'Unknown Center Leader', -- [OrgCenterLeaderName]
                          'Unknown Acting Center Leader', -- [OrgActingCenterLeaderName]
                          'Unknown Category', -- [OrgCategoryName ]
                          'Unknown Type Code', -- [OrgTypeCode ]
                          'Unknown Type', -- [OrgTypeName ]
                          'Unknown Partner Group Code', -- [OrgPartnerGroupCode ]
                          'Unknown Partner Group', -- [OrgPartnerGroupName ]
                          'Unknown Center Group Code', -- [OrgCenterGroupCode ]
                          'Unknown Center Group', -- [OrgCenterGroupName ]
                          'Unknown Legacy Division', -- [OrgDivisionLegacyName ]
                          'Unknown Org Line of Business', -- [OrgLineOfBusinessCode ]
                          'Unknown Brand Code', -- [OrgBrandCode ]
                          'Unknown Brand', -- [OrgBrandName ]
                          NULL, -- [OrgFlexAttribute1 ]
                          NULL, -- [OrgFlexAttribute2 ]
                          NULL, -- [OrgFlexAttribute3 ]
                          NULL, -- [OrgFlexAttribute4 ]
                          NULL, -- [OrgFlexAttribute5 ]
                          -1, -- [OrgCreatedUser ]
                          '19000101', -- [OrgCreatedDate ]
                          -1, -- [OrgModifiedUser ]
                          '19000101', -- [OrgModifiedDate]
                          '19000101', -- [EDWEffectiveDate]
                          NULL, -- [EDWEndDate]
                          GETDATE(), -- [EDWCreatedDate]
                          USER_NAME(), -- [EDWCreatedBy]
                          NULL  -- [Deleted]
                    UNION
                    SELECT-2, -- [OrgKey]
                          -2, -- [OrgID ]
                          '19000101', -- [OrgEffectiveDate ]
                          '99991231', -- [OrgEndDate]
                          NULL, -- [ParentOrgID]
                          -2, -- [DefaultLocationID]
                          '-2', -- [CostCenterNumber ]
                          '-2', -- [OrgNumber ]
                          'Not Applicable Org', -- [OrgName ]
                          NULL, -- [OrgHierarchyLevel1Name ]
                          NULL, -- [OrgHierarchyLevel2Name ]
                          NULL, -- [OrgHierarchyLevel3Name ]
                          NULL, -- [OrgHierarchyLevel4Name ]
                          NULL, -- [OrgHierarchyLevel5Name ]
                          NULL, -- [OrgHierarchyLevel6Name ]
                          NULL, -- [OrgHierarchyLevel7Name ]
                          NULL, -- [OrgHierarchyLevel8Name ]
                          NULL, -- [OrgHierarchyLevel9Name ]
                          NULL, -- [OrgHierarchyLevel10Name ]
                          NULL, -- [OrgHierarchyLevel11Name ]
                          'Not Applicable All', -- [OrgAllName ]
                          'Not Applicable Executive Function', -- [OrgExecutiveFunctionName ]
                          'Not Applicable Executive Function Leader', -- [OrgExecutiveFunctionLeaderName]
                          'Not Applicable Executive Subfunction', -- [OrgExecutiveSubFunctionName ]
                          'Not Applicable Executive Subfunction Leader', -- [OrgExecutiveSubFunctionLeaderName]
                          'Not Applicable Corporate Function', -- [OrgCorporateFunctionName ]
                          'Not Applicable Corporate Subfunction', -- [OrgCorporateSubFunctionName ]
                          'Not Applicable Division', -- [OrgDivisionName ]
                          'Not Applicable Division Leader', -- [OrgDivisionLeaderName ]
                          '-2', -- [OrgRegionNumber ]
                          'Not Applicable Region', -- [OrgRegionName ]
                          'Not Applicable Region Leader', -- [OrgRegionLeaderName]
                          '-2', -- [OrgMarketNumber ]
                          'Not Applicable Market', -- [OrgMarketName ]
                          'Not Applicable Market Leader', -- [OrgMarketLeaderName]
                          '-2', -- [OrgSubMarketNumber ]
                          'Not Applicable Submarket', -- [OrgSubMarketName ]
                          'Not Applicable Submarket Leader', -- [OrgSubMarketLeaderName]
                          '-2', -- [OrgDistrictNumber ]
                          'Not Applicable District', -- [OrgDistrictName ]
                          '-2', -- [OrgInterimDistrictNumber ]
                          'Not Applicable Interim District', -- [OrgInterimDistrictName ]
                          'Not Applicable District Leader', -- [OrgDistrictLeaderName]
                          'Not Applicable Acting District Leader', -- [OrgActingDistrictLeaderName]
                          'Not Applicable Interim District Leader', -- [OrgInterimDistrictLeaderName]
                          '-2', -- [OrgGroupNumber ]
                          'Not Applicable Group', -- [OrgGroupName ]
                          'Not Applicable Group Leader', -- [OrgGroupLeaderName]
                          '-2', -- [OrgSubgroupNumber ]
                          'Not Applicable Subgroup', -- [OrgSubGroupName ]
                          'Not Applicable Subgroup Leader', -- [OrgSubGroupLeaderName]
                          '-2', -- [OrgCampusNumber ]
                          'Not Applicable Campus', -- [OrgCampusName ]
                          'Not Applicable Campus Leader', -- [OrgCampusLeaderName]
                          'Not Applicable Center Leader', -- [OrgCenterLeaderName]
                          'Not Applicable Acting Center Leader', -- [OrgActingCenterLeaderName]
                          'Not Applicable Category', -- [OrgCategoryName ]
                          'Not Applicable Type Code', -- [OrgTypeCode ]
                          'Not Applicable Type', -- [OrgTypeName ]
                          'Not Applicable Partner Group Code', -- [OrgPartnerGroupCode ]
                          'Not Applicable Partner Group', -- [OrgPartnerGroupName ]
                          'Not Applicable Center Group Code', -- [OrgCenterGroupCode ]
                          'Not Applicable Center Group', -- [OrgCenterGroupName ]
                          'Not Applicable Legacy Division', -- [OrgDivisionLegacyName ]
                          'Not Applicable Org Line of Business', -- [OrgLineOfBusinessCode ]
                          'Not Applicable Brand Code', -- [OrgBrandCode ]
                          'Not Applicable Brand', -- [OrgBrandName ]
                          NULL, -- [OrgFlexAttribute1 ]
                          NULL, -- [OrgFlexAttribute2 ]
                          NULL, -- [OrgFlexAttribute3 ]
                          NULL, -- [OrgFlexAttribute4 ]
                          NULL, -- [OrgFlexAttribute5 ]
                          -2, -- [OrgCreatedUser ]
                          '19000101', -- [OrgCreatedDate ]
                          -2, -- [OrgModifiedUser ]
                          '19000101', -- [OrgModifiedDate]
                          '19000101', -- [EDWEffectiveDate]
                          NULL, -- [EDWEndDate]
                          GETDATE(), -- [EDWCreatedDate]
                          USER_NAME(), -- [EDWCreatedBy]
                          NULL;  -- [Deleted];
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimOrganization : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimOrganization OFF;

		   -- ====================================================================================================
             -- DimPayBasis
             -- ====================================================================================================
             DELETE FROM [dbo].[DimPayBasis]
             WHERE [PayBasisKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimPayBasis : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimPayBasis ON;
             INSERT INTO [dbo].[DimPayBasis]
             (PayBasisKey,
              PayBasisID,
              PayBasisName,
              PayBasisAnnualizationFactor,
              PayBasisFlexAttribute1,
              PayBasisFlexAttribute2,
              PayBasisFlexAttribute3,
              PayBasisFlexAttribute4,
              PayBasisFlexAttribute5,
              PayBasisCreatedDate,
              PayBasisCreatedUser,
              PayBasisModifiedDate,
              PayBasisModifiedUser,
              EDWCreatedDate,
              EDWModifiedDate
             )
                    SELECT-1,
                          -1,
                          'Unknown Pay Basis',
                          0,
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
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable Pay Basis',
                          0,
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
                          GETDATE();
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimPayBasis : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimPayBasis OFF;

		   -- ====================================================================================================
             -- DimPayRateChangeReason
             -- ====================================================================================================
             DELETE FROM [dbo].[DimPayRateChangeReason]
             WHERE [PayRateChangeReasonKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimPayRateChangeReason : Deleted seed row[s]';
             END;
             SET IDENTITY_INSERT [BING_EDW].[dbo].[DimPayRateChangeReason] ON;
             INSERT INTO [BING_EDW].[dbo].[DimPayRateChangeReason]
             ([PayRateChangeReasonKey],
              [PayRateChangeReasonCode],
              [PayRateChangeReasonName],
              [PayRateChangeReasonFlexAttribute1],
              [PayRateChangeReasonFlexAttribute2],
              [PayRateChangeReasonFlexAttribute3],
              [PayRateChangeReasonFlexAttribute4],
              [PayRateChangeReasonFlexAttribute5],
              [PayRateChangeReasonCreatedDate],
              [PayRateChangeReasonCreatedUser],
              [PayRateChangeReasonModifiedDate],
              [PayRateChangeReasonModifiedUser],
              [EDWCreatedDate],
              [EDWModifiedDate]
             )
                    SELECT-1,
                          '-1',
                          'Unknown Pay Rate Change Reason',
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          '1/1/1900',
                          -1,
                          '1/1/1900',
                          -1,
                          @EDWRunDateTime,
                          @EDWRunDateTime
                    UNION
                    SELECT-2,
                          '-2',
                          'Not Applicable Pay Rate Change Reason',
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          '1/1/1900',
                          -1,
                          '1/1/1900',
                          -1,
                          @EDWRunDateTime,
                          @EDWRunDateTime;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimPayRateChangeReason : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT [BING_EDW].[dbo].[DimPayRateChangeReason] OFF;
             -- ====================================================================================================
             -- DimPaymentType
             -- ====================================================================================================
             DELETE FROM [dbo].[DimPaymentType]
             WHERE [PaymentTypeKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimPaymentType : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimPaymentType ON;
             INSERT INTO [dbo].[DimPaymentType]
             ([PaymentTypeKey],
              [PaymentTypeID],
              [PaymentTypeName],
              [CSSTransactionCode],
              [SourceSystem],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1,
                          -1,
                          'Unknown PaymentType',
                          -1,
                          'UNK',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable PaymentType',
                          -2,
                          'N/A',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimPaymentType : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimPaymentType OFF;

		   -- ====================================================================================================
             -- DimPosition
             -- ====================================================================================================
             DELETE FROM [dbo].[DimPosition]
             WHERE [PositionKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimPosition : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimPosition ON;
             INSERT INTO [dbo].[DimPosition]
             (PositionKey,
              PositionEffectiveDate,
              PositionEndDate,
              PositionCurrentRecordFlag,
              PositionID,
              PositionCode,
              PositionName,
              RollupPositionName,
              PositionStatusCode,
              PositionStatusName,
              PositionFunctionalAreaName,
              PositionSubfunctionalAreaName,
              PositionCorporateOverheadValue,
              PositionBonusEligibleFlag,
              PositionBonusPlanName,
              PositionBonusTargetPercent,
              PositionLTIPEligibleFlag,
              PositionPayBasisID,
              PositionEntryPayGradeID,
              JobID,
              JobCode,
              JobName,
              JobGroupCode,
              JobGroupName,
              JobCategoryID,
              JobCategoryName,
              JobCCDGroupName,
              JobEEOCategoryID,
              JobEEOCategoryName,
              JobFLSACode,
              JobFLSAName,
              JobWorkersCompTypeCode,
              JobWorkersCompTypeName,
              JobAATypeCode,
              JobAATypeName,
              JobAACategoryName,
              JobLevelCode,
              JobLevelName,
              JobPeopleGroupName,
              JobTypeName,
              JobFamilyName,
              JobManagerFlag,
              JobPurchaseApprovalLevelName,
              JobPcardApproverFlag,
              PositionFlexValue1,
              PositionFlexValue2,
              PositionFlexValue3,
              PositionFlexValue4,
              PositionFlexValue5,
              JobFlexValue1,
              JobFlexValue2,
              JobFlexValue3,
              JobFlexValue4,
              JobFlexValue5,
              PositionCreatedDate,
              PositionCreatedUser,
              PositionModifiedDate,
              PositionModifiedUser,
              JobCreatedDate,
              JobCreatedUser,
              JobModifiedDate,
              JobModifiedUser,
              EDWCreatedDate,
              EDWModifiedDate
             )
                    SELECT-1,
                          '19000101',
                          '99991231',
                          'Y',
                          -1,
                          -1,
                          'Unknown Position',
                          'Unknown Rollup Position',
                          'Unknown Position Status',
                          'Unknown Position Status',
                          'Unknown Functional Area',
                          'Unknown Subfunctional Area',
                          'Unknown Corporate Overhead',
                          'Unknown Bonus Eligible',
                          'Unknown Bonus Plan',
                          0,
                          'Unknown LTIP Eligible',
                          -1,
                          -1,
                          -1,
                          -1,
                          'Unknown Job',
                          -1,
                          'Unknown Job Group',
                          -1,
                          'Unknown Job Category',
                          'Unknown Job CCD Group',
                          -1,
                          'Unknown EEO Category',
                          -1,
                          'Unknown FLSA',
                          -1,
                          'Unknown Workers Comp Type',
                          -1,
                          'Unknown AA Type',
                          'Unknown AA Category',
                          -1,
                          'Unknown Job Level',
                          'Unknown People Group',
                          'Unknown Job Type',
                          'Unknown Job Family',
                          'Unknown Manager',
                          'Unknown Purchase Approval Level',
                          'Unknown Pcard Approver',
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          '19000101',
                          -1,
                          '19000101',
                          -1,
                          '19000101',
                          -1,
                          '19000101',
                          -1,
                          GETDATE(),
                          GETDATE()
                    UNION
                    SELECT-2,
                          '19000101',
                          '99991231',
                          'Y',
                          -2,
                          -2,
                          'Not Applicable Position',
                          'Not Applicable Rollup Position',
                          'Not Applicable Position Status',
                          'Not Applicable Position Status',
                          'Not Applicable Functional Area',
                          'Not Applicable Subfunctional Area',
                          'Not Applicable Corporate Overhead',
                          'Not Applicable Bonus Eligible',
                          'Not Applicable Bonus Plan',
                          0,
                          'Not Applicable LTIP Eligible',
                          -2,
                          -2,
                          -2,
                          -2,
                          'Not Applicable Job',
                          -2,
                          'Not Applicable Job Group',
                          -2,
                          'Not Applicable Job Category',
                          'Not Applicable Job CCD Group',
                          -2,
                          'Not Applicable EEO Category',
                          -2,
                          'Not Applicable FLSA',
                          -2,
                          'Not Applicable Workers Comp Type',
                          -2,
                          'Not Applicable AA Type',
                          'Not Applicable AA Category',
                          -2,
                          'Not Applicable Job Level',
                          'Not Applicable People Group',
                          'Not Applicable Job Type',
                          'Not Applicable Job Family',
                          'Not Applicable Manager',
                          'Not Applicable Purchase Approval Level',
                          'Not Applicable Pcard Approver',
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          '19000101',
                          -1,
                          '19000101',
                          -1,
                          '19000101',
                          -1,
                          '19000101',
                          -1,
                          GETDATE(),
                          GETDATE();
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimPosition : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimPosition OFF;

             -- ====================================================================================================
             -- DimProgram
             -- ====================================================================================================
             DELETE FROM [dbo].[DimProgram]
             WHERE [ProgramKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimProgram : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimProgram ON;
             INSERT INTO [dbo].[DimProgram]
             ([ProgramKey],
              [ProgramID],
              [ProgramName],
              [ProgramDescription],
              [SourceSystem],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1,
                          -1,
                          'Unknown Program',
                          'Unknown Program Description',
                          'UNK',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable Program',
                          'Not Applicable Program Description',
                          'UNK',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimProgram : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimProgram OFF;

             -- ====================================================================================================
             -- DimScheduleType
             -- ====================================================================================================
             DELETE FROM [dbo].[DimScheduleType]
             WHERE [ScheduleTypeKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimScheduleType : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimScheduleType ON;
             INSERT INTO [dbo].[DimScheduleType]
             ([ScheduleTypeKey],
              [ScheduleTypeID],
              [ScheduleTypeName],
              [ScheduleTypeMinimumDays],
              [ScheduleTypeMaximumDays],
              [ScheduleGroup],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1,
                          -1,
                          'Unknown Schedule Type',
                          0,
                          0,
                          'Unknown Schedule Group',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable Schedule Type',
                          0,
                          0,
                          'Not Applicable Schedule Group',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimScheduleType : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimScheduleType OFF;

             -- ====================================================================================================
             -- DimScheduleWeek
             -- ====================================================================================================
             --
             -- DimScheduleWeek seed row populated by spBING_EDW_Generate_DimScheduleWeek
             --

             -- ====================================================================================================
             -- DimSession
             -- ====================================================================================================
             DELETE FROM [dbo].[DimSession]
             WHERE [SessionKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimSession : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimSession ON;
             INSERT INTO [dbo].[DimSession]
             ([SessionKey],
              [SessionID],
              [SessionName],
              [SessionCategory],
              [SessionFTE],
              [SourceSystem],
              [EDWEffectiveDate],
              [EDWEndDate],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [Deleted]
             )
                    SELECT-1,
                          -1,
                          'Unknown Session',
                          'Unknown Session Category',
                          0,
                          'UNK',
                          '01/01/1900',
                          NULL,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable Session',
                          'N/A Session Category',
                          0,
                          'N/A',
                          '01/01/1900',
                          NULL,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimSession : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimSession OFF;

-- ====================================================================================================
             -- DimSpecialInfo
             -- ====================================================================================================
             DELETE FROM [dbo].[DimSpecialInfo]
             WHERE [SpecialInfoKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimSpecialInfo : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimSpecialInfo ON;
             INSERT INTO [dbo].[DimSpecialInfo]
             (SpecialInfoKey,
              SpecialInfoID,
              SpecialInfoTypeID,
              SpecialInfoTypeName,
              SpecialInfoAttribute1,
              SpecialInfoAttribute2,
              SpecialInfoAttribute3,
              SpecialInfoAttribute4,
              SpecialInfoAttribute5,
              SpecialInfoAttribute6,
              SpecialInfoAttribute7,
              SpecialInfoAttribute8,
              SpecialInfoAttribute9,
              SpecialInfoAttribute10,
              SpecialInfoAttribute11,
              SpecialInfoAttribute12,
              SpecialInfoAttribute13,
              SpecialInfoAttribute14,
              SpecialInfoAttribute15,
              SpecialInfoAttribute16,
              SpecialInfoAttribute17,
              SpecialInfoAttribute18,
              SpecialInfoAttribute19,
              SpecialInfoAttribute20,
              SpecialInfoAttribute21,
              SpecialInfoAttribute22,
              SpecialInfoAttribute23,
              SpecialInfoAttribute24,
              SpecialInfoAttribute25,
              SpecialInfoAttribute26,
              SpecialInfoAttribute27,
              SpecialInfoAttribute28,
              SpecialInfoAttribute29,
              SpecialInfoAttribute30,
              SpecialInfoSummaryFlag,
              SpecialInfoEnabledFlag,
              SpecialInfoCreatedUser,
              SpecialInfoCreatedDate,
              SpecialInfoModifiedUser,
              SpecialInfoModifiedDate,
              EDWCreatedDate,
              EDWModifiedDate
             )
                    SELECT-1,
                          -1,
                          -1,
                          'Unknown Special Info Type',
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          'Unknown Summary',
                          'Unknown Enabled',
                          -1,
                          '19000101',
                          -1,
                          '19000101',
                          GETDATE(),
                          GETDATE()
                    UNION
                    SELECT-2,
                          -2,
                          -2,
                          'Not Applicable Special Info Type',
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          'Unknown Summary',
                          'Unknown Enabled',
                          -1,
                          '19000101',
                          -1,
                          '19000101',
                          GETDATE(),
                          GETDATE();
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimSpecialInfo : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimSpecialInfo OFF;

             -- ====================================================================================================
             -- DimSponsor
             -- ====================================================================================================
             DELETE FROM [dbo].[DimSponsor]
             WHERE [SponsorKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimSponsor : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimSponsor ON;
             INSERT INTO [dbo].[DimSponsor]
             ([SponsorKey],
              [SponsorID],
              [SponsorFirstName],
              [SponsorMiddleName],
              [SponsorLastName],
              [SponsorFullName],
              [SponsorPhonePrimary],
              [SponsorPhoneSecondary],
              [SponsorPhoneTertiary],
              [SponsorEmailPrimary],
              [SponsorEmailSecondary],
              [SponsorAddress1],
              [SponsorAddress2],
              [SponsorCity],
              [SponsorState],
              [SponsorZIP],
              [SponsorStudentRelationship],
              [SponsorGender],
              [SponsorInternalEmployee],
              [SponsorStatus],
              [SponsorDoNotEmail],
              [SponsorLeadManagementID],
              [CSSCenterNumber],
              [CSSFamilyNumber],
              [SourceSystem],
              [EDWEffectiveDate],
              [EDWEndDate],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [Deleted]
             )
                    SELECT-1,
                          -1,
                          'Unknown Sponsor Name',
                          'Unknown Sponsor Name',
                          'Unknown Sponsor Name',
                          'Unknown Sponsor Name',
                          'Unknown Phone Number',
                          'Unknown Phone Number',
                          'Unknown Phone Number',
                          'Unknown Email',
                          'Unknown Email',
                          'Unknown Address',
                          'Unknown Address',
                          'Unknown City',
                          'XX',
                          'Unknown',
                          'Unknown Relationship',
                          'Unknown',
                          'Unknown Employee Type',
                          'Unknown Sponsor Status',
                          'Unknown Email Sponsor',
                          '-1',
                          '-1',
                          '-1',
                          'UNK',
                          '01/01/1900',
                          NULL,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable Sponsor Name',
                          'Not Applicable Sponsor Name',
                          'Not Applicable Sponsor Name',
                          'Not Applicable Sponsor Name',
                          'N/A Phone Number',
                          'N/A Phone Number',
                          'N/A Phone Number',
                          'Not Applicable Email',
                          'Not Applicable Email',
                          'Not Applicable Address',
                          'Not Applicable Address',
                          'Not Applicable City',
                          'XX',
                          'N/A',
                          'Not Applicable Relationship',
                          'N/A',
                          'Not Applicable Employee Type',
                          'Not Applicable Sponsor Status',
                          'N/A Email Sponsor',
                          '-2',
                          '-2',
                          '-2',
                          'N/A',
                          '01/01/1900',
                          NULL,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimSponsor : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimSponsor OFF;

             -- ====================================================================================================
             -- DimStudent
             -- ====================================================================================================
             DELETE FROM [dbo].[DimStudent]
             WHERE [StudentKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimStudent : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimStudent ON;
             INSERT INTO [dbo].[DimStudent]
             ([StudentKey],
              [StudentID],
              [StudentFirstName],
              [StudentMiddleName],
              [StudentLastName],
              [StudentSuffixName],
              [StudentPreferredName],
              [StudentFullName],
              [StudentBirthDate],
              [StudentGender],
              [StudentEthnicity],
              [StudentStatus],
              [StudentLifetimeSubsidy],
              [StudentCategory],
              [StudentSubCategory],
              [StudentPhone],
              [StudentAddress1],
              [StudentAddress2],
              [StudentCity],
              [StudentState],
              [StudentZIP],
              [StudentPrimaryLanguage],
              [StudentFirstEnrollmentDate],
              [StudentCareSelectStatus],
              [CSSCenterNumber],
              [CSSFamilyNumber],
              [CSSStudentNumber],
              [SourceSystem],
              [EDWEffectiveDate],
              [EDWEndDate],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [Deleted]
             )
                    SELECT-1,
                          -1,
                          'Unknown Student Name',
                          'Unknown Student Name',
                          'Unknown Student Name',
                          'Unknown Student Name',
                          'Unknown Student Name',
                          'Unknown Student Name',
                          '01/01/1900',
                          'Unknown',
                          'Unknown Ethnicity',
                          'Unknown Status',
                          'Unknown Lifetime Subsidy',
                          'Unknown Category',
                          'Unknown Subcategory',
                          'Unknown Phone Number',
                          'Unknown Address',
                          'Unknown Address',
                          'Unknown City',
                          'XX',
                          'Unknown',
                          'Unknown Primary Language',
                          NULL,
                          'Unknown',
                          '-1',
                          '-1',
                          '-1',
                          'UNK',
                          '01/01/1900',
                          NULL,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable Student Name',
                          'Not Applicable Student Name',
                          'Not Applicable Student Name',
                          'Not Applicable Student Name',
                          'Not Applicable Student Name',
                          'Not Applicable Student Name',
                          '01/01/1900',
                          'N/A',
                          'Not Applicable Ethnicity',
                          'Not Applicable Status',
                          'Not Applicable Lifetime Subsidy',
                          'Not Applicable Category',
                          'Not Applicable Subcategory',
                          'N/A Phone Number',
                          'Not Applicable Address',
                          'Not Applicable Address',
                          'Not Applicable City',
                          'XX',
                          'N/A',
                          'Not Applicable Primary Language',
                          NULL,
                          'N/A',
                          '-1',
                          '-1',
                          '-1',
                          'N/A',
                          '01/01/1900',
                          NULL,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimStudent : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimStudent OFF;

             -- ====================================================================================================
             -- DimTier
             -- ====================================================================================================
             DELETE FROM [dbo].[DimTier]
             WHERE [TierKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimTier : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimTier ON;
             INSERT INTO [dbo].[DimTier]
             ([TierKey],
              [TierID],
              [TierName],
              [TierFriendlyName],
              [TierAssignment],
              [TierBillingFrequency],
              [TierLabel],
              [TierShowToSponsor],
              [CSSTierNumber],
              [SourceSystem],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1,
                          -1,
                          'Unknown Tier Name',
                          'Unknown Tier Friendly Name',
                          'Unknown',
                          'Unknown Billing Frequency',
                          'Unknown Tier Label',
                          'Unknown Show To Sponsor',
                          -1,
                          'UNK',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable Tier Name',
                          'Not Applicable Tier Friendly Name',
                          'N/A',
                          'Not Applicable Billing Frequency',
                          'Not Applicable Tier Label',
                          'Not Applicable Show To Sponsor',
                          -2,
                          'N/A',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimTier : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimTier OFF;

             -- ====================================================================================================
             -- DimTransactionCode
             -- ====================================================================================================
             DELETE FROM [dbo].[DimTransactionCode]
             WHERE [TransactionCodeKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimTransactionCode : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimTransactionCode ON;
             INSERT INTO [dbo].[DimTransactionCode]
             ([TransactionCodeKey],
              [TransactionCode],
              [TransactionCodeName],
              [TransactionTypeCode],
              [TransactionTypeName],
              [TransactionCodeFTE],
              [EDWEffectiveDate],
              [EDWEndDate],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [Deleted]
             )
                    SELECT-1,
                          'Unknown',
                          'Unknown Transaction Code',
                          'XX',
                          'Unknown Transaction Type',
                          0,
                          '01/01/1900',
                          NULL,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          'N/A',
                          'Not Applicable Transaction Code',
                          'XX',
                          'Not Applicable Transaction Type',
                          0,
                          '01/01/1900',
                          NULL,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimTransactionCode : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimTransactionCode OFF;

             -- ====================================================================================================
             -- DimTuitionAssistanceProvider
             -- ====================================================================================================
             DELETE FROM [dbo].[DimTuitionAssistanceProvider]
             WHERE [TuitionAssistanceProviderKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimTuitionAssistanceProvider : Deleted seed row[s]';
             END;
--
             SET IDENTITY_INSERT dbo.DimTuitionAssistanceProvider ON;
             INSERT INTO [dbo].[DimTuitionAssistanceProvider]
             ([TuitionAssistanceProviderKey],
              [TuitionAssistanceProviderID],
              [TuitionAssistanceProviderName],
              [TuitionAssistanceProviderType],
              [TuitionAssistanceProviderAddress1],
              [TuitionAssistanceProviderAddress2],
              [TuitionAssistanceProviderCity],
              [TuitionAssistanceProviderState],
              [TuitionAssistanceProviderZIP],
              [TuitionAssistanceProviderContact],
              [TuitionAssistanceProviderProvidesSubsidy],
              [TuitionAssistanceProviderBackupCare],
              [TuitionAssistanceProviderCareSelectDiscount],
              [TuitionAssistanceProviderFirstContractDate],
              [CSSCenterNumber],
              [CSSCustomerCode],
              [SourceSystem],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1,
                          -1,
                          'Unknown Tuition Assistance Provider',
                          'Unknown Tuition Assistance Provider Type',
                          'Unknown Address',
                          'Unknown Address',
                          'Unknown City',
                          'XX',
                          'Unknown',
                          'Unknown Tuition Assistance Provider Contact',
                          'Unknown Subsidy Provider',
                          'Unknown Backup Care Provider',
                          'Unknown Care Select Discount ',
                          NULL,
                          '-1',
                          '-1',
                          'UNK',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable Tuition Assistance Provider',
                          'Not Applicable  Tuition Assistance Provider Type',
                          'Not Applicable  Address',
                          'Not Applicable  Address',
                          'Not Applicable  City',
                          'XX',
                          'N/A',
                          'Not Applicable  Tuition Assistance Provider Contact',
                          'Not Applicable  Subsidy Provider',
                          'Not Applicable  Backup Care Provider',
                          'Not Applicable  Care Select Discount ',
                          NULL,
                          '-2',
                          '-2',
                          'N/A',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimTuitionAssistanceProvider : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimTuitionAssistanceProvider OFF;

             -- ====================================================================================================
             -- DimWebCampaign
             -- ====================================================================================================
             DELETE FROM [dbo].[DimWebCampaign]
             WHERE [WebCampaignKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimWebCampaign : Deleted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimWebCampaign ON;
             INSERT INTO dbo.DimWebCampaign
             (WebCampaignKey,
              WebCampaignID,
              EDWCreatedDate,
              EDWModifiedDate
             )
                    SELECT-1,
                          'Unknown Web Campaign ID',
                          GETDATE(),
                          GETDATE()
                    UNION
                    SELECT-2,
                          'Not Applicable Web Campaign ID',
                          GETDATE(),
                          GETDATE();
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimWebCampaign : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimWebCampaign OFF;
			  -- ====================================================================================================
             -- DimComplianceItem
             -- ====================================================================================================
             DELETE FROM [dbo].[DimComplianceItem]
             WHERE [ComplianceItemKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimComplianceItem : Deleted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimComplianceItem ON;
             INSERT INTO dbo.DimComplianceItem
             ([ComplianceItemKey],
              [ComplianceItemID],
              [ComplianceItemName],
              [ComplianceItemDescription],
              [ComplianceItemEvaluationMethodCode],
              [ComplianceItemEvaluationMethodName],
              [ComplianceItemFlexAttribute1],
              [ComplianceItemFlexAttribute2],
              [ComplianceItemFlexAttribute3],
              [ComplianceItemFlexAttribute4],
              [ComplianceItemFlexAttribute5],
              [ComplianceItemCreatedDate],
              [ComplianceItemCreatedUser],
              [ComplianceItemModifiedDate],
              [ComplianceItemModifiedUser],
              [EDWCreatedDate],
              [EDWModifiedDate]
             )
                    SELECT-1,
                          -1,
                          'Unknown Compliance Item',
                          'Unknown Compliance Item',
                          -1,
                          'Unknown Evaluation Method',
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          '1/1/1900',
                          -1,
                          '1/1/1900',
                          -1,
                          GETDATE(),
                          GETDATE()
                    UNION
                    SELECT-2,
                          -2,
                          'Not Applicable Compliance Item',
                          'Not Applicable Compliance Item',
                          -2,
                          'Not Applicable Evaluation Method',
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          '1/1/1900',
                          -1,
                          '1/1/1900',
                          -1,
                          GETDATE(),
                          GETDATE();
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimComplianceItem : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimComplianceItem OFF;
			 -- ====================================================================================================
             -- DimARAgencyType
             -- ====================================================================================================

             DELETE FROM [dbo].[DimARAgencyType]
             WHERE [ARAgencyTypeKey] < 0;
             -- Keep a running total of the Deletes
             SET @DeleteCount = @DeleteCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimARAgencyType : Deleted seed row[s]';
             END;

--
             SET IDENTITY_INSERT dbo.DimARAgencyType ON;
             INSERT INTO [dbo].[DimARAgencyType]
             ([ARAgencyTypeKey],
              [ARAgencyTypeName],
              [ARType],
              [SourceSystem],
              [EDWCreatedDate],
              [EDWCreatedBy]
             )
                    SELECT-1, -- [ARAgencyTypeKey]
                          'Unknown Agency Type', -- [ARAgencyTypeName]
                          'Unknown AR Type', -- [ARType]
                          'UNK', -- [SourceSystem]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER -- [EDWCreatedBy]
                    UNION
                    SELECT-2, -- [ARAgencyTypeKey]
                          'Not Applicable Agency Type', -- [ARAgencyTypeName]
                          'Not Applicable AR Type', -- [ARType]
                          'N/A', -- [SourceSystem]
                          GETDATE(), -- [EDWCreatedDate]
                          SYSTEM_USER; -- [EDWCreatedBy]
             -- Keep a running total of the inserts
             SET @InsertCount = @InsertCount + @@ROWCOUNT;
             IF @@ROWCOUNT > 0
                 BEGIN
                     PRINT 'DimARAgencyType : Inserted seed row[s]';
             END;
             SET IDENTITY_INSERT dbo.DimARAgencyType OFF;
		   --
		   -- Commit the successful transaction 
		   --
             COMMIT TRANSACTION;
             --
             -- Write our successful run to the EDW AuditLog 
             --
             EXEC [dbo].[spEDWEndAuditLog]
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
	    --
	    -- Catch, and throw the error back to the calling procedure or client
	    --
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
             EXEC [dbo].[spEDWErrorAuditLog]
                  @AuditId = @AuditId;
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = 'Sub-procedure '+@ProcName+' - '+ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
GO