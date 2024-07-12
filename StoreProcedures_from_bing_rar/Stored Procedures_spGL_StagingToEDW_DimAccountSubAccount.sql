CREATE PROCEDURE [dbo].[spGL_StagingToEDW_DimAccountSubAccount]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingToEDW_DimAccountSubAccount
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the DimAccountSubAccount table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimOrganization, 
    --                                 and create any helper indexes
    --                         Step 3: Perform the Insert / Update (Merge) / deletes required 
    --                                 for this EDW table load			 
    --                         Step 4: Execute any automated tests associated with this EDW table load
    --                         Step 5: Output Source / Insert / Update / Delete counts to caller, 
    --                                 commit the transaction, and tidy-up
    --
    -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                         making numerous GETDATE() calls  
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --
    -- Usage:              EXEC dbo.spGL_StagingToEDW_DimAccountSubAccount @DebugMode = 1	
    -- 
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 10/17/17    sburke              BNG-610 - De-duplicate Account - SubAccount level columns to support SSAS 2017.  
    --                                     Also refactor DimAccount – SubAccount ETL load to use stored proc over Data Flow in SSIS
    -- 12/04/17    sburke              BNG-757 - Addition of new Account Type Unary operator
    --                                      columns to support the handling (reversing) of revenue
    --                                      accounts in SSAS. 
    -- 04/02/21    hhebbalu            BI-4501 Added Fieldpath column in the Temptable since the 
	--                                 procedure dbo.spGL_StagingTransform_DimAccountSubAccount			 
	--                                 is modified to return that column which is needed to populate DW_Mart AccountSubaccount
	--- 06/24/22   Aniket             BI-2161: UAT - Implement Placeholder Solution 1 - ASA CMS Revenue/FTE
    -- ================================================================================    
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimAccountSubAccount';
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
		 DECLARE @InfSourceCount INT= 0;
         DECLARE @InfInsertCount INT= 0;
         DECLARE @InfUpdateCount INT= 0;
         DECLARE @InfDeleteCount INT= 0;
		 
	    --
	    -- Merge statement action table variables
	    --
         DECLARE @tblMergeActions TABLE(MergeAction VARCHAR(20));
         DECLARE @tblDeleteActions TABLE(MergeAction VARCHAR(20));
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
		   --
		   -- S T E P   1.
		   --
		   -- Create temporary landing #table
		   --
		   -- ================================================================================

             CREATE TABLE #DimAccountSubAccountUpsert
             ([AccountSubaccountID]   VARCHAR(11) NOT NULL,
              [AccountSubaccountName] VARCHAR(500) NOT NULL,
              [AccountID]             VARCHAR(4) NOT NULL,
              [AccountName]           VARCHAR(250) NOT NULL,
              [SubaccountID]          VARCHAR(6) NOT NULL,
              [SubaccountName]        VARCHAR(250) NOT NULL,
              [ASATCOUnary]           INT NOT NULL,
              [ASATCOSort]            INT NOT NULL,
              [ASATCODepth]           INT NOT NULL,
              [ASATCOLevel1ID]        VARCHAR(11) NULL,
              [ASATCOLevel1Name]      VARCHAR(250) NULL,
              [ASATCOLevel1Unary]     INT NULL,
              [ASATCOLevel1Sort]      INT NULL,
              [ASATCOLevel2ID]        VARCHAR(11) NULL,
              [ASATCOLevel2Name]      VARCHAR(250) NULL,
              [ASATCOLevel2Unary]     INT NULL,
              [ASATCOLevel2Sort]      INT NULL,
              [ASATCOLevel3ID]        VARCHAR(11) NULL,
              [ASATCOLevel3Name]      VARCHAR(250) NULL,
              [ASATCOLevel3Unary]     INT NULL,
              [ASATCOLevel3Sort]      INT NULL,
              [ASATCOLevel4ID]        VARCHAR(11) NULL,
              [ASATCOLevel4Name]      VARCHAR(250) NULL,
              [ASATCOLevel4Unary]     INT NULL,
              [ASATCOLevel4Sort]      INT NULL,
              [ASATCOLevel5ID]        VARCHAR(11) NULL,
              [ASATCOLevel5Name]      VARCHAR(250) NULL,
              [ASATCOLevel5Unary]     INT NULL,
              [ASATCOLevel5Sort]      INT NULL,
              [ASATCOLevel6ID]        VARCHAR(11) NULL,
              [ASATCOLevel6Name]      VARCHAR(250) NULL,
              [ASATCOLevel6Unary]     INT NULL,
              [ASATCOLevel6Sort]      INT NULL,
              [ASATCOLevel7ID]        VARCHAR(11) NULL,
              [ASATCOLevel7Name]      VARCHAR(250) NULL,
              [ASATCOLevel7Unary]     INT NULL,
              [ASATCOLevel7Sort]      INT NULL,
              [ASATCOLevel8ID]        VARCHAR(11) NULL,
              [ASATCOLevel8Name]      VARCHAR(250) NULL,
              [ASATCOLevel8Unary]     INT NULL,
              [ASATCOLevel8Sort]      INT NULL,
              [ASATCOLevel9ID]        VARCHAR(11) NULL,
              [ASATCOLevel9Name]      VARCHAR(250) NULL,
              [ASATCOLevel9Unary]     INT NULL,
              [ASATCOLevel9Sort]      INT NULL,
              [ASATCOLevel10ID]       VARCHAR(11) NULL,
              [ASATCOLevel10Name]     VARCHAR(250) NULL,
              [ASATCOLevel10Unary]    INT NULL,
              [ASATCOLevel10Sort]     INT NULL,
              [ASATCOLevel11ID]       VARCHAR(11) NULL,
              [ASATCOLevel11Name]     VARCHAR(250) NULL,
              [ASATCOLevel11Unary]    INT NULL,
              [ASATCOLevel11Sort]     INT NULL,
              [ASATCOLevel12ID]       VARCHAR(11) NULL,
              [ASATCOLevel12Name]     VARCHAR(250) NULL,
              [ASATCOLevel12Unary]    INT NULL,
              [ASATCOLevel12Sort]     INT NULL,
              [ASAFieldUnary]         INT NOT NULL,
              [ASAFieldSort]          INT NOT NULL,
              [ASAFieldDepth]         INT NOT NULL,
              [ASAFieldLevel1ID]      VARCHAR(11) NULL,
              [ASAFieldLevel1Name]    VARCHAR(250) NULL,
              [ASAFieldLevel1Unary]   INT NULL,
              [ASAFieldLevel1Sort]    INT NULL,
              [ASAFieldLevel2ID]      VARCHAR(11) NULL,
              [ASAFieldLevel2Name]    VARCHAR(250) NULL,
              [ASAFieldLevel2Unary]   INT NULL,
              [ASAFieldLevel2Sort]    INT NULL,
              [ASAFieldLevel3ID]      VARCHAR(11) NULL,
              [ASAFieldLevel3Name]    VARCHAR(250) NULL,
              [ASAFieldLevel3Unary]   INT NULL,
              [ASAFieldLevel3Sort]    INT NULL,
              [ASAFieldLevel4ID]      VARCHAR(11) NULL,
              [ASAFieldLevel4Name]    VARCHAR(250) NULL,
              [ASAFieldLevel4Unary]   INT NULL,
              [ASAFieldLevel4Sort]    INT NULL,
              [ASAFieldLevel5ID]      VARCHAR(11) NULL,
              [ASAFieldLevel5Name]    VARCHAR(250) NULL,
              [ASAFieldLevel5Unary]   INT NULL,
              [ASAFieldLevel5Sort]    INT NULL,
              [ASAFieldLevel6ID]      VARCHAR(11) NULL,
              [ASAFieldLevel6Name]    VARCHAR(250) NULL,
              [ASAFieldLevel6Unary]   INT NULL,
              [ASAFieldLevel6Sort]    INT NULL,
              [ASAFieldLevel7ID]      VARCHAR(11) NULL,
              [ASAFieldLevel7Name]    VARCHAR(250) NULL,
              [ASAFieldLevel7Unary]   INT NULL,
              [ASAFieldLevel7Sort]    INT NULL,
              [ASAFieldLevel8ID]      VARCHAR(11) NULL,
              [ASAFieldLevel8Name]    VARCHAR(250) NULL,
              [ASAFieldLevel8Unary]   INT NULL,
              [ASAFieldLevel8Sort]    INT NULL,
              [ASAFieldLevel9ID]      VARCHAR(11) NULL,
              [ASAFieldLevel9Name]    VARCHAR(250) NULL,
              [ASAFieldLevel9Unary]   INT NULL,
              [ASAFieldLevel9Sort]    INT NULL,
              [ASAFieldLevel10ID]     VARCHAR(11) NULL,
              [ASAFieldLevel10Name]   VARCHAR(250) NULL,
              [ASAFieldLevel10Unary]  INT NULL,
              [ASAFieldLevel10Sort]   INT NULL,
              [ASAFieldLevel11ID]     VARCHAR(11) NULL,
              [ASAFieldLevel11Name]   VARCHAR(250) NULL,
              [ASAFieldLevel11Unary]  INT NULL,
              [ASAFieldLevel11Sort]   INT NULL,
              [ASAFieldLevel12ID]     VARCHAR(11) NULL,
              [ASAFieldLevel12Name]   VARCHAR(250) NULL,
              [ASAFieldLevel12Unary]  INT NULL,
              [ASAFieldLevel12Sort]   INT NULL,
              [ASAGAUnary]            INT NOT NULL,
              [ASAGASort]             INT NOT NULL,
              [ASAGADepth]            INT NOT NULL,
              [ASAGALevel1ID]         VARCHAR(11) NULL,
              [ASAGALevel1Name]       VARCHAR(250) NULL,
              [ASAGALevel1Unary]      INT NULL,
              [ASAGALevel1Sort]       INT NULL,
              [ASAGALevel2ID]         VARCHAR(11) NULL,
              [ASAGALevel2Name]       VARCHAR(250) NULL,
              [ASAGALevel2Unary]      INT NULL,
              [ASAGALevel2Sort]       INT NULL,
              [ASAGALevel3ID]         VARCHAR(11) NULL,
              [ASAGALevel3Name]       VARCHAR(250) NULL,
              [ASAGALevel3Unary]      INT NULL,
              [ASAGALevel3Sort]       INT NULL,
              [ASAGALevel4ID]         VARCHAR(11) NULL,
              [ASAGALevel4Name]       VARCHAR(250) NULL,
              [ASAGALevel4Unary]      INT NULL,
              [ASAGALevel4Sort]       INT NULL,
              [ASAGALevel5ID]         VARCHAR(11) NULL,
              [ASAGALevel5Name]       VARCHAR(250) NULL,
              [ASAGALevel5Unary]      INT NULL,
              [ASAGALevel5Sort]       INT NULL,
              [ASAGALevel6ID]         VARCHAR(11) NULL,
              [ASAGALevel6Name]       VARCHAR(250) NULL,
              [ASAGALevel6Unary]      INT NULL,
              [ASAGALevel6Sort]       INT NULL,
              [ASAGALevel7ID]         VARCHAR(11) NULL,
              [ASAGALevel7Name]       VARCHAR(250) NULL,
              [ASAGALevel7Unary]      INT NULL,
              [ASAGALevel7Sort]       INT NULL,
              [ASAGALevel8ID]         VARCHAR(11) NULL,
              [ASAGALevel8Name]       VARCHAR(250) NULL,
              [ASAGALevel8Unary]      INT NULL,
              [ASAGALevel8Sort]       INT NULL,
              [ASATuitionType]        VARCHAR(250) NOT NULL,
              [ASALaborType]          VARCHAR(250) NOT NULL,
              [ASAEBITDAAddbackFlag]  VARCHAR(250) NOT NULL,
              [AccountTypeCode]       VARCHAR(1) NULL,
              [AccountTypeName]       VARCHAR(50) NULL,
              [AccountTypeUnary]      INT NULL,
			  [FieldPath]             VARCHAR(1000) NULL,
              [EDWCreatedDate]        DATETIME2(7) NOT NULL,
              [EDWCreatedBy]          VARCHAR(50) NOT NULL,
              [EDWModifiedDate]       DATETIME2(7) NOT NULL,
              [EDWModifiedBy]         VARCHAR(50) NOT NULL,
			  [RowStatus]             CHAR(1) NULL
             );
          
		  CREATE TABLE #MissingDimAccountSubAccountUpsert
             ([AccountSubaccountID]   VARCHAR(11) NOT NULL,
              [AccountSubaccountName] VARCHAR(500) NOT NULL,
              [AccountID]             VARCHAR(4) NOT NULL,
              [AccountName]           VARCHAR(250) NOT NULL,
              [SubaccountID]          VARCHAR(6) NOT NULL,
              [SubaccountName]        VARCHAR(250) NOT NULL,
              [ASATCOUnary]           INT NOT NULL,
              [ASATCOSort]            INT NOT NULL,
              [ASATCODepth]           INT NOT NULL,
              [ASATCOLevel1ID]        VARCHAR(11) NULL,
              [ASATCOLevel1Name]      VARCHAR(250) NULL,
              [ASATCOLevel1Unary]     INT NULL,
              [ASATCOLevel1Sort]      INT NULL,
              [ASATCOLevel2ID]        VARCHAR(11) NULL,
              [ASATCOLevel2Name]      VARCHAR(250) NULL,
              [ASATCOLevel2Unary]     INT NULL,
              [ASATCOLevel2Sort]      INT NULL,
              [ASATCOLevel3ID]        VARCHAR(11) NULL,
              [ASATCOLevel3Name]      VARCHAR(250) NULL,
              [ASATCOLevel3Unary]     INT NULL,
              [ASATCOLevel3Sort]      INT NULL,
              [ASATCOLevel4ID]        VARCHAR(11) NULL,
              [ASATCOLevel4Name]      VARCHAR(250) NULL,
              [ASATCOLevel4Unary]     INT NULL,
              [ASATCOLevel4Sort]      INT NULL,
              [ASATCOLevel5ID]        VARCHAR(11) NULL,
              [ASATCOLevel5Name]      VARCHAR(250) NULL,
              [ASATCOLevel5Unary]     INT NULL,
              [ASATCOLevel5Sort]      INT NULL,
              [ASATCOLevel6ID]        VARCHAR(11) NULL,
              [ASATCOLevel6Name]      VARCHAR(250) NULL,
              [ASATCOLevel6Unary]     INT NULL,
              [ASATCOLevel6Sort]      INT NULL,
              [ASATCOLevel7ID]        VARCHAR(11) NULL,
              [ASATCOLevel7Name]      VARCHAR(250) NULL,
              [ASATCOLevel7Unary]     INT NULL,
              [ASATCOLevel7Sort]      INT NULL,
              [ASATCOLevel8ID]        VARCHAR(11) NULL,
              [ASATCOLevel8Name]      VARCHAR(250) NULL,
              [ASATCOLevel8Unary]     INT NULL,
              [ASATCOLevel8Sort]      INT NULL,
              [ASATCOLevel9ID]        VARCHAR(11) NULL,
              [ASATCOLevel9Name]      VARCHAR(250) NULL,
              [ASATCOLevel9Unary]     INT NULL,
              [ASATCOLevel9Sort]      INT NULL,
              [ASATCOLevel10ID]       VARCHAR(11) NULL,
              [ASATCOLevel10Name]     VARCHAR(250) NULL,
              [ASATCOLevel10Unary]    INT NULL,
              [ASATCOLevel10Sort]     INT NULL,
              [ASATCOLevel11ID]       VARCHAR(11) NULL,
              [ASATCOLevel11Name]     VARCHAR(250) NULL,
              [ASATCOLevel11Unary]    INT NULL,
              [ASATCOLevel11Sort]     INT NULL,
              [ASATCOLevel12ID]       VARCHAR(11) NULL,
              [ASATCOLevel12Name]     VARCHAR(250) NULL,
              [ASATCOLevel12Unary]    INT NULL,
              [ASATCOLevel12Sort]     INT NULL,
              [ASAFieldUnary]         INT NOT NULL,
              [ASAFieldSort]          INT NOT NULL,
              [ASAFieldDepth]         INT NOT NULL,
              [ASAFieldLevel1ID]      VARCHAR(11) NULL,
              [ASAFieldLevel1Name]    VARCHAR(250) NULL,
              [ASAFieldLevel1Unary]   INT NULL,
              [ASAFieldLevel1Sort]    INT NULL,
              [ASAFieldLevel2ID]      VARCHAR(11) NULL,
              [ASAFieldLevel2Name]    VARCHAR(250) NULL,
              [ASAFieldLevel2Unary]   INT NULL,
              [ASAFieldLevel2Sort]    INT NULL,
              [ASAFieldLevel3ID]      VARCHAR(11) NULL,
              [ASAFieldLevel3Name]    VARCHAR(250) NULL,
              [ASAFieldLevel3Unary]   INT NULL,
              [ASAFieldLevel3Sort]    INT NULL,
              [ASAFieldLevel4ID]      VARCHAR(11) NULL,
              [ASAFieldLevel4Name]    VARCHAR(250) NULL,
              [ASAFieldLevel4Unary]   INT NULL,
              [ASAFieldLevel4Sort]    INT NULL,
              [ASAFieldLevel5ID]      VARCHAR(11) NULL,
              [ASAFieldLevel5Name]    VARCHAR(250) NULL,
              [ASAFieldLevel5Unary]   INT NULL,
              [ASAFieldLevel5Sort]    INT NULL,
              [ASAFieldLevel6ID]      VARCHAR(11) NULL,
              [ASAFieldLevel6Name]    VARCHAR(250) NULL,
              [ASAFieldLevel6Unary]   INT NULL,
              [ASAFieldLevel6Sort]    INT NULL,
              [ASAFieldLevel7ID]      VARCHAR(11) NULL,
              [ASAFieldLevel7Name]    VARCHAR(250) NULL,
              [ASAFieldLevel7Unary]   INT NULL,
              [ASAFieldLevel7Sort]    INT NULL,
              [ASAFieldLevel8ID]      VARCHAR(11) NULL,
              [ASAFieldLevel8Name]    VARCHAR(250) NULL,
              [ASAFieldLevel8Unary]   INT NULL,
              [ASAFieldLevel8Sort]    INT NULL,
              [ASAFieldLevel9ID]      VARCHAR(11) NULL,
              [ASAFieldLevel9Name]    VARCHAR(250) NULL,
              [ASAFieldLevel9Unary]   INT NULL,
              [ASAFieldLevel9Sort]    INT NULL,
              [ASAFieldLevel10ID]     VARCHAR(11) NULL,
              [ASAFieldLevel10Name]   VARCHAR(250) NULL,
              [ASAFieldLevel10Unary]  INT NULL,
              [ASAFieldLevel10Sort]   INT NULL,
              [ASAFieldLevel11ID]     VARCHAR(11) NULL,
              [ASAFieldLevel11Name]   VARCHAR(250) NULL,
              [ASAFieldLevel11Unary]  INT NULL,
              [ASAFieldLevel11Sort]   INT NULL,
              [ASAFieldLevel12ID]     VARCHAR(11) NULL,
              [ASAFieldLevel12Name]   VARCHAR(250) NULL,
              [ASAFieldLevel12Unary]  INT NULL,
              [ASAFieldLevel12Sort]   INT NULL,
              [ASAGAUnary]            INT NOT NULL,
              [ASAGASort]             INT NOT NULL,
              [ASAGADepth]            INT NOT NULL,
              [ASAGALevel1ID]         VARCHAR(11) NULL,
              [ASAGALevel1Name]       VARCHAR(250) NULL,
              [ASAGALevel1Unary]      INT NULL,
              [ASAGALevel1Sort]       INT NULL,
              [ASAGALevel2ID]         VARCHAR(11) NULL,
              [ASAGALevel2Name]       VARCHAR(250) NULL,
              [ASAGALevel2Unary]      INT NULL,
              [ASAGALevel2Sort]       INT NULL,
              [ASAGALevel3ID]         VARCHAR(11) NULL,
              [ASAGALevel3Name]       VARCHAR(250) NULL,
              [ASAGALevel3Unary]      INT NULL,
              [ASAGALevel3Sort]       INT NULL,
              [ASAGALevel4ID]         VARCHAR(11) NULL,
              [ASAGALevel4Name]       VARCHAR(250) NULL,
              [ASAGALevel4Unary]      INT NULL,
              [ASAGALevel4Sort]       INT NULL,
              [ASAGALevel5ID]         VARCHAR(11) NULL,
              [ASAGALevel5Name]       VARCHAR(250) NULL,
              [ASAGALevel5Unary]      INT NULL,
              [ASAGALevel5Sort]       INT NULL,
              [ASAGALevel6ID]         VARCHAR(11) NULL,
              [ASAGALevel6Name]       VARCHAR(250) NULL,
              [ASAGALevel6Unary]      INT NULL,
              [ASAGALevel6Sort]       INT NULL,
              [ASAGALevel7ID]         VARCHAR(11) NULL,
              [ASAGALevel7Name]       VARCHAR(250) NULL,
              [ASAGALevel7Unary]      INT NULL,
              [ASAGALevel7Sort]       INT NULL,
              [ASAGALevel8ID]         VARCHAR(11) NULL,
              [ASAGALevel8Name]       VARCHAR(250) NULL,
              [ASAGALevel8Unary]      INT NULL,
              [ASAGALevel8Sort]       INT NULL,
              [ASATuitionType]        VARCHAR(250) NOT NULL,
              [ASALaborType]          VARCHAR(250) NOT NULL,
              [ASAEBITDAAddbackFlag]  VARCHAR(250) NOT NULL,
              [AccountTypeCode]       VARCHAR(1) NULL,
              [AccountTypeName]       VARCHAR(50) NULL,
              [AccountTypeUnary]      INT NULL,
			  [FieldPath]             VARCHAR(1000) NULL,
              [EDWCreatedDate]        DATETIME2(7) NOT NULL,
              [EDWCreatedBy]          VARCHAR(50) NOT NULL,
              [EDWModifiedDate]       DATETIME2(7) NOT NULL,
              [EDWModifiedBy]         VARCHAR(50) NOT NULL,
			  [RowStatus]             CHAR(1) NULL
             );

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimAccountSubAccountUpsert
             EXEC dbo.spGL_StagingTransform_DimAccountSubAccount
                  @EDWRunDateTime;
		   
		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimAccountSubAccountUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XPKDimAccountSubAccountUpsert ON #DimAccountSubAccountUpsert
             ([AccountSubaccountID] ASC
             );

		   -- ================================================================================
		   --
		   -- S T E P   3.
		   --
		   -- Perform the Insert / Update (Merge) / deletes required for this EDW table load
		   --
		   -- ================================================================================
		   
		   --
		   -- Perform the Merge statement for insert / updates
		   --
             MERGE [BING_EDW].[dbo].[DimAccountSubaccount] T
             USING #DimAccountSubaccountUpsert S
             ON(S.AccountSubaccountID = T.AccountSubaccountID)
                 WHEN MATCHED AND S.AccountSubaccountName <> T.AccountSubaccountName
                                  OR S.AccountID <> T.AccountID
                                  OR S.AccountName <> T.AccountName
                                  OR S.SubaccountID <> T.SubaccountID
                                  OR S.SubaccountName <> T.SubaccountName
                                  OR S.ASATCOUnary <> T.ASATCOUnary
                                  OR S.ASATCOSort <> T.ASATCOSort
                                  OR S.ASATCODepth <> T.ASATCODepth
                                  OR S.ASATCOLevel1ID <> T.ASATCOLevel1ID
                                  OR S.ASATCOLevel1Name <> T.ASATCOLevel1Name
                                  OR S.ASATCOLevel1Unary <> T.ASATCOLevel1Unary
                                  OR S.ASATCOLevel1Sort <> T.ASATCOLevel1Sort
                                  OR S.ASATCOLevel2ID <> T.ASATCOLevel2ID
                                  OR S.ASATCOLevel2Name <> T.ASATCOLevel2Name
                                  OR S.ASATCOLevel2Unary <> T.ASATCOLevel2Unary
                                  OR S.ASATCOLevel2Sort <> T.ASATCOLevel2Sort
                                  OR S.ASATCOLevel3ID <> T.ASATCOLevel3ID
                                  OR S.ASATCOLevel3Name <> T.ASATCOLevel3Name
                                  OR S.ASATCOLevel3Unary <> T.ASATCOLevel3Unary
                                  OR S.ASATCOLevel3Sort <> T.ASATCOLevel3Sort
                                  OR S.ASATCOLevel4ID <> T.ASATCOLevel4ID
                                  OR S.ASATCOLevel4Name <> T.ASATCOLevel4Name
                                  OR S.ASATCOLevel4Unary <> T.ASATCOLevel4Unary
                                  OR S.ASATCOLevel4Sort <> T.ASATCOLevel4Sort
                                  OR S.ASATCOLevel5ID <> T.ASATCOLevel5ID
                                  OR S.ASATCOLevel5Name <> T.ASATCOLevel5Name
                                  OR S.ASATCOLevel5Unary <> T.ASATCOLevel5Unary
                                  OR S.ASATCOLevel5Sort <> T.ASATCOLevel5Sort
                                  OR S.ASATCOLevel6ID <> T.ASATCOLevel6ID
                                  OR S.ASATCOLevel6Name <> T.ASATCOLevel6Name
                                  OR S.ASATCOLevel6Unary <> T.ASATCOLevel6Unary
                                  OR S.ASATCOLevel6Sort <> T.ASATCOLevel6Sort
                                  OR S.ASATCOLevel7ID <> T.ASATCOLevel7ID
                                  OR S.ASATCOLevel7Name <> T.ASATCOLevel7Name
                                  OR S.ASATCOLevel7Unary <> T.ASATCOLevel7Unary
                                  OR S.ASATCOLevel7Sort <> T.ASATCOLevel7Sort
                                  OR S.ASATCOLevel8ID <> T.ASATCOLevel8ID
                                  OR S.ASATCOLevel8Name <> T.ASATCOLevel8Name
                                  OR S.ASATCOLevel8Unary <> T.ASATCOLevel8Unary
                                  OR S.ASATCOLevel8Sort <> T.ASATCOLevel8Sort
                                  OR S.ASATCOLevel9ID <> T.ASATCOLevel9ID
                                  OR S.ASATCOLevel9Name <> T.ASATCOLevel9Name
                                  OR S.ASATCOLevel9Unary <> T.ASATCOLevel9Unary
                                  OR S.ASATCOLevel9Sort <> T.ASATCOLevel9Sort
                                  OR S.ASATCOLevel10ID <> T.ASATCOLevel10ID
                                  OR S.ASATCOLevel10Name <> T.ASATCOLevel10Name
                                  OR S.ASATCOLevel10Unary <> T.ASATCOLevel10Unary
                                  OR S.ASATCOLevel10Sort <> T.ASATCOLevel10Sort
                                  OR S.ASATCOLevel11ID <> T.ASATCOLevel11ID
                                  OR S.ASATCOLevel11Name <> T.ASATCOLevel11Name
                                  OR S.ASATCOLevel11Unary <> T.ASATCOLevel11Unary
                                  OR S.ASATCOLevel11Sort <> T.ASATCOLevel11Sort
                                  OR S.ASATCOLevel12ID <> T.ASATCOLevel12ID
                                  OR S.ASATCOLevel12Name <> T.ASATCOLevel12Name
                                  OR S.ASATCOLevel12Unary <> T.ASATCOLevel12Unary
                                  OR S.ASATCOLevel12Sort <> T.ASATCOLevel12Sort
                                  OR S.ASAFieldUnary <> T.ASAFieldUnary
                                  OR S.ASAFieldSort <> T.ASAFieldSort
                                  OR S.ASAFieldDepth <> T.ASAFieldDepth
                                  OR S.ASAFieldLevel1ID <> T.ASAFieldLevel1ID
                                  OR S.ASAFieldLevel1Name <> T.ASAFieldLevel1Name
                                  OR S.ASAFieldLevel1Unary <> T.ASAFieldLevel1Unary
                                  OR S.ASAFieldLevel1Sort <> T.ASAFieldLevel1Sort
                                  OR S.ASAFieldLevel2ID <> T.ASAFieldLevel2ID
                                  OR S.ASAFieldLevel2Name <> T.ASAFieldLevel2Name
                                  OR S.ASAFieldLevel2Unary <> T.ASAFieldLevel2Unary
                                  OR S.ASAFieldLevel2Sort <> T.ASAFieldLevel2Sort
                                  OR S.ASAFieldLevel3ID <> T.ASAFieldLevel3ID
                                  OR S.ASAFieldLevel3Name <> T.ASAFieldLevel3Name
                                  OR S.ASAFieldLevel3Unary <> T.ASAFieldLevel3Unary
                                  OR S.ASAFieldLevel3Sort <> T.ASAFieldLevel3Sort
                                  OR S.ASAFieldLevel4ID <> T.ASAFieldLevel4ID
                                  OR S.ASAFieldLevel4Name <> T.ASAFieldLevel4Name
                                  OR S.ASAFieldLevel4Unary <> T.ASAFieldLevel4Unary
                                  OR S.ASAFieldLevel4Sort <> T.ASAFieldLevel4Sort
                                  OR S.ASAFieldLevel5ID <> T.ASAFieldLevel5ID
                                  OR S.ASAFieldLevel5Name <> T.ASAFieldLevel5Name
                                  OR S.ASAFieldLevel5Unary <> T.ASAFieldLevel5Unary
                                  OR S.ASAFieldLevel5Sort <> T.ASAFieldLevel5Sort
                                  OR S.ASAFieldLevel6ID <> T.ASAFieldLevel6ID
                                  OR S.ASAFieldLevel6Name <> T.ASAFieldLevel6Name
                                  OR S.ASAFieldLevel6Unary <> T.ASAFieldLevel6Unary
                                  OR S.ASAFieldLevel6Sort <> T.ASAFieldLevel6Sort
                                  OR S.ASAFieldLevel7ID <> T.ASAFieldLevel7ID
                                  OR S.ASAFieldLevel7Name <> T.ASAFieldLevel7Name
                                  OR S.ASAFieldLevel7Unary <> T.ASAFieldLevel7Unary
                                  OR S.ASAFieldLevel7Sort <> T.ASAFieldLevel7Sort
                                  OR S.ASAFieldLevel8ID <> T.ASAFieldLevel8ID
                                  OR S.ASAFieldLevel8Name <> T.ASAFieldLevel8Name
                                  OR S.ASAFieldLevel8Unary <> T.ASAFieldLevel8Unary
                                  OR S.ASAFieldLevel8Sort <> T.ASAFieldLevel8Sort
                                  OR S.ASAFieldLevel9ID <> T.ASAFieldLevel9ID
                                  OR S.ASAFieldLevel9Name <> T.ASAFieldLevel9Name
                                  OR S.ASAFieldLevel9Unary <> T.ASAFieldLevel9Unary
                                  OR S.ASAFieldLevel9Sort <> T.ASAFieldLevel9Sort
                                  OR S.ASAFieldLevel10ID <> T.ASAFieldLevel10ID
                                  OR S.ASAFieldLevel10Name <> T.ASAFieldLevel10Name
                                  OR S.ASAFieldLevel10Unary <> T.ASAFieldLevel10Unary
                                  OR S.ASAFieldLevel10Sort <> T.ASAFieldLevel10Sort
                                  OR S.ASAFieldLevel11ID <> T.ASAFieldLevel11ID
                                  OR S.ASAFieldLevel11Name <> T.ASAFieldLevel11Name
                                  OR S.ASAFieldLevel11Unary <> T.ASAFieldLevel11Unary
                                  OR S.ASAFieldLevel11Sort <> T.ASAFieldLevel11Sort
                                  OR S.ASAFieldLevel12ID <> T.ASAFieldLevel12ID
                                  OR S.ASAFieldLevel12Name <> T.ASAFieldLevel12Name
                                  OR S.ASAFieldLevel12Unary <> T.ASAFieldLevel12Unary
                                  OR S.ASAFieldLevel12Sort <> T.ASAFieldLevel12Sort
                                  OR S.ASAGAUnary <> T.ASAGAUnary
                                  OR S.ASAGASort <> T.ASAGASort
                                  OR S.ASAGADepth <> T.ASAGADepth
                                  OR S.ASAGALevel1ID <> T.ASAGALevel1ID
                                  OR S.ASAGALevel1Name <> T.ASAGALevel1Name
                                  OR S.ASAGALevel1Unary <> T.ASAGALevel1Unary
                                  OR S.ASAGALevel1Sort <> T.ASAGALevel1Sort
                                  OR S.ASAGALevel2ID <> T.ASAGALevel2ID
                                  OR S.ASAGALevel2Name <> T.ASAGALevel2Name
                                  OR S.ASAGALevel2Unary <> T.ASAGALevel2Unary
                                  OR S.ASAGALevel2Sort <> T.ASAGALevel2Sort
                                  OR S.ASAGALevel3ID <> T.ASAGALevel3ID
                                  OR S.ASAGALevel3Name <> T.ASAGALevel3Name
                                  OR S.ASAGALevel3Unary <> T.ASAGALevel3Unary
                                  OR S.ASAGALevel3Sort <> T.ASAGALevel3Sort
                                  OR S.ASAGALevel4ID <> T.ASAGALevel4ID
                                  OR S.ASAGALevel4Name <> T.ASAGALevel4Name
                                  OR S.ASAGALevel4Unary <> T.ASAGALevel4Unary
                                  OR S.ASAGALevel4Sort <> T.ASAGALevel4Sort
                                  OR S.ASAGALevel5ID <> T.ASAGALevel5ID
                                  OR S.ASAGALevel5Name <> T.ASAGALevel5Name
                                  OR S.ASAGALevel5Unary <> T.ASAGALevel5Unary
                                  OR S.ASAGALevel5Sort <> T.ASAGALevel5Sort
                                  OR S.ASAGALevel6ID <> T.ASAGALevel6ID
                                  OR S.ASAGALevel6Name <> T.ASAGALevel6Name
                                  OR S.ASAGALevel6Unary <> T.ASAGALevel6Unary
                                  OR S.ASAGALevel6Sort <> T.ASAGALevel6Sort
                                  OR S.ASAGALevel7ID <> T.ASAGALevel7ID
                                  OR S.ASAGALevel7Name <> T.ASAGALevel7Name
                                  OR S.ASAGALevel7Unary <> T.ASAGALevel7Unary
                                  OR S.ASAGALevel7Sort <> T.ASAGALevel7Sort
                                  OR S.ASAGALevel8ID <> T.ASAGALevel8ID
                                  OR S.ASAGALevel8Name <> T.ASAGALevel8Name
                                  OR S.ASAGALevel8Unary <> T.ASAGALevel8Unary
                                  OR S.ASAGALevel8Sort <> T.ASAGALevel8Sort
                                  OR S.ASATuitionType <> T.ASATuitionType
                                  OR S.ASALaborType <> T.ASALaborType
                                  OR S.ASAEBITDAAddbackFlag <> T.ASAEBITDAAddbackFlag
                                  OR S.AccountTypeCode <> T.AccountTypeCode
                                  OR S.AccountTypeName <> T.AccountTypeName
                                  OR S.AccountTypeUnary <> T.AccountTypeUnary
								  OR S.RowStatus<>T.RowStatus
                 THEN UPDATE SET
                                 T.AccountSubaccountName = S.AccountSubaccountName,
                                 T.AccountID = S.AccountID,
                                 T.AccountName = S.AccountName,
                                 T.SubaccountID = S.SubaccountID,
                                 T.SubaccountName = S.SubaccountName,
                                 T.ASATCOUnary = S.ASATCOUnary,
                                 T.ASATCOSort = S.ASATCOSort,
                                 T.ASATCODepth = S.ASATCODepth,
                                 T.ASATCOLevel1ID = S.ASATCOLevel1ID,
                                 T.ASATCOLevel1Name = S.ASATCOLevel1Name,
                                 T.ASATCOLevel1Unary = S.ASATCOLevel1Unary,
                                 T.ASATCOLevel1Sort = S.ASATCOLevel1Sort,
                                 T.ASATCOLevel2ID = S.ASATCOLevel2ID,
                                 T.ASATCOLevel2Name = S.ASATCOLevel2Name,
                                 T.ASATCOLevel2Unary = S.ASATCOLevel2Unary,
                                 T.ASATCOLevel2Sort = S.ASATCOLevel2Sort,
                                 T.ASATCOLevel3ID = S.ASATCOLevel3ID,
                                 T.ASATCOLevel3Name = S.ASATCOLevel3Name,
                                 T.ASATCOLevel3Unary = S.ASATCOLevel3Unary,
                                 T.ASATCOLevel3Sort = S.ASATCOLevel3Sort,
                                 T.ASATCOLevel4ID = S.ASATCOLevel4ID,
                                 T.ASATCOLevel4Name = S.ASATCOLevel4Name,
                                 T.ASATCOLevel4Unary = S.ASATCOLevel4Unary,
                                 T.ASATCOLevel4Sort = S.ASATCOLevel4Sort,
                                 T.ASATCOLevel5ID = S.ASATCOLevel5ID,
                                 T.ASATCOLevel5Name = S.ASATCOLevel5Name,
                                 T.ASATCOLevel5Unary = S.ASATCOLevel5Unary,
                                 T.ASATCOLevel5Sort = S.ASATCOLevel5Sort,
                                 T.ASATCOLevel6ID = S.ASATCOLevel6ID,
                                 T.ASATCOLevel6Name = S.ASATCOLevel6Name,
                                 T.ASATCOLevel6Unary = S.ASATCOLevel6Unary,
                                 T.ASATCOLevel6Sort = S.ASATCOLevel6Sort,
                                 T.ASATCOLevel7ID = S.ASATCOLevel7ID,
                                 T.ASATCOLevel7Name = S.ASATCOLevel7Name,
                                 T.ASATCOLevel7Unary = S.ASATCOLevel7Unary,
                                 T.ASATCOLevel7Sort = S.ASATCOLevel7Sort,
                                 T.ASATCOLevel8ID = S.ASATCOLevel8ID,
                                 T.ASATCOLevel8Name = S.ASATCOLevel8Name,
                                 T.ASATCOLevel8Unary = S.ASATCOLevel8Unary,
                                 T.ASATCOLevel8Sort = S.ASATCOLevel8Sort,
                                 T.ASATCOLevel9ID = S.ASATCOLevel9ID,
                                 T.ASATCOLevel9Name = S.ASATCOLevel9Name,
                                 T.ASATCOLevel9Unary = S.ASATCOLevel9Unary,
                                 T.ASATCOLevel9Sort = S.ASATCOLevel9Sort,
                                 T.ASATCOLevel10ID = S.ASATCOLevel10ID,
                                 T.ASATCOLevel10Name = S.ASATCOLevel10Name,
                                 T.ASATCOLevel10Unary = S.ASATCOLevel10Unary,
                                 T.ASATCOLevel10Sort = S.ASATCOLevel10Sort,
                                 T.ASATCOLevel11ID = S.ASATCOLevel11ID,
                                 T.ASATCOLevel11Name = S.ASATCOLevel11Name,
                                 T.ASATCOLevel11Unary = S.ASATCOLevel11Unary,
                                 T.ASATCOLevel11Sort = S.ASATCOLevel11Sort,
                                 T.ASATCOLevel12ID = S.ASATCOLevel12ID,
                                 T.ASATCOLevel12Name = S.ASATCOLevel12Name,
                                 T.ASATCOLevel12Unary = S.ASATCOLevel12Unary,
                                 T.ASATCOLevel12Sort = S.ASATCOLevel12Sort,
                                 T.ASAFieldUnary = S.ASAFieldUnary,
                                 T.ASAFieldSort = S.ASAFieldSort,
                                 T.ASAFieldDepth = S.ASAFieldDepth,
                                 T.ASAFieldLevel1ID = S.ASAFieldLevel1ID,
                                 T.ASAFieldLevel1Name = S.ASAFieldLevel1Name,
                                 T.ASAFieldLevel1Unary = S.ASAFieldLevel1Unary,
                                 T.ASAFieldLevel1Sort = S.ASAFieldLevel1Sort,
                                 T.ASAFieldLevel2ID = S.ASAFieldLevel2ID,
                                 T.ASAFieldLevel2Name = S.ASAFieldLevel2Name,
                                 T.ASAFieldLevel2Unary = S.ASAFieldLevel2Unary,
                                 T.ASAFieldLevel2Sort = S.ASAFieldLevel2Sort,
                                 T.ASAFieldLevel3ID = S.ASAFieldLevel3ID,
                                 T.ASAFieldLevel3Name = S.ASAFieldLevel3Name,
                                 T.ASAFieldLevel3Unary = S.ASAFieldLevel3Unary,
                                 T.ASAFieldLevel3Sort = S.ASAFieldLevel3Sort,
                                 T.ASAFieldLevel4ID = S.ASAFieldLevel4ID,
                                 T.ASAFieldLevel4Name = S.ASAFieldLevel4Name,
                                 T.ASAFieldLevel4Unary = S.ASAFieldLevel4Unary,
                                 T.ASAFieldLevel4Sort = S.ASAFieldLevel4Sort,
                                 T.ASAFieldLevel5ID = S.ASAFieldLevel5ID,
                                 T.ASAFieldLevel5Name = S.ASAFieldLevel5Name,
                                 T.ASAFieldLevel5Unary = S.ASAFieldLevel5Unary,
                                 T.ASAFieldLevel5Sort = S.ASAFieldLevel5Sort,
                                 T.ASAFieldLevel6ID = S.ASAFieldLevel6ID,
                                 T.ASAFieldLevel6Name = S.ASAFieldLevel6Name,
                                 T.ASAFieldLevel6Unary = S.ASAFieldLevel6Unary,
                                 T.ASAFieldLevel6Sort = S.ASAFieldLevel6Sort,
                                 T.ASAFieldLevel7ID = S.ASAFieldLevel7ID,
                                 T.ASAFieldLevel7Name = S.ASAFieldLevel7Name,
                                 T.ASAFieldLevel7Unary = S.ASAFieldLevel7Unary,
                                 T.ASAFieldLevel7Sort = S.ASAFieldLevel7Sort,
                                 T.ASAFieldLevel8ID = S.ASAFieldLevel8ID,
                                 T.ASAFieldLevel8Name = S.ASAFieldLevel8Name,
                                 T.ASAFieldLevel8Unary = S.ASAFieldLevel8Unary,
                                 T.ASAFieldLevel8Sort = S.ASAFieldLevel8Sort,
                                 T.ASAFieldLevel9ID = S.ASAFieldLevel9ID,
                                 T.ASAFieldLevel9Name = S.ASAFieldLevel9Name,
                                 T.ASAFieldLevel9Unary = S.ASAFieldLevel9Unary,
                                 T.ASAFieldLevel9Sort = S.ASAFieldLevel9Sort,
                                 T.ASAFieldLevel10ID = S.ASAFieldLevel10ID,
                                 T.ASAFieldLevel10Name = S.ASAFieldLevel10Name,
                                 T.ASAFieldLevel10Unary = S.ASAFieldLevel10Unary,
                                 T.ASAFieldLevel10Sort = S.ASAFieldLevel10Sort,
                                 T.ASAFieldLevel11ID = S.ASAFieldLevel11ID,
                                 T.ASAFieldLevel11Name = S.ASAFieldLevel11Name,
                                 T.ASAFieldLevel11Unary = S.ASAFieldLevel11Unary,
                                 T.ASAFieldLevel11Sort = S.ASAFieldLevel11Sort,
                                 T.ASAFieldLevel12ID = S.ASAFieldLevel12ID,
                                 T.ASAFieldLevel12Name = S.ASAFieldLevel12Name,
                                 T.ASAFieldLevel12Unary = S.ASAFieldLevel12Unary,
                                 T.ASAFieldLevel12Sort = S.ASAFieldLevel12Sort,
                                 T.ASAGAUnary = S.ASAGAUnary,
                                 T.ASAGASort = S.ASAGASort,
                                 T.ASAGADepth = S.ASAGADepth,
                                 T.ASAGALevel1ID = S.ASAGALevel1ID,
                                 T.ASAGALevel1Name = S.ASAGALevel1Name,
                                 T.ASAGALevel1Unary = S.ASAGALevel1Unary,
                                 T.ASAGALevel1Sort = S.ASAGALevel1Sort,
                                 T.ASAGALevel2ID = S.ASAGALevel2ID,
                                 T.ASAGALevel2Name = S.ASAGALevel2Name,
                                 T.ASAGALevel2Unary = S.ASAGALevel2Unary,
                                 T.ASAGALevel2Sort = S.ASAGALevel2Sort,
                                 T.ASAGALevel3ID = S.ASAGALevel3ID,
                                 T.ASAGALevel3Name = S.ASAGALevel3Name,
                                 T.ASAGALevel3Unary = S.ASAGALevel3Unary,
                                 T.ASAGALevel3Sort = S.ASAGALevel3Sort,
                                 T.ASAGALevel4ID = S.ASAGALevel4ID,
                                 T.ASAGALevel4Name = S.ASAGALevel4Name,
                                 T.ASAGALevel4Unary = S.ASAGALevel4Unary,
                                 T.ASAGALevel4Sort = S.ASAGALevel4Sort,
                                 T.ASAGALevel5ID = S.ASAGALevel5ID,
                                 T.ASAGALevel5Name = S.ASAGALevel5Name,
                                 T.ASAGALevel5Unary = S.ASAGALevel5Unary,
                                 T.ASAGALevel5Sort = S.ASAGALevel5Sort,
                                 T.ASAGALevel6ID = S.ASAGALevel6ID,
                                 T.ASAGALevel6Name = S.ASAGALevel6Name,
                                 T.ASAGALevel6Unary = S.ASAGALevel6Unary,
                                 T.ASAGALevel6Sort = S.ASAGALevel6Sort,
                                 T.ASAGALevel7ID = S.ASAGALevel7ID,
                                 T.ASAGALevel7Name = S.ASAGALevel7Name,
                                 T.ASAGALevel7Unary = S.ASAGALevel7Unary,
                                 T.ASAGALevel7Sort = S.ASAGALevel7Sort,
                                 T.ASAGALevel8ID = S.ASAGALevel8ID,
                                 T.ASAGALevel8Name = S.ASAGALevel8Name,
                                 T.ASAGALevel8Unary = S.ASAGALevel8Unary,
                                 T.ASAGALevel8Sort = S.ASAGALevel8Sort,
                                 T.ASATuitionType = S.ASATuitionType,
                                 T.ASALaborType = S.ASALaborType,
                                 T.ASAEBITDAAddbackFlag = S.ASAEBITDAAddbackFlag,
                                 T.AccountTypeCode = S.AccountTypeCode,
                                 T.AccountTypeName = S.AccountTypeName,
                                 T.AccountTypeUnary = S.AccountTypeUnary,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWCreatedBy = S.EDWCreatedBy,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.EDWModifiedBy = S.EDWModifiedBy,
								 T.RowStatus=S.RowStatus
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(AccountSubaccountID,
                          AccountSubaccountName,
                          AccountID,
                          AccountName,
                          SubaccountID,
                          SubaccountName,
                          ASATCOUnary,
                          ASATCOSort,
                          ASATCODepth,
                          ASATCOLevel1ID,
                          ASATCOLevel1Name,
                          ASATCOLevel1Unary,
                          ASATCOLevel1Sort,
                          ASATCOLevel2ID,
                          ASATCOLevel2Name,
                          ASATCOLevel2Unary,
                          ASATCOLevel2Sort,
                          ASATCOLevel3ID,
                          ASATCOLevel3Name,
                          ASATCOLevel3Unary,
                          ASATCOLevel3Sort,
                          ASATCOLevel4ID,
                          ASATCOLevel4Name,
                          ASATCOLevel4Unary,
                          ASATCOLevel4Sort,
                          ASATCOLevel5ID,
                          ASATCOLevel5Name,
                          ASATCOLevel5Unary,
                          ASATCOLevel5Sort,
                          ASATCOLevel6ID,
                          ASATCOLevel6Name,
                          ASATCOLevel6Unary,
                          ASATCOLevel6Sort,
                          ASATCOLevel7ID,
                          ASATCOLevel7Name,
                          ASATCOLevel7Unary,
                          ASATCOLevel7Sort,
                          ASATCOLevel8ID,
                          ASATCOLevel8Name,
                          ASATCOLevel8Unary,
                          ASATCOLevel8Sort,
                          ASATCOLevel9ID,
                          ASATCOLevel9Name,
                          ASATCOLevel9Unary,
                          ASATCOLevel9Sort,
                          ASATCOLevel10ID,
                          ASATCOLevel10Name,
                          ASATCOLevel10Unary,
                          ASATCOLevel10Sort,
                          ASATCOLevel11ID,
                          ASATCOLevel11Name,
                          ASATCOLevel11Unary,
                          ASATCOLevel11Sort,
                          ASATCOLevel12ID,
                          ASATCOLevel12Name,
                          ASATCOLevel12Unary,
                          ASATCOLevel12Sort,
                          ASAFieldUnary,
                          ASAFieldSort,
                          ASAFieldDepth,
                          ASAFieldLevel1ID,
                          ASAFieldLevel1Name,
                          ASAFieldLevel1Unary,
                          ASAFieldLevel1Sort,
                          ASAFieldLevel2ID,
                          ASAFieldLevel2Name,
                          ASAFieldLevel2Unary,
                          ASAFieldLevel2Sort,
                          ASAFieldLevel3ID,
                          ASAFieldLevel3Name,
                          ASAFieldLevel3Unary,
                          ASAFieldLevel3Sort,
                          ASAFieldLevel4ID,
                          ASAFieldLevel4Name,
                          ASAFieldLevel4Unary,
                          ASAFieldLevel4Sort,
                          ASAFieldLevel5ID,
                          ASAFieldLevel5Name,
                          ASAFieldLevel5Unary,
                          ASAFieldLevel5Sort,
                          ASAFieldLevel6ID,
                          ASAFieldLevel6Name,
                          ASAFieldLevel6Unary,
                          ASAFieldLevel6Sort,
                          ASAFieldLevel7ID,
                          ASAFieldLevel7Name,
                          ASAFieldLevel7Unary,
                          ASAFieldLevel7Sort,
                          ASAFieldLevel8ID,
                          ASAFieldLevel8Name,
                          ASAFieldLevel8Unary,
                          ASAFieldLevel8Sort,
                          ASAFieldLevel9ID,
                          ASAFieldLevel9Name,
                          ASAFieldLevel9Unary,
                          ASAFieldLevel9Sort,
                          ASAFieldLevel10ID,
                          ASAFieldLevel10Name,
                          ASAFieldLevel10Unary,
                          ASAFieldLevel10Sort,
                          ASAFieldLevel11ID,
                          ASAFieldLevel11Name,
                          ASAFieldLevel11Unary,
                          ASAFieldLevel11Sort,
                          ASAFieldLevel12ID,
                          ASAFieldLevel12Name,
                          ASAFieldLevel12Unary,
                          ASAFieldLevel12Sort,
                          ASAGAUnary,
                          ASAGASort,
                          ASAGADepth,
                          ASAGALevel1ID,
                          ASAGALevel1Name,
                          ASAGALevel1Unary,
                          ASAGALevel1Sort,
                          ASAGALevel2ID,
                          ASAGALevel2Name,
                          ASAGALevel2Unary,
                          ASAGALevel2Sort,
                          ASAGALevel3ID,
                          ASAGALevel3Name,
                          ASAGALevel3Unary,
                          ASAGALevel3Sort,
                          ASAGALevel4ID,
                          ASAGALevel4Name,
                          ASAGALevel4Unary,
                          ASAGALevel4Sort,
                          ASAGALevel5ID,
                          ASAGALevel5Name,
                          ASAGALevel5Unary,
                          ASAGALevel5Sort,
                          ASAGALevel6ID,
                          ASAGALevel6Name,
                          ASAGALevel6Unary,
                          ASAGALevel6Sort,
                          ASAGALevel7ID,
                          ASAGALevel7Name,
                          ASAGALevel7Unary,
                          ASAGALevel7Sort,
                          ASAGALevel8ID,
                          ASAGALevel8Name,
                          ASAGALevel8Unary,
                          ASAGALevel8Sort,
                          ASATuitionType,
                          ASALaborType,
                          ASAEBITDAAddbackFlag,
                          AccountTypeCode,
                          AccountTypeName,
                          AccountTypeUnary,
                          EDWCreatedDate,
                          EDWCreatedBy,
                          EDWModifiedDate,
                          EDWModifiedBy,
						  RowStatus)
                   VALUES
             (AccountSubaccountID,
              AccountSubaccountName,
              AccountID,
              AccountName,
              SubaccountID,
              SubaccountName,
              ASATCOUnary,
              ASATCOSort,
              ASATCODepth,
              ASATCOLevel1ID,
              ASATCOLevel1Name,
              ASATCOLevel1Unary,
              ASATCOLevel1Sort,
              ASATCOLevel2ID,
              ASATCOLevel2Name,
              ASATCOLevel2Unary,
              ASATCOLevel2Sort,
              ASATCOLevel3ID,
              ASATCOLevel3Name,
              ASATCOLevel3Unary,
              ASATCOLevel3Sort,
              ASATCOLevel4ID,
              ASATCOLevel4Name,
              ASATCOLevel4Unary,
              ASATCOLevel4Sort,
              ASATCOLevel5ID,
              ASATCOLevel5Name,
              ASATCOLevel5Unary,
              ASATCOLevel5Sort,
              ASATCOLevel6ID,
              ASATCOLevel6Name,
              ASATCOLevel6Unary,
              ASATCOLevel6Sort,
              ASATCOLevel7ID,
              ASATCOLevel7Name,
              ASATCOLevel7Unary,
              ASATCOLevel7Sort,
              ASATCOLevel8ID,
              ASATCOLevel8Name,
              ASATCOLevel8Unary,
              ASATCOLevel8Sort,
              ASATCOLevel9ID,
              ASATCOLevel9Name,
              ASATCOLevel9Unary,
              ASATCOLevel9Sort,
              ASATCOLevel10ID,
              ASATCOLevel10Name,
              ASATCOLevel10Unary,
              ASATCOLevel10Sort,
              ASATCOLevel11ID,
              ASATCOLevel11Name,
              ASATCOLevel11Unary,
              ASATCOLevel11Sort,
              ASATCOLevel12ID,
              ASATCOLevel12Name,
              ASATCOLevel12Unary,
              ASATCOLevel12Sort,
              ASAFieldUnary,
              ASAFieldSort,
              ASAFieldDepth,
              ASAFieldLevel1ID,
              ASAFieldLevel1Name,
              ASAFieldLevel1Unary,
              ASAFieldLevel1Sort,
              ASAFieldLevel2ID,
              ASAFieldLevel2Name,
              ASAFieldLevel2Unary,
              ASAFieldLevel2Sort,
              ASAFieldLevel3ID,
              ASAFieldLevel3Name,
              ASAFieldLevel3Unary,
              ASAFieldLevel3Sort,
              ASAFieldLevel4ID,
              ASAFieldLevel4Name,
              ASAFieldLevel4Unary,
              ASAFieldLevel4Sort,
              ASAFieldLevel5ID,
              ASAFieldLevel5Name,
              ASAFieldLevel5Unary,
              ASAFieldLevel5Sort,
              ASAFieldLevel6ID,
              ASAFieldLevel6Name,
              ASAFieldLevel6Unary,
              ASAFieldLevel6Sort,
              ASAFieldLevel7ID,
              ASAFieldLevel7Name,
              ASAFieldLevel7Unary,
              ASAFieldLevel7Sort,
              ASAFieldLevel8ID,
              ASAFieldLevel8Name,
              ASAFieldLevel8Unary,
              ASAFieldLevel8Sort,
              ASAFieldLevel9ID,
              ASAFieldLevel9Name,
              ASAFieldLevel9Unary,
              ASAFieldLevel9Sort,
              ASAFieldLevel10ID,
              ASAFieldLevel10Name,
              ASAFieldLevel10Unary,
              ASAFieldLevel10Sort,
              ASAFieldLevel11ID,
              ASAFieldLevel11Name,
              ASAFieldLevel11Unary,
              ASAFieldLevel11Sort,
              ASAFieldLevel12ID,
              ASAFieldLevel12Name,
              ASAFieldLevel12Unary,
              ASAFieldLevel12Sort,
              ASAGAUnary,
              ASAGASort,
              ASAGADepth,
              ASAGALevel1ID,
              ASAGALevel1Name,
              ASAGALevel1Unary,
              ASAGALevel1Sort,
              ASAGALevel2ID,
              ASAGALevel2Name,
              ASAGALevel2Unary,
              ASAGALevel2Sort,
              ASAGALevel3ID,
              ASAGALevel3Name,
              ASAGALevel3Unary,
              ASAGALevel3Sort,
              ASAGALevel4ID,
              ASAGALevel4Name,
              ASAGALevel4Unary,
              ASAGALevel4Sort,
              ASAGALevel5ID,
              ASAGALevel5Name,
              ASAGALevel5Unary,
              ASAGALevel5Sort,
              ASAGALevel6ID,
              ASAGALevel6Name,
              ASAGALevel6Unary,
              ASAGALevel6Sort,
              ASAGALevel7ID,
              ASAGALevel7Name,
              ASAGALevel7Unary,
              ASAGALevel7Sort,
              ASAGALevel8ID,
              ASAGALevel8Name,
              ASAGALevel8Unary,
              ASAGALevel8Sort,
              ASATuitionType,
              ASALaborType,
              ASAEBITDAAddbackFlag,
              AccountTypeCode,
              AccountTypeName,
              AccountTypeUnary,
              EDWCreatedDate,
              EDWCreatedBy,
              EDWModifiedDate,
              EDWModifiedBy,
			  RowStatus
             )
             OUTPUT $action
                    INTO @tblMergeActions;
             SELECT @InsertCount = SUM(Inserted),
                    @UpdateCount = SUM(Updated)
             FROM
             (
		   -- Count the number of inserts

                 SELECT COUNT(*) AS Inserted,
                        0 AS Updated
                 FROM @tblMergeActions
                 WHERE MergeAction = 'INSERT'
                 UNION ALL 

			  -- Count the number of updates 

                 SELECT 0 AS Inserted,
                        COUNT(*) AS Updated
                 FROM @tblMergeActions
                 WHERE MergeAction = 'UPDATE'
             ) merge_actions; 

		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' rows into Target.';
                     PRINT @DebugMsg;
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Updated '+CONVERT(NVARCHAR(20), @UpdateCount)+' rows into Target.';
                     PRINT @DebugMsg;
             END;  		               		   

		   -- ================================================================================
		   --
		   -- S T E P   4.
		   --
		   -- Execute any automated tests associated with this EDW table load
		   --
		   -- ================================================================================

		   ---4.1 Update existing RowStatus to 'A' is NULL and Truncate Table dbo.InfAccountSubaccount
		    UPDATE [BING_EDW].[dbo].[DimAccountSubaccount] SET [RowStatus] = 'A' WHERE [RowStatus] IS NULL;

		    Exec [dbo].[spGL_StagingTransform_TruncateEDWInfAccountSubaccount] ;


			--4.2 Insert records into Table [dbo].[InfAccountSubaccount] 

				INSERT INTO [dbo].[InfAccountSubaccount] 
				SELECT DISTINCT
					[AccountSubaccountID] = [F].[GLAccount] + '.' + [F].[GLSubAccount],
					[F].[GLAccount], [F].[GLSubAccount], @ProcName as ETLSource --ETLSource
				FROM [CMS_Staging].[dbo].[tfnNetRevenue](@EDWRunDateTime) [F]
				LEFT OUTER JOIN [BING_EDW].[dbo].[DimAccountSubaccount] [E]
					ON [F].[GLAccount] = [E].[AccountID]
					AND [F].[GLSubAccount] = [E].[SubaccountID]
				WHERE [E].[AccountID] IS NULL AND [E].[SubaccountID] IS NULL

				
             SELECT @InfSourceCount = COUNT(1)
             FROM  [dbo].[InfAccountSubaccount];
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @InfSourceCount)+' rows from Source into InfAccountSubaccount.';
             PRINT @DebugMsg;

			---4.3 Load Data into #MissingDimAccountSubAccountUpsert
				
				 INSERT INTO #MissingDimAccountSubAccountUpsert
				Exec [dbo].[spGL_StagingTransform_MissingDimAccountSubAccount]  @EDWRunDateTime;

				
             SELECT @InfSourceCount = COUNT(1)
             FROM  #MissingDimAccountSubAccountUpsert
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @InfSourceCount)+' rows from Source into MissingDimAccountSubAccountUpsert.';
             PRINT @DebugMsg;

				--4.4 Merge into Table 

				 MERGE [BING_EDW].[dbo].[DimAccountSubaccount] T
             USING #MissingDimAccountSubAccountUpsert S
             ON(S.AccountSubaccountID = T.AccountSubaccountID)
                 WHEN MATCHED AND S.AccountSubaccountName <> T.AccountSubaccountName
                                  OR S.AccountID <> T.AccountID
                                  OR S.AccountName <> T.AccountName
                                  OR S.SubaccountID <> T.SubaccountID
                                  OR S.SubaccountName <> T.SubaccountName
                                  OR S.ASATCOUnary <> T.ASATCOUnary
                                  OR S.ASATCOSort <> T.ASATCOSort
                                  OR S.ASATCODepth <> T.ASATCODepth
                                  OR S.ASATCOLevel1ID <> T.ASATCOLevel1ID
                                  OR S.ASATCOLevel1Name <> T.ASATCOLevel1Name
                                  OR S.ASATCOLevel1Unary <> T.ASATCOLevel1Unary
                                  OR S.ASATCOLevel1Sort <> T.ASATCOLevel1Sort
                                  OR S.ASATCOLevel2ID <> T.ASATCOLevel2ID
                                  OR S.ASATCOLevel2Name <> T.ASATCOLevel2Name
                                  OR S.ASATCOLevel2Unary <> T.ASATCOLevel2Unary
                                  OR S.ASATCOLevel2Sort <> T.ASATCOLevel2Sort
                                  OR S.ASATCOLevel3ID <> T.ASATCOLevel3ID
                                  OR S.ASATCOLevel3Name <> T.ASATCOLevel3Name
                                  OR S.ASATCOLevel3Unary <> T.ASATCOLevel3Unary
                                  OR S.ASATCOLevel3Sort <> T.ASATCOLevel3Sort
                                  OR S.ASATCOLevel4ID <> T.ASATCOLevel4ID
                                  OR S.ASATCOLevel4Name <> T.ASATCOLevel4Name
                                  OR S.ASATCOLevel4Unary <> T.ASATCOLevel4Unary
                                  OR S.ASATCOLevel4Sort <> T.ASATCOLevel4Sort
                                  OR S.ASATCOLevel5ID <> T.ASATCOLevel5ID
                                  OR S.ASATCOLevel5Name <> T.ASATCOLevel5Name
                                  OR S.ASATCOLevel5Unary <> T.ASATCOLevel5Unary
                                  OR S.ASATCOLevel5Sort <> T.ASATCOLevel5Sort
                                  OR S.ASATCOLevel6ID <> T.ASATCOLevel6ID
                                  OR S.ASATCOLevel6Name <> T.ASATCOLevel6Name
                                  OR S.ASATCOLevel6Unary <> T.ASATCOLevel6Unary
                                  OR S.ASATCOLevel6Sort <> T.ASATCOLevel6Sort
                                  OR S.ASATCOLevel7ID <> T.ASATCOLevel7ID
                                  OR S.ASATCOLevel7Name <> T.ASATCOLevel7Name
                                  OR S.ASATCOLevel7Unary <> T.ASATCOLevel7Unary
                                  OR S.ASATCOLevel7Sort <> T.ASATCOLevel7Sort
                                  OR S.ASATCOLevel8ID <> T.ASATCOLevel8ID
                                  OR S.ASATCOLevel8Name <> T.ASATCOLevel8Name
                                  OR S.ASATCOLevel8Unary <> T.ASATCOLevel8Unary
                                  OR S.ASATCOLevel8Sort <> T.ASATCOLevel8Sort
                                  OR S.ASATCOLevel9ID <> T.ASATCOLevel9ID
                                  OR S.ASATCOLevel9Name <> T.ASATCOLevel9Name
                                  OR S.ASATCOLevel9Unary <> T.ASATCOLevel9Unary
                                  OR S.ASATCOLevel9Sort <> T.ASATCOLevel9Sort
                                  OR S.ASATCOLevel10ID <> T.ASATCOLevel10ID
                                  OR S.ASATCOLevel10Name <> T.ASATCOLevel10Name
                                  OR S.ASATCOLevel10Unary <> T.ASATCOLevel10Unary
                                  OR S.ASATCOLevel10Sort <> T.ASATCOLevel10Sort
                                  OR S.ASATCOLevel11ID <> T.ASATCOLevel11ID
                                  OR S.ASATCOLevel11Name <> T.ASATCOLevel11Name
                                  OR S.ASATCOLevel11Unary <> T.ASATCOLevel11Unary
                                  OR S.ASATCOLevel11Sort <> T.ASATCOLevel11Sort
                                  OR S.ASATCOLevel12ID <> T.ASATCOLevel12ID
                                  OR S.ASATCOLevel12Name <> T.ASATCOLevel12Name
                                  OR S.ASATCOLevel12Unary <> T.ASATCOLevel12Unary
                                  OR S.ASATCOLevel12Sort <> T.ASATCOLevel12Sort
                                  OR S.ASAFieldUnary <> T.ASAFieldUnary
                                  OR S.ASAFieldSort <> T.ASAFieldSort
                                  OR S.ASAFieldDepth <> T.ASAFieldDepth
                                  OR S.ASAFieldLevel1ID <> T.ASAFieldLevel1ID
                                  OR S.ASAFieldLevel1Name <> T.ASAFieldLevel1Name
                                  OR S.ASAFieldLevel1Unary <> T.ASAFieldLevel1Unary
                                  OR S.ASAFieldLevel1Sort <> T.ASAFieldLevel1Sort
                                  OR S.ASAFieldLevel2ID <> T.ASAFieldLevel2ID
                                  OR S.ASAFieldLevel2Name <> T.ASAFieldLevel2Name
                                  OR S.ASAFieldLevel2Unary <> T.ASAFieldLevel2Unary
                                  OR S.ASAFieldLevel2Sort <> T.ASAFieldLevel2Sort
                                  OR S.ASAFieldLevel3ID <> T.ASAFieldLevel3ID
                                  OR S.ASAFieldLevel3Name <> T.ASAFieldLevel3Name
                                  OR S.ASAFieldLevel3Unary <> T.ASAFieldLevel3Unary
                                  OR S.ASAFieldLevel3Sort <> T.ASAFieldLevel3Sort
                                  OR S.ASAFieldLevel4ID <> T.ASAFieldLevel4ID
                                  OR S.ASAFieldLevel4Name <> T.ASAFieldLevel4Name
                                  OR S.ASAFieldLevel4Unary <> T.ASAFieldLevel4Unary
                                  OR S.ASAFieldLevel4Sort <> T.ASAFieldLevel4Sort
                                  OR S.ASAFieldLevel5ID <> T.ASAFieldLevel5ID
                                  OR S.ASAFieldLevel5Name <> T.ASAFieldLevel5Name
                                  OR S.ASAFieldLevel5Unary <> T.ASAFieldLevel5Unary
                                  OR S.ASAFieldLevel5Sort <> T.ASAFieldLevel5Sort
                                  OR S.ASAFieldLevel6ID <> T.ASAFieldLevel6ID
                                  OR S.ASAFieldLevel6Name <> T.ASAFieldLevel6Name
                                  OR S.ASAFieldLevel6Unary <> T.ASAFieldLevel6Unary
                                  OR S.ASAFieldLevel6Sort <> T.ASAFieldLevel6Sort
                                  OR S.ASAFieldLevel7ID <> T.ASAFieldLevel7ID
                                  OR S.ASAFieldLevel7Name <> T.ASAFieldLevel7Name
                                  OR S.ASAFieldLevel7Unary <> T.ASAFieldLevel7Unary
                                  OR S.ASAFieldLevel7Sort <> T.ASAFieldLevel7Sort
                                  OR S.ASAFieldLevel8ID <> T.ASAFieldLevel8ID
                                  OR S.ASAFieldLevel8Name <> T.ASAFieldLevel8Name
                                  OR S.ASAFieldLevel8Unary <> T.ASAFieldLevel8Unary
                                  OR S.ASAFieldLevel8Sort <> T.ASAFieldLevel8Sort
                                  OR S.ASAFieldLevel9ID <> T.ASAFieldLevel9ID
                                  OR S.ASAFieldLevel9Name <> T.ASAFieldLevel9Name
                                  OR S.ASAFieldLevel9Unary <> T.ASAFieldLevel9Unary
                                  OR S.ASAFieldLevel9Sort <> T.ASAFieldLevel9Sort
                                  OR S.ASAFieldLevel10ID <> T.ASAFieldLevel10ID
                                  OR S.ASAFieldLevel10Name <> T.ASAFieldLevel10Name
                                  OR S.ASAFieldLevel10Unary <> T.ASAFieldLevel10Unary
                                  OR S.ASAFieldLevel10Sort <> T.ASAFieldLevel10Sort
                                  OR S.ASAFieldLevel11ID <> T.ASAFieldLevel11ID
                                  OR S.ASAFieldLevel11Name <> T.ASAFieldLevel11Name
                                  OR S.ASAFieldLevel11Unary <> T.ASAFieldLevel11Unary
                                  OR S.ASAFieldLevel11Sort <> T.ASAFieldLevel11Sort
                                  OR S.ASAFieldLevel12ID <> T.ASAFieldLevel12ID
                                  OR S.ASAFieldLevel12Name <> T.ASAFieldLevel12Name
                                  OR S.ASAFieldLevel12Unary <> T.ASAFieldLevel12Unary
                                  OR S.ASAFieldLevel12Sort <> T.ASAFieldLevel12Sort
                                  OR S.ASAGAUnary <> T.ASAGAUnary
                                  OR S.ASAGASort <> T.ASAGASort
                                  OR S.ASAGADepth <> T.ASAGADepth
                                  OR S.ASAGALevel1ID <> T.ASAGALevel1ID
                                  OR S.ASAGALevel1Name <> T.ASAGALevel1Name
                                  OR S.ASAGALevel1Unary <> T.ASAGALevel1Unary
                                  OR S.ASAGALevel1Sort <> T.ASAGALevel1Sort
                                  OR S.ASAGALevel2ID <> T.ASAGALevel2ID
                                  OR S.ASAGALevel2Name <> T.ASAGALevel2Name
                                  OR S.ASAGALevel2Unary <> T.ASAGALevel2Unary
                                  OR S.ASAGALevel2Sort <> T.ASAGALevel2Sort
                                  OR S.ASAGALevel3ID <> T.ASAGALevel3ID
                                  OR S.ASAGALevel3Name <> T.ASAGALevel3Name
                                  OR S.ASAGALevel3Unary <> T.ASAGALevel3Unary
                                  OR S.ASAGALevel3Sort <> T.ASAGALevel3Sort
                                  OR S.ASAGALevel4ID <> T.ASAGALevel4ID
                                  OR S.ASAGALevel4Name <> T.ASAGALevel4Name
                                  OR S.ASAGALevel4Unary <> T.ASAGALevel4Unary
                                  OR S.ASAGALevel4Sort <> T.ASAGALevel4Sort
                                  OR S.ASAGALevel5ID <> T.ASAGALevel5ID
                                  OR S.ASAGALevel5Name <> T.ASAGALevel5Name
                                  OR S.ASAGALevel5Unary <> T.ASAGALevel5Unary
                                  OR S.ASAGALevel5Sort <> T.ASAGALevel5Sort
                                  OR S.ASAGALevel6ID <> T.ASAGALevel6ID
                                  OR S.ASAGALevel6Name <> T.ASAGALevel6Name
                                  OR S.ASAGALevel6Unary <> T.ASAGALevel6Unary
                                  OR S.ASAGALevel6Sort <> T.ASAGALevel6Sort
                                  OR S.ASAGALevel7ID <> T.ASAGALevel7ID
                                  OR S.ASAGALevel7Name <> T.ASAGALevel7Name
                                  OR S.ASAGALevel7Unary <> T.ASAGALevel7Unary
                                  OR S.ASAGALevel7Sort <> T.ASAGALevel7Sort
                                  OR S.ASAGALevel8ID <> T.ASAGALevel8ID
                                  OR S.ASAGALevel8Name <> T.ASAGALevel8Name
                                  OR S.ASAGALevel8Unary <> T.ASAGALevel8Unary
                                  OR S.ASAGALevel8Sort <> T.ASAGALevel8Sort
                                  OR S.ASATuitionType <> T.ASATuitionType
                                  OR S.ASALaborType <> T.ASALaborType
                                  OR S.ASAEBITDAAddbackFlag <> T.ASAEBITDAAddbackFlag
                                  OR S.AccountTypeCode <> T.AccountTypeCode
                                  OR S.AccountTypeName <> T.AccountTypeName
                                  OR S.AccountTypeUnary <> T.AccountTypeUnary
								  OR S.RowStatus<>T.RowStatus
                 THEN UPDATE SET
                                 T.AccountSubaccountName = S.AccountSubaccountName,
                                 T.AccountID = S.AccountID,
                                 T.AccountName = S.AccountName,
                                 T.SubaccountID = S.SubaccountID,
                                 T.SubaccountName = S.SubaccountName,
                                 T.ASATCOUnary = S.ASATCOUnary,
                                 T.ASATCOSort = S.ASATCOSort,
                                 T.ASATCODepth = S.ASATCODepth,
                                 T.ASATCOLevel1ID = S.ASATCOLevel1ID,
                                 T.ASATCOLevel1Name = S.ASATCOLevel1Name,
                                 T.ASATCOLevel1Unary = S.ASATCOLevel1Unary,
                                 T.ASATCOLevel1Sort = S.ASATCOLevel1Sort,
                                 T.ASATCOLevel2ID = S.ASATCOLevel2ID,
                                 T.ASATCOLevel2Name = S.ASATCOLevel2Name,
                                 T.ASATCOLevel2Unary = S.ASATCOLevel2Unary,
                                 T.ASATCOLevel2Sort = S.ASATCOLevel2Sort,
                                 T.ASATCOLevel3ID = S.ASATCOLevel3ID,
                                 T.ASATCOLevel3Name = S.ASATCOLevel3Name,
                                 T.ASATCOLevel3Unary = S.ASATCOLevel3Unary,
                                 T.ASATCOLevel3Sort = S.ASATCOLevel3Sort,
                                 T.ASATCOLevel4ID = S.ASATCOLevel4ID,
                                 T.ASATCOLevel4Name = S.ASATCOLevel4Name,
                                 T.ASATCOLevel4Unary = S.ASATCOLevel4Unary,
                                 T.ASATCOLevel4Sort = S.ASATCOLevel4Sort,
                                 T.ASATCOLevel5ID = S.ASATCOLevel5ID,
                                 T.ASATCOLevel5Name = S.ASATCOLevel5Name,
                                 T.ASATCOLevel5Unary = S.ASATCOLevel5Unary,
                                 T.ASATCOLevel5Sort = S.ASATCOLevel5Sort,
                                 T.ASATCOLevel6ID = S.ASATCOLevel6ID,
                                 T.ASATCOLevel6Name = S.ASATCOLevel6Name,
                                 T.ASATCOLevel6Unary = S.ASATCOLevel6Unary,
                                 T.ASATCOLevel6Sort = S.ASATCOLevel6Sort,
                                 T.ASATCOLevel7ID = S.ASATCOLevel7ID,
                                 T.ASATCOLevel7Name = S.ASATCOLevel7Name,
                                 T.ASATCOLevel7Unary = S.ASATCOLevel7Unary,
                                 T.ASATCOLevel7Sort = S.ASATCOLevel7Sort,
                                 T.ASATCOLevel8ID = S.ASATCOLevel8ID,
                                 T.ASATCOLevel8Name = S.ASATCOLevel8Name,
                                 T.ASATCOLevel8Unary = S.ASATCOLevel8Unary,
                                 T.ASATCOLevel8Sort = S.ASATCOLevel8Sort,
                                 T.ASATCOLevel9ID = S.ASATCOLevel9ID,
                                 T.ASATCOLevel9Name = S.ASATCOLevel9Name,
                                 T.ASATCOLevel9Unary = S.ASATCOLevel9Unary,
                                 T.ASATCOLevel9Sort = S.ASATCOLevel9Sort,
                                 T.ASATCOLevel10ID = S.ASATCOLevel10ID,
                                 T.ASATCOLevel10Name = S.ASATCOLevel10Name,
                                 T.ASATCOLevel10Unary = S.ASATCOLevel10Unary,
                                 T.ASATCOLevel10Sort = S.ASATCOLevel10Sort,
                                 T.ASATCOLevel11ID = S.ASATCOLevel11ID,
                                 T.ASATCOLevel11Name = S.ASATCOLevel11Name,
                                 T.ASATCOLevel11Unary = S.ASATCOLevel11Unary,
                                 T.ASATCOLevel11Sort = S.ASATCOLevel11Sort,
                                 T.ASATCOLevel12ID = S.ASATCOLevel12ID,
                                 T.ASATCOLevel12Name = S.ASATCOLevel12Name,
                                 T.ASATCOLevel12Unary = S.ASATCOLevel12Unary,
                                 T.ASATCOLevel12Sort = S.ASATCOLevel12Sort,
                                 T.ASAFieldUnary = S.ASAFieldUnary,
                                 T.ASAFieldSort = S.ASAFieldSort,
                                 T.ASAFieldDepth = S.ASAFieldDepth,
                                 T.ASAFieldLevel1ID = S.ASAFieldLevel1ID,
                                 T.ASAFieldLevel1Name = S.ASAFieldLevel1Name,
                                 T.ASAFieldLevel1Unary = S.ASAFieldLevel1Unary,
                                 T.ASAFieldLevel1Sort = S.ASAFieldLevel1Sort,
                                 T.ASAFieldLevel2ID = S.ASAFieldLevel2ID,
                                 T.ASAFieldLevel2Name = S.ASAFieldLevel2Name,
                                 T.ASAFieldLevel2Unary = S.ASAFieldLevel2Unary,
                                 T.ASAFieldLevel2Sort = S.ASAFieldLevel2Sort,
                                 T.ASAFieldLevel3ID = S.ASAFieldLevel3ID,
                                 T.ASAFieldLevel3Name = S.ASAFieldLevel3Name,
                                 T.ASAFieldLevel3Unary = S.ASAFieldLevel3Unary,
                                 T.ASAFieldLevel3Sort = S.ASAFieldLevel3Sort,
                                 T.ASAFieldLevel4ID = S.ASAFieldLevel4ID,
                                 T.ASAFieldLevel4Name = S.ASAFieldLevel4Name,
                                 T.ASAFieldLevel4Unary = S.ASAFieldLevel4Unary,
                                 T.ASAFieldLevel4Sort = S.ASAFieldLevel4Sort,
                                 T.ASAFieldLevel5ID = S.ASAFieldLevel5ID,
                                 T.ASAFieldLevel5Name = S.ASAFieldLevel5Name,
                                 T.ASAFieldLevel5Unary = S.ASAFieldLevel5Unary,
                                 T.ASAFieldLevel5Sort = S.ASAFieldLevel5Sort,
                                 T.ASAFieldLevel6ID = S.ASAFieldLevel6ID,
                                 T.ASAFieldLevel6Name = S.ASAFieldLevel6Name,
                                 T.ASAFieldLevel6Unary = S.ASAFieldLevel6Unary,
                                 T.ASAFieldLevel6Sort = S.ASAFieldLevel6Sort,
                                 T.ASAFieldLevel7ID = S.ASAFieldLevel7ID,
                                 T.ASAFieldLevel7Name = S.ASAFieldLevel7Name,
                                 T.ASAFieldLevel7Unary = S.ASAFieldLevel7Unary,
                                 T.ASAFieldLevel7Sort = S.ASAFieldLevel7Sort,
                                 T.ASAFieldLevel8ID = S.ASAFieldLevel8ID,
                                 T.ASAFieldLevel8Name = S.ASAFieldLevel8Name,
                                 T.ASAFieldLevel8Unary = S.ASAFieldLevel8Unary,
                                 T.ASAFieldLevel8Sort = S.ASAFieldLevel8Sort,
                                 T.ASAFieldLevel9ID = S.ASAFieldLevel9ID,
                                 T.ASAFieldLevel9Name = S.ASAFieldLevel9Name,
                                 T.ASAFieldLevel9Unary = S.ASAFieldLevel9Unary,
                                 T.ASAFieldLevel9Sort = S.ASAFieldLevel9Sort,
                                 T.ASAFieldLevel10ID = S.ASAFieldLevel10ID,
                                 T.ASAFieldLevel10Name = S.ASAFieldLevel10Name,
                                 T.ASAFieldLevel10Unary = S.ASAFieldLevel10Unary,
                                 T.ASAFieldLevel10Sort = S.ASAFieldLevel10Sort,
                                 T.ASAFieldLevel11ID = S.ASAFieldLevel11ID,
                                 T.ASAFieldLevel11Name = S.ASAFieldLevel11Name,
                                 T.ASAFieldLevel11Unary = S.ASAFieldLevel11Unary,
                                 T.ASAFieldLevel11Sort = S.ASAFieldLevel11Sort,
                                 T.ASAFieldLevel12ID = S.ASAFieldLevel12ID,
                                 T.ASAFieldLevel12Name = S.ASAFieldLevel12Name,
                                 T.ASAFieldLevel12Unary = S.ASAFieldLevel12Unary,
                                 T.ASAFieldLevel12Sort = S.ASAFieldLevel12Sort,
                                 T.ASAGAUnary = S.ASAGAUnary,
                                 T.ASAGASort = S.ASAGASort,
                                 T.ASAGADepth = S.ASAGADepth,
                                 T.ASAGALevel1ID = S.ASAGALevel1ID,
                                 T.ASAGALevel1Name = S.ASAGALevel1Name,
                                 T.ASAGALevel1Unary = S.ASAGALevel1Unary,
                                 T.ASAGALevel1Sort = S.ASAGALevel1Sort,
                                 T.ASAGALevel2ID = S.ASAGALevel2ID,
                                 T.ASAGALevel2Name = S.ASAGALevel2Name,
                                 T.ASAGALevel2Unary = S.ASAGALevel2Unary,
                                 T.ASAGALevel2Sort = S.ASAGALevel2Sort,
                                 T.ASAGALevel3ID = S.ASAGALevel3ID,
                                 T.ASAGALevel3Name = S.ASAGALevel3Name,
                                 T.ASAGALevel3Unary = S.ASAGALevel3Unary,
                                 T.ASAGALevel3Sort = S.ASAGALevel3Sort,
                                 T.ASAGALevel4ID = S.ASAGALevel4ID,
                                 T.ASAGALevel4Name = S.ASAGALevel4Name,
                                 T.ASAGALevel4Unary = S.ASAGALevel4Unary,
                                 T.ASAGALevel4Sort = S.ASAGALevel4Sort,
                                 T.ASAGALevel5ID = S.ASAGALevel5ID,
                                 T.ASAGALevel5Name = S.ASAGALevel5Name,
                                 T.ASAGALevel5Unary = S.ASAGALevel5Unary,
                                 T.ASAGALevel5Sort = S.ASAGALevel5Sort,
                                 T.ASAGALevel6ID = S.ASAGALevel6ID,
                                 T.ASAGALevel6Name = S.ASAGALevel6Name,
                                 T.ASAGALevel6Unary = S.ASAGALevel6Unary,
                                 T.ASAGALevel6Sort = S.ASAGALevel6Sort,
                                 T.ASAGALevel7ID = S.ASAGALevel7ID,
                                 T.ASAGALevel7Name = S.ASAGALevel7Name,
                                 T.ASAGALevel7Unary = S.ASAGALevel7Unary,
                                 T.ASAGALevel7Sort = S.ASAGALevel7Sort,
                                 T.ASAGALevel8ID = S.ASAGALevel8ID,
                                 T.ASAGALevel8Name = S.ASAGALevel8Name,
                                 T.ASAGALevel8Unary = S.ASAGALevel8Unary,
                                 T.ASAGALevel8Sort = S.ASAGALevel8Sort,
                                 T.ASATuitionType = S.ASATuitionType,
                                 T.ASALaborType = S.ASALaborType,
                                 T.ASAEBITDAAddbackFlag = S.ASAEBITDAAddbackFlag,
                                 T.AccountTypeCode = S.AccountTypeCode,
                                 T.AccountTypeName = S.AccountTypeName,
                                 T.AccountTypeUnary = S.AccountTypeUnary,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWCreatedBy = S.EDWCreatedBy,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.EDWModifiedBy = S.EDWModifiedBy,
								 T.RowStatus=S.RowStatus
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(AccountSubaccountID,
                          AccountSubaccountName,
                          AccountID,
                          AccountName,
                          SubaccountID,
                          SubaccountName,
                          ASATCOUnary,
                          ASATCOSort,
                          ASATCODepth,
                          ASATCOLevel1ID,
                          ASATCOLevel1Name,
                          ASATCOLevel1Unary,
                          ASATCOLevel1Sort,
                          ASATCOLevel2ID,
                          ASATCOLevel2Name,
                          ASATCOLevel2Unary,
                          ASATCOLevel2Sort,
                          ASATCOLevel3ID,
                          ASATCOLevel3Name,
                          ASATCOLevel3Unary,
                          ASATCOLevel3Sort,
                          ASATCOLevel4ID,
                          ASATCOLevel4Name,
                          ASATCOLevel4Unary,
                          ASATCOLevel4Sort,
                          ASATCOLevel5ID,
                          ASATCOLevel5Name,
                          ASATCOLevel5Unary,
                          ASATCOLevel5Sort,
                          ASATCOLevel6ID,
                          ASATCOLevel6Name,
                          ASATCOLevel6Unary,
                          ASATCOLevel6Sort,
                          ASATCOLevel7ID,
                          ASATCOLevel7Name,
                          ASATCOLevel7Unary,
                          ASATCOLevel7Sort,
                          ASATCOLevel8ID,
                          ASATCOLevel8Name,
                          ASATCOLevel8Unary,
                          ASATCOLevel8Sort,
                          ASATCOLevel9ID,
                          ASATCOLevel9Name,
                          ASATCOLevel9Unary,
                          ASATCOLevel9Sort,
                          ASATCOLevel10ID,
                          ASATCOLevel10Name,
                          ASATCOLevel10Unary,
                          ASATCOLevel10Sort,
                          ASATCOLevel11ID,
                          ASATCOLevel11Name,
                          ASATCOLevel11Unary,
                          ASATCOLevel11Sort,
                          ASATCOLevel12ID,
                          ASATCOLevel12Name,
                          ASATCOLevel12Unary,
                          ASATCOLevel12Sort,
                          ASAFieldUnary,
                          ASAFieldSort,
                          ASAFieldDepth,
                          ASAFieldLevel1ID,
                          ASAFieldLevel1Name,
                          ASAFieldLevel1Unary,
                          ASAFieldLevel1Sort,
                          ASAFieldLevel2ID,
                          ASAFieldLevel2Name,
                          ASAFieldLevel2Unary,
                          ASAFieldLevel2Sort,
                          ASAFieldLevel3ID,
                          ASAFieldLevel3Name,
                          ASAFieldLevel3Unary,
                          ASAFieldLevel3Sort,
                          ASAFieldLevel4ID,
                          ASAFieldLevel4Name,
                          ASAFieldLevel4Unary,
                          ASAFieldLevel4Sort,
                          ASAFieldLevel5ID,
                          ASAFieldLevel5Name,
                          ASAFieldLevel5Unary,
                          ASAFieldLevel5Sort,
                          ASAFieldLevel6ID,
                          ASAFieldLevel6Name,
                          ASAFieldLevel6Unary,
                          ASAFieldLevel6Sort,
                          ASAFieldLevel7ID,
                          ASAFieldLevel7Name,
                          ASAFieldLevel7Unary,
                          ASAFieldLevel7Sort,
                          ASAFieldLevel8ID,
                          ASAFieldLevel8Name,
                          ASAFieldLevel8Unary,
                          ASAFieldLevel8Sort,
                          ASAFieldLevel9ID,
                          ASAFieldLevel9Name,
                          ASAFieldLevel9Unary,
                          ASAFieldLevel9Sort,
                          ASAFieldLevel10ID,
                          ASAFieldLevel10Name,
                          ASAFieldLevel10Unary,
                          ASAFieldLevel10Sort,
                          ASAFieldLevel11ID,
                          ASAFieldLevel11Name,
                          ASAFieldLevel11Unary,
                          ASAFieldLevel11Sort,
                          ASAFieldLevel12ID,
                          ASAFieldLevel12Name,
                          ASAFieldLevel12Unary,
                          ASAFieldLevel12Sort,
                          ASAGAUnary,
                          ASAGASort,
                          ASAGADepth,
                          ASAGALevel1ID,
                          ASAGALevel1Name,
                          ASAGALevel1Unary,
                          ASAGALevel1Sort,
                          ASAGALevel2ID,
                          ASAGALevel2Name,
                          ASAGALevel2Unary,
                          ASAGALevel2Sort,
                          ASAGALevel3ID,
                          ASAGALevel3Name,
                          ASAGALevel3Unary,
                          ASAGALevel3Sort,
                          ASAGALevel4ID,
                          ASAGALevel4Name,
                          ASAGALevel4Unary,
                          ASAGALevel4Sort,
                          ASAGALevel5ID,
                          ASAGALevel5Name,
                          ASAGALevel5Unary,
                          ASAGALevel5Sort,
                          ASAGALevel6ID,
                          ASAGALevel6Name,
                          ASAGALevel6Unary,
                          ASAGALevel6Sort,
                          ASAGALevel7ID,
                          ASAGALevel7Name,
                          ASAGALevel7Unary,
                          ASAGALevel7Sort,
                          ASAGALevel8ID,
                          ASAGALevel8Name,
                          ASAGALevel8Unary,
                          ASAGALevel8Sort,
                          ASATuitionType,
                          ASALaborType,
                          ASAEBITDAAddbackFlag,
                          AccountTypeCode,
                          AccountTypeName,
                          AccountTypeUnary,
                          EDWCreatedDate,
                          EDWCreatedBy,
                          EDWModifiedDate,
                          EDWModifiedBy,
						  RowStatus)
                   VALUES
             (AccountSubaccountID,
              AccountSubaccountName,
              AccountID,
              AccountName,
              SubaccountID,
              SubaccountName,
              ASATCOUnary,
              ASATCOSort,
              ASATCODepth,
              ASATCOLevel1ID,
              ASATCOLevel1Name,
              ASATCOLevel1Unary,
              ASATCOLevel1Sort,
              ASATCOLevel2ID,
              ASATCOLevel2Name,
              ASATCOLevel2Unary,
              ASATCOLevel2Sort,
              ASATCOLevel3ID,
              ASATCOLevel3Name,
              ASATCOLevel3Unary,
              ASATCOLevel3Sort,
              ASATCOLevel4ID,
              ASATCOLevel4Name,
              ASATCOLevel4Unary,
              ASATCOLevel4Sort,
              ASATCOLevel5ID,
              ASATCOLevel5Name,
              ASATCOLevel5Unary,
              ASATCOLevel5Sort,
              ASATCOLevel6ID,
              ASATCOLevel6Name,
              ASATCOLevel6Unary,
              ASATCOLevel6Sort,
              ASATCOLevel7ID,
              ASATCOLevel7Name,
              ASATCOLevel7Unary,
              ASATCOLevel7Sort,
              ASATCOLevel8ID,
              ASATCOLevel8Name,
              ASATCOLevel8Unary,
              ASATCOLevel8Sort,
              ASATCOLevel9ID,
              ASATCOLevel9Name,
              ASATCOLevel9Unary,
              ASATCOLevel9Sort,
              ASATCOLevel10ID,
              ASATCOLevel10Name,
              ASATCOLevel10Unary,
              ASATCOLevel10Sort,
              ASATCOLevel11ID,
              ASATCOLevel11Name,
              ASATCOLevel11Unary,
              ASATCOLevel11Sort,
              ASATCOLevel12ID,
              ASATCOLevel12Name,
              ASATCOLevel12Unary,
              ASATCOLevel12Sort,
              ASAFieldUnary,
              ASAFieldSort,
              ASAFieldDepth,
              ASAFieldLevel1ID,
              ASAFieldLevel1Name,
              ASAFieldLevel1Unary,
              ASAFieldLevel1Sort,
              ASAFieldLevel2ID,
              ASAFieldLevel2Name,
              ASAFieldLevel2Unary,
              ASAFieldLevel2Sort,
              ASAFieldLevel3ID,
              ASAFieldLevel3Name,
              ASAFieldLevel3Unary,
              ASAFieldLevel3Sort,
              ASAFieldLevel4ID,
              ASAFieldLevel4Name,
              ASAFieldLevel4Unary,
              ASAFieldLevel4Sort,
              ASAFieldLevel5ID,
              ASAFieldLevel5Name,
              ASAFieldLevel5Unary,
              ASAFieldLevel5Sort,
              ASAFieldLevel6ID,
              ASAFieldLevel6Name,
              ASAFieldLevel6Unary,
              ASAFieldLevel6Sort,
              ASAFieldLevel7ID,
              ASAFieldLevel7Name,
              ASAFieldLevel7Unary,
              ASAFieldLevel7Sort,
              ASAFieldLevel8ID,
              ASAFieldLevel8Name,
              ASAFieldLevel8Unary,
              ASAFieldLevel8Sort,
              ASAFieldLevel9ID,
              ASAFieldLevel9Name,
              ASAFieldLevel9Unary,
              ASAFieldLevel9Sort,
              ASAFieldLevel10ID,
              ASAFieldLevel10Name,
              ASAFieldLevel10Unary,
              ASAFieldLevel10Sort,
              ASAFieldLevel11ID,
              ASAFieldLevel11Name,
              ASAFieldLevel11Unary,
              ASAFieldLevel11Sort,
              ASAFieldLevel12ID,
              ASAFieldLevel12Name,
              ASAFieldLevel12Unary,
              ASAFieldLevel12Sort,
              ASAGAUnary,
              ASAGASort,
              ASAGADepth,
              ASAGALevel1ID,
              ASAGALevel1Name,
              ASAGALevel1Unary,
              ASAGALevel1Sort,
              ASAGALevel2ID,
              ASAGALevel2Name,
              ASAGALevel2Unary,
              ASAGALevel2Sort,
              ASAGALevel3ID,
              ASAGALevel3Name,
              ASAGALevel3Unary,
              ASAGALevel3Sort,
              ASAGALevel4ID,
              ASAGALevel4Name,
              ASAGALevel4Unary,
              ASAGALevel4Sort,
              ASAGALevel5ID,
              ASAGALevel5Name,
              ASAGALevel5Unary,
              ASAGALevel5Sort,
              ASAGALevel6ID,
              ASAGALevel6Name,
              ASAGALevel6Unary,
              ASAGALevel6Sort,
              ASAGALevel7ID,
              ASAGALevel7Name,
              ASAGALevel7Unary,
              ASAGALevel7Sort,
              ASAGALevel8ID,
              ASAGALevel8Name,
              ASAGALevel8Unary,
              ASAGALevel8Sort,
              ASATuitionType,
              ASALaborType,
              ASAEBITDAAddbackFlag,
              AccountTypeCode,
              AccountTypeName,
              AccountTypeUnary,
              EDWCreatedDate,
              EDWCreatedBy,
              EDWModifiedDate,
              EDWModifiedBy,
			  RowStatus
             )
             OUTPUT $action
                    INTO @tblMergeActions;
             SELECT @InfInsertCount = SUM(Inserted),
                    @InfUpdateCount = SUM(Updated)
             FROM
             (
		   -- Count the number of inserts

                 SELECT COUNT(*) AS Inserted,
                        0 AS Updated
                 FROM @tblMergeActions
                 WHERE MergeAction = 'INSERT'
                 UNION ALL 

			  -- Count the number of updates 

                 SELECT 0 AS Inserted,
                        COUNT(*) AS Updated
                 FROM @tblMergeActions
                 WHERE MergeAction = 'UPDATE'
             ) merge_actions; 

		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InfInsertCount)+' rows into Target.';
                     PRINT @DebugMsg;
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Updated '+CONVERT(NVARCHAR(20), @InfUpdateCount)+' rows into Target.';
                     PRINT @DebugMsg;
             END;  		               		   



		   -- ================================================================================
		   --
		   -- S T E P   5.
		   --
		   -- Output Source / Insert / Update / Delete counts to caller, commit the transaction,
		   --	and tidy tup.
		   --
		   -- ================================================================================
		  
		  -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Committing transaction.';
             PRINT @DebugMsg;
		   
		   --
		   -- Commit the successful transaction 
		   --
             COMMIT TRANSACTION;

		   --
		   -- Drop the temp table
		   --
             DROP TABLE #DimAccountSubAccountUpsert;
			 DROP TABLE #MissingDimAccountSubAccountUpsert;
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