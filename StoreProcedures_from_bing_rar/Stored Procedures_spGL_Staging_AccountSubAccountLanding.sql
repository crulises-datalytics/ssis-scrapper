CREATE PROC [dbo].[spGL_Staging_AccountSubAccountLanding]
(
    @DebugMode INT = NULL,
    @SourceCount BIGINT OUTPUT,
    @InsertCount BIGINT OUTPUT
)
AS
-- ================================================================================    
-- 
-- Stored Procedure:   spGL_Staging_AccountSubAccountLanding
--
-- Purpose:            This procedure loads the GL_Staging.dbo.AccountSubaccountLanding
--                     table by calling dbo.spGL_StagingTransform_DimAccountSubAccount
--
-- Parameters:         No parameters                 
--
-- Usage:              EXEC dbo.spGL_Staging_AccountSubAccountLanding	
-- 
-- --------------------------------------------------------------------------------
--
-- Change Log:		   
-- ----------
--
-- Date        Modified By         Comments
-- ----        -----------         --------
-- 05/20/2021  Adevabhakthuni      BI-4690 added new rows to support SLD
-- 07/05/2022  Suhas De			   BI-2161 added RowStatus to #AccountSubaccount
-- ================================================================================    
BEGIN
    SET NOCOUNT ON;

    --
    -- Housekeeping Variables
    --
    DECLARE @ProcName NVARCHAR(500) = OBJECT_NAME(@@PROCID);
    DECLARE @DebugMsg NVARCHAR(500);
    --DECLARE @SourceCount INT;
    --DECLARE @InsertCount INT;
    --
    IF @DebugMode = 1
        SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Starting.';
    PRINT @DebugMsg;

    -- --------------------------------------------------------------------------------
    -- Extract from Source, Upserts and Deletes contained in a single transaction.  
    --	 Rollback on error
    -- --------------------------------------------------------------------------------
    BEGIN TRY
        BEGIN TRANSACTION;
        -- Debug output progress
        IF @DebugMode = 1
            SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Beginning transaction.';
        PRINT @DebugMsg;
        -- ================================================================================
        --
        -- Create temporary landing #table
        --
        -- ================================================================================
        CREATE TABLE #AccountSubaccount
        (
            [AccountSubaccountID] [VARCHAR](11) NOT NULL,
            [AccountSubaccountName] [VARCHAR](500) NOT NULL,
            [AccountID] [VARCHAR](4) NOT NULL,
            [AccountName] [VARCHAR](250) NOT NULL,
            [SubaccountID] [VARCHAR](6) NOT NULL,
            [SubaccountName] [VARCHAR](250) NOT NULL,
            [ASATCOUnary] [INT] NOT NULL,
            [ASATCOSort] [INT] NOT NULL,
            [ASATCODepth] [INT] NOT NULL,
            [ASATCOLevel1ID] [VARCHAR](11) NULL,
            [ASATCOLevel1Name] [VARCHAR](250) NULL,
            [ASATCOLevel1Unary] [INT] NULL,
            [ASATCOLevel1Sort] [INT] NULL,
            [ASATCOLevel2ID] [VARCHAR](11) NULL,
            [ASATCOLevel2Name] [VARCHAR](250) NULL,
            [ASATCOLevel2Unary] [INT] NULL,
            [ASATCOLevel2Sort] [INT] NULL,
            [ASATCOLevel3ID] [VARCHAR](11) NULL,
            [ASATCOLevel3Name] [VARCHAR](250) NULL,
            [ASATCOLevel3Unary] [INT] NULL,
            [ASATCOLevel3Sort] [INT] NULL,
            [ASATCOLevel4ID] [VARCHAR](11) NULL,
            [ASATCOLevel4Name] [VARCHAR](250) NULL,
            [ASATCOLevel4Unary] [INT] NULL,
            [ASATCOLevel4Sort] [INT] NULL,
            [ASATCOLevel5ID] [VARCHAR](11) NULL,
            [ASATCOLevel5Name] [VARCHAR](250) NULL,
            [ASATCOLevel5Unary] [INT] NULL,
            [ASATCOLevel5Sort] [INT] NULL,
            [ASATCOLevel6ID] [VARCHAR](11) NULL,
            [ASATCOLevel6Name] [VARCHAR](250) NULL,
            [ASATCOLevel6Unary] [INT] NULL,
            [ASATCOLevel6Sort] [INT] NULL,
            [ASATCOLevel7ID] [VARCHAR](11) NULL,
            [ASATCOLevel7Name] [VARCHAR](250) NULL,
            [ASATCOLevel7Unary] [INT] NULL,
            [ASATCOLevel7Sort] [INT] NULL,
            [ASATCOLevel8ID] [VARCHAR](11) NULL,
            [ASATCOLevel8Name] [VARCHAR](250) NULL,
            [ASATCOLevel8Unary] [INT] NULL,
            [ASATCOLevel8Sort] [INT] NULL,
            [ASATCOLevel9ID] [VARCHAR](11) NULL,
            [ASATCOLevel9Name] [VARCHAR](250) NULL,
            [ASATCOLevel9Unary] [INT] NULL,
            [ASATCOLevel9Sort] [INT] NULL,
            [ASATCOLevel10ID] [VARCHAR](11) NULL,
            [ASATCOLevel10Name] [VARCHAR](250) NULL,
            [ASATCOLevel10Unary] [INT] NULL,
            [ASATCOLevel10Sort] [INT] NULL,
            [ASATCOLevel11ID] [VARCHAR](11) NULL,
            [ASATCOLevel11Name] [VARCHAR](250) NULL,
            [ASATCOLevel11Unary] [INT] NULL,
            [ASATCOLevel11Sort] [INT] NULL,
            [ASATCOLevel12ID] [VARCHAR](11) NULL,
            [ASATCOLevel12Name] [VARCHAR](250) NULL,
            [ASATCOLevel12Unary] [INT] NULL,
            [ASATCOLevel12Sort] [INT] NULL,
            [ASAFieldUnary] [INT] NOT NULL,
            [ASAFieldSort] [INT] NOT NULL,
            [ASAFieldDepth] [INT] NOT NULL,
            [ASAFieldLevel1ID] [VARCHAR](11) NULL,
            [ASAFieldLevel1Name] [VARCHAR](250) NULL,
            [ASAFieldLevel1Unary] [INT] NULL,
            [ASAFieldLevel1Sort] [INT] NULL,
            [ASAFieldLevel2ID] [VARCHAR](11) NULL,
            [ASAFieldLevel2Name] [VARCHAR](250) NULL,
            [ASAFieldLevel2Unary] [INT] NULL,
            [ASAFieldLevel2Sort] [INT] NULL,
            [ASAFieldLevel3ID] [VARCHAR](11) NULL,
            [ASAFieldLevel3Name] [VARCHAR](250) NULL,
            [ASAFieldLevel3Unary] [INT] NULL,
            [ASAFieldLevel3Sort] [INT] NULL,
            [ASAFieldLevel4ID] [VARCHAR](11) NULL,
            [ASAFieldLevel4Name] [VARCHAR](250) NULL,
            [ASAFieldLevel4Unary] [INT] NULL,
            [ASAFieldLevel4Sort] [INT] NULL,
            [ASAFieldLevel5ID] [VARCHAR](11) NULL,
            [ASAFieldLevel5Name] [VARCHAR](250) NULL,
            [ASAFieldLevel5Unary] [INT] NULL,
            [ASAFieldLevel5Sort] [INT] NULL,
            [ASAFieldLevel6ID] [VARCHAR](11) NULL,
            [ASAFieldLevel6Name] [VARCHAR](250) NULL,
            [ASAFieldLevel6Unary] [INT] NULL,
            [ASAFieldLevel6Sort] [INT] NULL,
            [ASAFieldLevel7ID] [VARCHAR](11) NULL,
            [ASAFieldLevel7Name] [VARCHAR](250) NULL,
            [ASAFieldLevel7Unary] [INT] NULL,
            [ASAFieldLevel7Sort] [INT] NULL,
            [ASAFieldLevel8ID] [VARCHAR](11) NULL,
            [ASAFieldLevel8Name] [VARCHAR](250) NULL,
            [ASAFieldLevel8Unary] [INT] NULL,
            [ASAFieldLevel8Sort] [INT] NULL,
            [ASAFieldLevel9ID] [VARCHAR](11) NULL,
            [ASAFieldLevel9Name] [VARCHAR](250) NULL,
            [ASAFieldLevel9Unary] [INT] NULL,
            [ASAFieldLevel9Sort] [INT] NULL,
            [ASAFieldLevel10ID] [VARCHAR](11) NULL,
            [ASAFieldLevel10Name] [VARCHAR](250) NULL,
            [ASAFieldLevel10Unary] [INT] NULL,
            [ASAFieldLevel10Sort] [INT] NULL,
            [ASAFieldLevel11ID] [VARCHAR](11) NULL,
            [ASAFieldLevel11Name] [VARCHAR](250) NULL,
            [ASAFieldLevel11Unary] [INT] NULL,
            [ASAFieldLevel11Sort] [INT] NULL,
            [ASAFieldLevel12ID] [VARCHAR](11) NULL,
            [ASAFieldLevel12Name] [VARCHAR](250) NULL,
            [ASAFieldLevel12Unary] [INT] NULL,
            [ASAFieldLevel12Sort] [INT] NULL,
            [ASAGAUnary] [INT] NOT NULL,
            [ASAGASort] [INT] NOT NULL,
            [ASAGADepth] [INT] NOT NULL,
            [ASAGALevel1ID] [VARCHAR](11) NULL,
            [ASAGALevel1Name] [VARCHAR](250) NULL,
            [ASAGALevel1Unary] [INT] NULL,
            [ASAGALevel1Sort] [INT] NULL,
            [ASAGALevel2ID] [VARCHAR](11) NULL,
            [ASAGALevel2Name] [VARCHAR](250) NULL,
            [ASAGALevel2Unary] [INT] NULL,
            [ASAGALevel2Sort] [INT] NULL,
            [ASAGALevel3ID] [VARCHAR](11) NULL,
            [ASAGALevel3Name] [VARCHAR](250) NULL,
            [ASAGALevel3Unary] [INT] NULL,
            [ASAGALevel3Sort] [INT] NULL,
            [ASAGALevel4ID] [VARCHAR](11) NULL,
            [ASAGALevel4Name] [VARCHAR](250) NULL,
            [ASAGALevel4Unary] [INT] NULL,
            [ASAGALevel4Sort] [INT] NULL,
            [ASAGALevel5ID] [VARCHAR](11) NULL,
            [ASAGALevel5Name] [VARCHAR](250) NULL,
            [ASAGALevel5Unary] [INT] NULL,
            [ASAGALevel5Sort] [INT] NULL,
            [ASAGALevel6ID] [VARCHAR](11) NULL,
            [ASAGALevel6Name] [VARCHAR](250) NULL,
            [ASAGALevel6Unary] [INT] NULL,
            [ASAGALevel6Sort] [INT] NULL,
            [ASAGALevel7ID] [VARCHAR](11) NULL,
            [ASAGALevel7Name] [VARCHAR](250) NULL,
            [ASAGALevel7Unary] [INT] NULL,
            [ASAGALevel7Sort] [INT] NULL,
            [ASAGALevel8ID] [VARCHAR](11) NULL,
            [ASAGALevel8Name] [VARCHAR](250) NULL,
            [ASAGALevel8Unary] [INT] NULL,
            [ASAGALevel8Sort] [INT] NULL,
            [ASATuitionType] [VARCHAR](250) NOT NULL,
            [ASALaborType] [VARCHAR](250) NOT NULL,
            [ASAEBITDAAddbackFlag] [VARCHAR](250) NOT NULL,
            [AccountTypeCode] [VARCHAR](1) NULL,
            [AccountTypeName] [VARCHAR](50) NULL,
            [AccountTypeUnary] [INT] NULL,
            [FieldPath] [VARCHAR](1000) NULL,
            [EDWCreatedDate] [DATETIME2](7) NOT NULL,
            [EDWCreatedBy] [VARCHAR](50) NOT NULL,
            [EDWModifiedDate] [DATETIME2](7) NOT NULL,
            [EDWModifiedBy] [VARCHAR](50) NOT NULL,
			[RowStatus] [CHAR](1) NULL
        );


        -- ================================================================================   
        --
        -- populate the temp table with ASA data
        --
        -- ================================================================================
        INSERT INTO #AccountSubaccount
        EXEC dbo.spGL_StagingTransform_DimAccountSubAccount;

        -- Get how many rows were extracted from source 

        SET @SourceCount =
        (
            SELECT COUNT(1)FROM #AccountSubaccount
        );

        --  Debug output progress
        IF @DebugMode = 1
            SELECT @DebugMsg
                = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Extracted '
                  + CONVERT(NVARCHAR(20), @SourceCount) + N' rows from Source.';
        PRINT @DebugMsg;

        -- ================================================================================   
        --
        -- populate the Landing table from temp table
        --
        -- ================================================================================   
        --truncate the landing table before loading
        TRUNCATE TABLE AccountSubaccountLanding;

        INSERT INTO dbo.AccountSubaccountLanding
        SELECT AccountSubaccountID,
               AccountSubaccountName,
               AccountID,
               AccountName,
               SubaccountID,
               SubaccountName,
               ASATuitionType,
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
               FieldPath,
               EDWCreatedDate AS CreatedDate,
               EDWCreatedBy AS CreatedBy
        FROM #AccountSubaccount;


        -- Get how many rows were extracted from source 

        SET @InsertCount =
        (
            SELECT COUNT(1)FROM dbo.AccountSubaccountLanding
        );

        -- Debug output progress
        IF @DebugMode = 1
        BEGIN
            SELECT @DebugMsg
                = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Inserted '
                  + CONVERT(NVARCHAR(20), @InsertCount) + N' rows into Target.';
            PRINT @DebugMsg;
        END;

        -- ================================================================================
        -- Output Source / Insert / Update / Delete counts to caller, commit the transaction,
        --	and tidy tup.
        --
        -- ================================================================================

        -- Debug output progress
        IF @DebugMode = 1
            SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Committing transaction.';
        PRINT @DebugMsg;

        SET NOCOUNT OFF;
        --
        -- Commit the successful transaction 
        --
        COMMIT TRANSACTION;

        --
        -- Drop the temp table
        --
        DROP TABLE #AccountSubaccount;

        -- Debug output progress
        IF @DebugMode = 1
            SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Completing successfully.';
        PRINT @DebugMsg;
    END TRY
    BEGIN CATCH
        -- Debug output progress
        IF @DebugMode = 1
            SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Rolling back transaction.';
        PRINT @DebugMsg;
        -- Rollback the transaction
        ROLLBACK TRANSACTION;
        --
        -- Raiserror
        --				  
        DECLARE @ErrMsg NVARCHAR(4000),
                @ErrSeverity INT;
        SELECT @ErrMsg = ERROR_MESSAGE(),
               @ErrSeverity = ERROR_SEVERITY();
        RAISERROR(@ErrMsg, @ErrSeverity, 1);
    END CATCH;
END;