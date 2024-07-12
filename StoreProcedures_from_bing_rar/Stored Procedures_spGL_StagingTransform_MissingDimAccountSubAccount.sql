CREATE PROCEDURE [dbo].[spGL_StagingTransform_MissingDimAccountSubAccount] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingTransform_MissingDimAccountSubAccount
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
    --                     EXEC dbo.spGL_StagingTransform_MissingDimAccountSubAccount @EDWRunDateTime
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:                                
    -- ----------
    --
    -- Date        Modified By      Comments
    -- ----        -----------      --------
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
-- this replaces the table formerly returned from the table valued function
               CREATE TABLE #AccSubHierarchy_DimAccountSubaccount
               ([AccountSubaccountID]   [VARCHAR](11) NULL,
               [AccountSubaccountName] [VARCHAR](500) NULL,
               [AccountID]             [VARCHAR](4) NULL,
               [AccountName]           [VARCHAR](250) NULL,
               [SubaccountID]          [VARCHAR](6) NULL,
               [SubaccountName]        [VARCHAR](250) NULL,
               [TCOUnary]              [INT] NULL,
               [TCOSort]               [INT] NULL,
               [TCODepth]              [INT] NULL,
               [TCOLevel1ID]           [VARCHAR](11) NULL,
               [TCOLevel1Name]         [VARCHAR](250) NULL,
               [TCOLevel1Unary]        [INT] NULL,
               [TCOLevel1Sort]         [INT] NULL,
               [TCOLevel2ID]           [VARCHAR](11) NULL,
               [TCOLevel2Name]         [VARCHAR](250) NULL,
               [TCOLevel2Unary]        [INT] NULL,
               [TCOLevel2Sort]         [INT] NULL,
               [TCOLevel3ID]           [VARCHAR](11) NULL,
               [TCOLevel3Name]         [VARCHAR](250) NULL,
               [TCOLevel3Unary]        [INT] NULL,
               [TCOLevel3Sort]         [INT] NULL,
               [TCOLevel4ID]           [VARCHAR](11) NULL,
               [TCOLevel4Name]         [VARCHAR](250) NULL,
               [TCOLevel4Unary]        [INT] NULL,
               [TCOLevel4Sort]         [INT] NULL,
               [TCOLevel5ID]           [VARCHAR](11) NULL,
               [TCOLevel5Name]         [VARCHAR](250) NULL,
               [TCOLevel5Unary]        [INT] NULL,
               [TCOLevel5Sort]         [INT] NULL,
               [TCOLevel6ID]           [VARCHAR](11) NULL,
               [TCOLevel6Name]         [VARCHAR](250) NULL,
               [TCOLevel6Unary]        [INT] NULL,
               [TCOLevel6Sort]         [INT] NULL,
               [TCOLevel7ID]           [VARCHAR](11) NULL,
               [TCOLevel7Name]         [VARCHAR](250) NULL,
               [TCOLevel7Unary]        [INT] NULL,
               [TCOLevel7Sort]         [INT] NULL,
               [TCOLevel8ID]           [VARCHAR](11) NULL,
               [TCOLevel8Name]         [VARCHAR](250) NULL,
               [TCOLevel8Unary]        [INT] NULL,
               [TCOLevel8Sort]         [INT] NULL,
               [TCOLevel9ID]           [VARCHAR](11) NULL,
               [TCOLevel9Name]         [VARCHAR](250) NULL,
               [TCOLevel9Unary]        [INT] NULL,
               [TCOLevel9Sort]         [INT] NULL,
               [TCOLevel10ID]          [VARCHAR](11) NULL,
               [TCOLevel10Name]        [VARCHAR](250) NULL,
               [TCOLevel10Unary]       [INT] NULL,
               [TCOLevel10Sort]        [INT] NULL,
               [TCOLevel11ID]          [VARCHAR](11) NULL,
               [TCOLevel11Name]        [VARCHAR](250) NULL,
               [TCOLevel11Unary]       [INT] NULL,
               [TCOLevel11Sort]        [INT] NULL,
               [TCOLevel12ID]          [VARCHAR](11) NULL,
               [TCOLevel12Name]        [VARCHAR](250) NULL,
               [TCOLevel12Unary]       [INT] NULL,
               [TCOLevel12Sort]        [INT] NULL,
               [FieldUnary]            [INT] NULL,
               [FieldSort]             [INT] NULL,
               [FieldDepth]            [INT] NULL,
               [FieldLevel1ID]         [VARCHAR](11) NULL,
               [FieldLevel1Name]       [VARCHAR](250) NULL,
               [FieldLevel1Unary]      [INT] NULL,
               [FieldLevel1Sort]       [INT] NULL,
               [FieldLevel2ID]         [VARCHAR](11) NULL,
               [FieldLevel2Name]       [VARCHAR](250) NULL,
               [FieldLevel2Unary]      [INT] NULL,
               [FieldLevel2Sort]       [INT] NULL,
               [FieldLevel3ID]         [VARCHAR](11) NULL,
               [FieldLevel3Name]       [VARCHAR](250) NULL,
               [FieldLevel3Unary]      [INT] NULL,
               [FieldLevel3Sort]       [INT] NULL,
               [FieldLevel4ID]         [VARCHAR](11) NULL,
               [FieldLevel4Name]       [VARCHAR](250) NULL,
               [FieldLevel4Unary]      [INT] NULL,
               [FieldLevel4Sort]       [INT] NULL,
               [FieldLevel5ID]         [VARCHAR](11) NULL,
               [FieldLevel5Name]       [VARCHAR](250) NULL,
               [FieldLevel5Unary]      [INT] NULL,
               [FieldLevel5Sort]       [INT] NULL,
               [FieldLevel6ID]         [VARCHAR](11) NULL,
               [FieldLevel6Name]       [VARCHAR](250) NULL,
               [FieldLevel6Unary]      [INT] NULL,
               [FieldLevel6Sort]       [INT] NULL,
               [FieldLevel7ID]         [VARCHAR](11) NULL,
               [FieldLevel7Name]       [VARCHAR](250) NULL,
               [FieldLevel7Unary]      [INT] NULL,
               [FieldLevel7Sort]       [INT] NULL,
               [FieldLevel8ID]         [VARCHAR](11) NULL,
               [FieldLevel8Name]       [VARCHAR](250) NULL,
               [FieldLevel8Unary]      [INT] NULL,
               [FieldLevel8Sort]       [INT] NULL,
               [FieldLevel9ID]         [VARCHAR](11) NULL,
               [FieldLevel9Name]       [VARCHAR](250) NULL,
               [FieldLevel9Unary]      [INT] NULL,
               [FieldLevel9Sort]       [INT] NULL,
               [FieldLevel10ID]        [VARCHAR](11) NULL,
               [FieldLevel10Name]      [VARCHAR](250) NULL,
               [FieldLevel10Unary]     [INT] NULL,
               [FieldLevel10Sort]      [INT] NULL,
               [FieldLevel11ID]        [VARCHAR](11) NULL,
               [FieldLevel11Name]      [VARCHAR](250) NULL,
               [FieldLevel11Unary]     [INT] NULL,
               [FieldLevel11Sort]      [INT] NULL,
               [FieldLevel12ID]        [VARCHAR](11) NULL,
               [FieldLevel12Name]      [VARCHAR](250) NULL,
               [FieldLevel12Unary]     [INT] NULL,
               [FieldLevel12Sort]      [INT] NULL,
               [GAUnary]               [INT] NULL,
               [GASort]                [INT] NULL,
               [GADepth]               [INT] NULL,
               [GALevel1ID]            [VARCHAR](11) NULL,
               [GALevel1Name]          [VARCHAR](250) NULL,
               [GALevel1Unary]         [INT] NULL,
               [GALevel1Sort]          [INT] NULL,
               [GALevel2ID]            [VARCHAR](11) NULL,
               [GALevel2Name]          [VARCHAR](250) NULL,
               [GALevel2Unary]         [INT] NULL,
               [GALevel2Sort]          [INT] NULL,
               [GALevel3ID]            [VARCHAR](11) NULL,
               [GALevel3Name]          [VARCHAR](250) NULL,
               [GALevel3Unary]         [INT] NULL,
               [GALevel3Sort]          [INT] NULL,
               [GALevel4ID]            [VARCHAR](11) NULL,
               [GALevel4Name]          [VARCHAR](250) NULL,
               [GALevel4Unary]         [INT] NULL,
               [GALevel4Sort]          [INT] NULL,
               [GALevel5ID]            [VARCHAR](11) NULL,
               [GALevel5Name]          [VARCHAR](250) NULL,
               [GALevel5Unary]         [INT] NULL,
               [GALevel5Sort]          [INT] NULL,
               [GALevel6ID]            [VARCHAR](11) NULL,
               [GALevel6Name]          [VARCHAR](250) NULL,
               [GALevel6Unary]         [INT] NULL,
               [GALevel6Sort]          [INT] NULL,
               [GALevel7ID]            [VARCHAR](11) NULL,
               [GALevel7Name]          [VARCHAR](250) NULL,
               [GALevel7Unary]         [INT] NULL,
               [GALevel7Sort]          [INT] NULL,
               [GALevel8ID]            [VARCHAR](11) NULL,
               [GALevel8Name]          [VARCHAR](250) NULL,
               [GALevel8Unary]         [INT] NULL,
               [GALevel8Sort]          [INT] NULL,
               [TuitionType]           [VARCHAR](250) NULL,
               [LaborType]             [VARCHAR](250) NULL,
               [ASAEBITDAAddbackFlag]  [VARCHAR](250) NULL,
               [AccountTypeCode]       [VARCHAR](1) NULL,
               [AccountTypeName]       [VARCHAR](50) NULL,
               [AccountTypeUnary]      [INT] NULL
               );




               WITH AccountSubaccountHierarchies AS (
    SELECT ASAHierarchyName = CASE FLEX_VALUE_SET_NAME
                                WHEN 'KCE_TotCo_Acct_Hierarchy'
                                THEN 'TCO'
                                WHEN 'KCE_Field_Acct_Hierarchy'
                                THEN 'Field'
                                WHEN 'KCE_GA_Acct_Hierarchy'
                                THEN 'G&A'
                                ELSE NULL
                            END,
            ASAHierarchyLevel = b.HIERARCHY_LEVEL,
            ParentAccountSubaccountID = a.PARENT_FLEX_VALUE,
            AccountSubaccountID = a.CHILD_FLEX_VALUE_LOW,
            DESCRIPTION
    FROM FndFlexValueHierarchies a
        INNER JOIN vFlexSets b ON a.FLEX_VALUE_SET_ID = b.FLEX_VALUE_SET_ID
                                    AND a.PARENT_FLEX_VALUE = b.FLEX_VALUE
    WHERE FLEX_VALUE_SET_NAME IN('KCE_TotCo_Acct_Hierarchy', 'KCE_Field_Acct_Hierarchy', 'KCE_GA_Acct_Hierarchy')),

AccountSubaccountHierarchyAttributes AS (
    SELECT 
                              ASAHierarchyName = 
                                             CASE FLEX_VALUE_SET_NAME
                WHEN 'KCE_TotCo_Acct_Hierarchy'
                THEN 'TCO'
                WHEN 'KCE_Field_Acct_Hierarchy'
                THEN 'Field'
                WHEN 'KCE_GA_Acct_Hierarchy'
                THEN 'G&A'
                ELSE NULL
            END,
            AccountSubaccountID = FLEX_VALUE,
            AccountSubaccountName = DESCRIPTION,
            SortOrder = ATTRIBUTE1,
            Unary = 
                                                            CASE
                                                                           WHEN ATTRIBUTE2 = 'YES'
                                                                           THEN 1
                                                                           ELSE-1
                END,
            TuitionType = ATTRIBUTE3,
            LaborType = ATTRIBUTE4,
            AdjustedEBITDAAddbackFlag = ATTRIBUTE5,
            EBITDAAddbackFlag = ATTRIBUTE6,
            RowType = 
                                                            CASE
                    WHEN LEFT(FLEX_VALUE, 4) < '9000'
                    THEN 'Account'
                    WHEN LEFT(FLEX_VALUE, 4) = '9999'
                    THEN 'Stub'
                    ELSE 'Rollup'
                END
    FROM vFlexSets
    WHERE FLEX_VALUE_SET_NAME IN('KCE_TotCo_Acct_Hierarchy', 'KCE_Field_Acct_Hierarchy', 'KCE_GA_Acct_Hierarchy')    ),

AccountSubaccountsWithMultipleOperators AS (
    SELECT ASAHierarchyName,
            AccountSubaccountName,
            -1 AS Unary
    FROM AccountSubaccountHierarchyAttributes
    GROUP BY ASAHierarchyName,
            AccountSubaccountName
    HAVING COUNT(DISTINCT Unary) > 1),

AccountSubaccountLevels
   AS (
    SELECT 
                              a.ASAHierarchyName,
        ParentAccountSubaccountID = c.AccountSubaccountID,
        ParentAccountSubaccountName = --'(' + case when coalesce(d.Unary, c.Unary, 1) = 1 then '+' else '-' end + ') ' + --remove this line in production; only for POC            
        c.AccountSubaccountName,
        ParentSortOrder = c.SortOrder,
        ParentUnary = COALESCE(d.Unary, c.Unary),
        b.ASAHierarchyLevel,
        Pivot1 = b.ASAHierarchyLevel + 100,
        Pivot2 = b.ASAHierarchyLevel + 200,
        Pivot3 = b.ASAHierarchyLevel + 300,
        Pivot4 = b.ASAHierarchyLevel + 400,
        a.AccountSubaccountID,
        AccountSubAccountName = --'(' + case when coalesce(a.Unary, 1) = 1 then '+' else '-' end + ') ' + --remove this line in production; only for POC
        a.AccountSubaccountName,
        a.SortOrder,
        a.Unary,
        a.RowType
    FROM AccountSubaccountHierarchyAttributes a
        LEFT JOIN AccountSubaccountHierarchies b ON a.ASAHierarchyName = b.ASAHierarchyName
                                                    AND a.AccountSubaccountID = b.AccountSubaccountID
        LEFT JOIN AccountSubaccountHierarchyAttributes c ON b.ASAHierarchyName = c.ASAHierarchyName
                                                            AND b.ParentAccountSubaccountID = c.AccountSubaccountID
        LEFT JOIN AccountSubaccountsWithMultipleOperators d ON c.ASAHierarchyName = d.ASAHierarchyName
                                                                AND c.AccountSubaccountName = d.AccountSubaccountName) -- ,
-- AccountSubaccountFlattenedHierarchies AS (
-- Changing this to a temp table instead of a CTE for performance reasons 

               SELECT ASAHierarchyName,
                                             AccountSubaccountID,
                                             SortOrder,
                                             Unary,
                                             Level1ID = MIN([101]),
                                             Level1Name = MIN([201]),
                                             Level1Unary = MIN([301]),
                                             Level1Sort = MIN([401]),
                                             Level2ID = MIN([102]),
                                             Level2Name = MIN([202]),
                                             Level2Unary = MIN([302]),
                                             Level2Sort = MIN([402]),
                                             Level3ID = MIN([103]),
                                             Level3Name = MIN([203]),
                                             Level3Unary = MIN([303]),
                                             Level3Sort = MIN([403]),
                                             Level4ID = MIN([104]),
                                             Level4Name = MIN([204]),
                                             Level4Unary = MIN([304]),
                                             Level4Sort = MIN([404]),
                                             Level5ID = MIN([105]),
                                             Level5Name = MIN([205]),
                                             Level5Unary = MIN([305]),
                                             Level5Sort = MIN([405]),
                                             Level6ID = MIN([106]),
                                             Level6Name = MIN([206]),
                                             Level6Unary = MIN([306]),
                                             Level6Sort = MIN([406]),
                                             Level7ID = MIN([107]),
                                             Level7Name = MIN([207]),
                                             Level7Unary = MIN([307]),
                                             Level7Sort = MIN([407]),
                                             Level8ID = MIN([108]),
                                             Level8Name = MIN([208]),
                                             Level8Unary = MIN([308]),
                                             Level8Sort = MIN([408]),
                                             Level9ID = MIN([109]),
                                             Level9Name = MIN([209]),
                                             Level9Unary = MIN([309]),
                                             Level9Sort = MIN([409]),
                                             Level10ID = MIN([110]),
                                             Level10Name = MIN([210]),
                                             Level10Unary = MIN([310]),
                                             Level10Sort = MIN([410]),
                                             Level11ID = MIN([111]),
                                             Level11Name = MIN([211]),
                                             Level11Unary = MIN([311]),
                                             Level11Sort = MIN([411]),
                                             Level12ID = MIN([112]),
                                             Level12Name = MIN([212]),
                                             Level12Unary = MIN([312]),
                                             Level12Sort = MIN([412])
               into #AccountSubaccountFlattenedHierarchies
               FROM AccountSubaccountLevels 
                              PIVOT(MIN(ParentAccountSubaccountID) FOR PIVOT1 IN(
                                             [101],
                                             [102],
                                             [103],
                                             [104],
                                             [105],
                                             [106],
                                             [107],
                                             [108],
                                             [109],
                                             [110],
                                             [111],
                                             [112])) p 
                              PIVOT(MIN(ParentAccountSubaccountName) FOR PIVOT2 IN(
                                             [201],
                                             [202],
                                             [203],
                                             [204],
                                             [205],
                                             [206],
                                             [207],
                                             [208],
                                             [209],
                                             [210],
                                             [211],
                                             [212])) p 
                              PIVOT(MIN(ParentUnary) FOR PIVOT3 IN(
                                             [301],
                                             [302],
                                             [303],
                                             [304],
                                             [305],
                                             [306],
                                             [307],
                                             [308],
                                             [309],
                                             [310],
                                             [311],
                                             [312])) p 
                              PIVOT(MIN(ParentSortOrder) FOR PIVOT4 IN(
                                             [401],
                                             [402],
                                             [403],
                                             [404],
                                             [405],
                                             [406],
                                             [407],
                                             [408],
                                             [409],
                                             [410],
                                             [411],
                                             [412])) p
               WHERE RowType <> 'Rollup'
               GROUP BY 
                              ASAHierarchyName,
        AccountSubaccountID,
        SortOrder,
        Unary -- ),
;


WITH AccountSubaccountHierarchyAttributes AS (
    SELECT 
                              ASAHierarchyName = 
                                             CASE FLEX_VALUE_SET_NAME
                WHEN 'KCE_TotCo_Acct_Hierarchy'
                THEN 'TCO'
                WHEN 'KCE_Field_Acct_Hierarchy'
                THEN 'Field'
                WHEN 'KCE_GA_Acct_Hierarchy'
                THEN 'G&A'
                ELSE NULL
            END,
            AccountSubaccountID = FLEX_VALUE,
            AccountSubaccountName = DESCRIPTION,
            SortOrder = ATTRIBUTE1,
            Unary = 
                                                            CASE
                                                                           WHEN ATTRIBUTE2 = 'YES'
                                                                           THEN 1
                                                                           ELSE-1
                END,
            TuitionType = ATTRIBUTE3,
            LaborType = ATTRIBUTE4,
            AdjustedEBITDAAddbackFlag = ATTRIBUTE5,
            EBITDAAddbackFlag = ATTRIBUTE6,
            RowType = 
                                                            CASE
                    WHEN LEFT(FLEX_VALUE, 4) < '9000'
                    THEN 'Account'
                    WHEN LEFT(FLEX_VALUE, 4) = '9999'
                    THEN 'Stub'
                    ELSE 'Rollup'
                END
    FROM vFlexSets
    WHERE FLEX_VALUE_SET_NAME IN('KCE_TotCo_Acct_Hierarchy', 'KCE_Field_Acct_Hierarchy', 'KCE_GA_Acct_Hierarchy')    ),

Accounts AS (
    SELECT AccountID = FLEX_VALUE,
            AccountName = DESCRIPTION
    FROM vFlexSets
    WHERE FLEX_VALUE_SET_NAME = 'KLC Account Value Set'),

Subaccounts AS (
    SELECT SubaccountID = FLEX_VALUE,
            SubaccountName = DESCRIPTION
    FROM vFlexSets
    WHERE FLEX_VALUE_SET_NAME = 'KLC Future 1 Value Set'),

AccountSubaccountIDs    AS (
    SELECT DISTINCT
            [AccountSubaccountID],
            [AccountID],
            [SubaccountID],
            NULL AS AccountTypeCode,
            NULL AS AccountTypeName,
            NULL AS AccountTypeUnary
    FROM [dbo].[InfAccountSubaccount]),

TCOHierarchy AS (
               SELECT *
               FROM #AccountSubaccountFlattenedHierarchies
               WHERE ASAHierarchyName = 'TCO'),

FieldHierarchy AS (
               SELECT *
               FROM #AccountSubaccountFlattenedHierarchies
               WHERE ASAHierarchyName = 'Field'),

GAHierarchy AS (
    SELECT *
    FROM #AccountSubaccountFlattenedHierarchies
    WHERE ASAHierarchyName = 'G&A')

INSERT INTO #AccSubHierarchy_DimAccountSubaccount -- instead of the old TFN table returned we create this temp table
SELECT a.AccountSubaccountID,
    AccountSubaccountName = --'(' + coalesce(e.Unary, '~') + ') ' + --remove this line for production; only for POC
    a.AccountSubaccountID + 
                              CASE
                                             WHEN RowType = 'Stub'
                                             THEN ' - '+g.AccountSubaccountName
                                             ELSE CASE
                                                                           WHEN AccountName IS NULL
                                                                                          OR SubAccountName IS NULL
                                                                           THEN ''
                                                                           ELSE ' - '+AccountName+' : '+SubaccountName
                                                            END
                              END,
    a.AccountID,
    AccountName = 
                              CASE
                                             WHEN RowType = 'Stub'
                                             THEN 'STUB ACCOUNTS'
                                             ELSE b.AccountName
        END,
    a.SubaccountID,
    c.SubaccountName,
    COALESCE(d.Unary, 1),
    COALESCE(d.SortOrder, 99999),
    Depth = NULL,
    Level1ID = COALESCE(d.Level1ID, '0000.000000'),
    Level1Name = COALESCE(d.Level1Name, 'Unknown TCO Account Group'),
    Level1Unary = COALESCE(d.Level1Unary, 1),
    Level1Sort = COALESCE(d.Level1Sort, 99999),
    Level2ID = COALESCE(d.Level2ID, '0000.000000'),
    Level2Name = COALESCE(d.Level2Name, 'Unknown TCO Account Group'),
    Level2Unary = COALESCE(d.Level2Unary, 1),
    Level2Sort = COALESCE(d.Level2Sort, 99999),
    Level3ID = COALESCE(d.Level3ID, '0000.000000'),
    Level3Name = COALESCE(d.Level3Name, 'Unknown TCO Account Group'),
    Level3Unary = COALESCE(d.Level3Unary, 1),
    Level3Sort = COALESCE(d.Level3Sort, 99999),
    Level4ID = COALESCE(d.Level4ID, '0000.000000'),
    Level4Name = COALESCE(d.Level4Name, 'Unknown TCO Account Group'),
    Level4Unary = COALESCE(d.Level4Unary, 1),
    Level4Sort = COALESCE(d.Level4Sort, 99999),
    Level5ID = COALESCE(d.Level5ID, '0000.000000'),
    Level5Name = COALESCE(d.Level5Name, 'Unknown TCO Account Group'),
    Level5Unary = COALESCE(d.Level5Unary, 1),
    Level5Sort = COALESCE(d.Level5Sort, 99999),
    Level6ID = COALESCE(d.Level6ID, '0000.000000'),
    Level6Name = COALESCE(d.Level6Name, 'Unknown TCO Account Group'),
    Level6Unary = COALESCE(d.Level6Unary, 1),
    Level6Sort = COALESCE(d.Level6Sort, 99999),
    Level7ID = COALESCE(d.Level7ID, '0000.000000'),
    Level7Name = COALESCE(d.Level7Name, 'Unknown TCO Account Group'),
    Level7Unary = COALESCE(d.Level7Unary, 1),
    Level7Sort = COALESCE(d.Level7Sort, 99999),
    Level8ID = COALESCE(d.Level8ID, '0000.000000'),
    Level8Name = COALESCE(d.Level8Name, 'Unknown TCO Account Group'),
    Level8Unary = COALESCE(d.Level8Unary, 1),
    Level8Sort = COALESCE(d.Level8Sort, 99999),
    Level9ID = COALESCE(d.Level9ID, '0000.000000'),
    Level9Name = COALESCE(d.Level9Name, 'Unknown TCO Account Group'),
    Level9Unary = COALESCE(d.Level9Unary, 1),
    Level9Sort = COALESCE(d.Level9Sort, 99999),
    Level10ID = COALESCE(d.Level10ID, '0000.000000'),
    Level10Name = COALESCE(d.Level10Name, 'Unknown TCO Account Group'),
    Level10Unary = COALESCE(d.Level10Unary, 1),
    Level10Sort = COALESCE(d.Level10Sort, 99999),
    Level11ID = COALESCE(d.Level11ID, '0000.000000'),
    Level11Name = COALESCE(d.Level11Name, 'Unknown TCO Account Group'),
    Level11Unary = COALESCE(d.Level11Unary, 1),
    Level11Sort = COALESCE(d.Level11Sort, 99999),
    Level12ID = COALESCE(d.Level12ID, '0000.000000'),
    Level12Name = COALESCE(d.Level12Name, 'Unknown TCO Account Group'),
    Level12Unary = COALESCE(d.Level12Unary, 1),
    Level12Sort = COALESCE(d.Level12Sort, 99999),
    COALESCE(e.Unary, 1),
    COALESCE(e.SortOrder, 99999),
    Depth = NULL,
    Level1ID = COALESCE(e.Level1ID, '0000.000000'),
    Level1Name = COALESCE(e.Level1Name, 'Unknown Field Account Group'),
    Level1Unary = COALESCE(e.Level1Unary, 1),
    Level1Sort = COALESCE(e.Level1Sort, 99999),
    Level2ID = COALESCE(e.Level2ID, '0000.000000'),
    Level2Name = COALESCE(e.Level2Name, 'Unknown Field Account Group'),
    Level2Unary = COALESCE(e.Level2Unary, 1),
    Level2Sort = COALESCE(e.Level2Sort, 99999),
    Level3ID = COALESCE(e.Level3ID, '0000.000000'),
    Level3Name = COALESCE(e.Level3Name, 'Unknown Field Account Group'),
    Level3Unary = COALESCE(e.Level3Unary, 1),
    Level3Sort = COALESCE(e.Level3Sort, 99999),
    Level4ID = COALESCE(e.Level4ID, '0000.000000'),
    Level4Name = COALESCE(e.Level4Name, 'Unknown Field Account Group'),
    Level4Unary = COALESCE(e.Level4Unary, 1),
    Level4Sort = COALESCE(e.Level4Sort, 99999),
    Level5ID = COALESCE(e.Level5ID, '0000.000000'),
    Level5Name = COALESCE(e.Level5Name, 'Unknown Field Account Group'),
    Level5Unary = COALESCE(e.Level5Unary, 1),
    Level5Sort = COALESCE(e.Level5Sort, 99999),
    Level6ID = COALESCE(e.Level6ID, '0000.000000'),
    Level6Name = COALESCE(e.Level6Name, 'Unknown Field Account Group'),
    Level6Unary = COALESCE(e.Level6Unary, 1),
    Level6Sort = COALESCE(e.Level6Sort, 99999),
    Level7ID = COALESCE(e.Level7ID, '0000.000000'),
    Level7Name = COALESCE(e.Level7Name, 'Unknown Field Account Group'),
    Level7Unary = COALESCE(e.Level7Unary, 1),
    Level7Sort = COALESCE(e.Level7Sort, 99999),
    Level8ID = COALESCE(e.Level8ID, '0000.000000'),
    Level8Name = COALESCE(e.Level8Name, 'Unknown Field Account Group'),
    Level8Unary = COALESCE(e.Level8Unary, 1),
    Level8Sort = COALESCE(e.Level8Sort, 99999),
    Level9ID = COALESCE(e.Level9ID, '0000.000000'),
    Level9Name = COALESCE(e.Level9Name, 'Unknown Field Account Group'),
    Level9Unary = COALESCE(e.Level9Unary, 1),
    Level9Sort = COALESCE(e.Level9Sort, 99999),
    Level10ID = COALESCE(e.Level10ID, '0000.000000'),
    Level10Name = COALESCE(e.Level10Name, 'Unknown Field Account Group'),
    Level10Unary = COALESCE(e.Level10Unary, 1),
    Level10Sort = COALESCE(e.Level10Sort, 99999),
    Level11ID = COALESCE(e.Level11ID, '0000.000000'),
    Level11Name = COALESCE(e.Level11Name, 'Unknown Field Account Group'),
    Level11Unary = COALESCE(e.Level11Unary, 1),
    Level11Sort = COALESCE(e.Level11Sort, 99999),
    Level12ID = COALESCE(e.Level12ID, '0000.000000'),
    Level12Name = COALESCE(e.Level12Name, 'Unknown Field Account Group'),
    Level12Unary = COALESCE(e.Level12Unary, 1),
    Level12Sort = COALESCE(e.Level12Sort, 99999),
    COALESCE(f.Unary, 1),
    COALESCE(f.SortOrder, 99999),
    Depth = NULL,
    Level1ID = COALESCE(f.Level1ID, '0000.000000'),
    Level1Name = COALESCE(f.Level1Name, 'Unknown G&A Account Group'),
    Level1Unary = COALESCE(f.Level1Unary, 1),
    Level1Sort = COALESCE(f.Level1Sort, 99999),
    Level2ID = COALESCE(f.Level2ID, '0000.000000'),
    Level2Name = COALESCE(f.Level2Name, 'Unknown G&A Account Group'),
    Level2Unary = COALESCE(f.Level2Unary, 1),
    Level2Sort = COALESCE(f.Level2Sort, 99999),
    Level3ID = COALESCE(f.Level3ID, '0000.000000'),
    Level3Name = COALESCE(f.Level3Name, 'Unknown G&A Account Group'),
    Level3Unary = COALESCE(f.Level3Unary, 1),
    Level3Sort = COALESCE(f.Level3Sort, 99999),
    Level4ID = COALESCE(f.Level4ID, '0000.000000'),
    Level4Name = COALESCE(f.Level4Name, 'Unknown G&A Account Group'),
    Level4Unary = COALESCE(f.Level4Unary, 1),
    Level4Sort = COALESCE(f.Level4Sort, 99999),
    Level5ID = COALESCE(f.Level5ID, '0000.000000'),
    Level5Name = COALESCE(f.Level5Name, 'Unknown G&A Account Group'),
    Level5Unary = COALESCE(f.Level5Unary, 1),
    Level5Sort = COALESCE(f.Level5Sort, 99999),
    Level6ID = COALESCE(f.Level6ID, '0000.000000'),
    Level6Name = COALESCE(f.Level6Name, 'Unknown G&A Account Group'),
    Level6Unary = COALESCE(f.Level6Unary, 1),
    Level6Sort = COALESCE(f.Level6Sort, 99999),
    Level7ID = COALESCE(f.Level7ID, '0000.000000'),
    Level7Name = COALESCE(f.Level7Name, 'Unknown G&A Account Group'),
    Level7Unary = COALESCE(f.Level7Unary, 1),
    Level7Sort = COALESCE(f.Level7Sort, 99999),
    Level8ID = COALESCE(f.Level8ID, '0000.000000'),
    Level8Name = COALESCE(f.Level8Name, 'Unknown G&A Account Group'),
    Level8Unary = COALESCE(f.Level8Unary, 1),
    Level8Sort = COALESCE(f.Level8Sort, 99999),
    TuitionType,
    LaborType,
    EBITDAAddbackFlag,
    AccountTypeCode,
    AccountTypeName,
    AccountTypeUnary
FROM AccountSubaccountIDs a
    LEFT JOIN Accounts b ON a.AccountID = b.AccountID
    LEFT JOIN Subaccounts c ON a.SubaccountID = c.SubaccountID
    LEFT JOIN TCOHierarchy d ON a.AccountSubaccountID = d.AccountSubaccountID
    LEFT JOIN FieldHierarchy e ON a.AccountSubaccountID = e.AccountSubaccountID
    LEFT JOIN GAHierarchy f ON a.AccountSubaccountID = f.AccountSubaccountID
    LEFT JOIN AccountSubaccountHierarchyAttributes g ON a.AccountSubaccountID = g.AccountSubaccountID
                                                        AND g.ASAHierarchyName = 'TCO'
WHERE a.AccountSubaccountID NOT LIKE '%[A-z]%'
ORDER BY 1;

         -- Repeat this whole set of update statements once for each Level in each Hierarchy.  
         -- This "defrags" the groupings for consumption in SSAS
         DECLARE @Levels INT= 0;
         WHILE @Levels < 13
             BEGIN
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       TCOLevel12Name = AccountSubaccountName,
                       TCOLevel12ID = AccountSubaccountID,
                       TCOLevel12Unary = TCOUnary,
                       TCOLevel12Sort = TCOSort
                 WHERE TCOLevel11Name = TCOLevel12Name
                       OR TCOLevel12ID = '0000.000000';
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       TCOLevel11Name = TCOLevel12Name,
                       TCOLevel11ID = TCOLevel12ID,
                       TCOLevel11Unary = TCOLevel12Unary,
                       TCOLevel11Sort = TCOLevel12Sort
                 WHERE TCOLevel10Name = TCOLevel11Name
                       OR TCOLevel11ID = '0000.000000';
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       TCOLevel10Name = TCOLevel11Name,
                       TCOLevel10ID = TCOLevel11ID,
                       TCOLevel10Unary = TCOLevel11Unary,
                       TCOLevel10Sort = TCOLevel11Sort
                 WHERE TCOLevel9Name = TCOLevel10Name
                       OR TCOLevel10ID = '0000.000000';
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       TCOLevel9Name = TCOLevel10Name,
                       TCOLevel9ID = TCOLevel10ID,
                       TCOLevel9Unary = TCOLevel10Unary,
                       TCOLevel9Sort = TCOLevel10Sort
                 WHERE TCOLevel8Name = TCOLevel9Name
                       OR TCOLevel9ID = '0000.000000';
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       TCOLevel8Name = TCOLevel9Name,
                       TCOLevel8ID = TCOLevel9ID,
                       TCOLevel8Unary = TCOLevel9Unary,
                       TCOLevel8Sort = TCOLevel9Sort
                 WHERE TCOLevel7Name = TCOLevel8Name
                       OR TCOLevel8ID = '0000.000000';
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       TCOLevel7Name = TCOLevel8Name,
                       TCOLevel7ID = TCOLevel8ID,
                       TCOLevel7Unary = TCOLevel8Unary,
                       TCOLevel7Sort = TCOLevel8Sort
                 WHERE TCOLevel6Name = TCOLevel7Name
                       OR TCOLevel7ID = '0000.000000';
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       TCOLevel6Name = TCOLevel7Name,
                       TCOLevel6ID = TCOLevel7ID,
                       TCOLevel6Unary = TCOLevel7Unary,
                       TCOLevel6Sort = TCOLevel7Sort
                 WHERE TCOLevel5Name = TCOLevel6Name
                       OR TCOLevel6ID = '0000.000000';
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       TCOLevel5Name = TCOLevel6Name,
                       TCOLevel5ID = TCOLevel6ID,
                       TCOLevel5Unary = TCOLevel6Unary,
                       TCOLevel5Sort = TCOLevel6Sort
                 WHERE TCOLevel4Name = TCOLevel5Name
                       OR TCOLevel5ID = '0000.000000';
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       TCOLevel4Name = TCOLevel5Name,
                       TCOLevel4ID = TCOLevel5ID,
                       TCOLevel4Unary = TCOLevel5Unary,
                       TCOLevel4Sort = TCOLevel5Sort
                 WHERE TCOLevel3Name = TCOLevel4Name
                       OR TCOLevel4ID = '0000.000000';
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       TCOLevel3Name = TCOLevel4Name,
                       TCOLevel3ID = TCOLevel4ID,
                       TCOLevel3Unary = TCOLevel4Unary,
                       TCOLevel3Sort = TCOLevel4Sort
                 WHERE TCOLevel2Name = TCOLevel3Name
                       OR TCOLevel3ID = '0000.000000';
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       TCOLevel2Name = TCOLevel3Name,
                       TCOLevel2ID = TCOLevel3ID,
                       TCOLevel2Unary = TCOLevel3Unary,
                       TCOLevel2Sort = TCOLevel3Sort
                 WHERE TCOLevel1Name = TCOLevel2Name
                       OR TCOLevel2ID = '0000.000000';
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       FieldLevel12Name = AccountSubaccountName,
                       FieldLevel12ID = AccountSubaccountID,
                       FieldLevel12Unary = FieldUnary,
                       FieldLevel12Sort = FieldSort
                 WHERE FieldLevel11Name = FieldLevel12Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       FieldLevel11Name = FieldLevel12Name,
                       FieldLevel11ID = FieldLevel12ID,
                       FieldLevel11Unary = FieldLevel12Unary,
                       FieldLevel11Sort = FieldLevel12Sort
                 WHERE FieldLevel10Name = FieldLevel11Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       FieldLevel10Name = FieldLevel11Name,
                       FieldLevel10ID = FieldLevel11ID,
                       FieldLevel10Unary = FieldLevel11Unary,
                       FieldLevel10Sort = FieldLevel11Sort
                 WHERE FieldLevel9Name = FieldLevel10Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       FieldLevel9Name = FieldLevel10Name,
                       FieldLevel9ID = FieldLevel10ID,
                       FieldLevel9Unary = FieldLevel10Unary,
                       FieldLevel9Sort = FieldLevel10Sort
                 WHERE FieldLevel8Name = FieldLevel9Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       FieldLevel8Name = FieldLevel9Name,
                       FieldLevel8ID = FieldLevel9ID,
                      FieldLevel8Unary = FieldLevel9Unary,
                       FieldLevel8Sort = FieldLevel9Sort
                 WHERE FieldLevel7Name = FieldLevel8Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       FieldLevel7Name = FieldLevel8Name,
                       FieldLevel7ID = FieldLevel8ID,
                       FieldLevel7Unary = FieldLevel8Unary,
                       FieldLevel7Sort = FieldLevel8Sort
                 WHERE FieldLevel6Name = FieldLevel7Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       FieldLevel6Name = FieldLevel7Name,
                       FieldLevel6ID = FieldLevel7ID,
                       FieldLevel6Unary = FieldLevel7Unary,
                       FieldLevel6Sort = FieldLevel7Sort
                 WHERE FieldLevel5Name = FieldLevel6Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       FieldLevel5Name = FieldLevel6Name,
                       FieldLevel5ID = FieldLevel6ID,
                       FieldLevel5Unary = FieldLevel6Unary,
                       FieldLevel5Sort = FieldLevel6Sort
                 WHERE FieldLevel4Name = FieldLevel5Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       FieldLevel4Name = FieldLevel5Name,
                       FieldLevel4ID = FieldLevel5ID,
                       FieldLevel4Unary = FieldLevel5Unary,
                       FieldLevel4Sort = FieldLevel5Sort
                 WHERE FieldLevel3Name = FieldLevel4Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       FieldLevel3Name = FieldLevel4Name,
                       FieldLevel3ID = FieldLevel4ID,
                       FieldLevel3Unary = FieldLevel4Unary,
                       FieldLevel3Sort = FieldLevel4Sort
                 WHERE FieldLevel2Name = FieldLevel3Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       FieldLevel2Name = FieldLevel3Name,
                       FieldLevel2ID = FieldLevel3ID,
                       FieldLevel2Unary = FieldLevel3Unary,
                       FieldLevel2Sort = FieldLevel3Sort
                 WHERE FieldLevel1Name = FieldLevel2Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       GALevel8Name = AccountSubaccountName,
                       GALevel8ID = AccountSubaccountID,
                       GALevel8Unary = GAUnary,
                       GALevel8Sort = GASort
                 WHERE GALevel7Name = GALevel8Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       GALevel7Name = GALevel8Name,
                       GALevel7ID = GALevel8ID,
                       GALevel7Unary = GALevel8Unary,
                       GALevel7Sort = GALevel8Sort
                 WHERE GALevel6Name = GALevel7Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       GALevel6Name = GALevel7Name,
                       GALevel6ID = GALevel7ID,
                       GALevel6Unary = GALevel7Unary,
                       GALevel6Sort = GALevel7Sort
                 WHERE GALevel5Name = GALevel6Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       GALevel5Name = GALevel6Name,
                       GALevel5ID = GALevel6ID,
                       GALevel5Unary = GALevel6Unary,
                       GALevel5Sort = GALevel6Sort
                 WHERE GALevel4Name = GALevel5Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       GALevel4Name = GALevel5Name,
                       GALevel4ID = GALevel5ID,
                       GALevel4Unary = GALevel5Unary,
                       GALevel4Sort = GALevel5Sort
                 WHERE GALevel3Name = GALevel4Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       GALevel3Name = GALevel4Name,
                       GALevel3ID = GALevel4ID,
                       GALevel3Unary = GALevel4Unary,
                       GALevel3Sort = GALevel4Sort
                 WHERE GALevel2Name = GALevel3Name;
                 UPDATE #AccSubHierarchy_DimAccountSubaccount
                   SET
                       GALevel2Name = GALevel3Name,
                       GALevel2ID = GALevel3ID,
                       GALevel2Unary = GALevel3Unary,
                       GALevel2Sort = GALevel3Sort
                 WHERE GALevel1Name = GALevel2Name;
                 SET @Levels = @Levels + 1;
             END;

         -- Set proper unary operators for all levels, for use in SSAS downstream
         UPDATE a
           SET
               a.TCODepth = b.TCODepth,
               TCOLevel12Unary = TCOUnary * TCOLevel12Unary,
               TCOLevel11Unary = TCOUnary * TCOLevel12Unary * TCOLevel11Unary,
               TCOLevel10Unary = TCOUnary * TCOLevel12Unary * TCOLevel11Unary * TCOLevel10Unary,
               TCOLevel9Unary = TCOUnary * TCOLevel12Unary * TCOLevel11Unary * TCOLevel10Unary * TCOLevel9Unary,
               TCOLevel8Unary = TCOUnary * TCOLevel12Unary * TCOLevel11Unary * TCOLevel10Unary * TCOLevel9Unary * TCOLevel8Unary,
               TCOLevel7Unary = TCOUnary * TCOLevel12Unary * TCOLevel11Unary * TCOLevel10Unary * TCOLevel9Unary * TCOLevel8Unary * TCOLevel7Unary,
               TCOLevel6Unary = TCOUnary * TCOLevel12Unary * TCOLevel11Unary * TCOLevel10Unary * TCOLevel9Unary * TCOLevel8Unary * TCOLevel7Unary * TCOLevel6Unary,
               TCOLevel5Unary = TCOUnary * TCOLevel12Unary * TCOLevel11Unary * TCOLevel10Unary * TCOLevel9Unary * TCOLevel8Unary * TCOLevel7Unary * TCOLevel6Unary * TCOLevel5Unary,
               TCOLevel4Unary = TCOUnary * TCOLevel12Unary * TCOLevel11Unary * TCOLevel10Unary * TCOLevel9Unary * TCOLevel8Unary * TCOLevel7Unary * TCOLevel6Unary * TCOLevel5Unary * TCOLevel4Unary,
               TCOLevel3Unary = TCOUnary * TCOLevel12Unary * TCOLevel11Unary * TCOLevel10Unary * TCOLevel9Unary * TCOLevel8Unary * TCOLevel7Unary * TCOLevel6Unary * TCOLevel5Unary * TCOLevel4Unary * TCOLevel3Unary,
               TCOLevel2Unary = TCOUnary * TCOLevel12Unary * TCOLevel11Unary * TCOLevel10Unary * TCOLevel9Unary * TCOLevel8Unary * TCOLevel7Unary * TCOLevel6Unary * TCOLevel5Unary * TCOLevel4Unary * TCOLevel3Unary * TCOLevel2Unary,
               TCOLevel1Unary = TCOUnary * TCOLevel12Unary * TCOLevel11Unary * TCOLevel10Unary * TCOLevel9Unary * TCOLevel8Unary * TCOLevel7Unary * TCOLevel6Unary * TCOLevel5Unary * TCOLevel4Unary * TCOLevel3Unary * TCOLevel2Unary * TCOLevel1Unary
         FROM #AccSubHierarchy_DimAccountSubaccount a
              INNER JOIN
         (
             SELECT AccountSubaccountID,
                    TCODepth = COUNT(DISTINCT ID)
             FROM #AccSubHierarchy_DimAccountSubaccount a UNPIVOT(ID FOR Thing IN(TCOLevel1ID,
                                                                TCOLevel2ID,
                                                                TCOLevel3ID,
                                                                TCOLevel4ID,
                                                                TCOLevel5ID,
                                                                TCOLevel6ID,
                                                                TCOLevel7ID,
                                                                TCOLevel8ID,
                                                                TCOLevel9ID,
                                                                TCOLevel10ID,
                                                                TCOLevel11ID,
                                                                TCOLevel12ID)) u
             GROUP BY AccountSubaccountID
         ) b ON a.AccountSubaccountID = b.AccountSubaccountID;
         UPDATE a
           SET
               a.FieldDepth = b.FieldDepth,
              FieldLevel12Unary = FieldUnary * FieldLevel12Unary,
               FieldLevel11Unary = FieldUnary * FieldLevel12Unary * FieldLevel11Unary,
               FieldLevel10Unary = FieldUnary * FieldLevel12Unary * FieldLevel11Unary * FieldLevel10Unary,
               FieldLevel9Unary = FieldUnary * FieldLevel12Unary * FieldLevel11Unary * FieldLevel10Unary * FieldLevel9Unary,
               FieldLevel8Unary = FieldUnary * FieldLevel12Unary * FieldLevel11Unary * FieldLevel10Unary * FieldLevel9Unary * FieldLevel8Unary,
               FieldLevel7Unary = FieldUnary * FieldLevel12Unary * FieldLevel11Unary * FieldLevel10Unary * FieldLevel9Unary * FieldLevel8Unary * FieldLevel7Unary,
               FieldLevel6Unary = FieldUnary * FieldLevel12Unary * FieldLevel11Unary * FieldLevel10Unary * FieldLevel9Unary * FieldLevel8Unary * FieldLevel7Unary * FieldLevel6Unary,
               FieldLevel5Unary = FieldUnary * FieldLevel12Unary * FieldLevel11Unary * FieldLevel10Unary * FieldLevel9Unary * FieldLevel8Unary * FieldLevel7Unary * FieldLevel6Unary * FieldLevel5Unary,
               FieldLevel4Unary = FieldUnary * FieldLevel12Unary * FieldLevel11Unary * FieldLevel10Unary * FieldLevel9Unary * FieldLevel8Unary * FieldLevel7Unary * FieldLevel6Unary * FieldLevel5Unary * FieldLevel4Unary,
               FieldLevel3Unary = FieldUnary * FieldLevel12Unary * FieldLevel11Unary * FieldLevel10Unary * FieldLevel9Unary * FieldLevel8Unary * FieldLevel7Unary * FieldLevel6Unary * FieldLevel5Unary * FieldLevel4Unary * FieldLevel3Unary,
               FieldLevel2Unary = FieldUnary * FieldLevel12Unary * FieldLevel11Unary * FieldLevel10Unary * FieldLevel9Unary * FieldLevel8Unary * FieldLevel7Unary * FieldLevel6Unary * FieldLevel5Unary * FieldLevel4Unary * FieldLevel3Unary * FieldLevel2Unary,
               FieldLevel1Unary = FieldUnary * FieldLevel12Unary * FieldLevel11Unary * FieldLevel10Unary * FieldLevel9Unary * FieldLevel8Unary * FieldLevel7Unary * FieldLevel6Unary * FieldLevel5Unary * FieldLevel4Unary * FieldLevel3Unary * FieldLevel2Unary * FieldLevel1Unary
         FROM #AccSubHierarchy_DimAccountSubaccount a
              INNER JOIN
         (
             SELECT AccountSubaccountID,
                    FieldDepth = COUNT(DISTINCT ID)
             FROM #AccSubHierarchy_DimAccountSubaccount a UNPIVOT(ID FOR Thing IN(FieldLevel1ID,
                                                                FieldLevel2ID,
                                                                FieldLevel3ID,
                                                                FieldLevel4ID,
                                                                FieldLevel5ID,
                                                                FieldLevel6ID,
                                                                FieldLevel7ID,
                                                                FieldLevel8ID,
                                                                FieldLevel9ID,
                                                                FieldLevel10ID,
                                                                FieldLevel11ID,
                                                                FieldLevel12ID)) u
             GROUP BY AccountSubaccountID
         ) b ON a.AccountSubaccountID = b.AccountSubaccountID;
         UPDATE a
           SET
               a.GADepth = b.GADepth,
               GALevel8Unary = GAUnary * GALevel8Unary,
               GALevel7Unary = GAUnary * GALevel8Unary * GALevel7Unary,
               GALevel6Unary = GAUnary * GALevel8Unary * GALevel7Unary * GALevel6Unary,
               GALevel5Unary = GAUnary * GALevel8Unary * GALevel7Unary * GALevel6Unary * GALevel5Unary,
               GALevel4Unary = GAUnary * GALevel8Unary * GALevel7Unary * GALevel6Unary * GALevel5Unary * GALevel4Unary,
               GALevel3Unary = GAUnary * GALevel8Unary * GALevel7Unary * GALevel6Unary * GALevel5Unary * GALevel4Unary * GALevel3Unary,
               GALevel2Unary = GAUnary * GALevel8Unary * GALevel7Unary * GALevel6Unary * GALevel5Unary * GALevel4Unary * GALevel3Unary * GALevel2Unary,
               GALevel1Unary = GAUnary * GALevel8Unary * GALevel7Unary * GALevel6Unary * GALevel5Unary * GALevel4Unary * GALevel3Unary * GALevel2Unary * GALevel1Unary
        FROM #AccSubHierarchy_DimAccountSubaccount a
              INNER JOIN
         (
             SELECT AccountSubaccountID,
                    GADepth = COUNT(DISTINCT ID)
             FROM #AccSubHierarchy_DimAccountSubaccount a UNPIVOT(ID FOR Thing IN(GALevel1ID,
                                                                GALevel2ID,
                                                                GALevel3ID,
                                                                GALevel4ID,
                                                                GALevel5ID,
                                                                GALevel6ID,
                                                                GALevel7ID,
                                                                GALevel8ID)) u
             GROUP BY AccountSubaccountID
         ) b ON a.AccountSubaccountID = b.AccountSubaccountID;

                   -- ================================================================================
                   -- Remove Duplicates to [re]create ragged hierarchy in SSAS.
                   -- ----------------------------------------------------------
                   --
                   -- SSAS 2017 can only handle ragged hierarchies when the bottom levels are blank, so
                   --     go through #AccSubHierarchy_DimAccountSubaccount and set to null any hierarchy level that is a 
                   --     dupliacte of the one above.  
                   --
                   -- We do this for TCOLevels, Field Levels and GA Level 
                   -- ================================================================================
                   --
                   -- TCO Levels
                   --
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               TCOLevel12ID = NULL,
               TCOLevel12Name = NULL,
               TCOLevel12Unary = NULL,
               TCOLevel12Sort = NULL
         WHERE TCOLevel11ID = TCOLevel12ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               TCOLevel11ID = NULL,
               TCOLevel11Name = NULL,
               TCOLevel11Unary = NULL,
               TCOLevel11Sort = NULL
         WHERE TCOLevel10ID = TCOLevel11ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               TCOLevel10ID = NULL,
               TCOLevel10Name = NULL,
               TCOLevel10Unary = NULL,
               TCOLevel10Sort = NULL
         WHERE TCOLevel9ID = TCOLevel10ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               TCOLevel9ID = NULL,
               TCOLevel9Name = NULL,
               TCOLevel9Unary = NULL,
               TCOLevel9Sort = NULL
         WHERE TCOLevel8ID = TCOLevel9ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               TCOLevel8ID = NULL,
               TCOLevel8Name = NULL,
               TCOLevel8Unary = NULL,
               TCOLevel8Sort = NULL
         WHERE TCOLevel7ID = TCOLevel8ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               TCOLevel7ID = NULL,
               TCOLevel7Name = NULL,
               TCOLevel7Unary = NULL,
               TCOLevel7Sort = NULL
         WHERE TCOLevel6ID = TCOLevel7ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               TCOLevel6ID = NULL,
               TCOLevel6Name = NULL,
               TCOLevel6Unary = NULL,
               TCOLevel6Sort = NULL
         WHERE TCOLevel5ID = TCOLevel6ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               TCOLevel5ID = NULL,
               TCOLevel5Name = NULL,
               TCOLevel5Unary = NULL,
               TCOLevel5Sort = NULL
         WHERE TCOLevel4ID = TCOLevel5ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               TCOLevel4ID = NULL,
               TCOLevel4Name = NULL,
               TCOLevel4Unary = NULL,
               TCOLevel4Sort = NULL
         WHERE TCOLevel3ID = TCOLevel4ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               TCOLevel3ID = NULL,
               TCOLevel3Name = NULL,
               TCOLevel3Unary = NULL,
               TCOLevel3Sort = NULL
         WHERE TCOLevel2ID = TCOLevel3ID;

                   --
                   -- Field Levels
                   --
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               FieldLevel12ID = NULL,
               FieldLevel12Name = NULL,
               FieldLevel12Unary = NULL,
               FieldLevel12Sort = NULL
         WHERE FieldLevel11ID = FieldLevel12ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               FieldLevel11ID = NULL,
               FieldLevel11Name = NULL,
               FieldLevel11Unary = NULL,
               FieldLevel11Sort = NULL
         WHERE FieldLevel10ID = FieldLevel11ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               FieldLevel10ID = NULL,
               FieldLevel10Name = NULL,
               FieldLevel10Unary = NULL,
              FieldLevel10Sort = NULL
         WHERE FieldLevel9ID = FieldLevel10ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               FieldLevel9ID = NULL,
               FieldLevel9Name = NULL,
               FieldLevel9Unary = NULL,
               FieldLevel9Sort = NULL
         WHERE FieldLevel8ID = FieldLevel9ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               FieldLevel8ID = NULL,
               FieldLevel8Name = NULL,
               FieldLevel8Unary = NULL,
               FieldLevel8Sort = NULL
         WHERE FieldLevel7ID = FieldLevel8ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               FieldLevel7ID = NULL,
               FieldLevel7Name = NULL,
               FieldLevel7Unary = NULL,
               FieldLevel7Sort = NULL
         WHERE FieldLevel6ID = FieldLevel7ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               FieldLevel6ID = NULL,
               FieldLevel6Name = NULL,
               FieldLevel6Unary = NULL,
               FieldLevel6Sort = NULL
         WHERE FieldLevel5ID = FieldLevel6ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               FieldLevel5ID = NULL,
               FieldLevel5Name = NULL,
               FieldLevel5Unary = NULL,
               FieldLevel5Sort = NULL
         WHERE FieldLevel4ID = FieldLevel5ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               FieldLevel4ID = NULL,
               FieldLevel4Name = NULL,
               FieldLevel4Unary = NULL,
               FieldLevel4Sort = NULL
         WHERE FieldLevel3ID = FieldLevel4ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               FieldLevel3ID = NULL,
               FieldLevel3Name = NULL,
               FieldLevel3Unary = NULL,
               FieldLevel3Sort = NULL
         WHERE FieldLevel2ID = FieldLevel3ID;

                   --
                   -- GA Levels
                   --
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               GALevel8ID = NULL,
               GALevel8Name = NULL,
               GALevel8Unary = NULL,
               GALevel8Sort = NULL
         WHERE GALevel7ID = GALevel8ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               GALevel7ID = NULL,
               GALevel7Name = NULL,
               GALevel7Unary = NULL,
               GALevel7Sort = NULL
         WHERE GALevel6ID = GALevel7ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               GALevel6ID = NULL,
               GALevel6Name = NULL,
               GALevel6Unary = NULL,
               GALevel6Sort = NULL
         WHERE GALevel5ID = GALevel6ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               GALevel5ID = NULL,
               GALevel5Name = NULL,
               GALevel5Unary = NULL,
               GALevel5Sort = NULL
         WHERE GALevel4ID = GALevel5ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               GALevel4ID = NULL,
               GALevel4Name = NULL,
               GALevel4Unary = NULL,
               GALevel4Sort = NULL
         WHERE GALevel3ID = GALevel4ID;
         UPDATE #AccSubHierarchy_DimAccountSubaccount
           SET
               GALevel3ID = NULL,
               GALevel3Name = NULL,
               GALevel3Unary = NULL,
               GALevel3Sort = NULL
         WHERE GALevel2ID = GALevel3ID;

-------------------------------------------------
-- replaced the TFN table normally used by the below query with the temp table #AccSubHierarchy_DimAccountSubaccount that we built above
-------------------------------------------------

         BEGIN TRY
             SELECT AccountSubaccountID,
                    COALESCE(AccountSubaccountName, 'Unknown Account Subaccount') AS AccountSubaccountName,
                    COALESCE(AccountID, '0000') AS AccountID,
                    COALESCE(AccountName, 'Unknown Account') AS AccountName,
                    COALESCE(SubaccountID, '000000') AS SubaccountID,
                    COALESCE(SubaccountName, 'Unknown SubAccount') AS SubaccountName,
                    COALESCE(TCOUnary, 1) AS ASATCOUnary,
                    COALESCE(TCOSort, 99999) AS ASATCOSort,
                    COALESCE(TCODepth, -1) AS ASATCODepth,
                    COALESCE(TCOLevel1ID, NULL) AS ASATCOLevel1ID,
                    COALESCE(TCOLevel1Name, NULL) AS ASATCOLevel1Name,
                    COALESCE(TCOLevel1Unary, NULL) AS ASATCOLevel1Unary,
                    COALESCE(TCOLevel1Sort, NULL) AS ASATCOLevel1Sort,
                    COALESCE(TCOLevel2ID, NULL) AS ASATCOLevel2ID,
                    COALESCE(TCOLevel2Name, NULL) AS ASATCOLevel2Name,
                    COALESCE(TCOLevel2Unary, NULL) AS ASATCOLevel2Unary,
                    COALESCE(TCOLevel2Sort, NULL) AS ASATCOLevel2Sort,
                    COALESCE(TCOLevel3ID, NULL) AS ASATCOLevel3ID,
                    COALESCE(TCOLevel3Name, NULL) AS ASATCOLevel3Name,
                    COALESCE(TCOLevel3Unary, NULL) AS ASATCOLevel3Unary,
                    COALESCE(TCOLevel3Sort, NULL) AS ASATCOLevel3Sort,
                    COALESCE(TCOLevel4ID, NULL) AS ASATCOLevel4ID,
                    COALESCE(TCOLevel4Name, NULL) AS ASATCOLevel4Name,
                    COALESCE(TCOLevel4Unary, NULL) AS ASATCOLevel4Unary,
                    COALESCE(TCOLevel4Sort, NULL) AS ASATCOLevel4Sort,
                    COALESCE(TCOLevel5ID, NULL) AS ASATCOLevel5ID,
                    COALESCE(TCOLevel5Name, NULL) AS ASATCOLevel5Name,
                    COALESCE(TCOLevel5Unary, NULL) AS ASATCOLevel5Unary,
                    COALESCE(TCOLevel5Sort, NULL) AS ASATCOLevel5Sort,
                    COALESCE(TCOLevel6ID, NULL) AS ASATCOLevel6ID,
                    COALESCE(TCOLevel6Name, NULL) AS ASATCOLevel6Name,
                    COALESCE(TCOLevel6Unary, NULL) AS ASATCOLevel6Unary,
                    COALESCE(TCOLevel6Sort, NULL) AS ASATCOLevel6Sort,
                    COALESCE(TCOLevel7ID, NULL) AS ASATCOLevel7ID,
                    COALESCE(TCOLevel7Name, NULL) AS ASATCOLevel7Name,
                    COALESCE(TCOLevel7Unary, NULL) AS ASATCOLevel7Unary,
                    COALESCE(TCOLevel7Sort, NULL) AS ASATCOLevel7Sort,
                    COALESCE(TCOLevel8ID, NULL) AS ASATCOLevel8ID,
                    COALESCE(TCOLevel8Name, NULL) AS ASATCOLevel8Name,
                    COALESCE(TCOLevel8Unary, NULL) AS ASATCOLevel8Unary,
                    COALESCE(TCOLevel8Sort, NULL) AS ASATCOLevel8Sort,
                    COALESCE(TCOLevel9ID, NULL) AS ASATCOLevel9ID,
                    COALESCE(TCOLevel9Name, NULL) AS ASATCOLevel9Name,
                    COALESCE(TCOLevel9Unary, NULL) AS ASATCOLevel9Unary,
                    COALESCE(TCOLevel9Sort, NULL) AS ASATCOLevel9Sort,
                    COALESCE(TCOLevel10ID, NULL) AS ASATCOLevel10ID,
                    COALESCE(TCOLevel10Name, NULL) AS ASATCOLevel10Name,
                    COALESCE(TCOLevel10Unary, NULL) AS ASATCOLevel10Unary,
                    COALESCE(TCOLevel10Sort, NULL) AS ASATCOLevel10Sort,
                    COALESCE(TCOLevel11ID, NULL) AS ASATCOLevel11ID,
                    COALESCE(TCOLevel11Name, NULL) AS ASATCOLevel11Name,
                    COALESCE(TCOLevel11Unary, NULL) AS ASATCOLevel11Unary,
                    COALESCE(TCOLevel11Sort, NULL) AS ASATCOLevel11Sort,
                    COALESCE(TCOLevel12ID, NULL) AS ASATCOLevel12ID,
                    COALESCE(TCOLevel12Name, NULL) AS ASATCOLevel12Name,
                    COALESCE(TCOLevel12Unary, NULL) AS ASATCOLevel12Unary,
                    COALESCE(TCOLevel12Sort, NULL) AS ASATCOLevel12Sort,
                    COALESCE(FieldUnary, 1) AS ASAFieldUnary,
                    COALESCE(FieldSort, 99999) AS ASAFieldSort,
                    COALESCE(FieldDepth, -1) AS ASAFieldDepth,
                    COALESCE(FieldLevel1ID, NULL) AS ASAFieldLevel1ID,
                    COALESCE(FieldLevel1Name, NULL) AS ASAFieldLevel1Name,
                    COALESCE(FieldLevel1Unary, NULL) AS ASAFieldLevel1Unary,
                    COALESCE(FieldLevel1Sort, NULL) AS ASAFieldLevel1Sort,
                    COALESCE(FieldLevel2ID, NULL) AS ASAFieldLevel2ID,
                    COALESCE(FieldLevel2Name, NULL) AS ASAFieldLevel2Name,
                    COALESCE(FieldLevel2Unary, NULL) AS ASAFieldLevel2Unary,
                    COALESCE(FieldLevel2Sort, NULL) AS ASAFieldLevel2Sort,
                    COALESCE(FieldLevel3ID, NULL) AS ASAFieldLevel3ID,
                    COALESCE(FieldLevel3Name, NULL) AS ASAFieldLevel3Name,
                    COALESCE(FieldLevel3Unary, NULL) AS ASAFieldLevel3Unary,
                    COALESCE(FieldLevel3Sort, NULL) AS ASAFieldLevel3Sort,
                    COALESCE(FieldLevel4ID, NULL) AS ASAFieldLevel4ID,
                    COALESCE(FieldLevel4Name, NULL) AS ASAFieldLevel4Name,
                    COALESCE(FieldLevel4Unary, NULL) AS ASAFieldLevel4Unary,
                    COALESCE(FieldLevel4Sort, NULL) AS ASAFieldLevel4Sort,
                    COALESCE(FieldLevel5ID, NULL) AS ASAFieldLevel5ID,
                    COALESCE(FieldLevel5Name, NULL) AS ASAFieldLevel5Name,
                    COALESCE(FieldLevel5Unary, NULL) AS ASAFieldLevel5Unary,
                    COALESCE(FieldLevel5Sort, NULL) AS ASAFieldLevel5Sort,
                    COALESCE(FieldLevel6ID, NULL) AS ASAFieldLevel6ID,
                    COALESCE(FieldLevel6Name, NULL) AS ASAFieldLevel6Name,
                    COALESCE(FieldLevel6Unary, NULL) AS ASAFieldLevel6Unary,
                    COALESCE(FieldLevel6Sort, NULL) AS ASAFieldLevel6Sort,
                    COALESCE(FieldLevel7ID, NULL) AS ASAFieldLevel7ID,
                    COALESCE(FieldLevel7Name, NULL) AS ASAFieldLevel7Name,
                    COALESCE(FieldLevel7Unary, NULL) AS ASAFieldLevel7Unary,
                    COALESCE(FieldLevel7Sort, NULL) AS ASAFieldLevel7Sort,
                    COALESCE(FieldLevel8ID, NULL) AS ASAFieldLevel8ID,
                    COALESCE(FieldLevel8Name, NULL) AS ASAFieldLevel8Name,
                    COALESCE(FieldLevel8Unary, NULL) AS ASAFieldLevel8Unary,
                    COALESCE(FieldLevel8Sort, NULL) AS ASAFieldLevel8Sort,
                    COALESCE(FieldLevel9ID, NULL) AS ASAFieldLevel9ID,
                    COALESCE(FieldLevel9Name, NULL) AS ASAFieldLevel9Name,
                    COALESCE(FieldLevel9Unary, NULL) AS ASAFieldLevel9Unary,
                    COALESCE(FieldLevel9Sort, NULL) AS ASAFieldLevel9Sort,
                    COALESCE(FieldLevel10ID, NULL) AS ASAFieldLevel10ID,
                    COALESCE(FieldLevel10Name, NULL) AS ASAFieldLevel10Name,
                    COALESCE(FieldLevel10Unary, NULL) AS ASAFieldLevel10Unary,
                    COALESCE(FieldLevel10Sort, NULL) AS ASAFieldLevel10Sort,
                    COALESCE(FieldLevel11ID, NULL) AS ASAFieldLevel11ID,
                    COALESCE(FieldLevel11Name, NULL) AS ASAFieldLevel11Name,
                    COALESCE(FieldLevel11Unary, NULL) AS ASAFieldLevel11Unary,
                    COALESCE(FieldLevel11Sort, NULL) AS ASAFieldLevel11Sort,
                    COALESCE(FieldLevel11ID, NULL) AS ASAFieldLevel12ID,
                    COALESCE(FieldLevel11Name, NULL) AS ASAFieldLevel12Name,
                    COALESCE(FieldLevel11Unary, NULL) AS ASAFieldLevel12Unary,
                    COALESCE(FieldLevel11Sort, NULL) AS ASAFieldLevel12Sort,
                    COALESCE(GAUnary, 1) AS ASAGAUnary,
                    COALESCE(GASort, 99999) AS ASAGASort,
                    COALESCE(GADepth, -1) AS ASAGADepth,
                    COALESCE(GALevel1ID, NULL) AS ASAGALevel1ID,
                    COALESCE(GALevel1Name, NULL) AS ASAGALevel1Name,
                    COALESCE(GALevel1Unary, NULL) AS ASAGALevel1Unary,
                    COALESCE(GALevel1Sort, NULL) AS ASAGALevel1Sort,
                    COALESCE(GALevel2ID, NULL) AS ASAGALevel2ID,
                    COALESCE(GALevel2Name, NULL) AS ASAGALevel2Name,
                    COALESCE(GALevel2Unary, NULL) AS ASAGALevel2Unary,
                    COALESCE(GALevel2Sort, NULL) AS ASAGALevel2Sort,
                    COALESCE(GALevel3ID, NULL) AS ASAGALevel3ID,
                    COALESCE(GALevel3Name, NULL) AS ASAGALevel3Name,
                    COALESCE(GALevel3Unary, NULL) AS ASAGALevel3Unary,
                    COALESCE(GALevel3Sort, NULL) AS ASAGALevel3Sort,
                    COALESCE(GALevel4ID, NULL) AS ASAGALevel4ID,
                    COALESCE(GALevel4Name, NULL) AS ASAGALevel4Name,
                    COALESCE(GALevel4Unary, NULL) AS ASAGALevel4Unary,
                    COALESCE(GALevel4Sort, NULL) AS ASAGALevel4Sort,
                    COALESCE(GALevel5ID, NULL) AS ASAGALevel5ID,
                    COALESCE(GALevel5Name, NULL) AS ASAGALevel5Name,
                    COALESCE(GALevel5Unary, NULL) AS ASAGALevel5Unary,
                    COALESCE(GALevel5Sort, NULL) AS ASAGALevel5Sort,
                    COALESCE(GALevel6ID, NULL) AS ASAGALevel6ID,
                    COALESCE(GALevel6Name, NULL) AS ASAGALevel6Name,
                    COALESCE(GALevel6Unary, NULL) AS ASAGALevel6Unary,
                    COALESCE(GALevel6Sort, NULL) AS ASAGALevel6Sort,
                    COALESCE(GALevel6ID, NULL) AS ASAGALevel7ID,
                    COALESCE(GALevel6Name, NULL) AS ASAGALevel7Name,
                    COALESCE(GALevel6Unary, NULL) AS ASAGALevel7Unary,
                    COALESCE(GALevel6Sort, NULL) AS ASAGALevel7Sort,
                    COALESCE(GALevel6ID, NULL) AS ASAGALevel8ID,
                    COALESCE(GALevel6Name, NULL) AS ASAGALevel8Name,
                    COALESCE(GALevel6Unary, NULL) AS ASAGALevel8Unary,
                    COALESCE(GALevel6Sort, NULL) AS ASAGALevel8Sort,
                    COALESCE(TuitionType, 'Unknown Tuition Type') AS ASATuitionType,
                    COALESCE(LaborType, 'Unknown Labor Type') AS ASALaborType,
                    COALESCE(ASAEBITDAAddbackFlag, 'Unknown EBITDA Addback Flag') AS ASAEBITDAAddbackFlag,
                    COALESCE(AccountTypeCode, NULL) AS AccountTypeCode,
                    COALESCE(AccountTypeName, NULL) AS AccountTypeName,
                    COALESCE(AccountTypeUnary, NULL) AS AccountTypeUnary,
					COALESCE(FieldLevel1ID,'')
	                +COALESCE('|'+FieldLevel2ID,'') 
	                +COALESCE('|'+FieldLevel3ID,'')
	                +COALESCE('|'+FieldLevel4ID,'')
	                +COALESCE('|'+FieldLevel5ID,'')
	                +COALESCE('|'+FieldLevel6ID,'')
	                +COALESCE('|'+FieldLevel7ID,'')
	                +COALESCE('|'+FieldLevel8ID,'')
	                +COALESCE('|'+FieldLevel9ID,'')
	                +COALESCE('|'+FieldLevel10ID,'')
	                +COALESCE('|'+FieldLevel11ID,'')
	                +COALESCE('|'+FieldLevel12ID,'')  AS [FieldPath],
                    @EDWRunDateTime AS EDWCreatedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                    @EDWRunDateTime AS EDWModifiedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy,
					'I' AS [RowStatus]
             FROM #AccSubHierarchy_DimAccountSubaccount
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