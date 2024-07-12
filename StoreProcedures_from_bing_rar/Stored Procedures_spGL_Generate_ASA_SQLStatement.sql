

CREATE PROC [dbo].[spGL_Generate_ASA_SQLStatement] @Field MartLookup READONLY
AS
-- ================================================================================    
-- 
-- Stored Procedure:   spGL_StagingToEDW_DimAccountSubAccount
--
-- Purpose:            We need to filter source ASA records with the values in the FieldPath
--                     column in GL_Staging.dbo.MartASALookup table. We need to create 
--                     dynamic sql to pass column values as a filter to a sql query.
--                     Since the procedure with table valuesd parameter is read only,
--                     we can return the query result and so we're only generating the SQL
--                     query, so we can use this SQL query in SSIS as SQL Command Variable
--
-- Parameters:         No parameters                 
--
-- Usage:              EXEC dbo.spGL_Generate_ASA_SQLStatement	
-- 
-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------
--
-- Change Log:		   
-- ----------
--
-- Date           Modified By         Comments
-- ----------     -----------         --------
-- 04/02/2021     hhebbalu            Create the procedure to create a dynamic 
--                                    SQL statement filtering AS records 
-- 05/20/2021     adevabhakthuni       BI-4690  added columns to support SLD 
-- ================================================================================    
BEGIN
    SET NOCOUNT ON;

    -- ================================================================================   
    --
    -- create SQL to filter the ASA records based on the values in MartASALookup
    --
    -- ================================================================================   

    DECLARE @SQL NVARCHAR(MAX);

    SET @SQL
        = N'SELECT [AccountSubaccountID], [AccountSubaccountName], [AccountID], [AccountName], [SubaccountID], [SubaccountName], [ASATuitionType]       ,[ASAFieldDepth]
      ,[ASAFieldLevel1ID]
      ,[ASAFieldLevel1Name]
      ,[ASAFieldLevel1Unary]
      ,[ASAFieldLevel1Sort]
      ,[ASAFieldLevel2ID]
      ,[ASAFieldLevel2Name]
      ,[ASAFieldLevel2Unary]
      ,[ASAFieldLevel2Sort]
      ,[ASAFieldLevel3ID]
      ,[ASAFieldLevel3Name]
      ,[ASAFieldLevel3Unary]
      ,[ASAFieldLevel3Sort]
      ,[ASAFieldLevel4ID]
      ,[ASAFieldLevel4Name]
      ,[ASAFieldLevel4Unary]
      ,[ASAFieldLevel4Sort]
      ,[ASAFieldLevel5ID]
      ,[ASAFieldLevel5Name]
      ,[ASAFieldLevel5Unary]
      ,[ASAFieldLevel5Sort]
      ,[ASAFieldLevel6ID]
      ,[ASAFieldLevel6Name]
      ,[ASAFieldLevel6Unary]
      ,[ASAFieldLevel6Sort]
      ,[ASAFieldLevel7ID]
      ,[ASAFieldLevel7Name]
      ,[ASAFieldLevel7Unary]
      ,[ASAFieldLevel7Sort]
      ,[ASAFieldLevel8ID]
      ,[ASAFieldLevel8Name]
      ,[ASAFieldLevel8Unary]
      ,[ASAFieldLevel8Sort]
      ,[ASAFieldLevel9ID]
      ,[ASAFieldLevel9Name]
      ,[ASAFieldLevel9Unary]
      ,[ASAFieldLevel9Sort]
      ,[ASAFieldLevel10ID]
      ,[ASAFieldLevel10Name]
      ,[ASAFieldLevel10Unary]
      ,[ASAFieldLevel10Sort]
      ,[ASAFieldLevel11ID]
      ,[ASAFieldLevel11Name]
      ,[ASAFieldLevel11Unary]
      ,[ASAFieldLevel11Sort]
      ,[ASAFieldLevel12ID]
      ,[ASAFieldLevel12Name]
      ,[ASAFieldLevel12Unary]
      ,[ASAFieldLevel12Sort]' + NCHAR(10) + N'FROM dbo.AccountSubAccountLanding';
    SET @SQL = @SQL + ISNULL(NCHAR(10) + N'WHERE ' + STUFF(
                                                     (
                                                         SELECT NCHAR(10) + N'  OR ' + (f.FieldPath)
                                                         FROM @Field f
                                                         FOR XML PATH(N'')
                                                     ),
                                                     1,
                                                     6,
                                                     N''
                                                          ),
                             N''
                            ) + N';';
    SELECT @SQL;

END;