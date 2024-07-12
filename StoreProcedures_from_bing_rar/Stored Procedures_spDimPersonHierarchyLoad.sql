--=============================================================================================================================--
-- Title: BI-9256 Add Ragged Heirarchy to PBI dataset Objects
-- Desc: Creates a stored procedure needed to add a ragged heirarcy table to our PBI HR dataset.   

-- (Object Model and Workflow) --
-- [DW_Mart].[dbo].[spDimPersonHierarchyLoad] (Fills)>> [DW_Landing].[dbo].ValidEmployeeNumbersLanding
-- [DW_Mart].[dbo].[spDimPersonHierarchyLoad] (Fills)>> [DW_Mart].[dbo].[DimPersonHierarchy] 
--                                            (with data from)>> [DW_Landing].[dbo].ValidEmployeeNumbersLanding 
-- [DW_Mart].[dbo].[spDimPersonHierarchyLoad] <<(Looks up data using) [DW_Mart].[dbo].[fGetPersonIdByLevel]
-- [DW_Mart].[dbo].[spDimPersonHierarchyLoad] <<(Looks up data using) [DW_Mart].[dbo].[fGetPersonNameByID]
-- [DW_Mart].[dbo].[spDimPersonHierarchyLoad] <<(Looks up data using) [DW_Mart].[dbo].[fGetPersonNameByLevel]
-- [DW_Mart].[dbo].[DimPersonHierarchy] (supplies data for)>> [DW_View].[dbo].[vPersonHierarchy]


-- Change Log: When,Who,What
-- 10/12/2023,RRoot,Created Script
-- 10/26/2023,RRoot,Converted From Drop table with Select-Into to Truncate Table with Insert-Into
-- 10/26/2023,RRoot,Moved ValidEmployeeNumbers table to the Landing database as ValidEmployeeNumbersLanding
--==============================================================================================================================--

-- *********************************************************************************************************
-- Step 2 (AC1):  Let's get only current employees with a [SupervisorNumber] 
--                and an actual [EmployeeNumber], without Terminated or Historical rows

-- EXEC dbo.spDimPersonHierarchyLoad @DebugMode = 1	
-- *********************************************************************************************************

CREATE PROCEDURE spDimPersonHierarchyLoad
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = 0,
 @ExecutionID    VARCHAR(100) = NULL,
 @AuditId		     BIGINT = NULL OUTPUT
)
As
    -- ================================================================================
    -- 
    -- Stored Procedure:   spDimPersonHierarchyLoad
    --
    -- Purpose:            Performs the Full Load ETL process for the DimPersonHeirarchy table in DW_Mart.
    --
    --                         Step 1: Create and populate a temporary landing table for only employees with a valid supervisor from Source (DimEmployee)
    --                         Step 2: Pivot the data to place employee - supervisor names into columns depending on thier relationships 
    --                         Step 3: Create and populate a Dimension table (DimPersonHierarchy) with report data
    --                         Step 4: Execute any automated tests associated with this DW table load
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
    -- Returns:            Single-row results set containing the following columns:
    --                         SourceCount - Number of rows extracted from source
    --                         InsertCount - Number or rows inserted to target table
    --                         UpdateCount - Number or rows updated in target table (Not needed)
    --                         DeleteCount - Number or rows deleted in target table (Not needed)
    --
    -- Usage:              EXEC dbo.spDimPersonHierarchyLoad @DebugMode = 1	
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date           Modified By         Comments
    -- ----           -----------         --------
    --
    -- 10/25/2023     RRoot               BI-9256 Add Ragged Hierarchy to PBI Dataset
    -- 10/26/2023     RRoot               Added Direct Supervisor's Name Column
    -- 10/26/2023,RRoot,Converted From Drop table with Select-Into to Truncate Table with Insert-Into
    -- 10/26/2023,RRoot,Moved ValidEmployeeNumbers table to the Landing database as ValidEmployeeNumbersLanding   
	-- 11/02/2023,RRoot,Changed [Level08Name] to [Level09ID] in which were incorrect in two places.
    -- ================================================================================

Begin
    SET NOCOUNT ON;

	  --
	  -- Housekeeping Variables
	  --
    DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
    DECLARE @DebugMsg NVARCHAR(500);
    DECLARE @SourceName VARCHAR(100)= 'DimPersonHierarchy';

	  --
	  -- ETL status Variables
	  --
    DECLARE @RowCount INT;
    DECLARE @Error INT;
    DECLARE @SourceCount INT= 0;
    DECLARE @InsertCount INT= 0;
    --DECLARE @UpdateCount INT= 0;  -- (Not Needed)
    --DECLARE @DeleteCount INT= 0;  -- (Not Needed)
    --DECLARE @EffectiveTo Datetime2(3) = '9999-12-31 23:59:59.999' -- (Not Needed)

	  -- Merge statement action table variables -- (Not Needed) --
        --DECLARE @tblMergeActions TABLE(MergeAction VARCHAR(20));
        --DECLARE @tblDeleteActions TABLE(MergeAction VARCHAR(20));

	  -- If we do not get an @EDWRunDateTime input, set to current date
    IF @EDWRunDateTime IS NULL
        SET @EDWRunDateTime = GETDATE();
    
    -- Debug output progress
    IF @DebugMode = 1
      Begin
        Select @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Starting.';
        PRINT @DebugMsg;
      End
      
	  -- Write the starting time to DW_Mart AuditLog  
         EXEC [dbo].[spBeginAuditLog]
          @AuditId = @AuditId OUTPUT,
			    @SourceName = @SourceName,
          @ExecutionID = @ExecutionID; 



    -- ======================================================================================================================
    -- Step 1: Let's get only current employees with a [SupervisorNumber] and an actual [EmployeeNumber], 
    --         without Terminated or Historical rows
    -- ====================================================================================================================

     Begin Try

     	-- Debug output progress
      If @DebugMode = 1
        Begin
          Select @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Beginning ValidEmployeeNumbersLanding truncte and reload.';
          PRINT @DebugMsg;
        End

      -- Drop table before reload base table we need to build our heirarchy
      If Exists (Select * From [DW_Landing].Sys.Tables Where Name = 'ValidEmployeeNumbersLanding') 
        Truncate Table [DW_Landing].dbo.ValidEmployeeNumbersLanding;
 
      -- Load table with current data
      With OnlyActiveEmployeesWithSupervisors
      As (
      Select [EmployeeNumber], [PersonID], [PersonName], [SupervisorNumber], [RowStatus], [TerminationDate] 
        From [DW_Mart].dbo.DimPerson  -- Which supervisor numbers in here, ~ 2,561,606 rows
        Where Not [SupervisorNumber] in (-1, -2) -- Only employees with a known supervisor ~ 58,295 rows
          And [TerminationDate] is Null  -- Only current employees ~ 25,317 rows
          And [RowStatus] = 'A' -- Only the most recent, active, record ~ 4,757 rows
          And [EmployeeNumber] Not In ('N/A') -- ~ 4,247 rows
      ) Insert Into [DW_Landing].[dbo].[ValidEmployeeNumbersLanding] -- CREATE OUR BASE TABLE <<<
        ([PersonID], [PersonName], [SupervisorNumber])
        Select [PersonID],[PersonName],[SupervisorNumber]  
          From OnlyActiveEmployeesWithSupervisors -- 4,243
          Group By [PersonID],[PersonName],[SupervisorNumber]  -- Remove duplicates. 
        Union
        Select Distinct [PersonID],[PersonName], [SupervisorNumber] = Null
          From [DW_Mart].dbo.DimPerson
          Where [PersonId] = '858850'; -- Must include Wyatt, John Thomson (Tom)
	

      -- Debug output progress    
      SELECT @SourceCount = COUNT(1) FROM [DW_Mart].dbo.DimPerson;
      SELECT @InsertCount = COUNT(1) FROM [DW_Landing].dbo.ValidEmployeeNumbersLanding;

      -- Debug output progress
      IF @DebugMode = 1
        Begin
          SELECT @DebugMsg = @ProcName + ' : '+ CONVERT(NVARCHAR(20), GETDATE())
                                       + ' - Extracted ' + CONVERT(NVARCHAR(20), @SourceCount) 
                                       + ' rows from DimPerson and loaded ' +  CONVERT(NVARCHAR(20), @InsertCount)  
                                       + ' filtered rows into ValidEmployeeNumbersLanding.';
          PRINT @DebugMsg;
        End

    -- =====================================================================================================================
    -- Step 2: Pivot the data to place employee - supervisor names into columns depending on thier relationships 
    -- =====================================================================================================================

      
     	-- Debug output progress
      If @DebugMode = 1
        Begin
          Select @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Beginning DimPersonHierarchy drop and reload.';
          PRINT @DebugMsg;
        End


      -- Drop the table before reload
      If Exists (Select * From [DW_Mart].Sys.Tables Where Name = 'DimPersonHierarchy') 
        Truncate Table [DW_Mart].dbo.DimPersonHierarchy;

      -- Reload with current data
      WITH RaggedHierarchyWithPathIDAndNames -- 1 sec
      AS (
          -- Base case: Get the top-level employee (where MgrID is NULL)
          Select [PersonID], [PersonName], [SupervisorNumber]
		            ,CAST(IsNull([PersonID], 1) AS VARCHAR(100)) AS HierarchyPathIds
                ,CAST([PersonName] AS VARCHAR(5000)) AS HierarchyPathNames  -- Show the current hierarchy path
          FROM [DW_Landing].[dbo].[ValidEmployeeNumbersLanding]
          WHERE [PersonId] = '858850' -- Must include and start with Wyatt, John Thomson (Tom)
    
          UNION ALL
    
          -- Recursive case: Join on the next level down in the hierarchy
          Select dp.[PersonID], dp.[PersonName], dp.[SupervisorNumber]
		            ,CAST(rh.HierarchyPathIds + '|' + CAST(dp.[PersonID] AS VARCHAR(100)) as VARCHAR(100)) AS HierarchyPathIdss
                ,CAST(rh.HierarchyPathNames + '|' + dp.[PersonName] AS VARCHAR(5000)) AS HierarchyPathNames
          FROM [DW_Landing].[dbo].[ValidEmployeeNumbersLanding] as dp 
		      Inner JOIN RaggedHierarchyWithPathIDAndNames as rh  -- This is the name of the CTE, making this recursive
			      ON dp.[SupervisorNumber] = rh.[PersonID] -- 
          -- WHERE [DW_Mart].dbo.fGetPersonNameByID(dp.[SupervisorNumber]) Is Not Null -- ~ 799 rows  
      ) -- Select * From RaggedHierarchyWithPathIDAndNames 
      , WithLevelIds
      As (
        Select [PersonID], [PersonName], [SupervisorNumber], [HierarchyPathIds]+ '|' AS [HierarchyPathIds], [HierarchyPathNames]
              ,[LevelNumber] = (Select Count(value) FROM STRING_SPLIT(HierarchyPathIds, '|'))  -- Note: the word "value" is a placeholder and can be any name!
        FROM RaggedHierarchyWithPathIDAndNames
      ) -- Select * From  RaggedHierarchyWithPathIDAndNames
      , WithExtendedIdColumns
      As (
      Select [PersonID], [PersonName], [SupervisorNumber], [HierarchyPathIds], [HierarchyPathNames], [LevelNumber]
            -- Add Id Columns
            ,[Level01Id] = dbo.fGetPersonIdByLevel([HierarchyPathIds], 1) 
            ,[Level02Id] = dbo.fGetPersonIdByLevel([HierarchyPathIds], 2) 
            ,[Level03Id] = dbo.fGetPersonIdByLevel([HierarchyPathIds], 3) 
            ,[Level04Id] = dbo.fGetPersonIdByLevel([HierarchyPathIds], 4) 
            ,[Level05Id] = dbo.fGetPersonIdByLevel([HierarchyPathIds], 5) 
            ,[Level06Id] = dbo.fGetPersonIdByLevel([HierarchyPathIds], 6) 
            ,[Level07Id] = dbo.fGetPersonIdByLevel([HierarchyPathIds], 7) 
            ,[Level08Id] = dbo.fGetPersonIdByLevel([HierarchyPathIds], 8) 
            ,[Level09Id] = dbo.fGetPersonIdByLevel([HierarchyPathIds], 9) 
            ,[Level10Id] = dbo.fGetPersonIdByLevel([HierarchyPathIds], 10) 
      From WithLevelIds
      ) -- Select * From WithExtendedIdColumns
      , WithPersonNamesPerLevel
      As (
      Select [PersonID], [PersonName], [SupervisorNumber], [HierarchyPathIds], [HierarchyPathNames], [LevelNumber]
      ,[Level01Id],[Level02Id],[Level03Id],[Level04Id],[Level05Id],[Level06Id],[Level07Id],[Level08Id],[Level09Id],[Level10Id]
      ,[Level01Name] = [DW_Mart].dbo.fGetPersonNameByID([Level01Id])
      ,[Level02Name] = [DW_Mart].dbo.fGetPersonNameByID([Level02Id]) 
      ,[Level03Name] = [DW_Mart].dbo.fGetPersonNameByID([Level03Id]) 
      ,[Level04Name] = [DW_Mart].dbo.fGetPersonNameByID([Level04Id]) 
      ,[Level05Name] = [DW_Mart].dbo.fGetPersonNameByID([Level05Id]) 
      ,[Level06Name] = [DW_Mart].dbo.fGetPersonNameByID([Level06Id]) 
      ,[Level07Name] = [DW_Mart].dbo.fGetPersonNameByID([Level07Id]) 
      ,[Level08Name] = [DW_Mart].dbo.fGetPersonNameByID([Level08Id]) 
      ,[Level09Name] = [DW_Mart].dbo.fGetPersonNameByID([Level09Id]) 
      ,[Level10Name] = [DW_Mart].dbo.fGetPersonNameByID([Level10Id])
      From WithExtendedIdColumns
      ) --Select * From WithPersonNamesPerLevel          
      , WithNullsReplaced
      As (
      Select [PersonID], [PersonName], [SupervisorNumber], [SupervisorName] = dbo.fGetPersonNameByID([SupervisorNumber]), [HierarchyPathIds], [HierarchyPathNames], [LevelNumber]
            -- Add Id Columns (Note: Yes, this feels like a Hack, but I cannot figure out a elegant way to do this. Besides, it just needs to work and it runs quickly!)
            ,[Level01Id] = IsNull([Level01Id],[Level01Id])
            ,[Level02Id] = IsNull([Level02Id],[Level01Id])
            ,[Level03Id] = IsNull(IsNull([Level03Id],[Level02Id]),[Level01Id])
            ,[Level04Id] = IsNull(IsNull(IsNull([Level04Id],[Level03Id]),[Level02Id]),[Level01Id])
            ,[Level05Id] = IsNull(IsNull(IsNull(IsNull([Level05Id],[Level04Id]),[Level03Id]),[Level02Id]),[Level01Id])
            ,[Level06Id] = IsNull(IsNull(IsNull(IsNull(IsNull([Level06Id],[Level05Id]),[Level04Id]),[Level03Id]),[Level02Id]),[Level01Id])
            ,[Level07Id] = IsNull(IsNull(IsNull(IsNull(IsNull(IsNull([Level07Id],[Level06Id]),[Level05Id]),[Level04Id]),[Level03Id]),[Level02Id]),[Level01Id])
            ,[Level08Id] = IsNull(IsNull(IsNull(IsNull(IsNull(IsNull(IsNull([Level08Id],[Level07Id]),[Level06Id]),[Level05Id]),[Level04Id]),[Level03Id]),[Level02Id]),[Level01Id])
            ,[Level09Id] = IsNull(IsNull(IsNull(IsNull(IsNull(IsNull(IsNull(IsNull([Level09Id],[Level08Id]),[Level07Id]),[Level06Id]),[Level05Id]),[Level04Id]),[Level03Id]),[Level02Id]),[Level01Id])
            ,[Level10Id] = IsNull(IsNull(IsNull(IsNull(IsNull(IsNull(IsNull(IsNull(IsNull([Level10Id],[Level09Id]),[Level08Id]),[Level07Id]),[Level06Id]),[Level05Id]),[Level04Id]),[Level03Id]),[Level02Id]),[Level01Id])

            -- Add Name Columns
            ,[Level01Name] = IsNull([Level01Name],[Level01Name])
            ,[Level02Name] = IsNull([Level02Name],[Level01Name])
            ,[Level03Name] = IsNull(IsNull([Level03Name],[Level02Name]),[Level01Name])
            ,[Level04Name] = IsNull(IsNull(IsNull([Level04Name],[Level03Name]),[Level02Name]),[Level01Name])
            ,[Level05Name] = IsNull(IsNull(IsNull(IsNull([Level05Name],[Level04Name]),[Level03Name]),[Level02Name]),[Level01Name])
            ,[Level06Name] = IsNull(IsNull(IsNull(IsNull(IsNull([Level06Name],[Level05Name]),[Level04Name]),[Level03Name]),[Level02Name]),[Level01Name])
            ,[Level07Name] = IsNull(IsNull(IsNull(IsNull(IsNull(IsNull([Level07Name],[Level06Name]),[Level05Name]),[Level04Name]),[Level03Name]),[Level02Name]),[Level01Name])
            ,[Level08Name] = IsNull(IsNull(IsNull(IsNull(IsNull(IsNull(IsNull([Level08Name],[Level07Name]),[Level06Name]),[Level05Name]),[Level04Name]),[Level03Name]),[Level02Name]),[Level01Name])
            ,[Level09Name] = IsNull(IsNull(IsNull(IsNull(IsNull(IsNull(IsNull(IsNull([Level09Name],[Level08Name]),[Level07Name]),[Level06Name]),[Level05Name]),[Level04Name]),[Level03Name]),[Level02Name]),[Level01Name])
            ,[Level10Name] = IsNull(IsNull(IsNull(IsNull(IsNull(IsNull(IsNull(IsNull(IsNull([Level10Name],[Level09Name]),[Level08Name]),[Level07Name]),[Level06Name]),[Level05Name]),[Level04Name]),[Level03Name]),[Level02Name]),[Level01Name])
      From WithPersonNamesPerLevel
      ) -- =====================================================================================================================
        -- Step 3: Create and populate a Dimension table (DimPersonHierarchy) with report data
  	    -- =====================================================================================================================
      Insert Into [DW_Mart].[dbo].[DimPersonHierarchy]
      ([PersonID], [PersonName], [SupervisorNumber], [SupervisorName], [HierarchyPathIds], [HierarchyPathNames], 
       [LevelNumber], [Level01Id], [Level02Id], [Level03Id], [Level04Id], [Level05Id], [Level06Id], [Level07Id], [Level08Id], [Level09Id], [Level10Id], 
       [Level01Name], [Level02Name], [Level03Name], [Level04Name], [Level05Name], [Level06Name], [Level07Name], [Level08Name], [Level09Name], [Level10Name]
       )
        Select 
         [PersonID], [PersonName], [SupervisorNumber], [SupervisorName], [HierarchyPathIds], [HierarchyPathNames], [LevelNumber]
        ,[Level01Id]
        ,[Level02Id]
        ,[Level03Id]
        ,[Level04Id]
        ,[Level05Id]
        ,[Level06Id]
        ,[Level07Id]
        ,[Level08Id]
        ,[Level09Id]
        ,[Level10Id]
        ,[Level01Name]
        ,[Level02Name]
        ,[Level03Name] 
        ,[Level04Name] 
        ,[Level05Name] 
        ,[Level06Name] 
        ,[Level07Name] 
        ,[Level08Name] 
        ,[Level09Name] 
        ,[Level10Name]
        From WithNullsReplaced -- ~ 17 seconds
    
      -- Debug output progress    
      SELECT @SourceCount = COUNT(1) FROM [DW_Landing].[dbo].[ValidEmployeeNumbersLanding];
      SELECT @InsertCount = COUNT(1) FROM [DW_Mart].dbo.DimPersonHierarchy;

      IF @DebugMode = 1
        Begin
          SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE())
                                       + ' - Extracted ' + CONVERT(NVARCHAR(20), @SourceCount) 
                                       + ' rows from ValidEmployeeNumbersLanding and loaded ' +  CONVERT(NVARCHAR(20), @InsertCount)  
                                       + ' transformed rows into DimPersonHierarchy.';
          PRINT @DebugMsg;
        End

      EXEC [dbo].[spEndAuditLog]
          @InsertCount = @InsertCount,
          @UpdateCount = NULL,
          @DeleteCount = NULL,
          @SourceCount = @SourceCount,
          @AuditId = @AuditId;

      -- Debug output progress
      IF @DebugMode = 1
        Begin
          Select @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Ended.';
          PRINT @DebugMsg;
        End

     End Try
     Begin Catch
      -- Debug output progress
      IF @DebugMode = 1
        Select @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Drop and reload for ValidEmployeeNumbersLanding or DimPersonHierarchy failed.';
        PRINT @DebugMsg;

      -- Write our failed run to the AuditLog 
      EXEC [dbo].[spErrorAuditLog]
        @AuditId = @AuditId;

      --
      -- Raise error
      --	
      DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
      Select @ErrMsg = ERROR_MESSAGE(),
             @ErrSeverity = ERROR_SEVERITY();
      RAISERROR(@ErrMsg, @ErrSeverity, 1);
     End Catch
End
