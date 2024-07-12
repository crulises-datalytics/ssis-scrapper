--=============================================================================================================================--
-- Title: Create spDimPersonSupervisorLoad
-- Desc: Fills the [dbo].[DimPersonSupervisor] table with data FROM DimPerson. Runtime ~ 13 seconds
-- IMPORTANT: This must run AFTER DimPerson is filled!

-- (Object Model and Workflow) --
-- [DW_Mart].[dbo].[DimPerson] is filled with data 
-- [DW_Mart].[dbo].[spDimPersonSupervisorLoad] (Truncates and Fills)>> [DW_Mart].[dbo].[DimPersonSupervisor]
-- [DW_Mart].[dbo].[DimPerson] AND [DimPersonSupervisor] (supplies data for) >> [DW_View].[dbo].[vPerson]


-- Change Log: When,Who,What
-- 11/15/2023,RRoot,Created Script
-- 01/22/2024,RRoot,Updated Script to capture only latest supervisor ID

--==============================================================================================================================--

-- *********************************************************************************************************
-- EXEC dbo.spDimPersonSupervisorLoad @DebugMode = 1	
-- *********************************************************************************************************

CREATE PROCEDURE [dbo].[spDimPersonSupervisorLoad]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = 0,
 @ExecutionID    VARCHAR(100) = NULL,
 @AuditId		 BIGINT = NULL OUTPUT
)
As
    -- ================================================================================
    -- 
    -- Stored Procedure:   spDimPersonSupervisorLoad
    --
    -- Purpose:            Performs the Full Load ETL process for the DimPersonSupervisor table in DW_Mart.
    --
    --                         Step 1: Truncate and populate a Dimension table (DimPersonSupervisor) with report data
    --                         Step 2: Execute any automated tests associated with this DW table load
    --                         Step 3: Output Source / Insert / Update / Delete counts to log
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
    -- Usage:              EXEC dbo.spDimPersonSupervisorLoad @DebugMode = 1	
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
    -- 11/15/2023     RRoot               BI-11750 Add Supervisor Attributes to PBI Dataset
	-- 11/16/2023	  RRoot				  BI-11750-j Fixed incorrectly mapped [PhoneNumber*KinderCare*Mobile] (was set to [PhoneNumber*Personal*Mobile])
	-- 11/20/2023	  RRoot				  BI-11750-k Added Null to N/A and Blank transformations
	-- 01/22/2024     RRoot               DFTP-1055 Rowcount to use PersonID and EffectiveFrom. Did some code cleanup.
    -- ================================================================================

Begin
    SET NOCOUNT ON;

	  --
	  -- Housekeeping Variables
	  --
    DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
    DECLARE @DebugMsg NVARCHAR(500);
    DECLARE @SourceName VARCHAR(100)= 'DimPersonSupervisor';

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
    -- Step 1: Let's get employee supervisor data from DimPerson based on [DimPerson].[SupervisorNumber] 
    -- ======================================================================================================================

     Begin Try

     	-- Debug output progress
      If @DebugMode = 1
        Begin
          Select @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Beginning DimPersonSupervisor truncate and reload.';
          PRINT @DebugMsg;
        End

      -- Drop table before reload base table we need to build our heirarchy
      If Exists (Select * From [DW_Mart].Sys.Tables Where Name = 'DimPersonSupervisor') 
        Truncate Table [DW_Mart].[dbo].[DimPersonSupervisor];
 
      -- Load table with current data
      With PersonSupervisorColumns
		As (
		SELECT p.[PersonKey]						 AS [DimPersonPersonKey]
			  ,s.[PersonID]							 AS [SupervisorPersonID]
			  ,s.[PersonName]						 AS [SupervisorPersonName]
			  ,s.[EmployeeNumber]					 AS [SupervisorEmployeeNumber]
			  ,s.[EmailAddressKinderCare]			 AS [SupervisorEmailAddressKinderCare]
			  ,s.[PhoneNumberKinderCareWork]		 AS [SupervisorPhoneNumberKinderCareWork]
			  ,s.[PhoneNumberKinderCareMobile]		 AS [SupervisorPhoneNumberKinderCareMobile]
			  ,p.EffectiveStartDate
		  	  ,row_number() over (partition by p.PersonID order by p.[EffectiveFrom] DESC) as 'RowNumber' -- Find latest record for PersonID by EffectiveFrom
		  FROM [DW_MART].[dbo].[DimPerson] as p 
		  LEFT JOIN [DW_Mart].dbo.DimPerson as s -- (1,257,673 rows affected) 
			ON s.PersonID = p.SupervisorNumber AND p.[EffectiveFrom] Between s.[EffectiveFrom] And s.[EffectiveTo]
		) Insert Into [DimPersonSupervisor]
		([DimPersonPersonKey]
		,[SupervisorPersonID]
		,[SupervisorPersonName]
		,[SupervisorEmployeeNumber]
		,[SupervisorEmailAddressKinderCare]
		,[SupervisorPhoneNumberKinderCareWork]
		,[SupervisorPhoneNumberKinderCareMobile]
		) Select 
		   [DimPersonPersonKey]
		  ,IsNull([SupervisorPersonID], -1)
		  ,IsNull([SupervisorPersonName], 'N/A')
		  ,IsNull([SupervisorEmployeeNumber], 'N/A')
		  ,IsNull([SupervisorEmailAddressKinderCare], '')
		  ,IsNull([SupervisorPhoneNumberKinderCareWork], '')
		  ,IsNull([SupervisorPhoneNumberKinderCareMobile], '')
		  From PersonSupervisorColumns
		  Where RowNumber = 1  -- Only get the latest record

    -- ======================================================================================================================
    -- Step 2: Execute any automated tests associated with this DW table load 
    -- ======================================================================================================================

      -- Debug output progress    
      SELECT @SourceCount = COUNT(1) FROM [DW_Mart].dbo.DimPerson;
      SELECT @InsertCount = COUNT(1) FROM [DW_Mart].dbo.DimPersonSupervisor;

      -- Debug output progress
      IF @DebugMode = 1
        Begin
          SELECT @DebugMsg = @ProcName + ' : '+ CONVERT(NVARCHAR(20), GETDATE())
                                       + ' - Extracted ' + CONVERT(NVARCHAR(20), @SourceCount) 
                                       + ' rows from DimPerson and loaded ' +  CONVERT(NVARCHAR(20), @InsertCount)  
                                       + ' rows into DimPersonSupervisor.';
          PRINT @DebugMsg;
        End
 

    -- ======================================================================================================================
    -- Step 3: Output Source / Insert / Update / Delete counts to log
    -- ======================================================================================================================

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
        Select @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Drop and reload for DimPersonSupervisor failed.';
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