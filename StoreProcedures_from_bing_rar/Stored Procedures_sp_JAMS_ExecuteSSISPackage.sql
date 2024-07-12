
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'sp_JAMS_ExecuteSSISPackage'
)
    DROP PROCEDURE dbo.sp_JAMS_ExecuteSSISPackage;
GO
*/

CREATE PROCEDURE dbo.sp_JAMS_ExecuteSSISPackage
(@ProjectName     NVARCHAR(128),
 @PackageName     NVARCHAR(260),
 @FolderName      NVARCHAR(128) = NULL,
 @EnvrionmentName NVARCHAR(128) = NULL,
 @Use32BitRuntime BIT           = NULL
)
AS
     -- ================================================================================
     -- 
     -- Stored Procedure:  sp_JAMS_ExecuteSSISPackage
     --
     -- Purpose:           Stored proc to be executed by JAMS to run a particular
     --                         ETL SSIS Package.
     --
     --                    Step 1: Create an instance of a package execution in the Integration Services catalog on the server
     --                    Step 2: Set the logging level, and then execute the SSIS Package
     --                    Step 3: Loop every 10 seconds to check the status of the SSIS run.  If still running, wait for
     --                            60 seconds and check again.  If the package has completed, check the completion status
     --                            and if successful, return a success code back to JAMS.  If the package fail, return a
     --                            failure code from this proc.
     -- 
     -- Parameters:        @ProjectName      - Name of the SSIS Project where our chosen SSIS Package resides      
     --                    @PackageName      - Name of the SSIS Package we want to run
     --                    @FolderName       - The SSIS folder where the project resides 
     --                    @EnvrionmentName  - The SSIS Environment to use 
     --                    @@Use32BitRuntime - Flag for whether to use 32bit runtime (defaults to False, so 64bit) 
     --
     -- Usage:             EXEC dbo.sp_JAMS_ExecuteSSISPackage @ProjectName = N'StagingToEDW', @PackageName = N'StagingToEDW_Fact_GLBalances.dtsx';	
     -- 
     -- --------------------------------------------------------------------------------
     --
     -- Change Log:		   
     -- ----------
     --
     -- Date        Modified By         Comments
     -- ----        -----------         --------
     --
     -- 01/16/19    sburke               - Initial version of stored proc, for onboarding BING to JAMS
     --			 
     -- ================================================================================
     BEGIN	 
         -- ------------------------------------------------------------
         -- Set default values for @FolderName and @EnvrionmentName
         -- ------------------------------------------------------------
         IF @FolderName IS NULL
             SET @FolderName = N'BING'; -- Defaults to BING, as that is the only project we have at the moment
         IF @EnvrionmentName IS NULL
             SET @EnvrionmentName = N'BING_SSIS'; -- Defaults to BING_SSIS
         IF @Use32BitRuntime IS NULL
             SET @Use32BitRuntime = 0; -- Defaults to 64bit
         -- ------------------------------------------------------------
         -- Housekeeping
         -- ------------------------------------------------------------		 		          
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SSISEnvReference NVARCHAR(100);
         DECLARE @SSISExecutionId BIGINT;
         -- ------------------------------------------------------------
         -- SSIS Execution status codes (helper variables)
         -- ------------------------------------------------------------	
         DECLARE @SSISExecution_Running INT= 2;
         DECLARE @SSISExecution_Cancelled INT= 3;
         DECLARE @SSISExecution_Failed INT= 4;
         DECLARE @SSISExecution_Pending INT= 5;
         DECLARE @SSISExecution_EndedUnexpectedly INT= 6;
         DECLARE @SSISExecution_Succeeded INT= 7;
         DECLARE @SSISExecution_Stopping INT= 8;
         DECLARE @SSISExecution_Completed INT= 9;
         -- ------------------------------------------------------------	
         BEGIN TRY

             -- ------------------------------------------------------------		 
             -- Get the ReferenceId of the SSIS Environment on this server
             -- ------------------------------------------------------------

             SELECT @SSISEnvReference = reference_id
             FROM [SSISDB].[catalog].[environment_references] er
                  JOIN [SSISDB].[catalog].[projects] p ON p.project_id = er.project_id
             WHERE er.environment_name = @EnvrionmentName
                   AND p.name = @ProjectName;

             -- ------------------------------------------------------------
             -- Create an instance of a package execution in the Integration 
             --     Services catalog on the server.
             -- Returns @execution_id, the unique identifier for an instance 
             --     of execution, which we use to actually execute the 
             --     package.
             -- ------------------------------------------------------------		 
             EXEC [SSISDB].[catalog].[create_execution]
                  @package_name = @PackageName,
                  @execution_id = @SSISExecutionId OUTPUT,
                  @folder_name = @FolderName,
                  @project_name = @ProjectName,
                  @use32bitruntime = @Use32BitRuntime,
                  @reference_id = @SSISEnvReference;

             -- ------------------------------------------------------------	         
             -- This is the execution_id for the package we want to run
             -- ------------------------------------------------------------	

             SELECT @SSISExecutionId;
             DECLARE @LoggingLevel SMALLINT= 1;
             -- Sets the value of LOGGING_LEVEL parameter to 1
             EXEC [SSISDB].[catalog].[set_execution_parameter_value]
                  @SSISExecutionId,
                  @object_type = 50,
                  @parameter_name = N'LOGGING_LEVEL',
                  @parameter_value = @LoggingLevel;
             -- ------------------------------------------------------------				  
             -- Actually start the execution of the package
             -- ------------------------------------------------------------			 
             EXEC [SSISDB].[catalog].[start_execution]
                  @SSISExecutionId;

             -- ------------------------------------------------------------				  
             -- Loop to check the status of the package being executed.
             -- If the status is equal to @SSISExecution_Running then keep
             --     looping, else exit the loop and check the actual status
             --     of the package.		 		  		 		  
             -- ------------------------------------------------------------				  
             DECLARE @PackageRunStatus INT= 0;
             DECLARE @PackageCompletionStatus INT= 0;
             WHILE @PackageCompletionStatus = 0
                 BEGIN
                     WAITFOR DELAY '00:01:00'; -- Check every minute

                     SELECT @PackageRunStatus = status
                     FROM [SSISDB].[catalog].[executions]
                     WHERE execution_id = @SSISExecutionId;
                     SELECT @DebugMsg = CONVERT(VARCHAR(20), GETDATE(), 13)+' : Package Status for '+@PackageName+' - '+CONVERT(VARCHAR(10), @PackageRunStatus);
                     PRINT @DebugMsg;
                     IF @PackageRunStatus > @SSISExecution_Running
                         SET @PackageCompletionStatus = @PackageRunStatus;
                 END;
             -- ------------------------------------------------------------		
             -- If the status of the completed package is not equal to
             --     @SSISExecution_Succeeded, then throw an error back
             --     to the caller.		  		 		 			 
             -- ------------------------------------------------------------				 
             IF @PackageCompletionStatus <> @SSISExecution_Succeeded
                 THROW 51000, 'SSIS Package Execution failed', 1;
             PRINT 'SSIS Package Completed';
         END TRY
         BEGIN CATCH
             --
             -- Raise error
             --	
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
--
GO

/*
EXEC dbo.sp_JAMS_ExecuteSSISPackage
     @ProjectName = N'StagingToEDW',
     @PackageName = N'StagingToEDW_Fact_GLBalances.dtsx';
*/
