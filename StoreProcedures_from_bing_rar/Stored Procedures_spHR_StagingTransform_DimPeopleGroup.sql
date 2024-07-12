

CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimPeopleGroup] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimPeopleGroup
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
    -- Usage:              DECLARE @EDWRunDateTime DATETIME2 = GETDATE();              
    --                     INSERT #DimPeopleGroupUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_DimPeopleGroup @EDWRunDateTime
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 12/19/2017  sburke              BNG-267 INitial version of proc
    -- 12/20/2017  Banandesi           BNG-267 created
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
             SELECT COALESCE(PeopleGroupID, -1) AS PeopleGroupID,
                    COALESCE(PeopleGroupName, 'Unknown People Group') AS PeopleGroupName,
                    COALESCE(PeopleGroupAssignmentName, 'Unknown People Group Assignment') AS PeopleGroupAssignmentName,
                    COALESCE(PeopleGroupLineOfBusinessName, 'Unknown People Group Line of Business') AS PeopleGroupLineOfBusinessName,
                    COALESCE(PeopleGroupFlexAttribute1, NULL) AS PeopleGroupFlexAttribute1,
                    COALESCE(PeopleGroupFlexAttribute2, NULL) AS PeopleGroupFlexAttribute2,
                    COALESCE(PeopleGroupFlexAttribute3, NULL) AS PeopleGroupFlexAttribute3,
                    COALESCE(PeopleGroupFlexAttribute4, NULL) AS PeopleGroupFlexAttribute4,
                    COALESCE(PeopleGroupFlexAttribute5, NULL) AS PeopleGroupFlexAttribute5,
                    COALESCE(PeopleGroupCreatedDate, '19000101') AS PeopleGroupCreatedDate,
                    COALESCE(PeopleGroupCreatedUser, '-1') AS PeopleGroupCreatedUser,
                    COALESCE(PeopleGroupModifiedDate, '19000101') AS PeopleGroupModifiedDate,
                    COALESCE(PeopleGroupModifiedUser, '-1') AS PeopleGroupModifiedUser,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate
             FROM dbo.vPeopleGroups;
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
