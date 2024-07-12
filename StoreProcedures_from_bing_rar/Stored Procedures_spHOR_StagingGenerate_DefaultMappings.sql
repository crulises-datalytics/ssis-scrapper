CREATE PROCEDURE [dbo].[spHOR_StagingGenerate_DefaultMappings]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
     BEGIN
         -- ================================================================================
         -- 
         -- Stored Procedure:   spHOR_StagingGenerate_DefaultMappings
         --
         -- Purpose:            Populates Horizon Staging mapping tables with default values when none
         --                         are provided by the Horizon source.
         --
         -- Populates:          Loads the following tables with default data when they have no data
         --                         from Horizon
         --
         -- Usage:              EXEC dbo.spHOR_StagingGenerate_DefaultMappings @DebugMode = 1
         --
         -- --------------------------------------------------------------------------------
         --
         -- Change Log:		   
         -- ----------
         --
         -- Date         Modified By     Comments
         -- ----         -----------     --------
         --
         --  2/12/17     sburke          BNG-1212 - Initial version of proc
         --			 
         -- ================================================================================
         SET NOCOUNT ON;
         --
         -- Housekeeping Variables
         -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         --
         -- ETL status Variables
         --
         DECLARE @RowCount INT;
         DECLARE @Error INT;
         DECLARE @UserName VARCHAR(128);
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
         SET @UserName = SUSER_NAME();
	    --
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting.';
         PRINT @DebugMsg;
	 	 
         --
         BEGIN TRY
             -- ================================================================================
             -- If no data in BUSCompanyDefault, create default record
             -- ================================================================================
             IF NOT EXISTS
             (
                 SELECT 1
                 FROM dbo.BUSCompanyDefault
             )
                 BEGIN
                     INSERT INTO dbo.BUSCompanyDefault
                     (id,
                      CompanyDefault,
                      StgCreatedDate,
                      StgCreatedby
                     )
                     VALUES
                     (1,
                      '015',
                      @EDWRunDateTime,
                      @UserName
                     );
                     SELECT @SourceCount = @@ROWCOUNT;
                     SELECT @InsertCount = @SourceCount;
                     -- Log our Default insert
                     INSERT INTO dbo.StagingETLBatchControl
                     ([EventName],
                      [LastProcessedDate],
                      [Status]
                     )
                     VALUES
                     ('BUSCompanyDefault',
                      @EDWRunDateTime,
                      'Completed'
                     );
             END;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' into BUSCompanyDefault.';
                     PRINT @DebugMsg;
             END;
             -- ================================================================================
             -- If no data in CCTMapping, create default record
             -- ================================================================================
             IF NOT EXISTS
             (
                 SELECT 1
                 FROM dbo.CCTMapping
             )
                 BEGIN
                     INSERT INTO dbo.CCTMapping
                     (CostCenter,
                      CCT,
                      StgCreatedDate,
                      StgCreatedby
                     )
                            SELECT '700015',
                                   '3092',
                                   @EDWRunDateTime,
                                   @UserName
                            UNION
                            SELECT '700000',
                                   '1106',
                                   @EDWRunDateTime,
                                   @UserName
                            UNION
                            SELECT '069012',
                                   '3301',
                                   @EDWRunDateTime,
                                   @UserName
                            UNION
                            SELECT '006202',
                                   '0241',
                                   @EDWRunDateTime,
                                   @UserName
                            UNION
                            SELECT '006203',
                                   '0241',
                                   GETDATE(),
                                   @UserName;
                     SELECT @SourceCount = @@ROWCOUNT;
                     SELECT @InsertCount = @SourceCount;
                     -- Log our Default insert
                     INSERT INTO dbo.StagingETLBatchControl
                     ([EventName],
                      [LastProcessedDate],
                      [Status]
                     )
                     VALUES
                     ('CCTMapping',
                      @EDWRunDateTime,
                      'Completed'
                     );
             END;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' into CCTMapping.';
                     PRINT @DebugMsg;
             END;
             -- ================================================================================
             -- If no data in CompanyMapping, create default record
             -- ================================================================================
             IF NOT EXISTS
             (
                 SELECT 1
                 FROM dbo.CompanyMapping
             )
                 BEGIN
                     INSERT INTO dbo.CompanyMapping
                     (CostCenter,
                      SubAccountNumber,
                      Company,
                      StgCreatedDate,
                      StgCreatedby
                     )
                            SELECT '700000',
                                   '350150',
                                   '001',
                                   @EDWRunDateTime,
                                   @UserName
                            UNION
                            SELECT '700000',
                                   '010330',
                                   '001',
                                   @EDWRunDateTime,
                                   @UserName
                            UNION
                            SELECT '700000',
                                   '012010',
                                   '001',
                                   @EDWRunDateTime,
                                   @UserName;
                     SELECT @SourceCount = @@ROWCOUNT;
                     SELECT @InsertCount = @SourceCount;
                     -- Log our Default insert
                     INSERT INTO dbo.StagingETLBatchControl
                     ([EventName],
                      [LastProcessedDate],
                      [Status]
                     )
                     VALUES
                     ('CompanyMapping',
                      @EDWRunDateTime,
                      'Completed'
                     );
             END;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' into CompanyMapping.';
                     PRINT @DebugMsg;
             END;


             -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Completing successfully.';
             PRINT @DebugMsg;
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


