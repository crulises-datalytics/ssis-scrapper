CREATE PROCEDURE [dbo].[spBING_EDW_Generate_DimARAgingBucket]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
     BEGIN
         -- ================================================================================
         -- 
         -- Stored Procedure:   spBING_EDW_Generate_DimARAgingBucket
         --
         -- Purpose:            Populates the DimARAgingBucket table in BING_EDW.
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
         -- Populates:          Truncates and [re]loads BING_EDW..DimARAgingBucket
         --
         -- Usage:              EXEC dbo.spBING_EDW_Generate_DimARAgingBucket @DebugMode = 1
         --
         -- --------------------------------------------------------------------------------
         --
         -- Change Log:		   
         -- ----------
         --
         -- Date         Modified By     Comments
         -- ----         -----------     --------
         --
         -- 12/01/17     sburke          
         --  1/09/18     sburke          BNG-998 - Add -2 'Not Applicable' records for Dimension
         --                                 tables
         --  3/27/18     sburke          BNG-1428 - Change of AR Aging Buckets, as per new
         --                                 business requirements    	
         --  4/12/18     sburke          BNG-1428 (again) - Slight tweak to the AR Buckets	    	  		        
         --  4/23/18     sburke          BNG-1644 - Convert the AR Buckets to levels, so we
         --                                  can filter by different granularity with regards
         --                                  to aging	    	    			 
         --			 
         -- ================================================================================
         SET NOCOUNT ON;
         --
         -- Housekeeping Variables
         -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimARAgingBucket';
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
             SELECT @DeleteCount = COUNT(1)
             FROM dbo.DimARAgingBucket;
             TRUNCATE TABLE dbo.DimARAgingBucket;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' from Target.';
                     PRINT @DebugMsg;
             END;
             --
             -- Add Seed row
             --
             DBCC CHECKIDENT('[DimARAgingBucket]', RESEED, 1);
             SET IDENTITY_INSERT dbo.DimARAgingBucket ON;
             INSERT INTO [dbo].[DimARAgingBucket]
             ([ARAgingBucketKey],
              [ARAgingBucketLevel1Name],
              [ARAgingBucketLevel2Name],
              [ARAgingBucketLevel3Name],
              [ARAgingBucketLevel4Name],
              [ARAgingBucketLevel5Name],
              [ARAgingDaysFrom],
              [ARAgingDaysTo],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT-1,
                          'Unknown Aging Level 1 Bucket', -- ARAgingBucketLevel1Name
                          'Unknown Aging Level 2 Bucket', -- ARAgingBucketLevel2Name
                          'Unknown Aging Level 3 Bucket', -- ARAgingBucketLevel3Name
                          'Unknown Aging Level 4 Bucket', -- ARAgingBucketLevel4Name
                          'Unknown Aging Level 5 Bucket', -- ARAgingBucketLevel5Name
                          -1,
                          -1,
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          'Not Applicable Aging Level 1 Bucket', -- ARAgingBucketLevel1Name
                          'Not Applicable Aging Level 2 Bucket', -- ARAgingBucketLevel2Name
                          'Not Applicable Aging Level 3 Bucket', -- ARAgingBucketLevel3Name
                          'Not Applicable Aging Level 4 Bucket', -- ARAgingBucketLevel4Name
                          'Not Applicable Aging Level 5 Bucket', -- ARAgingBucketLevel5Name
                          -1,
                          -1,
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET IDENTITY_INSERT dbo.DimARAgingBucket OFF;

             -- ================================================================================
             -- Insert into dbo.DimARAgingBucket
             -- ================================================================================
             INSERT INTO [dbo].[DimARAgingBucket]
             ([ARAgingBucketLevel1Name],
              [ARAgingBucketLevel2Name],
              [ARAgingBucketLevel3Name],
              [ARAgingBucketLevel4Name],
              [ARAgingBucketLevel5Name],
              [ARAgingDaysFrom],
              [ARAgingDaysTo],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy],
              [Deleted]
             )
                    SELECT a.BucketLevel1,
                           a.BucketLevel2,
                           a.BucketLevel3,
                           a.BucketLevel4,
                           a.BucketLevel5,
                           a.BucketFrom,
                           a.BucketTo,
                           GETDATE() AS 'EDWCreatedDate',
                           CAST(system_user AS VARCHAR(50)) AS EDWCreatedBy,
                           GETDATE() AS 'EDWModifiedDate',
                           CAST(system_user AS VARCHAR(50)) AS 'EDWModifiedBy',
                           NULL AS 'Deleted'
                    FROM
                    (
                        SELECT BucketLevel1 = '0 to 7 Days',
                               BucketLevel2 = '30 Days Or Under',
                               BucketLevel3 = '60 Days Or Under',
                               BucketLevel4 = '90 Days Or Under',
                               BucketLevel5 = '180 Days Or Under',
                               BucketFrom = 0,
                               BucketTo = 7
                        UNION
                        SELECT '8 to 14 Days',
                               '30 Days Or Under',
                               '60 Days Or Under',
                               '90 Days Or Under',
                               '180 Days Or Under',
                               8,
                               14
                        UNION
                        SELECT '15 to 21 Days',
                               '30 Days Or Under',
                               '60 Days Or Under',
                               '90 Days Or Under',
                               '180 Days Or Under',
                               15,
                               21
                        UNION
                        SELECT '22 to 30 Days', -- BNG-1428: Change from 22-28 to 22-30
                               '30 Days Or Under',
                               '60 Days Or Under',
                               '90 Days Or Under',
                               '180 Days Or Under',
                               22,
                               30
                        UNION
                        SELECT '31 to 60 Days', -- BNG-1428: Change from 29-60 to 31-60
                               'Over 30 Days',
                               '60 Days Or Under',
                               '90 Days Or Under',
                               '180 Days Or Under',
                               31,
                               60
                        UNION
                        SELECT '61 to 90 Days',
                               BucketLevel2 = 'Over 30 Days',
                               BucketLevel3 = 'Over 60 Days',
                               BucketLevel4 = '90 Days Or Under',
                               BucketLevel5 = '180 Days Or Under',
                               61,
                               90
                        UNION
                        SELECT '91 to 180 Days', -- BNG-1428: Change from Over 90 to 91-180
                               BucketLevel2 = 'Over 30 Days',
                               BucketLevel3 = 'Over 60 Days',
                               BucketLevel4 = 'Over 90 Days',
                               BucketLevel5 = '180 Days Or Under',
                               91,
                               180
                        UNION
                        SELECT '> 180 Days', -- BNG-1428: Additional Bucket - Over 180 Days
                               BucketLevel2 = 'Over 30 Days',
                               BucketLevel3 = 'Over 60 Days',
                               BucketLevel4 = 'Over 90 Days',
                               BucketLevel5 = 'Over 180 Days',
                               181,
                               99999
                    ) a
                    ORDER BY BucketFrom,
                             BucketTo;
             SELECT @SourceCount = @@ROWCOUNT + 1; -- The Seed Row is the +1

             SELECT @InsertCount = @SourceCount;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' into Target.';
                     PRINT @DebugMsg;
             END;

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


