CREATE PROCEDURE [dbo].[spBING_EDW_Generate_DimScheduleWeek]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
     BEGIN
         -- ================================================================================
         -- 
         -- Stored Procedure:   spBING_EDW_Generate_DimScheduleWeek
         --
         -- Purpose:            Populates the DimScheduleWeek table in BING_EDW.
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
         -- Populates:          Truncates and [re]loads BING_EDW..DimScheduleWeek
         --
         -- Usage:              EXEC dbo.spBING_EDW_Generate_DimScheduleWeek @DebugMode = 1
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
         --  1/09/17     sburke          BNG-998 - Add -2 'Not Applicable' records for Dimension
         --                                 tables		
		 --01/11/17      hhebbalu        BNG-997 - Specified the source(CMS/CSS) in ScheduleWeekName
		 --								 for unknown schedules        
         --			 
         -- ================================================================================
         SET NOCOUNT ON;
         --
         -- Housekeeping Variables
         -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimScheduleWeek';
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
             FROM dbo.DimScheduleWeek;
             TRUNCATE TABLE dbo.DimScheduleWeek;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' from Target.';
                     PRINT @DebugMsg;
             END;
             --
             -- Add Seed row
             --
             DBCC CHECKIDENT('[DimScheduleWeek]', RESEED, 1);
             SET IDENTITY_INSERT dbo.DimScheduleWeek ON;
             INSERT INTO [dbo].[DimScheduleWeek]
             ([ScheduleWeekKey],
              [ScheduleWeekName],
              [ScheduleDaysInWeekCount],
              [ScheduleDaysInWeekCountName],
              [ScheduleWeekFlags],
              [ScheduledSunday],
              [ScheduledMonday],
              [ScheduledTuesday],
              [ScheduledWednesday],
              [ScheduledThursday],
              [ScheduledFriday],
              [ScheduledSaturday],
              [EDWCreatedDate],
              [EDWCreatedBy],
              [EDWModifiedDate],
              [EDWModifiedBy]
             )
                    SELECT-1,
                          'Unknown Schedule Week',
                          0,
                          'Unknown Schedule Days In Week Count',
                          '0000000',
                          'Unknown Schedule Week',
                          'Unknown Schedule Week',
                          'Unknown Schedule Week',
                          'Unknown Schedule Week',
                          'Unknown Schedule Week',
                          'Unknown Schedule Week',
                          'Unknown Schedule Week',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER
                    UNION
                    SELECT-2,
                          'Not Applicable Schedule Week',
                          0,
                          'Not Applicable Schedule Days In Week Count',
                          '0000000',
                          'N/A Schedule Week',
                          'N/A Schedule Week',
                          'N/A Schedule Week',
                          'N/A Schedule Week',
                          'N/A Schedule Week',
                          'N/A Schedule Week',
                          'N/A Schedule Week',
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER;
             SET IDENTITY_INSERT dbo.DimScheduleWeek OFF;

             -- ================================================================================
             -- Insert into dbo.DimScheduleWeek, building a dataset of all possible
             --     combinations of days in a week, so we have all possible schedules
             -- ================================================================================
             WITH Numbers(N) -- CTE building the 7 weekday numbers
                  AS (
                  SELECT N
                  FROM(VALUES(1), (2), (3), (4), (5), (6), (7)) Numbers(N)),
                  Recur(N,
                        CalendarID) -- CTE building a combinations of Days per Week as a CalendarID
                  AS (
                  SELECT N,
                         CAST(N AS VARCHAR(1000))
                  FROM Numbers
                  UNION ALL
                  SELECT n.N,
                         CAST(r.CalendarID+','+CAST(n.N AS VARCHAR(10)) AS VARCHAR(1000))
                  FROM Recur r
                       INNER JOIN Numbers n ON n.N > r.N),
                  ScheduleWeekFlags -- CTE building a dataset of 7 flag columns for each days of the week, linking to the CalendarID
                  AS (
                  SELECT CalendarID,
                         CASE
                             WHEN CalendarID LIKE '%1%'
                             THEN '1'
                             ELSE '0'
                         END S,
                         CASE
                             WHEN CalendarID LIKE '%2%'
                             THEN '1'
                             ELSE '0'
                         END M,
                         CASE
                             WHEN CalendarID LIKE '%3%'
                             THEN '1'
                             ELSE '0'
                         END T,
                         CASE
                             WHEN CalendarID LIKE '%4%'
                             THEN '1'
                             ELSE '0'
                         END W,
                         CASE
                             WHEN CalendarID LIKE '%5%'
                             THEN '1'
                             ELSE '0'
                         END TH,
                         CASE
                             WHEN CalendarID LIKE '%6%'
                             THEN '1'
                             ELSE '0'
                         END F,
                         CASE
                             WHEN CalendarID LIKE '%7%'
                             THEN '1'
                             ELSE '0'
                         END SA
                  FROM Recur)
                  INSERT INTO dbo.DimScheduleWeek
                  ([ScheduleWeekName],
                   [ScheduleDaysInWeekCount],
                   [ScheduleDaysInWeekCountName],
                   [ScheduleWeekFlags],
                   [ScheduledSunday],
                   [ScheduledMonday],
                   [ScheduledTuesday],
                   [ScheduledWednesday],
                   [ScheduledThursday],
                   [ScheduledFriday],
                   [ScheduledSaturday],
                   [EDWCreatedDate],
                   [EDWCreatedBy],
                   [EDWModifiedDate],
                   [EDWModifiedBy]
                  )
                         SELECT CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(r.CalendarID, '1', 'Su'), '2', 'M'), '3', 'Tu'), '4', 'W'), '5', 'Th'), '6', 'F'), '7', 'Sa') AS VARCHAR(50)) AS ScheduleWeekName,
                                (LEN(r.CalendarID)-LEN(REPLACE(r.CalendarID, ',', ''))+1) ScheduleDaysInWeekCount,
                                CASE
                                    WHEN(LEN(r.CalendarID)-LEN(REPLACE(r.CalendarID, ',', ''))+1) = 1
                                    THEN '1 Day'
                                    ELSE CAST((LEN(r.CalendarID)-LEN(REPLACE(r.CalendarID, ',', ''))+1) AS VARCHAR(2))+' Days'
                                END ScheduleDaysInWeekCountName,
                                CONCAT(S, M, T, W, TH, F, SA) AS ScheduleWeekFlags,
                                CASE
                                    WHEN S = 1
                                    THEN 'Scheduled Sunday'
                                    ELSE 'Not Scheduled Sunday'
                                END ScheduledSunday,
                                CASE
                                    WHEN M = 1
                                    THEN 'Scheduled Monday'
                                    ELSE 'Not Scheduled Monday'
                                END ScheduledMonday,
                                CASE
                                    WHEN T = 1
                                    THEN 'Scheduled Tuesday'
                                    ELSE 'Not Scheduled Tuesday'
                                END ScheduledTuesday,
                                CASE
                                    WHEN W = 1
                                    THEN 'Scheduled Wednesday'
                                    ELSE 'Not Scheduled Wednesday'
                                END ScheduledWednesday,
                                CASE
                                    WHEN TH = 1
                                    THEN 'Scheduled Thursday'
                                    ELSE 'Not Scheduled Thursday'
                                END ScheduledThursday,
                                CASE
                                    WHEN F = 1
                                    THEN 'Scheduled Friday'
                                    ELSE 'Not Scheduled Friday'
                                END ScheduledFriday,
                                CASE
                                    WHEN SA = 1
                                    THEN 'Scheduled Saturday'
                                    ELSE 'Not Scheduled Saturday'
                                END ScheduledSaturday,
                                GETDATE() AS EDWCreatedDate,
                                CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                                GETDATE() AS EDWModifiedDate,
                                CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy
                         FROM Recur r
                              JOIN ScheduleWeekFlags s ON r.CalendarID = s.CalendarID
                         ORDER BY LEN(r.CalendarID),
                                  r.CalendarID;
             SELECT @SourceCount = @@ROWCOUNT;
             -- ================================================================================
             -- Second insert into dbo.DimScheduleWeek, specifically for 'Unknown' schedules
             -- ================================================================================
             WITH ScheduleWeek
                  AS (
                  SELECT 'Unknown Days(CMS) ' AS ScheduleWeekName,
                         1 AS ScheduleDaysInWeekCount,
                         '1 Day' AS ScheduleDaysInWeekCountName,
                         '0000000' AS ScheduleWeekFlags,
                         'Unknown Schedule Day' AS ScheduledSunday,
                         'Unknown Schedule Day' AS ScheduledMonday,
                         'Unknown Schedule Day' AS ScheduledTuesday,
                         'Unknown Schedule Day' AS ScheduledWednesday,
                         'Unknown Schedule Day' AS ScheduledThursday,
                         'Unknown Schedule Day' AS ScheduledFriday,
                         'Unknown Schedule Day' AS ScheduledSaturday
                  UNION
                  SELECT 'Unknown Days(CMS)' AS ScheduleWeekName,
                         2 AS ScheduleDaysInWeekCount,
                         '2 Days' AS ScheduleDaysInWeekCountName,
                         '0000000' AS ScheduleWeekFlags,
                         'Unknown Schedule Day' AS ScheduledSunday,
                         'Unknown Schedule Day' AS ScheduledMonday,
                         'Unknown Schedule Day' AS ScheduledTuesday,
                         'Unknown Schedule Day' AS ScheduledWednesday,
                         'Unknown Schedule Day' AS ScheduledThursday,
                         'Unknown Schedule Day' AS ScheduledFriday,
                         'Unknown Schedule Day' AS ScheduledSaturday
                  UNION
                  SELECT 'Unknown Days(CMS)' AS ScheduleWeekName,
                         3 AS ScheduleDaysInWeekCount,
                         '3 Days' AS ScheduleDaysInWeekCountName,
                         '0000000' AS ScheduleWeekFlags,
                         'Unknown Schedule Day' AS ScheduledSunday,
                         'Unknown Schedule Day' AS ScheduledMonday,
                         'Unknown Schedule Day' AS ScheduledTuesday,
                         'Unknown Schedule Day' AS ScheduledWednesday,
                         'Unknown Schedule Day' AS ScheduledThursday,
                         'Unknown Schedule Day' AS ScheduledFriday,
                         'Unknown Schedule Day' AS ScheduledSaturday
                  UNION
                  SELECT 'Unknown Days(CMS)' AS ScheduleWeekName,
                         4 AS ScheduleDaysInWeekCount,
                         '4 Days' AS ScheduleDaysInWeekCountName,
                         '0000000' AS ScheduleWeekFlags,
                         'Unknown Schedule Day' AS ScheduledSunday,
                         'Unknown Schedule Day' AS ScheduledMonday,
                         'Unknown Schedule Day' AS ScheduledTuesday,
                         'Unknown Schedule Day' AS ScheduledWednesday,
                         'Unknown Schedule Day' AS ScheduledThursday,
                         'Unknown Schedule Day' AS ScheduledFriday,
                         'Unknown Schedule Day' AS ScheduledSaturday
                  UNION
                  SELECT 'Unknown Days(CMS)' AS ScheduleWeekName,
                         5 AS ScheduleDaysInWeekCount,
                         '5 Days' AS ScheduleDaysInWeekCountName,
                         '0000000' AS ScheduleWeekFlags,
                         'Unknown Schedule Day' AS ScheduledSunday,
                         'Unknown Schedule Day' AS ScheduledMonday,
                         'Unknown Schedule Day' AS ScheduledTuesday,
                         'Unknown Schedule Day' AS ScheduledWednesday,
                         'Unknown Schedule Day' AS ScheduledThursday,
                         'Unknown Schedule Day' AS ScheduledFriday,
                         'Unknown Schedule Day' AS ScheduledSaturday
                  UNION
                  SELECT 'Unknown Days(CSS)' AS ScheduleWeekName,
                         5 AS ScheduleDaysInWeekCount,
                         '1-5 Days' AS ScheduleDaysInWeekCountName,
                         '0000000' AS ScheduleWeekFlags,
                         'Unknown Schedule Day' AS ScheduledSunday,
                         'Unknown Schedule Day' AS ScheduledMonday,
                         'Unknown Schedule Day' AS ScheduledTuesday,
                         'Unknown Schedule Day' AS ScheduledWednesday,
                         'Unknown Schedule Day' AS ScheduledThursday,
                         'Unknown Schedule Day' AS ScheduledFriday,
                         'Unknown Schedule Day' AS ScheduledSaturday)
                  INSERT INTO DimScheduleWeek
                         SELECT ScheduleWeekName,
                                ScheduleDaysInWeekCount,
                                ScheduleDaysInWeekCountName,
                                ScheduleWeekFlags,
                                ScheduledSunday,
                                ScheduledMonday,
                                ScheduledTuesday,
                                ScheduledWednesday,
                                ScheduledThursday,
                                ScheduledFriday,
                                ScheduledSaturday,
                                GETDATE() AS EDWCreatedDate,
                                CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                                GETDATE() AS EDWModifiedDate,
                                CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy
                         FROM ScheduleWeek;
             SELECT @SourceCount = @SourceCount + @@ROWCOUNT + 1; -- The See Row is the +1

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


