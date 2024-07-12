CREATE PROCEDURE [dbo].[spSalesForce_StagingGenerate_LookupMethodOfContact]
(
    @EDWRunDateTime DATETIME2 = NULL,
    @DebugMode INT = NULL
)
AS
BEGIN
    -- ================================================================================
    -- 
    -- Stored Procedure:   spSalesForce_StagingGenerate_LookupMethodOfContact
    --
    -- Purpose:            Populates the SalesForce_Staging 'helper' table dbo.LookupMethodOfContact,
    --                         which is leveraged by the vLeads view for SalesForce ETL loads
    --
    -- Populates:          Truncates and [re]loads SalesForce_Staging..LookupMethodOfContact 
    --
    -- Usage:              EXEC dbo.spSalesForce_StagingGenerate_LookupMethodOfContact @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    --
    --  2/08/17     sburke          BNG-1223 - Initial version of proc
    --	11/03/2021 Adevabhakthuni   BI-2708 -- Updated according to new requirements
    -- ================================================================================
    SET NOCOUNT ON;
    --
    -- Housekeeping Variables
    -- 
    DECLARE @ProcName NVARCHAR(500) = OBJECT_NAME(@@PROCID);
    DECLARE @DebugMsg NVARCHAR(500);
    DECLARE @SourceName VARCHAR(100) = 'LookupMethodOfContact';
    DECLARE @AuditId BIGINT;
    --
    -- ETL status Variables
    --
    DECLARE @RowCount INT;
    DECLARE @Error INT;
    DECLARE @SourceCount INT = 0;
    DECLARE @InsertCount INT = 0;
    DECLARE @UpdateCount INT = 0;
    DECLARE @DeleteCount INT = 0;
    --
    -- If we do not get an @EDWRunDateTime input, set to current date
    --
    IF @EDWRunDateTime IS NULL
        SET @EDWRunDateTime = GETDATE();
    --
    IF @DebugMode = 1
        SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Starting.';
    PRINT @DebugMsg;

    --
    -- Write to EDW AuditLog we are starting
    --
    EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog] @SourceName = @SourceName,
                                               @AuditId = @AuditId OUTPUT;
    --
    BEGIN TRY
        SELECT @DeleteCount = COUNT(1)
        FROM dbo.LookupMethodOfContact;
        TRUNCATE TABLE dbo.LookupMethodOfContact;
        INSERT INTO dbo.LookupMethodOfContact
        (
            MethodOfContact,
            InquiryType
        )
        SELECT 'Chat',
               'Chat'
        UNION
        SELECT 'Email',
               'Email'
        UNION
        SELECT 'Expo',
               'Web'
        UNION
        SELECT 'Fayetteville Web Form',
               'Web'
        UNION
        SELECT 'Import',
               'Web'
        UNION
        SELECT 'Phone',
               'Phone'
        UNION
        SELECT 'Rainbow Enrolled Families',
               'Web'
        UNION
        SELECT 'Re-enroll',
               'Web'
        UNION
        SELECT 'Unknown',
               'Web'
        UNION
        SELECT 'Vendor Prospect List',
               'Vendor Prospect List'
        UNION
        SELECT 'Walk In',
               'Walk-In'
        UNION
        SELECT 'Walk-In',
               'Walk-In'
        UNION
        SELECT 'Web',
               'Web'
        UNION
        SELECT 'Web Form',
               'Web'
        UNION
        SELECT 'Web; Web',
               'Web';
        SELECT @SourceCount = @@ROWCOUNT;
        SELECT @InsertCount = @SourceCount;
        IF @DebugMode = 1
        BEGIN
            SELECT @DebugMsg
                = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Inserted '
                  + CONVERT(NVARCHAR(20), @InsertCount) + N' into Target.';
            PRINT @DebugMsg;
        END;

        --
        -- Write our successful run to the EDW AuditLog 
        --
        EXEC [BING_EDW].[dbo].[spEDWEndAuditLog] @InsertCount = @InsertCount,
                                                 @UpdateCount = @UpdateCount,
                                                 @DeleteCount = @DeleteCount,
                                                 @SourceCount = @SourceCount,
                                                 @AuditId = @AuditId;

        -- Debug output progress
        IF @DebugMode = 1
            SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(20), GETDATE()) + N' - Completing successfully.';
        PRINT @DebugMsg;
    END TRY
    --
    -- Catch, and throw the error back to the calling procedure or client
    --
    BEGIN CATCH
        --
        -- Write our failed run to the EDW AuditLog 
        --
        EXEC [BING_EDW].[dbo].[spEDWErrorAuditLog] @AuditId = @AuditId;
        DECLARE @ErrMsg NVARCHAR(4000),
                @ErrSeverity INT;
        SELECT @ErrMsg = N'Sub-procedure ' + @ProcName + N' - ' + ERROR_MESSAGE(),
               @ErrSeverity = ERROR_SEVERITY();
        RAISERROR(@ErrMsg, @ErrSeverity, 1);
    END CATCH;
END;
GO


