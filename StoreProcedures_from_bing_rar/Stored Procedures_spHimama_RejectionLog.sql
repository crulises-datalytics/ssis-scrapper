CREATE PROCEDURE [dbo].[spHimama_RejectionLog]
(@SourceFileName  VARCHAR(50),
 @EDWRunDateTime DATETIME2 = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHimama_RejectionLog
    --
    -- Purpose:         Inserts the bad records into the RejectionLog table
    --
    -- Parameters:		@EDWRunDateTime
    --------------------------------------------------------------------------------------------------------------------------------------------------
    -- Change Log:
    --------------------------------------------------------------------------------------------------------------------------------------------------
    -- Date          Modified By         Comments
    --------------------------------------------------------------------------------------------------------------------------------------------------
    --
    --10/23/20		 hhebbalu            BI-4196
    -- ================================================================================
    BEGIN
        SET NOCOUNT ON;
        --
        -- If we do not get an @EDWRunDateTime input, set to current date
        --
        IF @EDWRunDateTime IS NULL
            SET @EDWRunDateTime = GETDATE(); 


-- Create a CTE to get the old and new first and last names
WITH CTEHiMamaAttendanceLanding AS
(
	SELECT 
		ChildKindercareID
		, ChildName
		, CASE WHEN CHARINDEX(' ', RTRIM(LTRIM(ChildName))) = 0 THEN ChildName
			ELSE SUBSTRING(ChildName, 1, (CHARINDEX(' ', RTRIM(LTRIM(ChildName)))-1)) 
		END AS FirstName
		, CASE 
		WHEN CHARINDEX(' ', RTRIM(LTRIM(ChildName))) <> 0 
			THEN SUBSTRING(ChildName, (CHARINDEX(' ', RTRIM(LTRIM(ChildName)))+ 1), LEN(ChildName)) 
		ELSE NULL 
		END AS LastName
		, FirstName AS NewFirstName
		, LastName AS NewLastName
	FROM HiMamaAttendanceLanding
)
--Insert into RejectionLog table if there are bad records
INSERT INTO dbo.RejectionLog
	SELECT 
		'HiMamaAttendanceLanding' AS TableName
		, '[Child Name]' AS ColumnName
		, ChildName AS SourceValue
		, CASE WHEN LEN(FirstName) > 50 THEN NewFirstName
		WHEN LEN(LastName) > 50 THEN NewLastName
		WHEN LEN(FirstName) > 50 AND LEN(LastName) > 50 THEN NewFirstName + NewLastName
		END AS NewValue
		, 'Truncation' AS Reason
		, @SourceFileName AS SourceFileSystem
		, @EDWRunDateTime AS CreatedDate
		, suser_sname() AS CreatedBy
	FROM CTEHiMamaAttendanceLanding
	WHERE LEN(FirstName) > 50 OR LEN(LastName) > 50
    END;
GO