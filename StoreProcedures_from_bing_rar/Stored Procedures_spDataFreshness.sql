CREATE PROCEDURE dbo.spDataFreshness
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:	spDataFreshness
    --
    -- Purpose:				Gather information on how current key tables are and save them
    --						in BING_EDW for later reporting
    --
    -- Parameters:			n/a
    --
    -- Usage:				in SQL job DataFreshness, EXEC spDataFreshness
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date				Modified By     Comments
    -- ----------		-----------		--------
    --
    --  10/82018		aquitta         BNG-3778 - Initial version
    --			 
    -- ================================================================================
BEGIN

	CREATE TABLE #DataFreshTest (
		DataFreshID INT IDENTITY(1,1),
		TableName NVARCHAR(100),
		RecordCount BIGINT,
		MaxLoadDate DATETIME,
		MostRecentDate DATE,
		MaxTrxDate DATE,
		MaxYrWeek INT,
		ScriptRunDate DATETIME DEFAULT getdate()
		)

	INSERT INTO #DataFreshTest (TableName, RecordCount, MaxLoadDate)
	SELECT	'GL_Staging.dbo.vGLBalances',
			COUNT(1) AS vGLBalances_Count, MAX(MaxStgCreatedDate) as vGLBalances_MaxStgCreatedDate
	FROM	dbo.vGLBalances, 
			(
			SELECT	MAX(StgCreatedDate) AS MaxStgCreatedDate
			FROM	dbo.GLBalances
			UNION ALL
			SELECT	MAX(StgCreatedDate) AS MaxStgCreatedDate
			FROM	dbo.GLCodeCombinations
			) a

	INSERT INTO BING_EDW.dbo.DataFreshTest (
			TableName,
			RecordCount,
			MaxLoadDate,
			MostRecentDate,
			MaxTrxDate,
			MaxYrWeek,
			ScriptRunDate)
	SELECT	TableName,
			RecordCount,
			MaxLoadDate,
			MostRecentDate,
			MaxTrxDate,
			MaxYrWeek,
			getdate() as ScriptRunDate
	FROM	#DataFreshTest

END	-- end of create procedure DataFreshness
