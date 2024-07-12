
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCSS_StagingLoad_CenterCSSMigrations'
)
    DROP PROCEDURE dbo.spCSS_StagingLoad_CenterCSSMigrations;
GO
--*/

CREATE PROCEDURE [dbo].[spCSS_StagingLoad_CenterCSSMigrations]
AS
-- ================================================================================
-- 
-- Stored Procedure:   spCSS_StagingLoad_CenterCSSMigrations
--
-- Purpose:            Loads date for lookup table CenterCSSMigrations, which holds details
--                         of when CSS Centers are migrated over to CMS.
--
--                     This is a temporary measure until all CSS -> CMS migrations have 
--                         been completed.  Want to keep the migration list under wraps and version
--                         controlled - this seems as good a means as any.
--
-- Parameters:         None 
--				   
-- --------------------------------------------------------------------------------
--
-- Change Log:		   
-- ----------
--
-- Date         Modified By     Comments
-- ----         -----------     --------
--
-- 10/08/18     sburke          Initial version
-- 10/19/18     sburke          Added additional CSS Centers that closed before migration	
-- 12/03/18     sburke          Corrected final migration date to 01-Dec-18
--                              Also included CostCenters that were closed in CSS in 2018,
--                                 and so were not migrated to CMS		 
-- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	--
	-- Housekeeping Variables
	-- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
	--
	-- Execute the extract / transform from the Staging database source
	--
         BEGIN TRY
             TRUNCATE TABLE dbo.CenterCSSMigrations;
			-- --------------------------------------------------
			-- Migration Wave -1: CMS Centers eroneously on CSS
			--
			-- There are some Centers that have been on CMS since
			--   inception, but for some reason have records
			--   from CSS also.  Use Migration Wave 0 to have
			--   the CSS side of the transcation filtered, so
			--   so only CMS data is loaded										        
			-- -------------------------------------------------- 


			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000811', '4811', -1, '2011-01-08', 201101 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070337', '7337', -1, '2011-01-08', 201101 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '072102', '7102', -1, '2011-01-08', 201101 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301371', '1371', -1, '2011-01-08', 201101 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301376', '1376', -1, '2011-01-08', 201101 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301408', '1408', -1, '2011-01-08', 201101 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301410', '1410', -1, '2011-01-08', 201101 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301417', '1417', -1, '2011-01-08', 201101 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301425', '1425', -1, '2011-01-08', 201101 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301439', '1439', -1, '2011-01-08', 201101 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301606', '1606', -1, '2011-01-08', 201101 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301671', '1671', -1, '2011-01-08', 201101 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303011', '3011', -1, '2011-01-08', 201101 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303811', '3811', -1, '2011-01-08', 201101 );
			-- --------------------------------------------------
			-- Migration Wave 0: CCS Centers closed before migration
			--
			-- There are some Centers on CSS that closed before
			--   they were part of any migration.  We set them
			--   to have a Migration Wave of 0, but set the migration
			--   date to 2018-12-01 (Final Migration) to ensure 
			--   that we do not get CMS records for them										        
			-- -------------------------------------------------- 									

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000099', '4099', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000265', '4265', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000382', '4382', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000539', '4539', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000853', '4853', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070392', '7392', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071405', '7405', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300801', '0801', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301223', '1223', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301225', '1225', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301271', '1271', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301295', '1295', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301467', '1467', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301481', '1481', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301486', '1486', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301562', '1562', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301609', '1609', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301655', '1655', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301785', '1785', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301192', '1192', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301733', '1733', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300280', '0280', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301522', '1522', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070303', '7303', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301265', '1265', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000620', '4620', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301332', '1332', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000852', '4852', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303015', '3015', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301357', '1357', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300909', '0909', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303020', '3020', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300676', '0676', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300472', '0472', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300236', '0236', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301193', '1193', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070302', '7302', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000595', '4595', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000464', '4464', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300136', '0136', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301150', '1150', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301346', '1346', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301500', '1500', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300308', '0308', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000639', '4639', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000723', '4723', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000775', '4775', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301058', '1058', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301147', '1147', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300749', '0749', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300270', '0270', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300147', '0147', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301665', '1665', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300453', '0453', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070695', '7695', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000530', '4530', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070890', '7890', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300265', '0265', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300085', '0085', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301563', '1563', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301497', '1497', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000489', '4489', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300511', '0511', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000543', '4543', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000637', '4637', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301741', '1741', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301355', '1355', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300648', '0648', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000182', '4182', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303035', '3035', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300241', '0241', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301471', '1471', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000441', '4441', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070225', '7225', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300527', '0527', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000242', '4242', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000712', '4712', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '307001', '7001', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000140', '4140', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300764', '0764', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303521', '3521', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000503', '4503', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301400', '1400', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301656', '1656', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000016', '4016', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301387', '1387', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300612', '0612', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301151', '1151', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300629', '0629', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000418', '4418', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301662', '1662', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300751', '0751', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000491', '4491', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301237', '1237', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300832', '0832', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301706', '1706', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071406', '7406', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301185', '1185', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300819', '0819', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301569', '1569', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300769', '0769', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300991', '0991', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '305025', '5025', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000497', '4497', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301017', '1017', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303008', '3008', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301538', '1538', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300510', '0510', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300199', '0199', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300932', '0932', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301255', '1255', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300150', '0150', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301463', '1463', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300423', '0423', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300608', '0608', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300933', '0933', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000717', '4717', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301306', '1306', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000252', '4252', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000057', '4057', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000281', '4281', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301360', '1360', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000901', '4901', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301353', '1353', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300555', '0555', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300092', '0092', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301559', '1559', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303505', '3505', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300196', '0196', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000479', '4479', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300645', '0645', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070231', '7231', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '307003', '7003', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000079', '4079', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301557', '1557', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300816', '0816', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300094', '0094', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300173', '0173', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301096', '1096', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303059', '3059', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000567', '4567', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300266', '0266', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300739', '0739', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301703', '1703', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000245', '4245', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300533', '0533', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300299', '0299', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300951', '0951', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301598', '1598', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000577', '4577', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303030', '3030', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000001', '4001', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '305021', '5021', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000523', '4523', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300901', '0901', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301205', '1205', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300768', '0768', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303804', '3804', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073026', '7026', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301321', '1321', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301074', '1074', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071403', '7403', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300323', '0323', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000656', '4656', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301274', '1274', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300345', '0345', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300650', '0650', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300339', '0339', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301654', '1654', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000408', '4408', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000860', '4860', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300781', '0781', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300532', '0532', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300705', '0705', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303066', '3066', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301672', '1672', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000423', '4423', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071408', '7408', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303504', '3504', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301236', '1236', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300090', '0090', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000840', '4840', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000178', '4178', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301296', '1296', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301506', '1506', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300927', '0927', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300142', '0142', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303502', '3502', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300261', '0261', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300356', '0356', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000744', '4744', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000754', '4754', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300642', '0642', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000279', '4279', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000835', '4835', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300214', '0214', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300269', '0269', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '305037', '5037', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300309', '0309', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300646', '0646', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300179', '0179', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303048', '3048', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300205', '0205', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000895', '4895', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000487', '4487', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301318', '1318', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301354', '1354', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300640', '0640', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303058', '3058', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300779', '0779', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300853', '0853', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301320', '1320', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301171', '1171', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300200', '0200', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301691', '1691', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300775', '0775', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301008', '1008', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303026', '3026', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300260', '0260', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300807', '0807', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301239', '1239', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301449', '1449', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301145', '1145', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300849', '0849', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000821', '4821', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301737', '1737', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300341', '0341', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303004', '3004', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300615', '0615', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301724', '1724', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301199', '1199', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300086', '0086', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303520', '3520', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300240', '0240', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301259', '1259', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300178', '0178', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300106', '0106', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070327', '7327', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301298', '1298', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300551', '0551', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000129', '4129', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000891', '4891', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300293', '0293', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301088', '1088', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303055', '3055', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000173', '4173', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300294', '0294', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300806', '0806', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301226', '1226', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300373', '0373', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300303', '0303', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301680', '1680', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301605', '1605', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300663', '0663', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303050', '3050', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300407', '0407', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301359', '1359', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300797', '0797', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300948', '0948', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303899', '3899', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301100', '1100', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303046', '3046', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300003', '0003', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300107', '0107', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301688', '1688', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303503', '3503', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303016', '3016', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073048', '7048', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301289', '1289', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300509', '0509', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300193', '0193', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301026', '1026', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300684', '0684', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070968', '7968', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '305036', '5036', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303049', '3049', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300239', '0239', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301578', '1578', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300636', '0636', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300379', '0379', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000165', '4165', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301895', '1895', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300961', '0961', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301857', '1857', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '305013', '5013', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000087', '4087', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300923', '0923', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301106', '1106', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303014', '3014', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000309', '4309', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000424', '4424', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301203', '1203', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300382', '0382', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300554', '0554', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303067', '3067', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303057', '3057', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301558', '1558', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000381', '4381', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300371', '0371', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000443', '4443', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303515', '3515', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000472', '4472', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301516', '1516', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300391', '0391', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000384', '4384', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '075521', '8521', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000869', '4869', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000262', '4262', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300746', '0746', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300776', '0776', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000278', '4278', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300191', '0191', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000220', '4220', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301576', '1576', 0, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301174', '1174', 0, '2018-12-01', 201848 );
			-- --------------------------------------------------
			-- Migration Wave 1: 2017-07-08
			-- --------------------------------------------------

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070601', '7601', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070602', '7602', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300982', '0982', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300987', '0987', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301098', '1098', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301121', '1121', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301235', '1235', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301243', '1243', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301659', '1659', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301699', '1699', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000343', '4343', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000457', '4457', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070880', '7880', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300850', '0850', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301014', '1014', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301526', '1526', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301610', '1610', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301698', '1698', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000731', '4731', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300902', '0902', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300965', '0965', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300986', '0986', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301036', '1036', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301042', '1042', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301056', '1056', 1, '2017-07-08', 201727 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301702', '1702', 1, '2017-07-08', 201727 );
			-- --------------------------------------------------
			-- Migration Wave 2: 2018-02-24
			-- --------------------------------------------------

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301202', '1202', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300898', '0898', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301146', '1146', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301604', '1604', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301707', '1707', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300899', '0899', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301209', '1209', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300960', '0960', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300833', '0833', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000389', '4389', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301696', '1696', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300811', '0811', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300851', '0851', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301375', '1375', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301166', '1166', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300809', '0809', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301479', '1479', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300871', '0871', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000276', '4276', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000590', '4590', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301366', '1366', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301137', '1137', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301152', '1152', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300993', '0993', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301186', '1186', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300813', '0813', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301658', '1658', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301421', '1421', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000402', '4402', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000659', '4659', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070892', '7892', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300921', '0921', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301305', '1305', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301257', '1257', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301004', '1004', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300911', '0911', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301336', '1336', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070886', '7886', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300910', '0910', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301612', '1612', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301266', '1266', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301003', '1003', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301031', '1031', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301491', '1491', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300830', '0830', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300863', '0863', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301667', '1667', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301024', '1024', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301631', '1631', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301053', '1053', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301240', '1240', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300946', '0946', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301162', '1162', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300903', '0903', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301617', '1617', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301786', '1786', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301812', '1812', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000534', '4534', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301022', '1022', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300375', '0375', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300306', '0306', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300563', '0563', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300952', '0952', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300313', '0313', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301007', '1007', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301464', '1464', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300404', '0404', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301690', '1690', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301000', '1000', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300410', '0410', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300412', '0412', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301572', '1572', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300414', '0414', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300409', '0409', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301197', '1197', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301342', '1342', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300408', '0408', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300411', '0411', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000167', '4167', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000889', '4889', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301527', '1527', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301313', '1313', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301338', '1338', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300906', '0906', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '305003', '5003', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301175', '1175', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000214', '4214', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300793', '0793', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301132', '1132', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301020', '1020', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301341', '1341', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300950', '0950', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300792', '0792', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300969', '0969', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301277', '1277', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300920', '0920', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301109', '1109', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000112', '4112', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301846', '1846', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301190', '1190', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301315', '1315', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301343', '1343', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301720', '1720', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301285', '1285', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301629', '1629', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301608', '1608', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301221', '1221', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301728', '1728', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301333', '1333', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300774', '0774', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301228', '1228', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301388', '1388', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301390', '1390', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301379', '1379', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301586', '1586', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300725', '0725', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301329', '1329', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301217', '1217', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301601', '1601', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301200', '1200', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301218', '1218', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301093', '1093', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301021', '1021', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301245', '1245', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301553', '1553', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300934', '0934', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300885', '0885', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301513', '1513', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300887', '0887', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301037', '1037', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300216', '0216', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301138', '1138', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301254', '1254', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301290', '1290', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301139', '1139', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300342', '0342', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301046', '1046', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301264', '1264', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300531', '0531', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301833', '1833', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300753', '0753', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301334', '1334', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301261', '1261', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '302500', '2500', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301073', '1073', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300938', '0938', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301045', '1045', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301054', '1054', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301747', '1747', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301860', '1860', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000100', '4100', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000290', '4290', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000416', '4416', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000623', '4623', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300791', '0791', 2, '2018-02-24', 201808 );

			INSERT INTO dbo.CenterCSSMigrations
			-- --------------------------------------------------
			-- Migration Wave 3: 2018-06-02
			-- --------------------------------------------------
			VALUES( '000293', '4293', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000663', '4663', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070386', '7386', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300820', '0820', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300962', '0962', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301034', '1034', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301063', '1063', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301126', '1126', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301248', '1248', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301453', '1453', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301623', '1623', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301865', '1865', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070304', '7304', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300867', '0867', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300971', '0971', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301015', '1015', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301027', '1027', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301358', '1358', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301508', '1508', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301566', '1566', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301580', '1580', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301697', '1697', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301749', '1749', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301750', '1750', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000116', '4116', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000409', '4409', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070343', '7343', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070347', '7347', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070348', '7348', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300838', '0838', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300858', '0858', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300878', '0878', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300984', '0984', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301039', '1039', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301051', '1051', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301101', '1101', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301397', '1397', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301830', '1830', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000388', '4388', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070335', '7335', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300461', '0461', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300462', '0462', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300873', '0873', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301163', '1163', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301187', '1187', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301595', '1595', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301668', '1668', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000646', '4646', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300464', '0464', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300467', '0467', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300897', '0897', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301060', '1060', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301105', '1105', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301535', '1535', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301767', '1767', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301770', '1770', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303073', '3073', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070349', '7349', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300782', '0782', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301224', '1224', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301308', '1308', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301369', '1369', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301529', '1529', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301635', '1635', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301647', '1647', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301670', '1670', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301736', '1736', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301762', '1762', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303002', '3002', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300845', '0845', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300970', '0970', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301048', '1048', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301090', '1090', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301172', '1172', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301361', '1361', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301399', '1399', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301757', '1757', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301824', '1824', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301854', '1854', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303001', '3001', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303003', '3003', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303010', '3010', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300218', '0218', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300579', '0579', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300604', '0604', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300605', '0605', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301180', '1180', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301466', '1466', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301583', '1583', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301624', '1624', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301660', '1660', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301763', '1763', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301771', '1771', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303039', '3039', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070332', '7332', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070333', '7333', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070368', '7368', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070374', '7374', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070375', '7375', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300219', '0219', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300465', '0465', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300471', '0471', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300474', '0474', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300743', '0743', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301065', '1065', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301626', '1626', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070325', '7325', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070354', '7354', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300460', '0460', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300979', '0979', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301335', '1335', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301367', '1367', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301372', '1372', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301507', '1507', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301713', '1713', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301806', '1806', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000153', '4153', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000650', '4650', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000743', '4743', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300466', '0466', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300882', '0882', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300886', '0886', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301062', '1062', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301458', '1458', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301634', '1634', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301648', '1648', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301923', '1923', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303072', '3072', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000533', '4533', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000874', '4874', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300856', '0856', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301012', '1012', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301176', '1176', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301181', '1181', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301227', '1227', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301551', '1551', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301645', '1645', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301666', '1666', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000210', '4210', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000511', '4511', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000561', '4561', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300100', '0100', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300213', '0213', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300274', '0274', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300296', '0296', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300362', '0362', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300454', '0454', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300459', '0459', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300881', '0881', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300913', '0913', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301198', '1198', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000056', '4056', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000187', '4187', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301129', '1129', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301133', '1133', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301424', '1424', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000275', '4275', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000548', '4548', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000722', '4722', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '072108', '7108', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '072701', '8701', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300869', '0869', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301183', '1183', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301207', '1207', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301433', '1433', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301499', '1499', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000137', '4137', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000400', '4400', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000900', '4900', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300249', '0249', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300839', '0839', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300841', '0841', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300842', '0842', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300843', '0843', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300859', '0859', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301032', '1032', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301049', '1049', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301528', '1528', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301540', '1540', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000063', '4063', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000274', '4274', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000584', '4584', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300195', '0195', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300295', '0295', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300370', '0370', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300448', '0448', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300450', '0450', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300788', '0788', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300802', '0802', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300942', '0942', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301033', '1033', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301323', '1323', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301419', '1419', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000061', '4061', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000485', '4485', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000730', '4730', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000767', '4767', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '072126', '7126', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300783', '0783', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301023', '1023', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301057', '1057', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301446', '1446', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301476', '1476', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301490', '1490', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000277', '4277', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000632', '4632', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300953', '0953', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301312', '1312', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301365', '1365', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301465', '1465', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000359', '4359', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000544', '4544', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000598', '4598', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000749', '4749', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300155', '0155', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300455', '0455', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300847', '0847', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300864', '0864', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301009', '1009', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301164', '1164', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301911', '1911', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000065', '4065', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301246', '1246', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301252', '1252', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301434', '1434', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301448', '1448', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301457', '1457', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301496', '1496', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000066', '4066', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000504', '4504', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000505', '4505', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000506', '4506', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000748', '4748', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000827', '4827', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000858', '4858', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000859', '4859', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300152', '0152', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300974', '0974', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300975', '0975', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300976', '0976', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300977', '0977', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301352', '1352', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000015', '4015', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000224', '4224', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000385', '4385', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000413', '4413', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071402', '7402', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300491', '0491', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301103', '1103', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301462', '1462', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301663', '1663', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301795', '1795', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301822', '1822', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301840', '1840', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000156', '4156', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000253', '4253', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000477', '4477', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000564', '4564', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300324', '0324', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300325', '0325', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300900', '0900', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300943', '0943', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301493', '1493', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301597', '1597', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301721', '1721', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301761', '1761', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301818', '1818', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000092', '4092', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000098', '4098', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000168', '4168', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000366', '4366', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000617', '4617', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000772', '4772', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300389', '0389', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300992', '0992', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301018', '1018', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301412', '1412', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301450', '1450', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301525', '1525', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301539', '1539', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301723', '1723', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000758', '4758', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300160', '0160', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300170', '0170', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300184', '0184', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300257', '0257', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300498', '0498', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300545', '0545', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300760', '0760', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301059', '1059', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301300', '1300', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301472', '1472', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301622', '1622', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301657', '1657', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070955', '7955', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070964', '7964', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070965', '7965', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070996', '7996', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301664', '1664', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301701', '1701', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301837', '1837', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301519', '1519', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301567', '1567', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301571', '1571', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301600', '1600', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301746', '1746', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301807', '1807', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301903', '1903', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000102', '4102', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000378', '4378', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000664', '4664', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070221', '8221', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '074022', '8022', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300226', '0226', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300914', '0914', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301514', '1514', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301549', '1549', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301552', '1552', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301575', '1575', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301836', '1836', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300915', '0915', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301517', '1517', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301649', '1649', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301708', '1708', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301759', '1759', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301906', '1906', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000105', '4105', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300227', '0227', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300228', '0228', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300229', '0229', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300525', '0525', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300578', '0578', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300643', '0643', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301161', '1161', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301169', '1169', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301263', '1263', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301815', '1815', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '074053', '7053', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301170', '1170', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301520', '1520', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301532', '1532', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301565', '1565', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301614', '1614', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301722', '1722', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000226', '4226', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000322', '4322', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000643', '4643', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300468', '0468', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300944', '0944', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300945', '0945', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301160', '1160', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301564', '1564', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301582', '1582', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301599', '1599', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000086', '4086', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000455', '4455', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000786', '4786', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070232', '8232', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '072410', '7410', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301102', '1102', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301573', '1573', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301581', '1581', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301735', '1735', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301825', '1825', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000023', '4023', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000432', '4432', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300896', '0896', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301474', '1474', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301512', '1512', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301536', '1536', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301641', '1641', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301650', '1650', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301651', '1651', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071515', '7515', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071516', '7516', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300211', '0211', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300790', '0790', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300796', '0796', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300803', '0803', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300817', '0817', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301111', '1111', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301910', '1910', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071231', '8231', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071233', '7233', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071234', '7234', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300291', '0291', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300292', '0292', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300729', '0729', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300815', '0815', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300837', '0837', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301113', '1113', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301431', '1431', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301789', '1789', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301829', '1829', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '305011', '5011', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000307', '4307', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000635', '4635', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071706', '7706', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301380', '1380', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301589', '1589', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301790', '1790', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301876', '1876', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000231', '4231', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000247', '4247', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000608', '4608', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000721', '4721', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000876', '4876', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000878', '4878', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071703', '7703', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300548', '0548', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301498', '1498', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301521', '1521', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301531', '1531', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301773', '1773', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000025', '4025', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000403', '4403', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000450', '4450', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000640', '4640', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000649', '4649', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000733', '4733', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071702', '7702', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300713', '0713', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300748', '0748', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300827', '0827', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300990', '0990', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301011', '1011', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301325', '1325', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000095', '4095', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000126', '4126', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000241', '4241', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000855', '4855', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071704', '7704', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301077', '1077', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301148', '1148', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301201', '1201', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301212', '1212', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301503', '1503', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301777', '1777', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301780', '1780', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000058', '4058', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000266', '4266', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000725', '4725', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300894', '0894', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301451', '1451', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301755', '1755', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301852', '1852', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000075', '4075', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000633', '4633', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000645', '4645', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300556', '0556', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300649', '0649', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301120', '1120', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301215', '1215', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301287', '1287', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301398', '1398', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301477', '1477', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301478', '1478', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301843', '1843', 3, '2018-06-02', 201822 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301909', '1909', 3, '2018-06-02', 201822 );
			-- --------------------------------------------------
			-- Migration Wave 4: 2018-12-01 (Final)
			-- --------------------------------------------------

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000139', '4139', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000228', '4228', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000619', '4619', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073093', '7093', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073094', '7094', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300222', '0222', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301055', '1055', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301086', '1086', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301311', '1311', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301319', '1319', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301615', '1615', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303038', '3038', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303071', '3071', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070202', '7202', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070211', '7211', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070212', '7212', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070242', '7242', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070279', '7279', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301383', '1383', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301483', '1483', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301556', '1556', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301627', '1627', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301661', '1661', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301791', '1791', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301810', '1810', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073008', '7008', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073020', '7020', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300754', '0754', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300872', '0872', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300947', '0947', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300954', '0954', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300955', '0955', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301091', '1091', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301484', '1484', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301845', '1845', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301878', '1878', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000014', '4014', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300689', '0689', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301652', '1652', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301653', '1653', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301700', '1700', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303022', '3022', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303025', '3025', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303033', '3033', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303040', '3040', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303047', '3047', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000834', '4834', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070520', '7520', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070521', '7521', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070522', '7522', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070523', '7523', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070524', '7524', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300674', '0674', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301005', '1005', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301389', '1389', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301588', '1588', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303028', '3028', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000888', '4888', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300691', '0691', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301384', '1384', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301669', '1669', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303012', '3012', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303041', '3041', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303064', '3064', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303070', '3070', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303077', '3077', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303081', '3081', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000103', '4103', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000541', '4541', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300658', '0658', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301485', '1485', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301545', '1545', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301560', '1560', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301632', '1632', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301711', '1711', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303069', '3069', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303080', '3080', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300665', '0665', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300671', '0671', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301704', '1704', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301756', '1756', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301902', '1902', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303005', '3005', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303013', '3013', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303019', '3019', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303029', '3029', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303037', '3037', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303065', '3065', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000468', '4468', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000846', '4846', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070506', '7506', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300670', '0670', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301377', '1377', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301554', '1554', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301839', '1839', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303007', '3007', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303036', '3036', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303078', '3078', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000096', '4096', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073012', '7012', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073037', '7037', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073079', '7079', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300224', '0224', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301405', '1405', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303023', '3023', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303034', '3034', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303042', '3042', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303051', '3051', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303052', '3052', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303053', '3053', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303054', '3054', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303056', '3056', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303060', '3060', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000141', '4141', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000616', '4616', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000709', '4709', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300221', '0221', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300704', '0704', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301072', '1072', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301327', '1327', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301402', '1402', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301591', '1591', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301613', '1613', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301817', '1817', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303009', '3009', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303810', '3810', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070220', '7220', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070292', '7292', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070294', '7294', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070295', '7295', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070296', '7296', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300755', '0755', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300767', '0767', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301247', '1247', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301381', '1381', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301603', '1603', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301676', '1676', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301764', '1764', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301792', '1792', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073097', '7097', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300868', '0868', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300907', '0907', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300908', '0908', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300956', '0956', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301089', '1089', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301278', '1278', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301279', '1279', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301280', '1280', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301281', '1281', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301577', '1577', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073016', '7016', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073018', '7018', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073022', '7022', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073024', '7024', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073029', '7029', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073043', '7043', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300732', '0732', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301025', '1025', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301050', '1050', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301282', '1282', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301283', '1283', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301284', '1284', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301628', '1628', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301716', '1716', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000638', '4638', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000658', '4658', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070457', '7457', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300745', '0745', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300972', '0972', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301167', '1167', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301275', '1275', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301368', '1368', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301382', '1382', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301415', '1415', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301714', '1714', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301729', '1729', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000136', '4136', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000367', '4367', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000757', '4757', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000897', '4897', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300884', '0884', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301501', '1501', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301541', '1541', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301625', '1625', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301630', '1630', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301675', '1675', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301743', '1743', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070456', '7456', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070462', '7462', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300301', '0301', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300380', '0380', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300924', '0924', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300968', '0968', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301043', '1043', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301068', '1068', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301087', '1087', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301095', '1095', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301584', '1584', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301788', '1788', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000094', '4094', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000225', '4225', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000607', '4607', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300835', '0835', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300912', '0912', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301067', '1067', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301302', '1302', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301347', '1347', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301534', '1534', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301579', '1579', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301778', '1778', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301866', '1866', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000073', '4073', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000291', '4291', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000529', '4529', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000551', '4551', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070464', '7464', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300340', '0340', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301303', '1303', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301459', '1459', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301587', '1587', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301751', '1751', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301752', '1752', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000235', '4235', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000435', '4435', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000881', '4881', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000896', '4896', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300892', '0892', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300935', '0935', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300978', '0978', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301142', '1142', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301392', '1392', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301432', '1432', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301694', '1694', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301727', '1727', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301769', '1769', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000301', '4301', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000368', '4368', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000667', '4667', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000863', '4863', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070452', '7452', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070455', '7455', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300922', '0922', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300983', '0983', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301085', '1085', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301211', '1211', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301326', '1326', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301438', '1438', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301495', '1495', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000574', '4574', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000668', '4668', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000810', '4810', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000829', '4829', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070461', '7461', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301002', '1002', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301270', '1270', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301328', '1328', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301364', '1364', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301618', '1618', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301674', '1674', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301730', '1730', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000026', '4026', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000042', '4042', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000084', '4084', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000171', '4171', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070203', '7203', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070451', '7451', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300966', '0966', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301141', '1141', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301168', '1168', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301179', '1179', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301416', '1416', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000011', '4011', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000315', '4315', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300854', '0854', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300865', '0865', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300989', '0989', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301428', '1428', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000421', '4421', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000555', '4555', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300044', '0044', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300056', '0056', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300852', '0852', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301165', '1165', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301182', '1182', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301269', '1269', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301570', '1570', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000549', '4549', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000738', '4738', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071802', '7802', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300387', '0387', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300808', '0808', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300821', '0821', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300822', '0822', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301189', '1189', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301242', '1242', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301454', '1454', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000085', '4085', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000176', '4176', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000394', '4394', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000439', '4439', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000711', '4711', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071123', '7123', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071124', '7124', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300937', '0937', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300941', '0941', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300959', '0959', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301035', '1035', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301304', '1304', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000143', '4143', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000201', '4201', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000481', '4481', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000546', '4546', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000609', '4609', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000761', '4761', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000774', '4774', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301019', '1019', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301064', '1064', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301070', '1070', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301079', '1079', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301084', '1084', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301349', '1349', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301548', '1548', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000317', '4317', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000321', '4321', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000579', '4579', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000762', '4762', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000826', '4826', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071120', '7120', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '071121', '7121', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300757', '0757', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300891', '0891', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301069', '1069', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301078', '1078', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301123', '1123', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301317', '1317', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301356', '1356', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000556', '4556', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000594', '4594', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000766', '4766', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000815', '4815', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300083', '0083', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300879', '0879', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300880', '0880', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300999', '0999', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301487', '1487', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301689', '1689', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000053', '4053', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000055', '4055', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000150', '4150', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000207', '4207', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000271', '4271', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000340', '4340', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000493', '4493', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300421', '0421', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300422', '0422', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300552', '0552', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300733', '0733', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300737', '0737', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300738', '0738', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301561', '1561', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300416', '0416', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300417', '0417', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300418', '0418', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300419', '0419', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300420', '0420', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300424', '0424', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300512', '0512', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300936', '0936', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300963', '0963', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000297', '4297', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000405', '4405', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000535', '4535', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000625', '4625', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000812', '4812', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000837', '4837', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300708', '0708', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300889', '0889', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301241', '1241', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301546', '1546', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301550', '1550', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000032', '4032', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000373', '4373', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000412', '4412', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000833', '4833', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300328', '0328', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300877', '0877', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301494', '1494', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301683', '1683', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301848', '1848', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000062', '4062', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301028', '1028', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301066', '1066', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301244', '1244', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301249', '1249', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301523', '1523', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301524', '1524', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301872', '1872', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000008', '4008', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000230', '4230', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000233', '4233', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000352', '4352', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300091', '0091', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300111', '0111', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300123', '0123', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300124', '0124', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300153', '0153', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300516', '0516', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300530', '0530', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300905', '0905', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300949', '0949', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301480', '1480', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000260', '4260', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000406', '4406', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070733', '7733', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070735', '7735', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300126', '0126', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300570', '0570', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300925', '0925', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301339', '1339', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301443', '1443', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301488', '1488', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301813', '1813', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301859', '1859', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000052', '4052', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000320', '4320', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000542', '4542', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000634', '4634', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000839', '4839', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000867', '4867', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300514', '0514', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300707', '0707', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301159', '1159', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301393', '1393', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301682', '1682', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301684', '1684', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300870', '0870', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300919', '0919', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300939', '0939', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301092', '1092', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301518', '1518', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301677', '1677', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301678', '1678', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301679', '1679', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301681', '1681', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301685', '1685', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301686', '1686', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301849', '1849', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301850', '1850', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000372', '4372', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070731', '7731', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070736', '7736', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070738', '7738', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070739', '7739', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070755', '7755', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300125', '0125', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300128', '0128', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300131', '0131', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300133', '0133', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300134', '0134', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301177', '1177', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000557', '4557', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070737', '7737', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '070748', '7748', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300571', '0571', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300574', '0574', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301715', '1715', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000090', '4090', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000448', '4448', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000565', '4565', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300765', '0765', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301029', '1029', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301094', '1094', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301796', '1796', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301808', '1808', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000091', '4091', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000727', '4727', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300926', '0926', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301766', '1766', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303061', '3061', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303062', '3062', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303063', '3063', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000002', '4002', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000272', '4272', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000540', '4540', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000734', '4734', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000875', '4875', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '075111', '7111', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '075122', '7122', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300844', '0844', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300888', '0888', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301010', '1010', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301130', '1130', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301468', '1468', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000344', '4344', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000417', '4417', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000631', '4631', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000741', '4741', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300800', '0800', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301030', '1030', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301107', '1107', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301232', '1232', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301537', '1537', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301542', '1542', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301616', '1616', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '303031', '3031', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000144', '4144', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000313', '4313', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000589', '4589', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000604', '4604', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000759', '4759', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300848', '0848', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300917', '0917', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301044', '1044', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301104', '1104', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301340', '1340', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000130', '4130', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000132', '4132', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000208', '4208', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000773', '4773', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300385', '0385', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300940', '0940', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300964', '0964', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301230', '1230', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301238', '1238', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301267', '1267', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301288', '1288', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301331', '1331', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301348', '1348', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300215', '0215', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300434', '0434', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300435', '0435', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300436', '0436', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300437', '0437', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300501', '0501', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300620', '0620', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300621', '0621', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300931', '0931', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301071', '1071', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301195', '1195', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '075301', '8301', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '075302', '8302', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300354', '0354', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300360', '0360', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300502', '0502', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300518', '0518', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300814', '0814', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300828', '0828', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300860', '0860', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300861', '0861', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300957', '0957', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301041', '1041', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301291', '1291', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301363', '1363', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301401', '1401', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000049', '4049', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000740', '4740', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300805', '0805', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300874', '0874', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300995', '0995', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301001', '1001', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000157', '4157', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073204', '7204', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073276', '7276', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300876', '0876', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301016', '1016', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301118', '1118', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301136', '1136', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301178', '1178', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301301', '1301', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301533', '1533', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301543', '1543', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301835', '1835', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000822', '4822', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073146', '7146', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300045', '0045', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300834', '0834', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301006', '1006', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301157', '1157', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301158', '1158', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301351', '1351', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301633', '1633', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301800', '1800', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000268', '4268', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000318', '4318', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073287', '7287', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301082', '1082', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301229', '1229', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301407', '1407', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301470', '1470', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301511', '1511', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301596', '1596', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000350', '4350', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000882', '4882', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '073274', '7274', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300331', '0331', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301061', '1061', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301117', '1117', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301124', '1124', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301156', '1156', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301482', '1482', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301502', '1502', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301718', '1718', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000068', '4068', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000114', '4114', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000655', '4655', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300297', '0297', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301233', '1233', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301268', '1268', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301272', '1272', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301307', '1307', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301492', '1492', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301673', '1673', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000361', '4361', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300208', '0208', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300210', '0210', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300536', '0536', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '300641', '0641', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301208', '1208', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301258', '1258', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301260', '1260', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '301403', '1403', 4, '2018-12-01', 201848 );

			INSERT INTO dbo.CenterCSSMigrations
			VALUES( '000097', '4097', 4, '2018-12-01', 201848 );
         END TRY
	    --
	    -- Catch, and throw the error back to the calling procedure or client
	    --
         BEGIN CATCH
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = 'Stored procedure '+@ProcName+' - '+ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;