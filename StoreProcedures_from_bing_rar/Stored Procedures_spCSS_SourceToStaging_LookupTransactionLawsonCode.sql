
-- ================================================================================
-- 
-- Stored Procedure:   spCSS_SourceToStaging_LookupTransactionLawsonCode
--
-- Purpose:            Truncate and reload the LookupTransactionLawsonCode.
--                         this table is used to lookup transaction code and lawson codes
--						   in CSS, a single transaction type and code can be associated to
--						   1 or more Account SubAccounts and it can be distinguished by the
--						   lawson code.
--					   This procedure takers the staged data and transforms it into the lookup table.
--
-- Parameters:        None
--
--
--Usage:              EXEC dbo.spCSS_SourceToStaging_LookupTransactionLawsonCode
--
-- --------------------------------------------------------------------------------
--
-- Change Log:		   
-- ----------
--
-- Date          Modified By         Comments
-- ----          -----------         --------
--
--  04/03/18     anmorales           BNG-1542 - FTE Subsidy Measures - CSS - Update ETL for TRansactionCode lookup
--			 
-- ================================================================================
CREATE PROCEDURE [dbo].[spCSS_SourceToStaging_LookupTransactionLawsonCode]
    AS
BEGIN
    SET NOCOUNT ON;
	BEGIN TRY
		BEGIN TRAN;

		TRUNCATE TABLE dbo.LookupTransactionLawsonCode;

		INSERT INTO dbo.LookupTransactionLawsonCode (TransactionType, TransactionCode, TransactionCodeDescription, LawsonCode, GLAccount, GLSubAccount, Description, ActiveFlag, RecurringFlag, FTE)
		   SELECT TransactionType
				 ,TransactionCode
				 ,TransactionCodeDescription
				 ,Default_LawsonCode
				 ,Default_GLAccount
				 ,Default_GLSubAccount
				 ,Default_Description 
				 ,ActiveFlag
				 ,RecurringFlag
				 ,NumericFTE
			  FROM dbo.CSSLawsonOracleMappingLanding
		   UNION ALL 
		   SELECT TransactionType
				 ,TransactionCode
				 ,TransactionCodeDescription
				 ,CCA_LawsonCode
				 ,CCA_GLAccount
				 ,CCA_GLSubAccount
				 ,CCA_Description 
				 ,ActiveFlag
				 ,RecurringFlag
				 ,NumericFTE
			  FROM dbo.CSSLawsonOracleMappingLanding
			  WHERE CCA_LawsonCode IS NOT NULL;
			  
		COMMIT TRAN
		RETURN(0);
	END TRY
	BEGIN CATCH
		THROW;
		ROLLBACK TRAN;
		RETURN(-1);
	END CATCH
END
