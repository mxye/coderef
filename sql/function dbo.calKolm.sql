-- ****************************************************************
-- header
-- ****************************************************************

use search;
GO

-- ****************************************************************
-- define table type to feed into function
-- ****************************************************************
--drop type decimalColumn;

CREATE TYPE floatColumn
AS TABLE (col1 float)
GO 

-- here's how to insert values of interest
DECLARE @myTable floatColumn
INSERT INTO @myTable(col1) (select cr from CRMarket);

-- SELECT * FROM @myTable;

select dbo.calKolm(@myTable,1);


-- ****************************************************************
-- define function
-- ****************************************************************
IF OBJECT_ID(N'dbo.calKolm', N'FN') IS NOT NULL
	DROP FUNCTION dbo.calKolm;
GO

CREATE FUNCTION dbo.calKolm (
	@tobecal floatColumn readonly
	,@k INT
	)
RETURNS FLOAT
AS
BEGIN
	DECLARE @return FLOAT;
	DECLARE @avg FLOAT;
	DECLARE @avg2 FLOAT;
	DECLARE @Table1 floatColumn;

	SET @avg = (
			SELECT avg(col1)
			FROM @tobecal
			);

	INSERT INTO @Table1 (col1) (SELECT exp(@k * (@avg - col1)) FROM @tobecal);

	SET @avg2 = (
			SELECT avg(col1)
			FROM @Table1
			);

	SET @return = 1 / @k * log(@avg2);

	RETURN (@return);
END

