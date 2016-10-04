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

SELECT * FROM @myTable;

select dbo.CalGini(@myTable);


-- ****************************************************************
-- define function
-- ****************************************************************
IF OBJECT_ID(N'dbo.CalGini', N'FN') IS NOT NULL
	DROP FUNCTION dbo.CalGini;
GO

CREATE FUNCTION dbo.CalGini (@tobecal floatColumn readonly)
RETURNS FLOAT
AS
BEGIN
	DECLARE @return FLOAT;
	DECLARE @N BIGINT;
	DECLARE @PiXi_sum FLOAT;
	DECLARE @sum FLOAT;

	SET @N = (
			SELECT count(*)
			FROM @tobecal
			);
	SET @sum = (
			SELECT sum(col1)
			FROM @tobecal
			);
	SET @PiXi_sum = (
			SELECT sum(num * col1)
			FROM (
				SELECT num = ROW_NUMBER() OVER (
						ORDER BY col1 DESC
						)
					,col1
				FROM @tobecal
				) x
			);

	SET @return = (((@N + 1.0) / @N) - (@PiXi_sum * 2.0 / (@N * @sum)));


	RETURN (@return);
END


