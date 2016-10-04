
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

select dbo.calRosenbluth(@myTable);


-- ****************************************************************
-- define function
-- ****************************************************************
IF OBJECT_ID(N'dbo.CalRosenbluth', N'FN') IS NOT NULL
	DROP FUNCTION dbo.calrosenbluth;
GO

CREATE FUNCTION dbo.calRosenbluth (@tobecal floatColumn readonly)
RETURNS FLOAT
AS
BEGIN
	DECLARE @return FLOAT;
	DECLARE @PiXi_sum FLOAT;
	DECLARE @sum FLOAT;


	SET @sum = (
			SELECT sum(col1)
			FROM @tobecal
			);
	SET @PiXi_sum = (
			SELECT sum(num * col1)/@sum
			FROM (
				SELECT num = ROW_NUMBER() OVER (
						ORDER BY col1 DESC
						)
					,col1
				FROM @tobecal
				) x
			);
	SET @return = 1 / (@PiXi_sum * 2.0 -1);

	RETURN (@return);
END

