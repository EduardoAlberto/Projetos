SELECT * FROM DBDWP511.DBO.FactInternetSales;
SELECT * FROM DBDWP511.DBO.DimCustomer;

USE DBDWP511
CREATE OR ALTER VIEW VW_ANALISIE AS 
WITH TMP_Customer AS 
(SELECT 
    CustomerKey,
    FirstName,
    LastName,
    EnglishEducation,
    CommuteDistance,
    RANK()OVER (PARTITION BY EnglishEducation, CommuteDistance ORDER BY FirstName) AS RANK,
    YearlyIncome,
    Gender,
    BirthDate
FROM DBDWP511.DBO.DimCustomer)

SELECT 
a.*,
b.UnitPrice,
b.ProductStandardCost,
b.TotalProductCost,
b.SalesAmount,
b.TaxAmt,
b.Freight,
b.OrderDate,
b.DueDate,
b.ShipDate
FROM TMP_Customer a 
inner join DBDWP511.DBO.FactInternetSales b
on a.CustomerKey = b.CustomerKey
and a.rank >= 100
-- ORDER BY a.rank;

