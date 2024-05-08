RESTORE DATABASE AdventureWorksDW2022
FROM DISK = '/var/opt/mssql/backup/AdventureWorksDW2022.bak'
WITH MOVE 'AdventureWorksDW2022' TO '/var/opt/mssql/data/AdventureWorksDW2022_Data.mdf',
     MOVE 'AdventureWorksDW2022_log' TO '/var/opt/mssql/data/AdventureWorksDW2022_Log.ldf',
     REPLACE;
GO

RESTORE DATABASE AdventureWorks2022
FROM DISK = '/var/opt/mssql/backup/AdventureWorks2022.bak'
WITH MOVE 'AdventureWorks2022' TO '/var/opt/mssql/data/AdventureWorks2022_Data.mdf',
     MOVE 'AdventureWorks2022_log' TO '/var/opt/mssql/data/AdventureWorks2022_Log.ldf',
     REPLACE;
GO


RESTORE DATABASE AdventureWorksLT2022
FROM DISK = '/var/opt/mssql/backup/AdventureWorksLT2022.bak'
WITH MOVE 'AdventureWorksLT2022' TO '/var/opt/mssql/data/AdventureWorksLT2022_Data.mdf',
     MOVE 'AdventureWorksLT2022_log' TO '/var/opt/mssql/data/AdventureWorksLT2022_Log.ldf',
     REPLACE;
GO

