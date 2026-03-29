/*
===================================================
Create Dataabse and Schema
===================================================
Script Purpose: 
    This script creates a new databse named 'DadtaWarehouse'* after checking if it already exists.
If the database exists, it is dropped and recreated. Additionaly, the script sets up three schcemas within the database: 'bornze' 'silver' 'gold'.

WARNING:
   Running this script will drop the entire 'DataWarehouse' database if it exists.
  All data in the database will be permanently deleted . proceed with caution and ensure you have proper backups before running this script.
*/


USE master;
GO 

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

USE DataWarehouse;
GO

--Create the 'DataWarehouse' dataset
CREATE DataBase DataWarehouse;
GO
  
USE DataWarehouse;
GO

  --Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
