/*
SCRIPT PURPOSE:
   This script creates a new DATABASE named "DataWarehouse". Additionally we created  3 schemas within database :'bronze',;silevre','gold'.
WARNING:
   Running this script  will drop the entire "DataWarehouse" DATABASE if it exists.proceed with cution and make sure you have a proper
   backups before runnimg  this script.
*/
use master;

CREATE DATABASE DataWarehouse;

use DataWarehouse;

--create schemas(bronze,sliver, gold)
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
