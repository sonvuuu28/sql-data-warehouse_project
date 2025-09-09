/*
========================
Create Databases and Schemas
========================

Purpose:
	This scripts will create new database named "DataWarehouse". If the db exists, It will be dropped and recreated. 
	Additionally 3 schemas Bronze, Silver, Gold also be initialized.

========================

Warning:
	Backup your data before running this script.

*/

use master;
go

if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin
	alter database DataWarehouse set single_user with rollback immediate;
	drop database DataWarehouse;
end;
go

create database DataWarehouse;
go

use DataWarehouse;
go

create schema bronze;
go
create schema silver;
go
create schema gold;
go

 