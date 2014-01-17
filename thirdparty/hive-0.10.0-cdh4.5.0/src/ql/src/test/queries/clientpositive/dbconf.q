set hive.current.db;

DROP DATABASE IF EXISTS db_name1;
CREATE DATABASE db_name1; 
USE db_name1; 
set hive.current.db;
USE default;
set hive.current.db;

DROP DATABASE IF EXISTS db_name2;
CREATE DATABASE db_name2; 
USE db_name2; 
set hive.current.db;

USE default;
DROP DATABASE IF EXISTS db_name1;
DROP DATABASE IF EXISTS db_name2;
