# lib_odbc
C++ light wrapper for the ODBC C API (Windows, Linux)



Dependencies
---
Installing the Microsoft ODBC Driver for SQL Server on Linux and macOS

https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server

Usage
---

`usage: ./dc311 -s SERVER -u USER OR -i UID -p PASSWORD <-d DATABASE> <-f CSV> <-t STATEMENT>`
`-s SERVER: server IP adress or localhost`
`-u USER: user name`
`-i UID: user ID`
`-p PASSWORD: password`
`-d DATABASE: database name`
`-t STATEMENT: SQL statement`
`-f CSV: CSV file name`

Example
----
DC311 database. 

Import CSV file and generate Microsoft SQL Server database on localhost

`./dc311 -s 127.0.0.1 -u <user> -p <password> -f dc_311-2016_100.csv`

This generates a table named "dc_311-2016_100" 

List all rows from table "dc_311-2016_100" 

`./dc311 -s 127.0.0.1 -u <user> -p <password>  -d dc311 -t "SELECT * FROM [dc_311-2016_100]"`





