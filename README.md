# lib_odbc
C++ light wrapper for the ODBC C API (Windows, Linux)

Dependencies
---
Installing the Microsoft ODBC Driver for SQL Server on Linux and macOS

https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server

Usage
---

`usage: ./dc311 -S SERVER -d DATABASE <-u USER OR -i UID -p PASSWORD>  <-f CSV> <-t STATEMENT>`
`-S SERVER: server IP address or localhost`
`-u USER: user name`
`-i UID: user ID`
`-p PASSWORD: password`
`-d DATABASE: database name`
`-t STATEMENT: SQL statement`
`-f CSV: CSV file name`

## API

```cpp
 int connect(const std::string& conn);
 int disconnect();
 int exec_direct(const std::string& sql);
 int fetch(const std::string& sql, table_t& table);
 int set_auto_commit();
 int set_manual();
 int commit_transaction();
 int rollback_transaction();
```

## Table structures

A row is a vector of strings

```cpp
struct row_t
{
  std::vector<std::string> col;
};
```

A column has a name and a SQL type

```cpp
struct column_t
{
  std::string name;
  SQLSMALLINT sqltype;
};
```

A table is a vector with rows, with column information (name, SQL type)

```cpp
class table_t
{
public:
  table_t()
  {

  }
  std::vector<column_t> cols;
  std::vector<row_t> rows;
  void remove();
  std::string get_row_col_value(int row, const std::string& col_name);

  int to_csv(const std::string& fname);
};
```

Example
----
DC311 database. 

Import CSV file and generate Microsoft SQL Server database on localhost

`./dc311 -s 127.0.0.1 -u <user> -p <password> -f dc_311-2016_100.csv`

This generates a table named "dc_311-2016_100" 

List all rows from table "dc_311-2016_100" 

`./dc311 -s 127.0.0.1 -u <user> -p <password>  -d dc311 -t "SELECT * FROM [dc_311-2016_100]"`

### Output 

```csv
SERVICEREQUESTID,SERVICEPRIORITY,SERVICECODE,SERVICECODEDESCRIPTION,SERVICETYPECODE,SERVICETYPECODEDESCRIPTION,SERVICEORDERDATE,SERVICEORDERSTATUS,SERVICECALLCOUNT,AGENCYABBREVIATION,INSPECTIONFLAG,INSPECTIONDATE,RESOLUTION,RESOLUTIONDATE,SERVICEDUEDATE,SERVICENOTES,PARENTSERVICEREQUESTID,ADDDATE,LASTMODIFIEDDATE,SITEADDRESS,LATITUDE,LONGITUDE,ZIPCODE,MARADDRESSREPOSITORYID,DCSTATADDRESSKEY,DCSTATLOCATIONKEY,WARD,ANC,SMD,DISTRICT,PSA,NEIGHBORHOODCLUSTER,HOTSPOT2006NAME,HOTSPOT2005NAME,HOTSPOT2004NAME,SERVICESOURCECODE
16-00777589,Standard,S0311,Rodent Inspection and Treatment,DEPAHEAL,DOH- Department Of Health,2016-10-29 15:09:22.000,Closed,1,DOH,NA,2017-01-12 19:00:00.000,"Closed, issue resolved",2017-01-12 19:00:16.000,2016-11-18 22:00:00.000,"On 1/11/17 R Herrington baited 2 rat burrows in the front & rear yds.  Treatment will continue until rodent activity ceases. Ditrac/powder/ EPA#12455-56/ 0.2% 3 oz, B G duster/ gloves shovel.",NA,2016-10-29 15:09:22.000,2017-01-12 19:00:16.000,1727 3RD STREET N,38.91374182,-77.00176208,20002,56927,NA,NA,5,5E,5E03,Fifth,502,21,NA,NA,NA,PHONE
16-00784971,Standard,S0276,Parking Meter Repair,TRAOP001,Transportation Operations Administration,2016-11-02 17:51:20.000,Closed,1,DDOT,NA,NULL,NA,2017-01-12 21:28:17.000,2016-11-09 18:51:00.000,NA,NA,2016-11-02 17:51:20.000,2017-01-12 21:28:17.000,19TH STREET NW AND R STREET NW,38.9126078487478,-77.0434318585617,NULL,907185,NA,NA,2,NA,NA,NA,NULL,NULL,NA,NA,NA,PHONE
16-00785648,Standard,S0276,Parking Meter Repair,TRAOP001,Transportation Operations Administration,2016-11-02 21:12:32.000,Closed,1,DDOT,NA,NULL,NA,2017-01-12 21:28:18.000,2016-11-09 22:00:00.000,NA,NA,2016-11-02 21:12:32.000,2017-01-12 21:28:18.000,1900 - 1999 BLOCK OF R STREET N,38.9126048635355,-77.0441525970653,NULL,806127,NA,NA,2,NA,NA,NA,NULL,NULL,NA,NA,NA,311-API
16-00778205,Standard,S04TP,Tree Planting,URBAFORR,Urban Forrestry,2016-10-31 03:32:41.000,In-Progress,1,DDOT,NA,NULL,NA,NULL,2019-09-18 21:00:00.000,NA,NA,2016-10-31 03:32:41.000,2017-01-11 16:33:07.000,1629 14TH STREET N,38.91228721,-77.03171514,20009,218384,NA,NA,2,2F,2F01,Third,307,NULL,NA,NA,NA,311-API
```