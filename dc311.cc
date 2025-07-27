#include "odbc.hh"
#include "csv.hh"
#include "clock.hh"
#include <iostream>
#include "sqlite3.h"

int dc311_csv_to_sqlite(const std::string& name);
int dc311_csv_to_odbc(const std::string& name, odbc& query);

/////////////////////////////////////////////////////////////////////////////////////////////////////
//usage
//Note: database name [dc311] must be created in before running this program
//-S localhost -d dc311 -f dc_311-2016_100.csv
//-S 127.0.0.1 -u pedro -p <password> -d dc311 -f dc_311-2016_100.csv
//-S 127.0.0.1 -u pedro -p <password> -d dc311 -q "SELECT * FROM [dc_311-2016_100]"
/////////////////////////////////////////////////////////////////////////////////////////////////////

void usage()
{
  std::cout << "usage: ./dc311 -S SERVER -d DATABASE <-u USER OR -i UID> <-p PASSWORD> <-f CSV> <-q STATEMENT> " << std::endl;
  std::cout << "-S SERVER: server IP address or localhost" << std::endl;
  std::cout << "-u USER: user name" << std::endl;
  std::cout << "-i UID: user ID" << std::endl;
  std::cout << "-p PASSWORD: password" << std::endl;
  std::cout << "-d DATABASE: database name" << std::endl;
  std::cout << "-q STATEMENT: SQL statement" << std::endl;
  std::cout << "-f CSV: CSV file name" << std::endl;
  exit(0);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//main
/////////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
  std::string user;
  std::string uid;
  std::string password;
  std::string server;
  std::string database;
  std::string file_name;
  std::string sql;
  int do_sqlite = 0;

  if (argc < 2)
  {
    usage();
  }

  for (int i = 1; i < argc; i++)
  {
    if (argv[i][0] == '-')
    {
      switch (argv[i][1])
      {
      case 'h':
        usage();
        break;
      case 'u':
        user = argv[i + 1];
        i++;
        break;
      case 'i':
        uid = argv[i + 1];
        i++;
        break;
      case 'p':
        password = argv[i + 1];
        i++;
        break;
      case 'S':
        server = argv[i + 1];
        i++;
        break;
      case 'd':
        database = argv[i + 1];
        i++;
        break;
      case 'f':
        file_name = argv[i + 1];
        i++;
        break;
      case 'l':
        do_sqlite = 1;
        file_name = argv[i + 1];
        i++;
        break;
      case 'q':
        sql = argv[i + 1];
        i++;
        break;
      default:
        usage();
      }
    }
    else
    {
      usage();
    }
  }

  if (server.empty() || database.empty())
  {
    usage();
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //do a SQLite database
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  if (do_sqlite)
  {
    if (dc311_csv_to_sqlite(file_name) < 0)
    {
      return 1;
    }
    return 0;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //do a ODBC database
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  odbc query;

  std::string conn = make_conn(server, database);
  if (query.connect(conn) < 0)
  {
    assert(0);
  }

  if (!file_name.empty())
  {
    if (dc311_csv_to_odbc(file_name, query) < 0)
    {
      query.disconnect();
      return 1;
    }
  }
  if (!sql.empty())
  {

    table_t table;
    std::cout << sql << std::endl;
    if (query.fetch(sql, table) < 0)
    {
      assert(0);
    }

    std::string fname(database);
    database += ".csv";
    table.to_csv(database);
  }
  query.disconnect();

  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//dc311_csv_to_odbc
/////////////////////////////////////////////////////////////////////////////////////////////////////

int dc311_csv_to_odbc(const std::string& name, odbc& query)
{
  clock_gettime_t timer;
  read_csv_t csv;
  size_t nbr_rows = 0;
  std::vector<std::string> row;
  size_t nbr_cols;
  std::string sql;

  if (csv.open(name) < 0)
  {
    std::cout << "Cannot open file " << name.c_str() << std::endl;
    return -1;
  }

  timer.start();

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //read first row with headers 
  //SERVICECALLCOUNT 8
  //LATITUDE 20
  //LONGITUDE 21
  //ZIPCODE 22
  //WARD 26
  //PSA 30
  //NEIGHBORHOODCLUSTER 31
  //
  //SERVICEORDERDATE 6
  //INSPECTIONDATE 11
  //RESOLUTIONDATE 13
  //SERVICEDUEDATE 14
  //ADDDATE 17
  //LASTMODIFIEDDATE 18
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  row = csv.read_row();
  nbr_cols = row.size();

  size_t found = name.find(".");
  std::string table = "[";
  table += name.substr(0, found);
  table += "]";

  sql = "DROP TABLE ";
  sql += table;
  sql += " ;";

  if (query.exec_direct(sql) < 0)
  {
    assert(0);
  }

  sql = "CREATE TABLE ";
  sql += table;
  sql += " (";
  for (size_t idx = 0; idx < nbr_cols; idx++)
  {
    sql += row.at(idx);
    std::string col = row.at(idx);
    if (col == "LATITUDE" || col == "LONGITUDE")
    {
      sql += " float";
    }
    else if (col == "SERVICECALLCOUNT"
      || col == "ZIPCODE"
      || col == "WARD"
      || col == "PSA"
      || col == "NEIGHBORHOODCLUSTER")
    {
      sql += " int";
    }
    else if (col == "SERVICEORDERDATE"
      || col == "INSPECTIONDATE"
      || col == "RESOLUTIONDATE"
      || col == "SERVICEDUEDATE"
      || col == "ADDDATE"
      || col == "LASTMODIFIEDDATE")
    {
      sql += " datetime";
    }
    else
    {
      sql += " varchar(255)";
    }
    if (idx < nbr_cols - 1)
    {
      sql += ", ";
    }
  }
  sql += " );";

  std::cout << sql << std::endl;
  if (query.exec_direct(sql) < 0)
  {
    assert(0);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //iterate until an empty row is returned (end of file)
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  while (true)
  {
    row = csv.read_row();
    if (row.size() == 0)
    {
      break;
    }

    if (row.size() != nbr_cols)
    {
      std::cout << "row " << nbr_rows << " is invalid" << std::endl;
      continue;
    }

    sql = "INSERT INTO ";
    sql += table;
    sql += " VALUES (";
    for (size_t idx = 0; idx < nbr_cols; idx++)
    {
      std::string col = row.at(idx);

      //doubles
      if (idx == 20 || idx == 21)
      {
        try
        {
          double d = std::stod(col);
          sql += col;
        }
        catch (const std::invalid_argument& ia)
        {
          sql += "NULL";
        }
      }
      //int
      else if (idx == 8 || idx == 22 || idx == 26 || idx == 30 || idx == 31)
      {
        try
        {
          int n = std::stoi(col);
          sql += col;
        }
        catch (const std::invalid_argument& ia)
        {
          sql += "NULL";
        }
      }
      //datetime
      else if (idx == 6 || idx == 11 || idx == 13 || idx == 14 || idx == 17 || idx == 18)
      {
        if (col == "NA")
        {
          sql += "NULL";
        }
        else
        {
          //format 2017-01-12T19:00:16Z
          try
          {
            std::string syear = col.substr(0, 4);
            std::string smonth = col.substr(5, 2);
            std::string sday = col.substr(8, 2);
            std::string shour = col.substr(11, 2);
            std::string sminute = col.substr(15, 2);
            std::string ssecond = col.substr(17, 2);
            int year = std::stoi(syear);
            int month = std::stoi(smonth);
            int day = std::stoi(sday);
            int hour = std::stoi(shour);
            int minute = std::stoi(sminute);
            int second = std::stoi(ssecond);
            //ISO 8601
            std::string iso8601 = col.substr(0, 19);
            sql += "'";
            sql += iso8601;
            sql += "'";
          }
          catch (const std::invalid_argument& ia)
          {
            sql += "NULL";
          }
        }
      }
      //varchar
      else
      {
        sql += "'";
        sql += col;
        sql += "'";
      }

      if (idx < nbr_cols - 1)
      {
        sql += ", ";
      }
    }
    sql += " );";

    std::cout << sql << std::endl;
    if (query.exec_direct(sql) < 0)
    {
      assert(0);
    }
    nbr_rows++;
  }


  std::cout << "processed " << nbr_rows << " rows" << std::endl;
  timer.now();
  timer.stop();
  return 0;
}


/////////////////////////////////////////////////////////////////////////////////////////////////////
//dc311_csv_to_sqlite
//SQLite does not have a storage class set aside for storing dates and/or times. 
//Instead, the built-in Date And Time Functions of SQLite are capable of 
//storing dates and times as TEXT, REAL, or INTEGER values:
/////////////////////////////////////////////////////////////////////////////////////////////////////

int dc311_csv_to_sqlite(const std::string& name)
{
  clock_gettime_t timer;
  read_csv_t csv;
  size_t nbr_rows = 0;
  std::vector<std::string> row;
  size_t nbr_cols;
  std::string sql;
  sqlite3* db = 0;
  char* msg = 0;

  if (csv.open(name) < 0)
  {
    std::cout << "Cannot open file " << name.c_str() << std::endl;
    return -1;
  }

  timer.start();

  std::string fname(name);
  fname += ".sqlite";

  if (sqlite3_open(fname.c_str(), &db) != SQLITE_OK)
  {
    sqlite3_errmsg(db);
    sqlite3_close(db);
    return -1;
  }


  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //read first row with headers 
  //SERVICECALLCOUNT 8
  //LATITUDE 20
  //LONGITUDE 21
  //ZIPCODE 22
  //WARD 26
  //PSA 30
  //NEIGHBORHOODCLUSTER 31
  //
  //SERVICEORDERDATE 6
  //INSPECTIONDATE 11
  //RESOLUTIONDATE 13
  //SERVICEDUEDATE 14
  //ADDDATE 17
  //LASTMODIFIEDDATE 18
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  row = csv.read_row();
  nbr_cols = row.size();

  size_t found = name.find(".");
  std::string table_name = "[";
  table_name += name.substr(0, found);
  table_name += "]";

  sql = "DROP TABLE ";
  sql += table_name;
  sql += " ;";

  if (sqlite3_exec(db, sql.c_str(), NULL, 0, &msg) != SQLITE_OK)
  {
    fprintf(stderr, "SQL error: %s\n", msg);
    sqlite3_free(msg);
    return -1;
  }

  sql = "CREATE TABLE ";
  sql += table_name;
  sql += " (";
  for (size_t idx_col = 0; idx_col < nbr_cols; idx_col++)
  {
    sql += row.at(idx_col);
    std::string col = row.at(idx_col);
    if (col == "LATITUDE" || col == "LONGITUDE")
    {
      sql += " real";
    }
    else if (col == "SERVICECALLCOUNT"
      || col == "ZIPCODE"
      || col == "WARD"
      || col == "PSA"
      || col == "NEIGHBORHOODCLUSTER")
    {
      sql += " integer";
    }
    else if (col == "SERVICEORDERDATE"
      || col == "INSPECTIONDATE"
      || col == "RESOLUTIONDATE"
      || col == "SERVICEDUEDATE"
      || col == "ADDDATE"
      || col == "LASTMODIFIEDDATE")
    {
      sql += " text";
    }
    else
    {
      sql += " text";
    }
    if (idx_col < nbr_cols - 1)
    {
      sql += ", ";
    }
  }
  sql += " );";

  std::cout << sql << std::endl;

  if (sqlite3_exec(db, sql.c_str(), NULL, 0, &msg) != SQLITE_OK)
  {
    fprintf(stderr, "SQL error: %s\n", msg);
    sqlite3_free(msg);
    return -1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //iterate until an empty row is returned (end of file)
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  while (true)
  {
    row = csv.read_row();
    if (row.size() == 0)
    {
      break;
    }

    if (row.size() != nbr_cols)
    {
      std::cout << "row " << nbr_rows << " is invalid" << std::endl;
      continue;
    }

    sql = "INSERT INTO ";
    sql += table_name;
    sql += " VALUES (";
    for (size_t idx = 0; idx < nbr_cols; idx++)
    {
      std::string col = row.at(idx);

      //doubles
      if (idx == 20 || idx == 21)
      {
        try
        {
          double d = std::stod(col);
          sql += col;
        }
        catch (const std::invalid_argument& ia)
        {
          sql += "NULL";
        }
      }
      //int
      else if (idx == 8 || idx == 22 || idx == 26 || idx == 30 || idx == 31)
      {
        try
        {
          int n = std::stoi(col);
          sql += col;
        }
        catch (const std::invalid_argument& ia)
        {
          sql += "NULL";
        }
      }
      //datetime
      else if (idx == 6 || idx == 11 || idx == 13 || idx == 14 || idx == 17 || idx == 18)
      {
        if (col == "NA")
        {
          sql += "NULL";
        }
        else
        {
          //format 2017-01-12T19:00:16Z
          try
          {
            std::string syear = col.substr(0, 4);
            std::string smonth = col.substr(5, 2);
            std::string sday = col.substr(8, 2);
            std::string shour = col.substr(11, 2);
            std::string sminute = col.substr(15, 2);
            std::string ssecond = col.substr(17, 2);
            int year = std::stoi(syear);
            int month = std::stoi(smonth);
            int day = std::stoi(sday);
            int hour = std::stoi(shour);
            int minute = std::stoi(sminute);
            int second = std::stoi(ssecond);
            //ISO 8601
            std::string iso8601 = col.substr(0, 19);
            sql += "'";
            sql += iso8601;
            sql += "'";
          }
          catch (const std::invalid_argument& ia)
          {
            sql += "NULL";
          }
        }
      }
      //varchar
      else
      {
        sql += "'";
        sql += col;
        sql += "'";
      }

      if (idx < nbr_cols - 1)
      {
        sql += ", ";
      }
    }
    sql += " );";

    std::cout << sql << std::endl;

    if (sqlite3_exec(db, sql.c_str(), NULL, 0, &msg) != SQLITE_OK)
    {
      fprintf(stderr, "SQL error: %s\n", msg);
      sqlite3_free(msg);
      return -1;
    }

    nbr_rows++;
  }

  sqlite3_close(db);

  std::cout << "processed " << nbr_rows << " rows" << std::endl;
  timer.now();
  timer.stop();
  return 0;
}
