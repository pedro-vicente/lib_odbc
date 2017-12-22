#include "odbc.hh"
#include "csv.hh"
#include "clock.hh"
#include <iostream>

int import_csv(const std::string &name, odbc &query);

/////////////////////////////////////////////////////////////////////////////////////////////////////
//usage
//-d dc311 -t "SELECT * FROM [dc_311-2016_100]"
/////////////////////////////////////////////////////////////////////////////////////////////////////

void usage()
{
  std::cout << "usage: ./dc311 -s SERVER -u USER OR -i UID -p PASSWORD <-d DATABASE> <-f CSV> <-t STATEMENT> " << std::endl;
  std::cout << "-s SERVER: server IP adress or localhost" << std::endl;
  std::cout << "-u USER: user name" << std::endl;
  std::cout << "-i UID: user ID" << std::endl;
  std::cout << "-p PASSWORD: password" << std::endl;
  std::cout << "-d DATABASE: database name" << std::endl;
  std::cout << "-t STATEMENT: SQL statement" << std::endl;
  std::cout << "-f CSV: CSV file name" << std::endl;
  exit(0);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//main
/////////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char *argv[])
{
  std::string user;
  std::string uid;
  std::string password;
  std::string server;
  std::string database;
  std::string file_name;
  std::string sql;

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
      case 's':
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
      case 't':
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

  if (!uid.empty() && !user.empty())
  {
    usage();
  }

  odbc query;
  std::string conn = make_conn(user, uid, password, server, database);
  query.connect(conn);
  if (!file_name.empty())
  {
    import_csv(file_name, query);
  }
  if (!sql.empty())
  {
    table_t table = query.fetch(sql);
    table.to_csv("fetch.csv");
  }
  query.disconnect();

  return 0;
}


/////////////////////////////////////////////////////////////////////////////////////////////////////
//import_csv
/////////////////////////////////////////////////////////////////////////////////////////////////////

int import_csv(const std::string &name, odbc &query)
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

  query.exec_direct(sql);

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
  query.exec_direct(sql);

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
    query.exec_direct(sql);
    nbr_rows++;
  }


  std::cout << "processed " << nbr_rows << " rows" << std::endl;
  timer.now();
  timer.stop();
  return 0;
}



