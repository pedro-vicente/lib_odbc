#include "csv.hh"
#include "clock.hh"
#include <iostream>
#include "sqlite3.h"

int csv_to_sqlite(const std::string &name);

/////////////////////////////////////////////////////////////////////////////////////////////////////
//usage
/////////////////////////////////////////////////////////////////////////////////////////////////////

void usage()
{
  std::cout << "usage: ./csv_sqlite -f CSV" << std::endl;
  std::cout << "-f CSV: CSV file name" << std::endl;
  exit(0);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//main
/////////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char *argv[])
{
  std::string file_name;

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
      case 'f':
        file_name = argv[i + 1];
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

  if (file_name.empty())
  {
    return 1;
  }

  if (csv_to_sqlite(file_name) < 0)
  {
    return 1;
  }

  return 0;
}


/////////////////////////////////////////////////////////////////////////////////////////////////////
//dc311_csv_to_sqlite
//SQLite does not have a storage class set aside for storing dates and/or times. 
//Instead, the built-in Date And Time Functions of SQLite are capable of 
//storing dates and times as TEXT, REAL, or INTEGER values:
/////////////////////////////////////////////////////////////////////////////////////////////////////

int csv_to_sqlite(const std::string &name)
{
  clock_gettime_t timer;
  read_csv_t csv;
  size_t nbr_rows = 0;
  std::vector<std::string> row;
  std::string sql;
  sqlite3 *db = 0;
  char *msg = 0;

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


  sqlite3_close(db);
  std::cout << "processed " << nbr_rows << " rows" << std::endl;
  timer.now();
  timer.stop();
  return 0;
}



