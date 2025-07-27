#include "odbc.hh"
#include "csv.hh"
#include "clock.hh"
#include <iostream>

std::string sqlserver_type(SQLSMALLINT sqltype);
std::string sqlserver_type_data(SQLSMALLINT sqltype, const std::string& col_value);
int csv_to_odbc(const std::string& fname_csv, odbc& query, const std::string& information_schema_columns);

void usage()
{
  std::cout << "usage: ./csv_odbc -S SERVER -u USER OR -i UID -p PASSWORD <-d DATABASE> <-f CSV> <-x SCHEMA> <-q STATEMENT> " << std::endl;
  std::cout << "-S SERVER: server IP address or localhost" << std::endl;
  std::cout << "-u USER: user name" << std::endl;
  std::cout << "-i UID: user ID" << std::endl;
  std::cout << "-p PASSWORD: password" << std::endl;
  std::cout << "-d DATABASE: database name" << std::endl;
  std::cout << "-q STATEMENT: SQL statement" << std::endl;
  std::cout << "-f CSV: CSV file name" << std::endl;
  std::cout << "-x SCHEMA: Column schema file name" << std::endl;
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
  std::string fname_csv;
  std::string sql;
  std::string fname_information_schema_columns;

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
        fname_csv = argv[i + 1];
        i++;
        break;
      case 'x':
        fname_information_schema_columns = argv[i + 1];
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

  odbc query;
  std::string conn = make_conn(server, database);
  if (query.connect(conn) < 0)
  {
    return 1;
  }
  if (!fname_csv.empty())
  {
    if (csv_to_odbc(fname_csv, query, fname_information_schema_columns) < 0)
    {
      query.disconnect();
      assert(0);
      return 1;
    }
  }
  else
  {
    std::ifstream ifs;
    ifs.open("tables.txt");
    std::string str;
    while (std::getline(ifs, str))
    {

    }
    ifs.close();
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
//csv_to_odbc
/////////////////////////////////////////////////////////////////////////////////////////////////////

int csv_to_odbc(const std::string& fname_csv, odbc& query, const std::string& information_schema_columns)
{
  clock_gettime_t timer;
  read_csv_t csv;
  size_t nbr_rows = 0;
  std::vector<std::string> row;
  size_t nbr_cols;
  std::string sql;
  bool has_column_types = !information_schema_columns.empty();
  std::vector<column_t> cols; //import column types from information_schema_columns

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //column name and type
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::ifstream ifs;
  if (has_column_types)
  {
    ifs.open(information_schema_columns);
    if (ifs.fail())
    {
      std::cout << "Cannot open file " << information_schema_columns << std::endl;
      return -1;
    }

    column_t col;
    std::string str;
    int is_name = 1;
    while (ifs >> str)
    {
      if (is_name)
      {
        col.name = str;
        is_name = 0;
      }
      else
      {
        col.sqltype = std::stoi(str);
        is_name = 1;
        std::cout << col.name << " " << col.sqltype << "\t";
        cols.push_back(col);
      }
    }
    ifs.close();
    std::cout << "\n";
  }

  if (csv.open(fname_csv) < 0)
  {
    std::cout << "Cannot open file " << fname_csv.c_str() << std::endl;
    return -1;
  }

  timer.start();

  row = csv.read_row();
  nbr_cols = row.size();
  assert(cols.size() == nbr_cols);

  size_t found = fname_csv.find(".");
  std::string table = "[";
  table += fname_csv.substr(0, found);
  table += "]";

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //DROP TABLE
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  sql = "DROP TABLE ";
  sql += table;
  sql += " ;";

  if (query.exec_direct(sql) < 0)
  {

  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //CREATE TABLE
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  sql = "CREATE TABLE ";
  sql += table;
  sql += " (";
  for (size_t idx = 0; idx < nbr_cols; idx++)
  {
    std::string col = row.at(idx);
    sql += col;
    sql += " ";
    if (has_column_types)
    {
      sql += sqlserver_type(cols[idx].sqltype);
    }
    else
    {
      sql += "varchar(255)";
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
      nbr_rows++;

      for (size_t idx_col = 0; idx_col < row.size(); idx_col++)
      {
        std::string col = row.at(idx_col);
        std::cout << col << "\t";
      }
      std::cout << std::endl;

      continue;
    }

    sql = "INSERT INTO ";
    sql += table;
    sql += " VALUES (";
    for (size_t idx_col = 0; idx_col < nbr_cols; idx_col++)
    {
      std::string col = row.at(idx_col);

      /////////////////////////////////////////////////////////////////////////////////////////////////////
      //escape single quotes; single quotes are escaped by doubling them up
      /////////////////////////////////////////////////////////////////////////////////////////////////////

      if (cols.at(idx_col).name.compare("TextMsg") == 0)
      {
        size_t pos = col.find("'");
        if (pos != std::string::npos)
        {
          std::cout << col << "\n";
          size_t len = col.size();
          std::string col_rep = col;
          size_t pos_inc = 0;
          for (size_t idx_chr = 0; idx_chr < len; idx_chr++)
          {
            if (col_rep.at(idx_chr) == '\'')
            {
              col.insert(idx_chr + pos_inc, 1, '\'');
              pos_inc++; //count the additional insert
              //inserts additional characters into the string right 
              //before the character indicated by pos
            }
          } //for

        } //pos
      }//compare

      if (has_column_types)
      {
        sql += sqlserver_type_data(cols[idx_col].sqltype, col);
      }
      else
      {
        //quote data for text type
        sql += "'";
        sql += col;
        sql += "'";
      }
      if (idx_col < nbr_cols - 1)
      {
        sql += ", ";
      }
    }
    sql += " );";

    if (query.exec_direct(sql) < 0)
    {
      std::cout << "\nError in SQL:\n";
      std::cout << sql << "\n";
    }
    else
    {
      nbr_rows++;
      std::cout << nbr_rows << " ";
    }

#ifdef LIMIT_ROWS
    if (nbr_rows > 20000)
    {
      break;
    }
#endif
  }

  std::cout << "processed " << nbr_rows << " rows" << std::endl;
  timer.now();
  timer.stop();
  return 0;
}


/////////////////////////////////////////////////////////////////////////////////////////////////////
//sqlserver_type
//SQL_UNKNOWN_TYPE    0
//SQL_CHAR            1
//SQL_NUMERIC         2
//SQL_DECIMAL         3
//SQL_INTEGER         4
//SQL_SMALLINT        5
//SQL_FLOAT           6
//SQL_REAL            7
//SQL_DOUBLE          8
//SQL_DATETIME        9
//SQL_VARCHAR         12
//SQL_TYPE_DATE       91
//SQL_TYPE_TIME       92
//SQL_TYPE_TIMESTAMP  93
/////////////////////////////////////////////////////////////////////////////////////////////////////

std::string sqlserver_type(SQLSMALLINT sqltype)
{
  std::string str;

  switch (sqltype)
  {
  case -2:
  case -1: //text
    str = "varchar(max)";
    break;
  case 1:
    str = "varchar(255)";
    break;
  case 2:
  case 3:
  case 6:
  case 7:
  case 8:
    str = "real";
    break;
  case 4:
  case 5:
    str = "int";
    break;
  case 9:
  case 91:
  case 92:
  case 93:
    str = "datetime";
    break;
  case SQL_VARCHAR:
    str = "varchar(255)";
    break;
  case -6:
    str = "tinyint";
    break;
  case -7:
    str = "int";
    break;
  default:
    assert(0);
  };

  return str;
}

std::string sqlserver_type_data(SQLSMALLINT sqltype, const std::string& col)
{
  std::string sql;

  switch (sqltype)
  {
  case -2:
  case -1: //text
    sql += "'";
    sql += col;
    sql += "'";
    break;
  case 1:
    sql += "'";
    sql += col;
    sql += "'";
    break;
  case 2:
  case 3:
  case 6:
  case 7:
  case 8:
    try
    {
      double d = std::stod(col);
      sql += col;
    }
    catch (const std::invalid_argument& ia)
    {
      sql += "NULL";
    }
    break;
  case 4:
  case 5:
    try
    {
      int n = std::stoi(col);
      sql += col;
    }
    catch (const std::invalid_argument& ia)
    {
      sql += "NULL";
    }
    break;
  case 9:
  case 91:
  case 92:
  case 93:
    if (col == "NULL")
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
    break;
  case 12:
    sql += "'";
    sql += col;
    sql += "'";
    break;
  case -6:
  case -7:
    try
    {
      int n = std::stoi(col);
      sql += col;
    }
    catch (const std::invalid_argument& ia)
    {
      sql += "NULL";
    }
    break;
  default:
    assert(0);
  };

  return sql;
}
