#include "odbc.hh"
#include <iostream>
#include <fstream>
#include <iomanip>
#include <stdexcept>

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::odbc
/////////////////////////////////////////////////////////////////////////////////////////////////////

odbc::odbc() :
  m_henv(0),
  m_hdbc(0)
{
  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_henv)))
  {

  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::~odbc
/////////////////////////////////////////////////////////////////////////////////////////////////////

odbc::~odbc()
{
  disconnect();
  SQLFreeHandle(SQL_HANDLE_ENV, m_henv);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::connect
/////////////////////////////////////////////////////////////////////////////////////////////////////

int odbc::connect(const std::string &conn)
{
  SQLCHAR outstr[1024];
  SQLSMALLINT outstrlen;
  SQLCHAR* str_conn = (SQLCHAR*)conn.c_str();
  std::vector<std::string> tables;

  std::cout << conn.c_str() << std::endl;

  if (!SQL_SUCCEEDED(SQLSetEnvAttr(m_henv, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0)))
  {

  }
  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_DBC, m_henv, &m_hdbc)))
  {

  }

  switch (SQLDriverConnect(
    m_hdbc,
    NULL,
    str_conn,
    SQL_NTS,
    outstr,
    sizeof(outstr),
    &outstrlen,
    SQL_DRIVER_COMPLETE))
  {
  case SQL_SUCCESS:
    break;
  case SQL_SUCCESS_WITH_INFO:
    break;
  case SQL_INVALID_HANDLE:
  case SQL_ERROR:
    extract_error(m_hdbc, SQL_HANDLE_DBC);
    return -1;
    break;
  default:
    break;
  }

  std::cout << outstr << std::endl;
  get_version();
  get_tables();
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::disconnect
/////////////////////////////////////////////////////////////////////////////////////////////////////

int odbc::disconnect()
{
  SQLDisconnect(m_hdbc);
  SQLFreeHandle(SQL_HANDLE_DBC, m_hdbc);
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::get_tables
/////////////////////////////////////////////////////////////////////////////////////////////////////

int odbc::get_tables()
{
  SQLHSTMT hstmt;
  SQLSMALLINT nbr_cols;
  std::ofstream ofs;
  ofs.open("tables.txt");

  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, m_hdbc, &hstmt)))
  {

  }

  if (!SQL_SUCCEEDED(SQLTables(hstmt,
    NULL, 0, // no specific catalog 
    NULL, 0, // no specific schema 
    NULL, 0, // no specific table
    (SQLCHAR*)"TABLE", SQL_NTS))) // only tables, no views 
  {

  }

  if (!SQL_SUCCEEDED(SQLNumResultCols(hstmt, &nbr_cols)))
  {

  }

  while (SQL_SUCCEEDED(SQLFetch(hstmt)))
  {
    bool is_dbo = false;
    for (SQLUSMALLINT idx_col = 1; idx_col <= nbr_cols; idx_col++)
    {
      SQLLEN indicator;
      char buf[512];

      //retrieve column data as a string
      if (SQL_SUCCEEDED(SQLGetData(hstmt, idx_col, SQL_C_CHAR, buf, sizeof(buf), &indicator)))
      {
        if (indicator == SQL_NULL_DATA) strcpy(buf, "NULL");
        std::cout << buf;
        ofs << buf;
        if (idx_col == 2)
        {
          std::string str(buf);
          if (str.compare("dbo") == 0)
          {
            is_dbo = true;
          }
        }
        else if (idx_col == 3)
        {
          if (is_dbo)
          {
          }
        }
        if (idx_col < nbr_cols)
        {
          std::cout << std::setw(25);
          ofs << std::setw(25);
        }
      }
    }
    std::cout << std::endl;
    ofs << std::endl;
  }

  ofs.close();
  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::get_version
/////////////////////////////////////////////////////////////////////////////////////////////////////

int odbc::get_version()
{
  SQLHSTMT hstmt;
  SQLCHAR str[256];

  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, m_hdbc, &hstmt)))
  {

  }

  if (!SQL_SUCCEEDED(SQLExecDirect(hstmt, (SQLCHAR*) "SELECT @@Version", SQL_NTS)))
  {

  }

  if (!SQL_SUCCEEDED(SQLBindCol(hstmt, 1, SQL_C_CHAR, &str, 256, 0)))
  {

  }

  while (SQL_SUCCEEDED(SQLFetch(hstmt)))
  {
    std::cout << str;
  }
  std::cout << std::endl;
  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::exec_direct
/////////////////////////////////////////////////////////////////////////////////////////////////////

int odbc::exec_direct(const std::string &sql)
{
  SQLHSTMT hstmt;
  SQLCHAR* sqlstr = (SQLCHAR*)sql.c_str();

  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, m_hdbc, &hstmt)))
  {

  }

  if (!SQL_SUCCEEDED(SQLExecDirect(hstmt, sqlstr, SQL_NTS)))
  {
    extract_error(hstmt, SQL_HANDLE_STMT);
  }

  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//extract_error
/////////////////////////////////////////////////////////////////////////////////////////////////////

void extract_error(SQLHANDLE handle, SQLSMALLINT type)
{
  SQLSMALLINT idx = 0;
  SQLINTEGER native_error;
  SQLCHAR sql_state[7];
  SQLCHAR str[256];
  SQLSMALLINT str_len;
  SQLRETURN rc;

  do
  {
    rc = SQLGetDiagRec(type,
      handle,
      ++idx,
      sql_state,
      &native_error,
      str,
      sizeof(str),
      &str_len);
    if (SQL_SUCCEEDED(rc))
    {
      printf("%s:%ld:%ld:%s\n", sql_state, (long)idx, (long)native_error, str);
    }
  } while (rc == SQL_SUCCESS);
}


/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::fetch
/////////////////////////////////////////////////////////////////////////////////////////////////////

table_t odbc::fetch(const std::string &sql)
{
  SQLHSTMT hstmt;
  SQLSMALLINT nbr_cols;
  SQLCHAR* sqlstr = (SQLCHAR*)sql.c_str();
  struct bind_column_data_t* bind_data = NULL;
  table_t table;

  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, m_hdbc, &hstmt)))
  {

  }

  if (!SQL_SUCCEEDED(SQLExecDirect(hstmt, sqlstr, SQL_NTS)))
  {
    extract_error(hstmt, SQL_HANDLE_STMT);
    return table;
  }

  if (!SQL_SUCCEEDED(SQLNumResultCols(hstmt, &nbr_cols)))
  {

  }

  bind_data = (bind_column_data_t*)malloc(nbr_cols * sizeof(bind_column_data_t));
  for (SQLUSMALLINT idx = 0; idx < nbr_cols; idx++)
  {
    bind_data[idx].target_value_ptr = NULL;
  }

  for (SQLUSMALLINT idx = 0; idx < nbr_cols; idx++)
  {
    SQLCHAR buf[1024];
    SQLSMALLINT sqltype = 0;
    SQLSMALLINT scale = 0;
    SQLSMALLINT nullable = 0;
    SQLSMALLINT len = 0;
    SQLULEN sqlsize = 0;

    if (!SQL_SUCCEEDED(SQLDescribeCol(
      hstmt,
      idx + 1,
      (SQLCHAR*)buf, //column name
      sizeof(buf) / sizeof(SQLCHAR),
      &len,
      &sqltype,
      &sqlsize,
      &scale,
      &nullable)))
    {
      extract_error(hstmt, SQL_HANDLE_STMT);
    }

    table.col_name.push_back((char*)buf);
  }

  for (SQLUSMALLINT idx = 0; idx < nbr_cols; idx++)
  {
    bind_data[idx].target_type = SQL_C_CHAR;
    bind_data[idx].buf_len = (1024 + 1);
    bind_data[idx].target_value_ptr = malloc(sizeof(unsigned char)*bind_data[idx].buf_len);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //SQLBindCol assigns the storage and data type for a column in a result set, 
  //including:
  //a storage buffer that will receive the contents of a column of data
  //the length of the storage buffer
  //a storage location that will receive the actual length of the column of data 
  //returned by the fetch operation data type conversion
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  for (SQLUSMALLINT idx = 0; idx < nbr_cols; idx++)
  {
    if (!SQL_SUCCEEDED(SQLBindCol(
      hstmt,
      idx + 1,
      bind_data[idx].target_type,
      bind_data[idx].target_value_ptr,
      bind_data[idx].buf_len,
      &(bind_data[idx].strlen_or_ind))))
    {
      extract_error(hstmt, SQL_HANDLE_STMT);
    }
  }

  while (SQL_SUCCEEDED(SQLFetch(hstmt)))
  {
    row_t row;
    for (int idx_col = 0; idx_col < nbr_cols; idx_col++)
    {
      std::string str;
      if (bind_data[idx_col].strlen_or_ind != SQL_NULL_DATA)
      {
        str = (char *)bind_data[idx_col].target_value_ptr;
      }
      else
      {
        str = "NULL";
      }
      row.col.push_back(str);
    }
    table.rows.push_back(row);
  }

  for (SQLUSMALLINT idx_col = 0; idx_col < nbr_cols; idx_col++)
  {
    if (bind_data[idx_col].target_value_ptr != NULL)
    {
      free(bind_data[idx_col].target_value_ptr);
    }
  }
  if (bind_data != NULL)
  {
    free(bind_data);
  }
  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return table;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//table_t::to_csv
/////////////////////////////////////////////////////////////////////////////////////////////////////

int table_t::to_csv(const std::string &fname)
{
  FILE *stream = fopen(fname.c_str(), "w");
  if (stream == NULL)
  {
    return -1;
  }

  size_t nbr_cols = col_name.size();
  size_t nbr_rows = rows.size();
  for (size_t idx_col = 0; idx_col < nbr_cols; idx_col++)
  {
    char *fmt;
    if (idx_col < nbr_cols - 1) fmt = "%s\t";
    else fmt = "%s\n";
    fprintf(stream, fmt, col_name.at(idx_col).c_str());
  }

  for (size_t idx_row = 0; idx_row < nbr_rows; idx_row++)
  {
    for (size_t idx_col = 0; idx_col < nbr_cols; idx_col++)
    {
      std::string col = rows.at(idx_row).col.at(idx_col);
      std::size_t found = col.find('\n');
      if (found != std::string::npos)
      {
        col.insert(0, "\"");
        col.append("\"");
      }
      fprintf(stream, "%s", col.c_str());
      char *buf;
      if (idx_col < nbr_cols - 1) buf = "\t";
      else buf = "\n";
      fprintf(stream, buf);
    }
  }

  fclose(stream);
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//table_t::remove
/////////////////////////////////////////////////////////////////////////////////////////////////////

void table_t::remove()
{
  col_name.clear();
  for (size_t idx_row = 0; idx_row < rows.size(); idx_row++)
  {
    rows.at(idx_row).col.clear();
  }
  rows.clear();
}


/////////////////////////////////////////////////////////////////////////////////////////////////////
//make_conn
//connect to SQL Server (Windows, Linux)
/////////////////////////////////////////////////////////////////////////////////////////////////////

std::string make_conn(std::string user,
  std::string uid,
  std::string password,
  std::string server,
  std::string database)
{
  std::string conn;
#ifdef _MSC_VER
  std::string driver = "DRIVER={SQL Server};";
#else
  std::string driver = "DRIVER=ODBC Driver 13 for SQL Server;";
#endif

  if (!uid.empty() && !user.empty())
  {
    return conn;
  }

  conn += driver;
  conn += "SERVER=";
  conn += server;
  conn += ", 1433;";
  if (!uid.empty())
  {
    conn += "UID=";
    conn += uid;
    conn += ";";
    conn += "PWD=";
    conn += password;
    conn += ";";
  }
  else if (!user.empty())
  {
    conn += "User Id=";
    conn += user;
    conn += ";";
    conn += "Password=";
    conn += password;
    conn += ";";
  }

  if (!database.empty())
  {
    conn += "DATABASE=";
    conn += database;
    conn += ";";
  }

  return conn;
}

///////////////////////////////////////////////////////////////////////////////
//list_databases
///////////////////////////////////////////////////////////////////////////////

int list_databases(std::string user,
  std::string uid,
  std::string password,
  std::string server)
{
  odbc query;
  std::string conn;
  int get_tables = 0;

  conn = make_conn(user, uid, password, server, "master");
  if (query.connect(conn) < 0)
  {
    return -1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //list all databases
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  table_t dbs = query.fetch("SELECT [name] FROM sys.databases d WHERE d.database_id > 6");
  query.disconnect();

  size_t nbr_dbs = dbs.rows.size();
  std::cout << "Databases: " << nbr_dbs << '\n';

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //for each database, list all tables
  //SELECT table_name FROM <database_name>.information_schema.tables WHERE table_type = 'base table'
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  for (size_t idx = 0; idx < nbr_dbs; idx++)
  {
    row_t row = dbs.rows.at(idx);
    assert(row.col.size() == 1);
    std::string db_name = row.col.at(0);
    std::cout << db_name.c_str() << '\n';

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //connect to database
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    conn = make_conn(user, uid, password, server, db_name);
    if (query.connect(conn) < 0)
    {
      assert(0);
    }

    std::string sql;
    sql = "SELECT table_name FROM ";
    sql += db_name;
    sql += ".information_schema.tables WHERE table_type = 'base table'";

    table_t tbl_db = query.fetch(sql);
    size_t nbr_tbl = tbl_db.rows.size();
    std::cout << "Tables: " << nbr_tbl << '\n';

    std::string fname(db_name);
    fname += ".database.csv";
    tbl_db.to_csv(fname);

    for (size_t idx_tbl = 0; idx_tbl < nbr_tbl; idx_tbl++)
    {
      assert(tbl_db.rows.at(idx_tbl).col.size() == 1);
      std::string tbl_name = tbl_db.rows.at(idx_tbl).col.at(0);
      std::cout << tbl_name.c_str() << '\n';

      if (get_tables)
      {
        sql = "SELECT * FROM [";
        sql += tbl_name;
        sql += "]";
        table_t tbl = query.fetch(sql);
        std::string fname(tbl_name);
        fname += ".table.csv";
        tbl.to_csv(fname);
      }
    }

    query.disconnect();
  }

  return 0;
}

