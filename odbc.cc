#include "odbc.hh"
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <stdexcept>

#ifdef _MSC_VER
std::string default_driver = "DRIVER={SQL Server};";
#else
std::string default_driver = "DRIVER=ODBC Driver 17 for SQL Server;";
#endif

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::odbc
//Open Database Connectivity wrapper
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
  SQLFreeHandle(SQL_HANDLE_ENV, m_henv);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::connect
/////////////////////////////////////////////////////////////////////////////////////////////////////

int odbc::connect(const std::string& conn)
{
  SQLCHAR outstr[1024];
  SQLSMALLINT outstrlen;
  SQLCHAR* str_conn = (SQLCHAR*)conn.c_str();

  if (!SQL_SUCCEEDED(SQLSetEnvAttr(m_henv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0)))
  {

  }
  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_DBC, m_henv, &m_hdbc)))
  {
    extract_error(m_henv, SQL_HANDLE_ENV);
    return -1;
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
    return -1;
    break;
  case SQL_ERROR:
    extract_error(m_hdbc, SQL_HANDLE_DBC);
    SQLFreeHandle(SQL_HANDLE_DBC, m_hdbc);
    m_hdbc = 0;
    return -1;
    break;
  default:
    break;
  }

  std::cout << reinterpret_cast<const char*>(outstr) << std::endl;
  get_version();
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
//odbc::get_version
/////////////////////////////////////////////////////////////////////////////////////////////////////

int odbc::get_version()
{
  SQLHSTMT hstmt;
  SQLCHAR str[256];

  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, m_hdbc, &hstmt)))
  {

  }

  if (!SQL_SUCCEEDED(SQLExecDirect(hstmt, (SQLCHAR*)"SELECT @@Version", SQL_NTS)))
  {

  }

  if (!SQL_SUCCEEDED(SQLBindCol(hstmt, 1, SQL_C_CHAR, &str, 256, 0)))
  {

  }

  while (SQL_SUCCEEDED(SQLFetch(hstmt)))
  {
    std::cout << reinterpret_cast<const char*>(str) << std::endl;
  }
  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::set_auto_commit
/////////////////////////////////////////////////////////////////////////////////////////////////////

int odbc::set_auto_commit()
{
  if (!SQL_SUCCEEDED(SQLSetConnectAttr(m_hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)TRUE, 0)))
  {
    return -1;
  }
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::set_manual
/////////////////////////////////////////////////////////////////////////////////////////////////////

int odbc::set_manual()
{
  if (!SQL_SUCCEEDED(SQLSetConnectAttr(m_hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)FALSE, 0)))
  {
    return -1;
  }
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::commit_transaction
/////////////////////////////////////////////////////////////////////////////////////////////////////

int odbc::commit_transaction()
{
  if (!SQL_SUCCEEDED(SQLEndTran(SQL_HANDLE_DBC, m_hdbc, SQL_COMMIT)))
  {
    return -1;
  }
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::rollback_transaction
/////////////////////////////////////////////////////////////////////////////////////////////////////

int odbc::rollback_transaction()
{
  if (!SQL_SUCCEEDED(SQLEndTran(SQL_HANDLE_DBC, m_hdbc, SQL_ROLLBACK)))
  {
    return -1;
  }
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::exec_direct
//return values from SQLExecDirect
//SQL_SUCCESS, 
//SQL_SUCCESS_WITH_INFO, 
//SQL_NEED_DATA, 
//SQL_STILL_EXECUTING,
//SQL_ERROR, 
//SQL_NO_DATA, 
//SQL_INVALID_HANDLE, 
//SQL_PARAM_DATA_AVAILABLE
//If SQLExecDirect executes a searched update, insert, or delete statement that does not affect any rows at the data source, 
//the call to SQLExecDirect returns SQL_NO_DATA.
/////////////////////////////////////////////////////////////////////////////////////////////////////

int odbc::exec_direct(const std::string& sql)
{
  //find incorrect NULL insertions (quoted); use function 'check_tables_for_NULL()' to detect 
  size_t pos = sql.find("'NULL'");
  if (pos != std::string::npos)
  {
    std::cout << sql << std::endl;
  }

  std::cout << sql << std::endl;
  SQLHSTMT hstmt;
  SQLCHAR* sqlstr = (SQLCHAR*)sql.c_str();

  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, m_hdbc, &hstmt)))
  {
  }

  SQLRETURN rc = SQLExecDirect(hstmt, sqlstr, SQL_NTS);
  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO || rc == SQL_NO_DATA)
  {
  }
  else
  {
    extract_error(hstmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    return -1;
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
      std::stringstream ss;
      ss << idx << ":" << sql_state << ":" << native_error << ":" << str;
      std::cout << ss.str().c_str() << std::endl;
    }
  } while (rc == SQL_SUCCESS);
}


/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc::fetch
/////////////////////////////////////////////////////////////////////////////////////////////////////

int odbc::fetch(const std::string& sql, table_t& table)
{
  table.remove();
  std::cout << sql << std::endl;
  SQLHSTMT hstmt;
  SQLSMALLINT nbr_cols;
  SQLCHAR* sqlstr = (SQLCHAR*)sql.c_str();
  struct bind_column_data_t* bind_data = NULL;

  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, m_hdbc, &hstmt)))
  {

  }

  if (!SQL_SUCCEEDED(SQLExecDirect(hstmt, sqlstr, SQL_NTS)))
  {
    extract_error(hstmt, SQL_HANDLE_STMT);
    return -1;
  }

  if (!SQL_SUCCEEDED(SQLNumResultCols(hstmt, &nbr_cols)))
  {

  }

  bind_data = (bind_column_data_t*)malloc(nbr_cols * sizeof(bind_column_data_t));
  for (SQLUSMALLINT idx = 0; idx < nbr_cols; idx++)
  {
    bind_data[idx].target_value_ptr = NULL;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //get column names
  /////////////////////////////////////////////////////////////////////////////////////////////////////

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

    column_t col;
    col.name = (char*)buf;
    col.sqltype = sqltype;
    table.cols.push_back(col);
  }

  for (SQLUSMALLINT idx = 0; idx < nbr_cols; idx++)
  {
    bind_data[idx].target_type = SQL_C_CHAR;
    bind_data[idx].buf_len = (1024 + 1);
    bind_data[idx].target_value_ptr = malloc(sizeof(unsigned char) * bind_data[idx].buf_len);
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

  size_t nbr_rows = 0;
  while (SQL_SUCCEEDED(SQLFetch(hstmt)))
  {
    row_t row;
    for (int idx_col = 0; idx_col < nbr_cols; idx_col++)
    {
      std::string str;
      if (bind_data[idx_col].strlen_or_ind != SQL_NULL_DATA)
      {
        str = (char*)bind_data[idx_col].target_value_ptr;
      }
      else
      {
        str = ODBC::SQL_NULL;
      }
      row.col.push_back(str);
    }
    table.rows.push_back(row);
    nbr_rows++;
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
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//table_t::remove
/////////////////////////////////////////////////////////////////////////////////////////////////////

void table_t::remove()
{
  cols.clear();
  for (size_t idx_row = 0; idx_row < rows.size(); idx_row++)
  {
    rows.at(idx_row).col.clear();
  }
  rows.clear();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//table_t::get_row_col_value
/////////////////////////////////////////////////////////////////////////////////////////////////////

std::string table_t::get_row_col_value(int row, const std::string& col_name)
{
  int col_num = -1;
  for (int c = 0; c < cols.size(); c++)
  {
    if (cols.at(c).name.compare(col_name) == 0)
    {
      col_num = c;
      break;
    }
  }
  return std::string(rows.at(row).col.at(col_num));
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//make_conn
//make connection string without DNS (Windows, Linux)
//Driver=ODBC Driver 17 for SQL Server;Server=tcp:localhost,1433;UID=my_username;PWD=my_password
//To allow services to connect user 'NT AUTHORITY\SYSTEM', in SQL Server Management Studio:
// 1. Security->Logins->NT AUTHORITY\SYSTEM
// 2. Properties->Server roles.
// 3. check 'sysadmin'
/////////////////////////////////////////////////////////////////////////////////////////////////////

std::string make_conn(const std::string& server, const std::string& database, std::string user, std::string password)
{
  std::string conn;
  conn += default_driver;

  conn += "SERVER=";
  conn += server;
  conn += ", 1433;";

  if (!user.empty())
  {
    conn += "UID=";
    conn += user;
    conn += ";";
  }

  if (!password.empty())
  {
    conn += "PWD=";
    conn += password;
    conn += ";";
  }

  if (!database.empty())
  {
    conn += "DATABASE=";
    conn += database;
    conn += ";";
  }

  if (!password.empty())
  {
    conn += "Trusted_Connection=False;";
    conn += "Integrated Security=False;";
  }

  std::cout << conn << std::endl;
  return conn;
}


///////////////////////////////////////////////////////////////////////////////
//list_databases
///////////////////////////////////////////////////////////////////////////////

int list_databases(std::string server)
{
  odbc query;
  std::string conn;
  int get_tables = 0;

  conn = make_conn(server, "master");
  if (query.connect(conn) < 0)
  {
    return -1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //list all databases
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  table_t table;
  std::string sql("SELECT [name] FROM sys.databases d WHERE d.database_id > 6");
  std::cout << sql << std::endl;
  if (query.fetch(sql, table) < 0)
  {
    assert(0);
  }

  query.disconnect();

  size_t nbr_dbs = table.rows.size();
  std::cout << "Databases: " << nbr_dbs << '\n';

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //for each database, list all tables
  //SELECT table_name FROM <database_name>.information_schema.tables WHERE table_type = 'base table'
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  for (size_t idx = 0; idx < nbr_dbs; idx++)
  {
    row_t row = table.rows.at(idx);
    assert(row.col.size() == 1);
    std::string db_name = row.col.at(0);
    std::cout << db_name.c_str() << '\n';

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //connect to database
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    conn = make_conn(server, db_name);
    if (query.connect(conn) < 0)
    {
      assert(0);
    }

    std::string sql;
    sql = "SELECT table_name FROM ";
    sql += db_name;
    sql += ".information_schema.tables WHERE table_type = 'base table'";

    table_t tbl_db;
    if (query.fetch(sql, tbl_db) < 0)
    {
      assert(0);
    }

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
        table_t tbl;
        if (query.fetch(sql, tbl) < 0)
        {
          assert(0);
        }
        std::string fname(tbl_name);
        fname += ".table.csv";
        tbl.to_csv(fname);
      }
    }

    query.disconnect();
  }

  return 0;
}


/////////////////////////////////////////////////////////////////////////////////////////////////////
//table_t::to_csv
/////////////////////////////////////////////////////////////////////////////////////////////////////

int table_t::to_csv(const std::string& fname)
{
  FILE* stream = fopen(fname.c_str(), "w");
  if (stream == NULL)
  {
    return -1;
  }

  size_t nbr_cols = cols.size();
  size_t nbr_rows = rows.size();
  for (size_t idx_col = 0; idx_col < nbr_cols; idx_col++)
  {
    char* fmt;
    if (idx_col < nbr_cols - 1) fmt = "%s\t";
    else fmt = "%s\n";
    fprintf(stream, fmt, cols.at(idx_col).name.c_str());
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
      char* buf;
      if (idx_col < nbr_cols - 1) buf = "\t";
      else buf = "\n";
      fprintf(stream, buf);
    }
  }

  fclose(stream);
  return 0;
}