#ifndef odbc_HH
#define odbc_HH 1

#ifdef _MSC_VER
#include <windows.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include <sql.h>
#include <sqlext.h>
#include <time.h>
#include <string>
#include <vector>
#include <assert.h>

/////////////////////////////////////////////////////////////////////////////////////////////////////
//row_t
//a row is a vector of strings
/////////////////////////////////////////////////////////////////////////////////////////////////////

struct row_t
{
  std::vector<std::string> col;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
//column_t
//a column has a name and a SQL type
/////////////////////////////////////////////////////////////////////////////////////////////////////

struct column_t
{
  std::string name;
  SQLSMALLINT sqltype;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
//table_t
//a table is a vector with rows, with column information (name, SQL type)
/////////////////////////////////////////////////////////////////////////////////////////////////////

class table_t
{
public:
  table_t()
  {

  }
  std::vector<column_t> cols;
  std::vector<row_t> rows;
  int to_csv(const std::string &fname);
  void remove();
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc
/////////////////////////////////////////////////////////////////////////////////////////////////////

class odbc
{
public:
  odbc();
  ~odbc();
  int connect(const std::string &conn);
  int disconnect();
  int exec_direct(const std::string &sql);
  table_t fetch(const std::string &sql, std::string file_schema = std::string());
  SQLHENV m_henv;
  SQLHDBC m_hdbc;
  int get_tables(int save);

private:
  int get_version();

  struct bind_column_data_t
  {
    SQLSMALLINT target_type; //the C data type of the result data
    SQLPOINTER target_value_ptr; //pointer to storage for the data
    SQLINTEGER buf_len; //maximum length of the buffer being bound for data (including null-termination byte for char) 
    SQLLEN strlen_or_ind; //number of bytes(excluding the null termination byte for character data) available
    //to return in the buffer prior to calling SQLFetch
  };
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
//global functions
/////////////////////////////////////////////////////////////////////////////////////////////////////

void extract_error(SQLHANDLE handle, SQLSMALLINT type);

std::string make_conn(std::string server, std::string database);
int list_databases(std::string server);


#endif

