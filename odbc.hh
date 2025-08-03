#ifndef ODBC_HH_
#define ODBC_HH_ 1

#ifdef _WIN64
#include <windows.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include <sql.h>
#include <sqlext.h>
#include <string>
#include <vector>
#include <assert.h>

#include "table.hh"

#ifndef _MSC_VER
#ifndef FALSE
#define FALSE 0
#endif
#ifndef TRUE
#define TRUE 1
#endif
#endif

namespace ODBC
{
  //return value for a cell that is SQL_NULL_DATA as a string
  const std::string SQL_NULL = "SQL_NULL_DATA";
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//odbc
/////////////////////////////////////////////////////////////////////////////////////////////////////

class odbc
{
public:
  odbc();
  ~odbc();
  int connect(const std::string& conn);
  int disconnect();
  int exec_direct(const std::string& sql);
  int fetch(const std::string& sql, table_t& table);
  int set_auto_commit();
  int set_manual();
  int commit_transaction();
  int rollback_transaction();
  SQLHENV m_henv; //environment handle
  SQLHDBC m_hdbc; //connection handle

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
std::string make_conn(const std::string& server, const std::string& database,
  std::string user = std::string(),
  std::string password = std::string());
int list_databases(std::string server);


#endif

