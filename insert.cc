#include "odbc.hh"
#include <iostream>

/////////////////////////////////////////////////////////////////////////////////////////////////////
//main
/////////////////////////////////////////////////////////////////////////////////////////////////////

int main()
{
  std::string sql;
  std::string server("localhost");
  std::string database("test_db");


  odbc query;
  std::string conn = make_conn(server, database);
  if (query.connect(conn) < 0)
  {
    assert(0);
  }

  sql = "CREATE TABLE Persons ([Id] [int] NOT NULL, [Name] [char](36) NULL, [Adress] [varchar](100) NULL, [Time] [datetime] NULL);";
  if (query.exec_direct(sql) < 0)
  {
  }

  sql = "DELETE FROM Persons;";
  if (query.exec_direct(sql) < 0)
  {
  }

  sql = "INSERT INTO Persons ([Id], [Name], [Adress], [Time]) VALUES (0, 'Bob', '123 Tree Rd.', '2022-02-22');";
  if (query.exec_direct(sql) < 0)
  {
  }

  sql = "INSERT INTO Persons ([Id], [Name], [Adress], [Time]) VALUES (1, 'Susan', '456 Bee Rd.', '2022-02-22');";
  if (query.exec_direct(sql) < 0)
  {
  }

  sql = "INSERT INTO Persons ([Id], [Name], [Adress], [Time]) VALUES (2, '', '', '');";
  if (query.exec_direct(sql) < 0)
  {
  }

  sql = "INSERT INTO Persons ([Id], [Name], [Adress], [Time]) VALUES (3, NULL, NULL, NULL);";
  if (query.exec_direct(sql) < 0)
  {
  }

  table_t table = query.fetch("SELECT * FROM [Persons];");

  for (size_t idx_row = 0; idx_row < table.rows.size(); idx_row++)
  {
    row_t r = table.rows.at(idx_row);
    for (size_t idx_col = 0; idx_col < table.cols.size(); idx_col++)
    {
      std::string s = r.col.at(idx_col);
      std::cout << s << " (" << s.size() << ") ";
    }
    std::cout << std::endl;
  }

  query.disconnect();
  return 0;
}