#include "odbc.hh"
#include "table.hh"
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <stdexcept>

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