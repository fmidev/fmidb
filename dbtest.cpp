#include <NeonsDB.h>
#include <CLDB.h>
#include <TestbedDB.h>
#include <iostream>

using namespace std;

int main()
{
 
  // CLDB

  CLDB::Instance().Connect();
 
  try {
    CLDB::Instance().ExecuteProcedure("STATION_QP.get_station_list_rc(20)");
  }
  catch (int e)
  {
    cerr << "Got ORA-" << e << endl;
    exit(1);
  }

  int j = 0;
  
  while(true) {
    vector<string> row = CLDB::Instance().FetchRowFromCursor();

    if(row.empty()) break;
 
    ++j;

    cout << "row " << j << ": " ;

    for (unsigned int i = 0; i < row.size() ; i++)
    {
      cout << row[i] << " ";
    }

    cout << endl;
    
    if (j == 100)
      break;
  } 
  
  try {
    CLDB::Instance().Execute("CREATE TABLE testi (id number)");
  }
  catch (int e)
  {
    if (e == 955)
    {
      // ORA-00955
      try
      {
        cout << "table exists, dropping it" << endl;
        CLDB::Instance().Execute("DROP TABLE testi");
        CLDB::Instance().Execute("CREATE TABLE testi (id number)");
      }
      catch (int e)
      { 
        cerr << "got exception code " << e << endl;
        exit(1);
      }
    }
  }

  try {
    CLDB::Instance().Execute("INSERT INTO testi VALUES (1)");
  }
  catch (int e)
  {
    cerr << "got exception code " << e << endl;
    exit(1);
  }

  try {
    CLDB::Instance().Query("SELECT * FROM testi");
  } catch (int e)
  {
    cout << "got code " << e << endl;
  }

  while(true)
  {
    vector<string> row = CLDB::Instance().FetchRow();

    if(row.empty()) break;
 
    ++j;

    cout << "row " << j << ": " ;

    for (unsigned int i = 0; i < row.size() ; i++)
    {
      cout << row[i] << " ";
    }

    cout << endl;
  }

  // NEONS

  NeonsDB::Instance().Connect();

  NeonsDB::Instance().Query("SELECT * FROM fmi_producers");

  j = 0;

  while(true)
  {
    vector<string> row = NeonsDB::Instance().FetchRow();

    if(row.empty()) break;
 
    ++j;

    cout << "row " << j << ": " ;

    for (unsigned int i = 0; i < row.size() ; i++)
    {
      cout << row[i] << " ";
    }

    cout << endl;
  }

  // TESTBED (POSTGRESQL)

  TestbedDB::Instance().Connect();

  try {
    TestbedDB::Instance().Query("SELECT * FROM stations");
  }
  catch (int e)
  {
    cerr << "got " << e << endl;
//    exit(1);
  }

  j = 0;

  while(true)
  {
    vector<string> row = TestbedDB::Instance().FetchRow();

    if(row.empty()) break;
 
    ++j;

    cout << "row " << j << ": " ;

    for (unsigned int i = 0; i < row.size() ; i++)
    {
      if (row[i].size() == 0)
      {
        cout << "NULL ";   
      }
      else
      {
        cout << row[i] << " ";
      }
    }

    cout << endl;
  }


  NeonsDB::Instance().Disconnect();
  CLDB::Instance().Disconnect();
  TestbedDB::Instance().Disconnect();

  exit (0);
}
