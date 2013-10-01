#include <NFmiNeonsDB.h>
#include <NFmiCLDB.h>
#include <NFmiTestbedDB.h>
#include <iostream>

using namespace std;

int main()
{
 
  // CLDB

  NFmiCLDB::Instance().Connect();
 
  try {
    NFmiCLDB::Instance().ExecuteProcedure("STATION_QP.get_station_list_rc(20)");
  }
  catch (int e)
  {
    cerr << "Got ORA-" << e << endl;
    exit(1);
  }

  int j = 0;
  
  while(true) {
    vector<string> row = NFmiCLDB::Instance().FetchRowFromCursor();

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
    NFmiCLDB::Instance().Execute("CREATE TABLE testi (id number)");
  }
  catch (int e)
  {
    if (e == 955)
    {
      // ORA-00955
      try
      {
        cout << "table exists, dropping it" << endl;
        NFmiCLDB::Instance().Execute("DROP TABLE testi");
        NFmiCLDB::Instance().Execute("CREATE TABLE testi (id number)");
      }
      catch (int e)
      { 
        cerr << "got exception code " << e << endl;
        exit(1);
      }
    }
  }

  try {
    NFmiCLDB::Instance().Execute("INSERT INTO testi VALUES (1)");
  }
  catch (int e)
  {
    cerr << "got exception code " << e << endl;
    exit(1);
  }

  try {
    NFmiCLDB::Instance().Query("SELECT * FROM testi");
  } catch (int e)
  {
    cout << "got code " << e << endl;
  }

  while(true)
  {
    vector<string> row = NFmiCLDB::Instance().FetchRow();

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

  NFmiNeonsDB::Instance().Connect();

  NFmiNeonsDB::Instance().Query("SELECT * FROM fmi_producers");

  j = 0;

  while(true)
  {
    vector<string> row = NFmiNeonsDB::Instance().FetchRow();

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

  NFmiTestbedDB::Instance().Connect();

  try {
    NFmiTestbedDB::Instance().Query("SELECT * FROM stations");
  }
  catch (int e)
  {
    cerr << "got " << e << endl;
//    exit(1);
  }

  j = 0;

  while(true)
  {
    vector<string> row = NFmiTestbedDB::Instance().FetchRow();

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


  NFmiNeonsDB::Instance().Disconnect();
  NFmiCLDB::Instance().Disconnect();
  NFmiTestbedDB::Instance().Disconnect();

  exit (0);
}
