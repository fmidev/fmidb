#include <string>
#include <vector>
#include "ODBC.h"

class TestbedDB : public ODBC
{

public:

  TestbedDB();
  ~TestbedDB();
        
  static TestbedDB & Instance();

};
