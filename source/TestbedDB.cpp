#include <TestbedDB.h>

TestbedDB & TestbedDB::Instance() {
  static TestbedDB instance_;
  return instance_; 
}

TestbedDB::TestbedDB() : ODBC() {
  connected_ = false;
  user_ = "neons_client";
  password_ = "kikka8si";
  database_ = "testbed";
}
 
TestbedDB::~TestbedDB() {
  Disconnect();              
}
