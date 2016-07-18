#include <NFmiTestbedDB.h>

NFmiTestbedDB& NFmiTestbedDB::Instance()
{
	static NFmiTestbedDB instance_;
	return instance_;
}

NFmiTestbedDB::NFmiTestbedDB() : NFmiODBC()
{
	connected_ = false;
	user_ = "neons_client";
	password_ = "kikka8si";
	database_ = "testbed";
}

NFmiTestbedDB::~NFmiTestbedDB() { Disconnect(); }
