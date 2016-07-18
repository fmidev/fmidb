#pragma once

#include <string>
#include <vector>
#include "NFmiDatabase.h"
#include "otlsettings.h"

class NFmiODBC : public NFmiDatabase
{
   public:
	NFmiODBC();
	NFmiODBC(short theId);
	NFmiODBC(const std::string& user, const std::string& password, const std::string& database);
	virtual ~NFmiODBC();

	std::string ClassName() const { return "NFmiODBC"; }
	static NFmiODBC& Instance();

	virtual void Connect();
	virtual void Connect(const int threadedMode);

	virtual void Connect(const std::string& user, const std::string& password, const std::string& database,
	                     const int threadedMode = 0);

	virtual void Disconnect(void);

	virtual void Query(const std::string& sql);
	virtual std::vector<std::string> FetchRow(void);

	virtual void Execute(const std::string& sql) throw(int);

	virtual std::string MakeStandardDate(const otl_datetime& datetime);

	void Commit() throw(int);
	void Rollback() throw(int);

	short Id() { return id_; }
   protected:
	odbc::otl_connect db_;
	odbc::otl_stream stream_;
	otl_stream_read_iterator<odbc::otl_stream, odbc::otl_exception, odbc::otl_lob_stream> rs_iterator_;

	int id_;
};
