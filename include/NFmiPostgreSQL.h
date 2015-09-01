#pragma once

#include <string>
#include <vector>
#include <pqxx/connection>
#include <pqxx/nontransaction>
#include <pqxx/result>
#include <memory>
#include "NFmiDatabase.h"

class NFmiPostgreSQL : public NFmiDatabase {

public:

	NFmiPostgreSQL();
	NFmiPostgreSQL(int theId);
	NFmiPostgreSQL(const std::string& user, const std::string& password, const std::string& database, const std::string& hostname, int port = 5432);
	virtual ~NFmiPostgreSQL();

    std::string ClassName() const { return "NFmiPostgreSQL"; }
	
	static NFmiPostgreSQL & Instance();

	virtual void Connect();

	virtual void Connect( const std::string & user,
								const std::string & password,
								const std::string & database,
								const std::string& hostname, 
								int port = 5432);
				
	virtual void Disconnect (void);
				
	virtual void Query(const std::string & sql);
	virtual std::vector<std::string> FetchRow(void);

	virtual void Execute(const std::string & sql);

	//virtual std::string MakeStandardDate(const otl_datetime &datetime);

	void Commit();
	void Rollback();

	int Id() { return id_; }

protected:

	std::unique_ptr<pqxx::connection> db_;
	std::unique_ptr<pqxx::nontransaction> wrk_;
	pqxx::result res_;
	pqxx::result::const_iterator iter_;

	std::string hostname_;
	
	int port_;
	 
	int id_;

};
