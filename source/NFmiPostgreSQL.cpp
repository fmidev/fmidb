#include <NFmiPostgreSQL.h>
#include <exception>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include "boost/lexical_cast.hpp"

#ifdef DEBUG
#include <assert.h>
#endif

using namespace std;

NFmiPostgreSQL& NFmiPostgreSQL::Instance()
{
	static NFmiPostgreSQL instance_;
	return instance_;
}

NFmiPostgreSQL::NFmiPostgreSQL() : port_(5432), id_(0) {}
NFmiPostgreSQL::NFmiPostgreSQL(int theId) : port_(5432), id_(theId) {}
NFmiPostgreSQL::NFmiPostgreSQL(const std::string& user, const std::string& password, const std::string& database,
                               const std::string& hostname, int port)
    : NFmiDatabase(user, password, database), hostname_(hostname), port_(port), id_(0)
{
}

void NFmiPostgreSQL::Connect(const string& user, const string& password, const string& database,
                             const std::string& hostname, int port)
{
	if (connected_) return;

	user_ = user;
	password_ = password;
	database_ = database;
	hostname_ = hostname;
	port_ = port;

	NFmiPostgreSQL::Connect();
}

void NFmiPostgreSQL::Connect()
{
	if (connected_) return;

#ifdef DEBUG
	cout << "DEBUG: dsn is 'user=" << user_ << " password=xxx host=" << hostname_ << " dbname=" << database_
	     << " port=" + boost::lexical_cast<string>(port_) << "'" << endl;
#endif

	connection_string_ = "user=" + user_ + " password=" + password_ + " host=" + hostname_ + " dbname=" + database_ +
	                     " port=" + boost::lexical_cast<string>(port_);
	db_ = unique_ptr<pqxx::connection>(new pqxx::connection(connection_string_));
	wrk_ = unique_ptr<pqxx::nontransaction>(new pqxx::nontransaction(*db_));
	connected_ = true;

#ifdef DEBUG
	cout << "DEBUG: connected to PostgreSQL " << database_ << endl;
#endif
}

void NFmiPostgreSQL::Query(const string& sql)
{
	if (!connected_)
	{
		cerr << "ERROR: must be connected before executing query" << endl;
		exit(1);
	}

#ifdef DEBUG
	cout << "DEBUG: " << sql << endl;
#endif

	res_ = wrk_->exec(sql);
	iter_ = res_.begin();
#ifdef DEBUG
	cout << "DEBUG: query returned " << res_.size() << " rows" << endl;
#endif
}

vector<string> NFmiPostgreSQL::FetchRow()
{
	if (!connected_) throw runtime_error("NFmiPostgreSQL: Cannot perform SQL query before connected");

	vector<string> ret;

	if (iter_ == res_.end())
	{
		return ret;
	}

	int rowsize = static_cast<int>(iter_.size());

	ret.resize(rowsize);

	for (int n = 0; n < rowsize; ++n)
	{
		auto field = iter_[n];

		if (field.is_null())
		{
			ret.push_back("");
			continue;
		}
		ret[n] = string(field.c_str());
	}

	++iter_;

	return ret;
}

/*
 * Execute(string)
 *
 * Executes an SQL DML command.
 *
 */

void NFmiPostgreSQL::Execute(const string& sql)
{
#ifdef DEBUG
	cout << "DEBUG: " << sql << endl;
#endif
	try
	{
		wrk_->exec(sql);
	}
	catch (const pqxx::unique_violation& e)
	{
		// let caller deal with this
		throw e;
	}
}

NFmiPostgreSQL::~NFmiPostgreSQL() { Disconnect(); }
void NFmiPostgreSQL::Disconnect()
{
	if (connected_)
	{
		db_->disconnect();
		connected_ = false;
	}
}

void NFmiPostgreSQL::Commit()
{
#ifdef DEBUG
	cout << "DEBUG: COMMIT" << endl;
#endif

	try
	{
		wrk_->commit();
	}
	catch (const pqxx::usage_error& e)
	{
	}

	wrk_ = unique_ptr<pqxx::nontransaction>(new pqxx::nontransaction(*db_));
}

void NFmiPostgreSQL::Rollback()
{
#ifdef DEBUG
	cout << "DEBUG: ROLLBACK" << endl;
#endif
	try
	{
		wrk_->abort();
	}
	catch (const pqxx::usage_error& e)
	{
	}

	// new transaction
	wrk_ = unique_ptr<pqxx::nontransaction>(new pqxx::nontransaction(*db_));
}

/*
 * MakeStandardDate()
 *
 * Format an OTL datetime to standard ISO format.
*/

/*string NFmiPostgreSQL::MakeStandardDate(const otl_datetime &time) {

    char date[20];
    sprintf(date, "%4d-%02d-%02d %02d:%02d:%02d", time.year, time.month, time.day, time.hour, time.minute, time.second);
    date[19] = '\0';

    return static_cast<string>(date);

}*/
