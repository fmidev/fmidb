#include "NFmiPostgreSQL.h"

#include <iostream>

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

	FMIDEBUG(cout << "DEBUG: dsn is 'user=" << user_ << " password=xxx host=" << hostname_ << " dbname=" << database_
			 << " port=" + to_string(port_) << "'" << endl);

	connection_string_ = "user=" + user_ + " password=" + password_ + " host=" + hostname_ + " dbname=" + database_ +
	                     " port=" + to_string(port_);
	db_ = unique_ptr<pqxx::connection>(new pqxx::connection(connection_string_));
	wrk_ = unique_ptr<pqxx::nontransaction>(new pqxx::nontransaction(*db_));
	connected_ = true;

	FMIDEBUG(cout << "DEBUG: connected to PostgreSQL " << database_ << endl);
}

void NFmiPostgreSQL::Query(const string& sql)
{
	if (!connected_)
	{
		cerr << "ERROR: must be connected before executing query" << endl;
		exit(1);
	}

	FMIDEBUG(cout << "DEBUG: " << sql << endl);

	res_ = wrk_->exec(sql);
	iter_ = res_.begin();

	FMIDEBUG(cout << "DEBUG: query returned " << res_.size() << " rows" << endl);
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
	FMIDEBUG(cout << "DEBUG: " << sql << endl);

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
#if PQXX_VERSION_MAJOR < 7
		db_->disconnect();
#else
		db_->close();
#endif
		connected_ = false;
	}
}

void NFmiPostgreSQL::Commit()
{
	FMIDEBUG(cout << "DEBUG: COMMIT" << endl);

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
	FMIDEBUG(cout << "DEBUG: ROLLBACK" << endl);

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
