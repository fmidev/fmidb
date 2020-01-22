#pragma once

#if defined(__GNUC__) && (__GNUC__ >= 4) && defined(__GNUC_MINOR__) && __GNUC_MINOR__ >= 7
#define FINAL final
#define OVERRIDE override
#else
#define FINAL
#define OVERRIDE
#endif

#include <iostream>
#ifdef DEBUG
static const char* fmideb = getenv("FMIDB_DEBUG");
#endif

// clang-format off

#if defined(DEBUG)
#define FMIDEBUG(Expr) do { Expr; } while (0)
#else
#define FMIDEBUG(Expr) if (fmideb) do { Expr; } while (0)
#endif

// clang-format on

#include <sstream>
#include <string>
#include <vector>

class NFmiDatabase
{
   public:
	NFmiDatabase() : connected_(false), user_(""), password_(""), database_(""), connection_string_(""){};
	NFmiDatabase(const std::string& user, const std::string& password, const std::string& database)
	    : connected_(false), user_(user), password_(password), database_(database), connection_string_(""){};

	virtual ~NFmiDatabase(){};

	virtual void Connect() = 0;
	virtual void Disconnect() = 0;

	virtual void Query(const std::string& sql) = 0;
	virtual std::vector<std::string> FetchRow(void) = 0;

	virtual void Execute(const std::string& sql) = 0;

	virtual void Commit() = 0;
	virtual void Rollback() = 0;

	virtual std::string ClassName() const = 0;

   protected:
	bool connected_;

	std::string user_;
	std::string password_;
	std::string database_;

	std::string connection_string_;
};
