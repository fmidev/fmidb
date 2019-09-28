#include "NFmiRadonDB.h"

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string_regex.hpp>
#include <iomanip>

using namespace std;

const float kFloatMissing = 32700.f;

once_flag paramGrib1Cache, paramGrib2Cache;

#pragma GCC diagnostic ignored "-Wwrite-strings"

NFmiRadonDB& NFmiRadonDB::Instance()
{
	static NFmiRadonDB instance_;
	return instance_;
}

NFmiRadonDB::NFmiRadonDB(short theId) : NFmiPostgreSQL(), itsId(theId)
{
}
NFmiRadonDB::~NFmiRadonDB()
{
	Disconnect();
}
void NFmiRadonDB::Connect()
{
	string password;
	const auto pw = getenv("RADON_RADONCLIENT_PASSWORD");
	if (pw)
	{
		password = string(pw);
	}
	else
	{
		throw runtime_error("Environment variable RADON_RADONCLIENT_PASSWORD must be set");
	}

	NFmiRadonDB::Connect("radon_client", password, "radon", "vorlon", 5432);
}

void NFmiRadonDB::Connect(const std::string& user, const std::string& password, const std::string& database,
                          const std::string& hostname, int port)
{
	assert(!user.empty());
	assert(!password.empty());
	assert(!database.empty());
	assert(!hostname.empty());
	NFmiPostgreSQL::Connect(user, password, database, hostname, port);
}

map<string, string> NFmiRadonDB::GetProducerFromGrib(long centre, long process, long type_id)
{
	const string key = to_string(centre) + "_" + to_string(process);

	if (gribproducerinfo.find(key) != gribproducerinfo.end())
	{
		FMIDEBUG(cout << "DEBUG: GetProducerFromGrib() cache hit!" << endl);

		return gribproducerinfo[key];
	}

	stringstream query;

	query << "SELECT f.id, f.name, f.class_id, f.type_id "
	      << "FROM fmi_producer f, producer_grib p, producer_type t "
	      << "WHERE f.id = p.producer_id AND f.type_id = t.id"
	      << " AND p.centre = " << centre << " AND p.ident = " << process << " AND t.id = " << type_id;

	Query(query.str());

	vector<string> row = FetchRow();

	map<string, string> ret;

	if (row.empty())
	{
		// gridparamid[key] = -1;
		FMIDEBUG(cout << "DEBUG Producer not found\n");
	}
	else
	{
		ret["id"] = row[0];
		ret["name"] = row[1];
		ret["class_id"] = row[2];
		ret["type_id"] = row[3];
		ret["centre"] = to_string(centre);
		ret["ident"] = to_string(process);

		gribproducerinfo[key] = ret;
	}

	return ret;
}

map<string, string> NFmiRadonDB::GetParameterFromNewbaseId(unsigned long producer_id, unsigned long universal_id)
{
	string key = to_string(producer_id) + "_" + to_string(universal_id);

	if (paramnewbaseinfo.find(key) != paramnewbaseinfo.end())
	{
		FMIDEBUG(cout << "DEBUG: GetParameterFromNewbaseId() cache hit!" << endl);

		return paramnewbaseinfo[key];
	}

	map<string, string> ret;

	string prod_id = to_string(producer_id);
	string univ_id = to_string(universal_id);

	stringstream query;

	query << "SELECT "
	      << "p.id, "
	      << "p.name, "
	      << "g.base, "
	      << "g.scale, "
	      << "g.univ_id "
	      << "FROM param_newbase g, param p "
	      << "WHERE "
	      << " p.id = g.param_id "
	      << " AND g.univ_id = " << univ_id << " AND g.producer_id = " << producer_id;

	Query(query.str());

	vector<string> row = FetchRow();

	if (row.empty())
		return ret;

	ret["id"] = row[0];
	ret["name"] = row[1];
	ret["parm_name"] = row[1];  // backwards compatibility
	ret["base"] = row[2];
	ret["scale"] = row[3];
	ret["univ_id"] = row[4];

	paramnewbaseinfo[key] = ret;

	return ret;
}

map<string, string> NFmiRadonDB::GetParameterFromDatabaseName(long producerId, const string& parameterName, int levelId,
                                                              double levelValue)
{
	string key = to_string(producerId) + "_" + parameterName + "_" + to_string(levelId) + "_" + to_string(levelValue);

	if (paramdbinfo.find(key) != paramdbinfo.end())
	{
		FMIDEBUG(cout << "DEBUG: GetParameterFromDatabaseName() cache hit!" << endl);

		return paramdbinfo[key];
	}

	stringstream query;

	map<string, string> ret;

	query << "SELECT "
	      // Database information
	      << "p.id, "
	      << "p.name, "
	      << "p.version, "
	      // Grib1 information
	      << "g1.table_version, "
	      << "g1.number, "
	      << "g1.timerange_indicator, "
	      // Producer specific grib2
	      << "g2.discipline, "
	      << "g2.category, "
	      << "g2.number, "
	      << "g2.type_of_statistical_processing, "
	      // WMO template grib2
	      << "g2t.discipline AS template_discipline, "
	      << "g2t.category AS template_category, "
	      << "g2t.number AS template_number, "
	      << "g2t.type_of_statistical_processing, "
	      // Newbase
	      << "n.univ_id, "
	      << "n.scale, "
	      << "n.base, "
	      // Precision
	      << "pp.precision "
	      // Rest of the SQL
	      << "FROM "
	      << "param p "
	      << "LEFT OUTER JOIN param_grib1_v g1 "
	      << "ON ( "
	      << "p.id = g1.param_id AND "
	      << "g1.producer_id = " << producerId << " AND "
	      << "(g1.level_id IS NULL OR g1.level_id = " << levelId << ") AND "
	      << "(g1.level_value IS NULL OR g1.level_value = " << levelValue << ")"
	      << ") "
	      << "LEFT OUTER JOIN param_grib2_v g2 "
	      << "ON ( "
	      << "g2.param_id = p.id AND "
	      << "g2.producer_id = " << producerId << " AND "
	      << "(g2.level_id IS NULL OR g2.level_id = " << levelId << ") AND "
	      << "(g2.level_value IS NULL OR g2.level_value = " << levelValue << ")"
	      << ") "
	      << "LEFT OUTER JOIN param_grib2_template g2t "
	      << "ON ( "
	      << "g2t.param_id = p.id"
	      << ") "
	      << "LEFT OUTER JOIN param_newbase n "
	      << "ON ("
	      << "p.id = n.param_id AND "
	      << "n.producer_id = " << producerId << ") "
	      << "LEFT OUTER JOIN param_precision pp "
	      << "ON (p.id = pp.param_id) "
	      << "WHERE "
	      << "p.name = '" << parameterName << "'"
	      << "ORDER BY "
	      << "g1.level_id NULLS LAST, g1.level_value NULLS LAST,"
	      << "g2.level_id NULLS LAST, g2.level_value NULLS LAST";
	;

	Query(query.str());
	auto row = FetchRow();

	if (row.empty())
	{
		paramdbinfo[key] = ret;
		return ret;
	}

	//  0 p.id
	//  1 p.name
	//  2 p.version
	//  3 g1.table_version
	//  4 g1.number
	//  5 g1.timerange_indicator
	//  6 g2.discipline
	//  7 g2.category
	//  8 g2.number
	//  9 g2.type_of_statistical_processing
	// 10 g2t.discipline AS template_discipline
	// 11 g2t.category AS template_category
	// 12 g2t.number AS template_number
	// 13 g2t.type_of_statistical_processing
	// 14 n.univ_id
	// 15 n.scale
	// 16 n.base
	// 17 pp.precision

	ret["id"] = row[0];
	ret["name"] = row[1];
	ret["version"] = row[2];
	ret["grib1_table_version"] = row[3];
	ret["grib1_number"] = row[4];
	ret["grib1_timerange_indicator"] = row[5];
	ret["grib2_discipline"] = row[6].empty() ? row[10] : row[6];
	ret["grib2_category"] = row[7].empty() ? row[11] : row[7];
	ret["grib2_number"] = row[8].empty() ? row[12] : row[8];
	ret["grib2_type_of_statistical_processing"] = row[9].empty() ? row[13] : row[9];
	ret["univ_id"] = row[14];
	ret["scale"] = row[15];
	ret["base"] = row[16];
	ret["precision"] = row[17];

	paramdbinfo[key] = ret;

	return ret;
}

void NFmiRadonDB::WarmGrib1ParameterCache(long producerId)
{
	// For loading of large files, it is beneficiary to pre-load all mapping data to memory.
	// This is a bit convoluted since in database we might not have level id / value set, but
	// that information is always used when GetParameterFromGrib1() is called.
	//
	// Therefore what we do is generate all possible values for those level mappings that don't
	// have type / value. We'll not really *all* possible, that would not be worth the effort.
	// We do generate the values for that data which is the most common: level 105 and level 109.
	//
	// For 105, we generate level value entries for 0, 2 and 10 meters. It doesn't really matter
	// if there are no parameters for all the generated entries.
	//
	// For 109, we check from producer_meta what are the hybrid levels for current producer and
	// generate a list of levels.

	call_once(paramGrib1Cache, [&]() {

		const auto firstHybridLevel = GetProducerMetaData(producerId, "first hybrid level number");
		const auto lastHybridLevel = GetProducerMetaData(producerId, "last hybrid level number");

		int first = 0, last = 0;

		if (!firstHybridLevel.empty() && !lastHybridLevel.empty())
		{
			first = stoi(firstHybridLevel);
			last = stoi(lastHybridLevel);
		}

		stringstream query;

		query << "SELECT "
		      << "p.id, p.name, p.version, p.interpolation_id, g.level_value,"
		      << "g.table_version, g.number, g.timerange_indicator, l.grib_level_id "
		      << "FROM param_grib1 g JOIN param p ON (g.param_id = p.id) "
		      << " LEFT OUTER JOIN level_grib1 l ON (g.level_id = l.level_id) "
		      << "WHERE "
		      << " g.producer_id = " << producerId << " AND (g.level_id IN (3,6) OR g.level_id IS NULL)"
		      << " GROUP BY 1,2,3,4,5,6,7,8,9 "
		      << " ORDER BY l.grib_level_id NULLS LAST, g.level_value NULLS LAST";

		Query(query.str());

		while (true)
		{
			const auto row = FetchRow();

			if (row.empty())
				break;

			const auto id = row[0];
			const auto name = row[1];
			const auto version = row[2];
			const auto interp = row[3];
			const auto level_value = row[4];
			const auto table = row[5];
			const auto number = row[6];
			const auto tri = row[7];
			const auto grib_level = row[8];

			map<string, string> ret;

			ret["id"] = id;
			ret["name"] = name;
			ret["version"] = version;
			ret["grib1_table_version"] = table;
			ret["grib1_number"] = number;
			ret["interpolation_method"] = interp;

			string keybase = to_string(producerId) + "_" + table + "_" + number + "_" + tri + "_";

			auto AddToCache = [&](int levelType) {
				vector<int> levels;

				if (levelType == 105)
				{
					levels = {0, 2, 10};
				}
				else if (levelType == 109)
				{
					levels.resize(last - first + 1);
					iota(levels.begin(), levels.end(), first);
				}

				for (const auto& i : levels)
				{
					const auto _key = keybase + to_string(levelType) + "_" + to_string(i);

					// We don't overwrite existing entries!
					if (paramgrib1info.find(_key) == paramgrib1info.end())
					{
						paramgrib1info[_key] = ret;
					}
				}
			};

			/// Level type and values set
			if (!grib_level.empty() && !level_value.empty())
			{
				const auto _key = keybase + grib_level + "_" + level_value;

				if (paramgrib1info.find(_key) == paramgrib1info.end())
				{
					paramgrib1info[_key] = ret;
				}
			}
			// Level type set, but level value is NULL
			else if (!grib_level.empty())
			{
				AddToCache(stoi(grib_level));
			}
			// Level type is NULL
			else
			{
				vector<int> levelTypes({105, 109});

				for (auto ltype : levelTypes)
				{
					AddToCache(ltype);
				}
			}
		}

		FMIDEBUG(cout << "DEBUG: Grib1ParameterCache warmed with " << paramgrib1info.size() << " entries" << endl);
	});
}

void NFmiRadonDB::WarmGrib2ParameterCache(long producerId)
{
	call_once(paramGrib2Cache, [&]() {

		const auto firstHybridLevel = GetProducerMetaData(producerId, "first hybrid level number");
		const auto lastHybridLevel = GetProducerMetaData(producerId, "last hybrid level number");

		int first = 0, last = 0;

		if (!firstHybridLevel.empty() && !lastHybridLevel.empty())
		{
			first = stoi(firstHybridLevel);
			last = stoi(lastHybridLevel);
		}

		stringstream query;

		query << "SELECT "
		      << "p.id, p.name, p.version, p.interpolation_id, g.level_value,"
		      << "g.discipline, g.category, g.number, l.grib_level_id, g.type_of_statistical_processing "
		      << "FROM param_grib2 g JOIN param p ON (g.param_id = p.id) "
		      << " LEFT OUTER JOIN level_grib2 l ON (g.level_id = l.level_id) "
		      << "WHERE "
		      << " g.producer_id = " << producerId << " AND (g.level_id IN (3,6) OR g.level_id IS NULL)"
		      << "UNION ALL SELECT "
		      << "p2.id, p2.name, p2.version, p2.interpolation_id, NULL::numeric, "
		      << "t.discipline, t.category, t.number, NULL::int, t.type_of_statistical_processing "
		      << "FROM param_grib2_template t JOIN param p2 ON (t.param_id = p2.id) "
		      << " GROUP BY 1,2,3,4,5,6,7,8,9,10";

		Query(query.str());

		while (true)
		{
			const auto row = FetchRow();

			if (row.empty())
				break;

			const auto id = row[0];
			const auto name = row[1];
			const auto version = row[2];
			const auto interp = row[3];
			const auto level_value = row[4];
			const auto discipline = row[5];
			const auto category = row[6];
			const auto number = row[7];
			const auto grib_level = row[8];
			const auto type_of_statistical_processing = row[9];

			map<string, string> ret;

			ret["id"] = id;
			ret["name"] = name;
			ret["version"] = version;
			ret["grib2_discipline"] = discipline;
			ret["grib2_category"] = category;
			ret["grib2_number"] = number;
			ret["interpolation_method"] = interp;
			ret["type_of_statistical_processing"] = type_of_statistical_processing;

			string keybase = to_string(producerId) + "_" + discipline + "_" + category + "_" + number + "_" +
			                 type_of_statistical_processing + "_";

			auto AddToCache = [&](int levelType) {
				vector<double> levels;

				if (levelType == 103)
				{
					levels = {0, 2, 10};
				}
				else if (levelType == 105)
				{
					levels.resize(last - first + 1);
					iota(levels.begin(), levels.end(), first);
				}

				for (const auto& i : levels)
				{
					const auto _key = keybase + to_string(levelType) + "_" + to_string(i);
					// We don't overwrite existing entries!
					if (paramgrib2info.find(_key) == paramgrib2info.end())
					{
						paramgrib2info[_key] = ret;
					}
				}
			};

			/// Level type and values set
			if (!grib_level.empty() && !level_value.empty())
			{
				const auto _key = keybase + grib_level + "_" + level_value;

				if (paramgrib2info.find(_key) == paramgrib2info.end())
				{
					paramgrib2info[_key] = ret;
				}
			}
			// Level type set, but level value is NULL
			else if (!grib_level.empty())
			{
				AddToCache(stoi(grib_level));
			}
			// Level type is NULL
			else
			{
				vector<int> levelTypes({103, 105});

				for (auto ltype : levelTypes)
				{
					AddToCache(ltype);
				}
			}
		}

		FMIDEBUG(cout << "DEBUG: Grib2ParameterCache warmed with " << paramgrib2info.size() << " entries" << endl);
	});
}

map<string, string> NFmiRadonDB::GetParameterFromGrib1(long producerId, long tableVersion, long paramId,
                                                       long timeRangeIndicator, long levelId, double levelValue)
{
	string key = to_string(producerId) + "_" + to_string(tableVersion) + "_" + to_string(paramId) + "_" +
	             to_string(timeRangeIndicator) + "_" + to_string(levelId) + "_" +
	             to_string(static_cast<int>(levelValue));

	if (paramgrib1info.find(key) != paramgrib1info.end())
	{
		FMIDEBUG(cout << "DEBUG: ParameterFromGrib1() cache hit!" << endl);

		return paramgrib1info[key];
	}

	stringstream query;

	query << "SELECT p.id, p.name, p.version, p.interpolation_id "
	      << "FROM param_grib1 g, level_grib1 l, param p "
	      << "WHERE g.param_id = p.id"
	      << " AND g.producer_id = " << producerId << " AND table_version = " << tableVersion
	      << " AND number = " << paramId << " AND timerange_indicator = " << timeRangeIndicator
	      << " AND (g.level_id IS NULL OR (g.level_id = l.level_id AND l.grib_level_id = " << levelId << "))"
	      << " AND (level_value IS NULL OR level_value = " << levelValue << ")"
	      << " ORDER BY g.level_id NULLS LAST, level_value NULLS LAST LIMIT 1";

	Query(query.str());

	vector<string> row = FetchRow();

	map<string, string> ret;

	if (row.empty())
	{
		FMIDEBUG(cout << "DEBUG Parameter not found\n");

		if (levelId == 109)
		{
			// Ok, so if this is a check for hybrid level and we found no metadata, we might as well fill
			// the cache with empty data for all hybrid levels since PROBABLY we are next gonna try to load
			// the next hybrid level

			const auto firstHybridLevel = GetProducerMetaData(producerId, "first hybrid level number");
			const auto lastHybridLevel = GetProducerMetaData(producerId, "last hybrid level number");

			int first = 0, last = 0;

			if (!firstHybridLevel.empty() && !lastHybridLevel.empty())
			{
				first = stoi(firstHybridLevel);
				last = stoi(lastHybridLevel);

				vector<int> levels(last - first + 1);
				iota(levels.begin(), levels.end(), first);

				string keybase = to_string(producerId) + "_" + to_string(tableVersion) + "_" + to_string(paramId) +
				                 "_" + to_string(timeRangeIndicator) + "_" + to_string(levelId) + "_";

				for (const auto& i : levels)
				{
					const auto _key = keybase + to_string(i);

					// We don't overwrite existing entries!
					if (paramgrib1info.find(_key) == paramgrib1info.end())
					{
						paramgrib1info[_key] = ret;
					}
				}
			}
		}
	}
	else
	{
		ret["id"] = row[0];
		ret["name"] = row[1];
		ret["version"] = row[2];
		ret["grib1_table_version"] = to_string(tableVersion);
		ret["grib1_number"] = to_string(paramId);
		ret["interpolation_method"] = row[3];
	}

	paramgrib1info[key] = ret;

	return ret;
}

map<string, string> NFmiRadonDB::GetParameterFromGrib2(long producerId, long discipline, long category, long paramId,
                                                       long levelId, double levelValue,
                                                       long typeOfStatisticalProcessing)
{
	string key = to_string(producerId) + "_" + to_string(discipline) + "_" + to_string(category) + "_" +
	             to_string(paramId) + "_" + to_string(typeOfStatisticalProcessing) + "_" + to_string(levelId) + "_" +
	             to_string(levelValue);

	if (paramgrib2info.find(key) != paramgrib2info.end())
	{
		FMIDEBUG(cout << "DEBUG: ParameterFromGrib2() cache hit for " << key << endl);

		return paramgrib2info[key];
	}

	stringstream query;

	query << "SELECT p.id, p.name, p.version, u.name AS unit_name, "
	         "p.interpolation_id, i.name AS interpolation_name, "
	         "g.level_id "
	      << "FROM param_grib2 g, level_grib2 l, param p, param_unit u, interpolation_method i, "
	         "fmi_producer f "
	      << "WHERE g.param_id = p.id AND p.unit_id = u.id AND "
	         "p.interpolation_id = i.id AND f.id = g.producer_id "
	      << " AND f.id = " << producerId << " AND discipline = " << discipline << " AND category = " << category
	      << " AND number = " << paramId
	      << " AND (g.level_id IS NULL OR (g.level_id = l.level_id AND l.grib_level_id = " << levelId << "))"
	      << " AND (level_value IS NULL OR level_value = " << levelValue << ")"
	      << " AND g.type_of_statistical_processing = " << typeOfStatisticalProcessing
	      << " ORDER BY g.level_id NULLS LAST, level_value NULLS LAST LIMIT 1";

	Query(query.str());

	vector<string> row = FetchRow();

	map<string, string> ret;

	if (row.empty())
	{
		query.str("");
		query << "SELECT p.id, p.name, p.version, p.interpolation_id, "
		      << "NULL, NULL FROM param p, param_grib2_template t WHERE "
		      << "p.id = t.param_id AND t.discipline = " << discipline << " AND t.category = " << category << " AND "
		      << "t.number = " << paramId << " AND t.type_of_statistical_processing = " << typeOfStatisticalProcessing;

		Query(query.str());
		row = FetchRow();

		if (row.empty())
		{
			FMIDEBUG(cout << "DEBUG Parameter not found\n");

			return ret;
		}
	}

	ret["id"] = row[0];
	ret["name"] = row[1];
	ret["version"] = row[2];
	ret["grib2_discipline"] = to_string(discipline);
	ret["grib2_category"] = to_string(category);
	ret["grib2_number"] = to_string(paramId);
	ret["interpolation_method"] = row[4];
	ret["level_id"] = row[5];
	ret["level_value"] = row[6];
	ret["type_of_statistical_processing"] = to_string(typeOfStatisticalProcessing);

	paramgrib2info[key] = ret;

	return ret;
}

map<string, string> NFmiRadonDB::GetParameterFromNetCDF(long producerId, const string& paramName, long levelId,
                                                        double levelValue)
{
	const string key = to_string(producerId) + "_" + paramName + "_" + to_string(levelId) + "_" + to_string(levelValue);

	if (paramnetcdfinfo.find(key) != paramnetcdfinfo.end())
	{
		FMIDEBUG(cout << "DEBUG: GetParameterFromNetCDF() cache hit!" << endl);

		return paramnetcdfinfo[key];
	}

	stringstream query;

	query << "SELECT p.id, p.name, p.version, u.name AS unit_name, "
	         "p.interpolation_id, i.name AS interpolation_name, "
	         "g.level_id, g.level_value "
	      << "FROM param_netcdf g, param p, param_unit u, interpolation_method "
	         "i, fmi_producer f "
	      << "WHERE g.param_id = p.id AND p.unit_id = u.id AND "
	         "p.interpolation_id = i.id AND f.id = g.producer_id "
	      << " AND f.id = " << producerId << " AND g.netcdf_name = '" << paramName << "'"
	      << " AND (level_id IS NULL OR level_id = " << levelId << ")"
	      << " AND (level_value IS NULL OR level_value = " << levelValue << ")"
	      << " ORDER BY level_id NULLS LAST, level_value NULLS LAST LIMIT 1";

	Query(query.str());

	vector<string> row = FetchRow();

	map<string, string> ret;

	if (row.empty())
	{
		FMIDEBUG(cout << "DEBUG Parameter not found\n");
	}
	else
	{
		ret["id"] = row[0];
		ret["name"] = row[1];
		ret["version"] = row[2];
		ret["netcdf_name"] = paramName;
		ret["interpolation_method"] = row[4];
		ret["level_id"] = row[5];
		ret["level_value"] = row[6];

		paramnetcdfinfo[key] = ret;
	}

	return ret;
}

map<string, string> NFmiRadonDB::GetParameterPrecision(const std::string& paramName)
{
	map<string, string> ret;

	stringstream query;
	query << "SELECT pp.id, pp.precision FROM param_precision pp, param p  WHERE p.name = '" << paramName
	      << "' AND p.id = pp.param_id";

	Query(query.str());

	vector<string> row = FetchRow();

	if (row.empty())
	{
		FMIDEBUG(cout << "DEBUG Parameter " << paramName << " not found\n");
	}
	else
	{
		ret["id"] = row[0];
		ret["precision"] = row[1];
	}

	return ret;
}

string NFmiRadonDB::GetNewbaseNameFromUnivId(unsigned long univ_id)
{
	string ret;
	stringstream q;

	q << "SELECT p.name FROM param_newbase_name p WHERE p.univ_id = " << univ_id;
	Query(q.str());

	vector<string> row = FetchRow();
	if (row.empty())
	{
		FMIDEBUG(cout << "DEBUG Parameter not found: " << univ_id << "\n");
	}
	else
	{
		ret = row[0];
	}
	return ret;
}

map<string, string> NFmiRadonDB::GetLevelFromDatabaseName(const std::string& name)
{
	if (levelnameinfo.find(name) != levelnameinfo.end())
	{
		FMIDEBUG(cout << "DEBUG: GetLevelFromDatabaseName() cache hit for " << name << endl);

		return levelnameinfo[name];
	}

	stringstream query;

	query << "SELECT id, name "
	      << "FROM level "
	      << " WHERE upper('" << name << "') = name ";

	Query(query.str());

	vector<string> row = FetchRow();

	map<string, string> ret;

	if (row.empty())
	{
		FMIDEBUG(cout << "DEBUG Level not found\n");
	}
	else
	{
		ret["id"] = row[0];
		ret["name"] = row[1];

		levelnameinfo[name] = ret;
	}

	return ret;
}

map<string, string> NFmiRadonDB::GetLevelFromGrib(long producerId, long levelNumber, long edition)
{
	const string key = to_string(producerId) + "_" + to_string(levelNumber) + "_" + to_string(edition);

	if (levelinfo.find(key) != levelinfo.end())
	{
		FMIDEBUG(cout << "DEBUG: GetLevelFromGrib() cache hit for " << key << endl);

		return levelinfo[key];
	}

	stringstream query;

	query << "SELECT id, name "
	      << "FROM level l, " << (edition == 2 ? "level_grib2 g " : "level_grib1 g") << " WHERE l.id = g.level_id "
	      << " AND g.producer_id = " << producerId << " AND g.grib_level_id = " << levelNumber;

	Query(query.str());

	vector<string> row = FetchRow();

	map<string, string> ret;

	if (row.empty())
	{
		FMIDEBUG(cout << "DEBUG Level not found\n");
	}
	else
	{
		ret["id"] = row[0];
		ret["name"] = row[1];
		ret["grib1Number"] = to_string(levelNumber);

		levelinfo[key] = ret;
	}

	return ret;
}

vector<vector<string>> NFmiRadonDB::GetGridGeoms(const string& ref_prod, const string& analtime,
                                                 const string& geom_name)
{
	const string key = ref_prod + "_" + analtime + "_" + geom_name;
	if (gridgeoms.count(key) > 0)
	{
		FMIDEBUG(cout << "DEBUG: GetGridGeoms() cache hit!" << endl);

		return gridgeoms[key];
	}

	stringstream query;

	query << "SELECT g.geometry_id, a.table_name, a.id, "
	         "g.geom_name, a.schema_name, a.partition_name"
	      << " FROM as_grid_v a, fmi_producer f, geom_v g"
	      << " WHERE a.record_count > 0"
	      << " AND f.name = '" << ref_prod << "'"
	      << " AND a.producer_id = f.id"
	      << " AND (min_analysis_time, max_analysis_time) OVERLAPS ('" << analtime << "', '" << analtime << "')"
	      << " AND a.geometry_name = g.geom_name";

	if (!geom_name.empty())
	{
		query << " AND g.geom_name = '" << geom_name << "'";
	}

	Query(query.str());

	vector<vector<string>> ret;

	while (true)
	{
		vector<string> values = FetchRow();

		if (values.empty())
		{
			break;
		}

		ret.push_back(values);
	}

	gridgeoms[key] = ret;

	return ret;
}

map<string, string> NFmiRadonDB::GetGeometryDefinition(const string& geom_name)
{
	if (geometryinfo.find(geom_name) != geometryinfo.end())
	{
		FMIDEBUG(cout << "DEBUG: GetGeometryDefinition() cache hit!" << endl);

		return geometryinfo[geom_name];
	}

	// find geom name corresponding id

	stringstream query;

	query.str("");

	// First get the grid type, so we know from which table to fetch detailed
	// information

	query << "SELECT id, name, projection_id FROM geom WHERE name = '" << geom_name << "'";

	Query(query.str());

	vector<string> row = FetchRow();

	if (row.empty())
	{
		return map<string, string>();
	}

	map<string, string> ret;

	ret["id"] = row[0];
	ret["name"] = row[1];
	ret["grid_type_id"] = row[2];

	int grid_type_id = std::stoi(row[2]);

	query.str("");

	switch (grid_type_id)
	{
		case 1:
		{
			query << "SELECT ni, nj, first_lat, first_lon, di, dj, scanning_mode FROM "
			         "geom_latitude_longitude_v WHERE geometry_id = "
			      << row[0];
			Query(query.str());
			row = FetchRow();

			if (row.empty())
				return map<string, string>();

			ret["ni"] = row[0];
			ret["nj"] = row[1];
			ret["first_point_lat"] = row[2];
			ret["first_point_lon"] = row[3];
			ret["di"] = row[4];
			ret["dj"] = row[5];
			ret["scanning_mode"] = row[6];
			ret["col_cnt"] = ret["ni"];               // neons
			ret["row_cnt"] = ret["nj"];               // neons
			ret["pas_longitude"] = ret["di"];         // neons
			ret["pas_latitude"] = ret["dj"];          // neons
			ret["stor_desc"] = ret["scanning_mode"];  // neons
			ret["geom_parm_1"] = "0";
			ret["geom_parm_2"] = "0";
			ret["geom_parm_3"] = "0";
			ret["lat_orig"] = ret["first_point_lat"];   // neons
			ret["long_orig"] = ret["first_point_lon"];  // neons

			geometryinfo[geom_name] = ret;

			return ret;
		}

		case 2:
		{
			query << "SELECT ni, nj, first_lat, first_lon, di, dj, scanning_mode, "
			         "orientation FROM geom_stereographic_v WHERE "
			         "geometry_id = "
			      << row[0];
			Query(query.str());
			row = FetchRow();

			if (row.empty())
				return map<string, string>();

			ret["ni"] = row[0];
			ret["nj"] = row[1];
			ret["first_point_lat"] = row[2];
			ret["first_point_lon"] = row[3];
			ret["di"] = row[4];
			ret["dj"] = row[5];
			ret["scanning_mode"] = row[6];
			ret["col_cnt"] = ret["ni"];               // neons
			ret["row_cnt"] = ret["nj"];               // neons
			ret["pas_longitude"] = ret["di"];         // neons
			ret["pas_latitude"] = ret["dj"];          // neons
			ret["stor_desc"] = ret["scanning_mode"];  // neons
			ret["geom_parm_1"] = row[7];
			ret["geom_parm_2"] = "0";
			ret["geom_parm_3"] = "0";
			ret["lat_orig"] = ret["first_point_lat"];   // neons
			ret["long_orig"] = ret["first_point_lon"];  // neons

			geometryinfo[geom_name] = ret;

			return ret;
		}
		case 5:
			query << "SELECT ni,nj, first_lat, first_lon, "
			         "di, dj, scanning_mode, orientation, latin1, latin2, "
			         "south_pole_lat, south_pole_lon FROM "
			         "geom_lambert_conformal_v WHERE geometry_id = "
			      << row[0];

			Query(query.str());
			row = FetchRow();

			if (row.empty())
				return map<string, string>();

			ret["ni"] = row[0];
			ret["nj"] = row[1];
			ret["first_point_lat"] = row[2];
			ret["first_point_lon"] = row[3];
			ret["di"] = row[4];
			ret["dj"] = row[5];
			ret["scanning_mode"] = row[6];
			ret["orientation"] = row[7];
			ret["latin1"] = row[8];
			ret["latin2"] = row[9];
			ret["south_pole_lat"] = row[10];
			ret["south_pole_lon"] = row[11];

			geometryinfo[geom_name] = ret;

			return ret;

		case 4:
		{
			query << "SELECT ni, nj, first_lat, first_lon, di, dj, scanning_mode, "
			         "south_pole_lat, south_pole_lon FROM geom_rotated_latitude_longitude_v "
			         "WHERE geometry_id = "
			      << row[0];
			Query(query.str());
			row = FetchRow();

			if (row.empty())
				return map<string, string>();

			ret["ni"] = row[0];
			ret["nj"] = row[1];
			ret["first_point_lat"] = row[2];
			ret["first_point_lon"] = row[3];
			ret["di"] = row[4];
			ret["dj"] = row[5];
			ret["scanning_mode"] = row[6];
			ret["col_cnt"] = ret["ni"];               // neons
			ret["row_cnt"] = ret["nj"];               // neons
			ret["pas_longitude"] = ret["di"];         // neons
			ret["pas_latitude"] = ret["dj"];          // neons
			ret["stor_desc"] = ret["scanning_mode"];  // neons
			ret["geom_parm_1"] = row[7];
			ret["geom_parm_2"] = row[8];
			ret["geom_parm_3"] = "0";
			ret["lat_orig"] = ret["first_point_lat"];   // neons
			ret["long_orig"] = ret["first_point_lon"];  // neons

			geometryinfo[geom_name] = ret;

			return ret;
		}
		case 6:
		{
			query << "SELECT nj, first_lat, first_lon, last_lat, last_lon,"
			         "n, scanning_mode, points_along_parallels FROM "
			         "geom_reduced_gaussian_v WHERE geometry_id = "
			      << row[0];
			Query(query.str());
			row = FetchRow();

			if (row.empty())
				return map<string, string>();

			ret["nj"] = row[0];
			ret["first_point_lat"] = row[1];
			ret["first_point_lon"] = row[2];
			ret["last_point_lat"] = row[3];
			ret["last_point_lon"] = row[4];
			ret["n"] = row[5];
			ret["scanning_mode"] = row[6];
			ret["longitudes_along_parallels"] = boost::trim_copy_if(row[7], boost::is_any_of("{}"));

			geometryinfo[geom_name] = ret;

			return ret;
		}
	}

	return map<string, string>();
}

map<string, string> NFmiRadonDB::GetGeometryDefinition(size_t ni, size_t nj, double lat, double lon, double di,
                                                       double dj, int gribedition, int gridtype)
{
	string key = to_string(ni) + "_" + to_string(nj) + "_" + to_string(lat) + "_" + to_string(lon) + "_" +
	             to_string(di) + "_" + to_string(dj) + "_" + to_string(gribedition) + "_" + to_string(gridtype);

	if (geometryinfo_fromarea.find(key) != geometryinfo_fromarea.end())
	{
		FMIDEBUG(cout << "DEBUG: GetGeometryDefinition() cache hit!" << endl);

		return geometryinfo_fromarea[key];
	}

	stringstream query;

	query << "SELECT id FROM projection WHERE grib" << gribedition << "_number = " << gridtype;

	Query(query.str());

	auto row = FetchRow();

	map<string, string> ret;

	if (row.empty())
	{
		return ret;
	}

	query.str("");

	int projection_id = stoi(row[0]);

	// TODO: for projections other than latlon, extra properties should be checked,
	// such as south pole, orientation etc.

	switch (projection_id)
	{
		case 1:
			query << "SELECT geometry_id, geometry_name FROM geom_latitude_longitude_v "
			      << "WHERE nj = " << nj << " AND ni = " << ni << " AND first_lon = " << setprecision(10) << lon
			      << " AND first_lat = " << lat << " AND di = " << di << " AND dj = " << dj;
			break;
		case 2:
			query << "SELECT geometry_id, geometry_name FROM geom_stereographic_v "
			      << "WHERE nj = " << nj << " AND ni = " << ni << " AND first_lon = " << setprecision(10) << lon
			      << " AND first_lat = " << lat << " AND di = " << di << " AND dj = " << dj;
			break;
		case 4:
			query << "SELECT geometry_id, geometry_name FROM geom_rotated_latitude_longitude_v "
			      << "WHERE nj = " << nj << " AND ni = " << ni << " AND first_lon = " << setprecision(10) << lon
			      << " AND first_lat = " << lat << " AND di = " << di << " AND dj = " << dj;
			break;
		case 5:
			query << "SELECT geometry_id, geometry_name FROM geom_lambert_conformal_v "
			      << "WHERE nj = " << nj << " AND ni = " << ni << " AND first_lon = " << setprecision(10) << lon
			      << " AND first_lat = " << lat << " AND di = " << di << " AND dj = " << dj;
			break;
		case 6:
			query << "SELECT geometry_id, geometry_name FROM geom_reduced_gaussian_v "
			      << "WHERE nj = " << nj << " AND ni = " << ni << " AND first_lon = " << setprecision(10) << lon
			      << " AND first_lat = " << lat << " AND di = " << di << " AND dj = " << dj;
			break;

		default:
			throw std::runtime_error("Unsupported database projection id: " + row[0]);
	}

	Query(query.str());

	row = FetchRow();

	if (!row.empty())
	{
		ret["id"] = row[0];
		ret["name"] = row[1];

		geometryinfo_fromarea[key] = ret;
	}

	return ret;
}

map<string, string> NFmiRadonDB::GetProducerDefinition(unsigned long producer_id)
{
	if (producerinfo.count(producer_id) > 0)
	{
		FMIDEBUG(cout << "DEBUG: GetProducerDefinition() cache hit!" << endl);

		return producerinfo[producer_id];
	}

	stringstream query;

	query << "SELECT f.id, f.name, f.class_id, g.centre, g.ident "
	      << "FROM fmi_producer f "
	      << "LEFT OUTER JOIN producer_grib g ON (f.id = g.producer_id) "
	      << "WHERE f.id = " << producer_id;

	map<string, string> ret;

	Query(query.str());

	vector<string> row = FetchRow();

	if (!row.empty())
	{
		ret["producer_id"] = row[0];
		ret["ref_prod"] = row[1];
		ret["producer_class"] = row[2];
		ret["model_id"] = row[4];
		ret["ident_id"] = row[3];

		producerinfo[producer_id] = ret;
	}

	return ret;
}

/*
 * GetProducerDefinition(string)
 *
 * Retrieves producer definition from radon meta-tables.
 *
 *
 */

map<string, string> NFmiRadonDB::GetProducerDefinition(const string& producer_name)
{
	stringstream query;

	query << "SELECT id "
	      << "FROM fmi_producer"
	      << " WHERE name = '" << producer_name << "'";

	Query(query.str());

	vector<string> row = FetchRow();

	if (!row.empty())
	{
		return GetProducerDefinition(std::stoul(row[0]));
	}

	map<string, string> empty;
	return empty;
}

string NFmiRadonDB::GetLatestTime(const std::string& ref_prod, const std::string& geom_name, unsigned int offset)
{
	auto prod = GetProducerDefinition(ref_prod);
	if (prod.empty())
		return "";

	return GetLatestTime(std::stoi(prod["producer_id"]), geom_name, offset);
}

string NFmiRadonDB::GetLatestTime(int producer_id, const std::string& geom_name, unsigned int offset)
{
	// First check if we have grid or previ producer

	auto prod = GetProducerDefinition(producer_id);

	if (prod.empty())
	{
		return "";
	}

	string asTableName = "as_grid_v";
	string partitioningType;

	stringstream query;

	if (prod["producer_class"] == "3")
	{
		asTableName = "as_previ_v";
		partitioningType = "ANALYSISTIME";
	}
	else
	{
		// A bit of a hack: we assume that a single producer has only single type of partitioning
		// period defined. That is the case as of now, but nothing is stopping from someone to
		// do otherwise.

		query << "SELECT distinct partitioning_period FROM table_meta_grid WHERE producer_id = " << producer_id;

		Query(query.str());
		auto row = FetchRow();

		assert(!row.empty());
		partitioningType = row[0];
	}

	query.str("");

	if (partitioningType == "ANALYSISTIME")
	{
		query << "SELECT min_analysis_time::timestamp, max_analysis_time::timestamp, partition_name "
		      << "FROM " << asTableName << " WHERE producer_id = " << producer_id << " AND record_count > 0 ";

		if (!geom_name.empty())
		{
			query << " AND geometry_name = '" << geom_name << "'";
		}

		query << " GROUP BY min_analysis_time, max_analysis_time, partition_name "
		      << " ORDER BY max_analysis_time DESC LIMIT 1 OFFSET " << offset;

		Query(query.str());

		auto row = FetchRow();

		if (row.size() == 0)
		{
			return "";
		}

		assert(row[0] == row[1]);
		return row[0];
	}
	else
	{
		// With PG11 we could just do SELECT max(analysis_time) FROM grid_tablename

		query << "SELECT distinct table_name FROM " << asTableName << " WHERE producer_id = " << producer_id
		      << " AND record_count > 0";

		if (!geom_name.empty())
		{
			query << " AND geometry_name = '" << geom_name << "'";
		}

		Query(query.str());
		auto row = FetchRow();

		if (row.empty())
		{
			return "";
		}

		query.str("");

		query << "SELECT distinct analysis_time::timestamp FROM " << row[0]
		      << " ORDER BY analysis_time DESC LIMIT 1 OFFSET " << offset;

		Query(query.str());

		row = FetchRow();

		if (row.empty())
		{
			return "";
		}

		return row[0];
	}
}

map<string, string> NFmiRadonDB::GetStationDefinition(FmiRadonStationNetwork networkType, unsigned long stationId,
                                                      bool aggressive_cache)
{
	string key = to_string(static_cast<int>(networkType)) + "_" + to_string(stationId);

	if (stationinfo.find(key) != stationinfo.end())
		return stationinfo[key];

	stringstream query;

	query << "SELECT s.id,"
	      << " s.name,"
	      << " st_x(s.position) as longitude,"
	      << " st_y(s.position) as latitude,"
	      << " s.elevation,"
	      << " wmo.local_station_id as wmoid,"
	      << " icao.local_station_id as icaoid,"
	      << " lpnn.local_station_id as lpnn,"
	      << " rw.local_station_id as road_weather_id, "
	      << " fs.local_station_id as fmisid "
	      << "FROM station s "
	      << "LEFT OUTER JOIN station_network_mapping wmo ON (s.id = "
	         "wmo.station_id AND wmo.network_id = 1) "
	      << "LEFT OUTER JOIN station_network_mapping icao ON (s.id = "
	         "icao.station_id AND icao.network_id = 2) "
	      << "LEFT OUTER JOIN station_network_mapping lpnn ON (s.id = "
	         "lpnn.station_id AND lpnn.network_id = 3) "
	      << "LEFT OUTER JOIN station_network_mapping rw ON (s.id = "
	         "rw.station_id AND rw.network_id = 4) "
	      << "LEFT OUTER JOIN station_network_mapping fs ON (s.id = "
	         "fs.station_id AND fs.network_id = 5) "
	      << "JOIN station_network_mapping m ON (s.id = m.station_id AND m.network_id = "
	      << static_cast<int>(networkType);

	if (!aggressive_cache)
	{
		query << " AND m.local_station_id = '" << stationId << "')";
	}
	else
	{
		query << ")";
	}

	Query(query.str());

	while (true)
	{
		auto row = FetchRow();

		if (row.empty())
		{
			break;
		}

		map<string, string> stat;

		stat["id"] = row[0];
		stat["station_name"] = row[1];
		stat["longitude"] = row[2];
		stat["latitude"] = row[3];
		stat["altitude"] = row[4];
		stat["wmoid"] = row[5];
		stat["icaoid"] = row[6];
		stat["lpnn"] = row[7];
		stat["rwid"] = row[8];
		stat["fmisid"] = row[9];

		string localId;

		switch (networkType)
		{
			default:
			case kWMONetwork:
				localId = stat["wmoid"];
				break;
			case kICAONetwork:
				localId = stat["icaoid"];
				break;
			case kLPNNNetwork:
				localId = stat["lpnn"];
				break;
			case kRoadWeatherNetwork:
				localId = stat["rwid"];
				break;
			case kFmiSIDNetwork:
				localId = stat["fmisid"];
				break;
		}

		string key = to_string(static_cast<int>(networkType)) + "_" + localId;
		stationinfo[key] = stat;
	}

	if (stationinfo.find(key) != stationinfo.end())
		return stationinfo[key];

	return map<string, string>();
}

std::map<string, string> NFmiRadonDB::GetLevelTransform(long producer_id, long param_id, long fmi_level_id,
                                                        double fmi_level_value)
{
	string key = to_string(producer_id) + "_" + to_string(param_id) + "_" + to_string(fmi_level_id) + "_" +
	             to_string(fmi_level_value);
	stringstream ss;

	if (leveltransforminfo.find(key) != leveltransforminfo.end())
	{
		FMIDEBUG(cout << "DEBUG: GetLevelTransform() cache hit!" << endl);

		return leveltransforminfo[key];
	}

	ss << "SELECT x.other_level_id, l.name AS other_level_name, "
	   << "x.other_level_value "
	   << "FROM param_level_transform x, level l "
	   << "WHERE "
	   << "x.other_level_id = l.id AND "
	   << "x.producer_id = " << producer_id << " AND "
	   << "x.param_id = " << param_id << " AND "
	   << "x.fmi_level_id = " << fmi_level_id << " AND "
	   << "(x.fmi_level_value IS NULL OR x.fmi_level_value = " << fmi_level_value << ") "
	   << "ORDER BY other_level_id, other_level_value NULLS LAST";

	Query(ss.str());

	auto row = FetchRow();

	map<string, string> ret;

	if (row.empty())
	{
		leveltransforminfo[key] = ret;
		return ret;
	}

	ret["id"] = row[0];
	ret["name"] = row[1];
	ret["value"] = row[2];

	leveltransforminfo[key] = ret;

	return ret;
}

std::string NFmiRadonDB::GetProducerMetaData(long producer_id, const string& attribute)
{
	string key = to_string(producer_id) + "_" + attribute;

	if (producermetadatainfo.find(key) != producermetadatainfo.end())
	{
		FMIDEBUG(cout << "DEBUG: GetProducerMetaData() cache hit!" << endl);

		return producermetadatainfo[key];
	}

	string query = "SELECT value FROM producer_meta WHERE producer_id = " + to_string(producer_id) +
	               " AND attribute = '" + attribute + "'";

	Query(query);

	auto row = FetchRow();

	if (row.empty())
	{
		return "";
	}

	producermetadatainfo[key] = row[0];
	return row[0];
}

map<string, string> NFmiRadonDB::GetTableName(long producerId, const string& analysisTime, const string& geomName)
{
	const string key = to_string(producerId) + "_" + analysisTime + "_" + geomName;

	if (tablenameinfo.find(key) != tablenameinfo.end())
	{
		FMIDEBUG(cout << "DEBUG: GetTableName() cache hit!" << endl);
		return tablenameinfo[key];
	}

	stringstream ss;

	ss << "SELECT "
	   << "id, schema_name, table_name, partition_name, record_count "
	   << "FROM as_grid_v "
	   << "WHERE geometry_name = '" << geomName << "'"
	   << " AND (min_analysis_time, max_analysis_time) OVERLAPS ('" << analysisTime << "'"
	   << ", '" << analysisTime << "')"
	   << " AND producer_id = " << producerId;

	Query(ss.str());

	const auto row = FetchRow();

	map<string, string> ret;

	if (row.empty())
	{
		return ret;
	}

	ret["id"] = row[0];
	ret["schema_name"] = row[1];
	ret["table_name"] = row[2];
	ret["partition_name"] = row[3];
	ret["record_count"] = row[4];

	tablenameinfo[key] = ret;
	return ret;
}

double NFmiRadonDB::GetProbabilityLimitForStation(long stationId, const std::string& paramName)
{
	std::stringstream ss;
	ss << "SELECT probability_limit FROM station_probability_limit_v WHERE station_id = " << stationId
	   << " AND param_name = '" << paramName << "'";

	Query(ss.str());

	auto row = FetchRow();

	if (row.empty())
	{
		return kFloatMissing;
	}

	return stod(row[0]);
}

NFmiRadonDBPool* NFmiRadonDBPool::itsInstance = NULL;

NFmiRadonDBPool* NFmiRadonDBPool::Instance()
{
	if (!itsInstance)
	{
		itsInstance = new NFmiRadonDBPool();
	}

	return itsInstance;
}

NFmiRadonDBPool::NFmiRadonDBPool()
    : itsMaxWorkers(2),
      itsWorkingList(itsMaxWorkers, -1),
      itsWorkerList(itsMaxWorkers, NULL),
      itsUsername(""),
      itsPassword(""),
      itsDatabase(""),
      itsHostname(""),
      itsPort(5432)
{
}

NFmiRadonDBPool::~NFmiRadonDBPool()
{
	for (unsigned int i = 0; i < itsWorkerList.size(); i++)
	{
		if (itsWorkerList[i])
		{
			itsWorkerList[i]->Disconnect();
			delete itsWorkerList[i];
		}
	}

	delete itsInstance;
}
/*
 * GetConnection()
 *
 * Returns a read-only connection to radon. When calling program has
 * finished, it should return the connection to the pool.
 *
 * TODO: smart pointers ?
 *
 */

NFmiRadonDB* NFmiRadonDBPool::GetConnection()
{
	/*
	 *  1 --> active
	 *  0 --> inactive
	 * -1 --> uninitialized
	 *
	 * Logic of returning connections:
	 *
	 * 1. Check if worker is idle, if so return that worker.
	 * 2. Check if worker is uninitialized, if so create worker and return that.
	 * 3. Sleep and start over
	 */

	lock_guard<mutex> lock(itsGetMutex);

	while (true)
	{
		for (unsigned int i = 0; i < itsWorkingList.size(); i++)
		{
			// Return connection that has been initialized but is idle
			if (itsWorkingList[i] == 0)
			{
				itsWorkingList[i] = 1;

				FMIDEBUG(cout << "DEBUG: Idle worker returned with id " << itsWorkerList[i]->Id() << endl);

				return itsWorkerList[i];
			}
			else if (itsWorkingList[i] == -1)
			{
				if (itsUsername.empty())
				{
					throw std::runtime_error("NFmiRadonDBPool: empty username");
				}

				if (itsPassword.empty())
				{
					throw std::runtime_error("NFmiRadonDBPool: empty password");
				}

				if (itsDatabase.empty())
				{
					throw std::runtime_error("NFmiRadonDBPool: empty database name");
				}

				if (itsHostname.empty())
				{
					throw std::runtime_error("NFmiRadonDBPool: empty hostname");
				}

				// Create new connection
				itsWorkerList[i] = new NFmiRadonDB(static_cast<short>(i));
				itsWorkerList[i]->Connect(itsUsername, itsPassword, itsDatabase, itsHostname, itsPort);

				itsWorkingList[i] = 1;

				FMIDEBUG(cout << "DEBUG: New worker returned with id " << itsWorkerList[i]->Id() << endl);

				return itsWorkerList[i];
			}
		}

		// All threads active
		FMIDEBUG(cout << "DEBUG: Waiting for worker release. Pool size=" << itsWorkerList.size() << endl);
		assert(itsWorkerList.size() == itsWorkingList.size());

		usleep(100000);  // 100 ms
	}
}

/*
 * Release()
 *
 * Clears the database connection (does not disconnect!) and returns it
 * to pool.
 */

void NFmiRadonDBPool::Release(NFmiRadonDB* theWorker)
{
	theWorker->Rollback();
	itsWorkingList[theWorker->Id()] = 0;

	FMIDEBUG(cout << "DEBUG: Worker released for id " << theWorker->Id() << endl);
}

void NFmiRadonDBPool::MaxWorkers(int theMaxWorkers)
{
	if (theMaxWorkers == itsMaxWorkers)
		return;

	// Making pool smaller is not supported

	if (theMaxWorkers < itsMaxWorkers)
		throw runtime_error("Making RadonDB pool size smaller is not supported (" + to_string(itsMaxWorkers) + " to " +
		                    to_string(theMaxWorkers) + ")");

	itsMaxWorkers = theMaxWorkers;

	itsWorkingList.resize(itsMaxWorkers, -1);
	itsWorkerList.resize(itsMaxWorkers, NULL);
}
