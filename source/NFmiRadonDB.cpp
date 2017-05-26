#include "NFmiRadonDB.h"

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string_regex.hpp>

using namespace std;

const float kFloatMissing = 32700.f;

#pragma GCC diagnostic ignored "-Wwrite-strings"

NFmiRadonDB &NFmiRadonDB::Instance()
{
	static NFmiRadonDB instance_;
	return instance_;
}

NFmiRadonDB::NFmiRadonDB(short theId) : NFmiPostgreSQL(), itsId(theId) {}
NFmiRadonDB::~NFmiRadonDB() { Disconnect(); }
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

void NFmiRadonDB::Connect(const std::string &user, const std::string &password, const std::string &database,
                          const std::string &hostname, int port)
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

	/*map <string, string> producer_info = GetProducerDefinition(producer_id);

	if (producer_info.empty())
	  return ret;
  */

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

	if (row.empty()) return ret;

	ret["id"] = row[0];
	ret["name"] = row[1];
	ret["parm_name"] = row[1];  // backwards compatibility
	ret["base"] = row[2];
	ret["scale"] = row[3];
	ret["univ_id"] = row[4];

	paramnewbaseinfo[key] = ret;

	return ret;
}

map<string, string> NFmiRadonDB::GetParameterFromDatabaseName(long producerId, const string &parameterName, int levelId,
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

	// Fetch database information

	query << "SELECT id, name, version FROM param WHERE name = '" << parameterName << "'";

	Query(query.str());
	auto row = FetchRow();

	if (row.empty())
	{
		return ret;
	}

	ret["id"] = row[0];
	ret["name"] = row[1];
	ret["version"] = row[2];

	// Fetch grib1 information

	// levelId is database level id (not grib level id!)

	query.str("");
	query << "SELECT table_version, number FROM param_grib1_v WHERE"
	      << " param_id = " << ret["id"] << " AND producer_id = " << producerId
	      << " AND (level_id IS NULL OR level_id = " << levelId << ")"
	      << " AND (level_value IS NULL OR level_value = " << levelValue << ")"
	      << " ORDER BY level_id NULLS LAST, level_value NULLS LAST LIMIT 1";

	Query(query.str());
	row = FetchRow();

	if (!row.empty())
	{
		ret["grib1_table_version"] = row[0];
		ret["grib1_number"] = row[1];
	}
	else
	{
		ret["grib1_table_version"] = "";
		ret["grib1_number"] = "";
	}

	// Get grib2 information

	query.str("");
	query << "SELECT discipline, category, number FROM param_grib2 WHERE"
	      << " param_id = " << ret["id"] << " AND producer_id = " << producerId
	      << " AND (level_id IS NULL OR level_id = " << levelId << ")"
	      << " AND (level_value IS NULL OR level_value = " << levelValue << ")"
	      << " ORDER BY level_id NULLS LAST, level_value NULLS LAST LIMIT 1";

	Query(query.str());
	row = FetchRow();

	if (!row.empty())
	{
		ret["grib2_discipline"] = row[0];
		ret["grib2_category"] = row[1];
		ret["grib2_number"] = row[2];
	}
	else
	{
		query.str("");
		query << "SELECT discipline, category, number FROM "
		      << "param_grib2_template t "
		      << "WHERE param_id = " << ret["id"];

		Query(query.str());
		row = FetchRow();

		if (!row.empty())
		{
			ret["grib2_discipline"] = row[0];
			ret["grib2_category"] = row[1];
			ret["grib2_number"] = row[2];
		}
		else
		{
			ret["grib2_discipline"] = "";
			ret["grib2_category"] = "";
			ret["grib2_number"] = "";
		}
	}

	// Get newbase information

	query.str("");
	query << "SELECT univ_id,scale,base FROM param_newbase WHERE"
	      << " param_id = " << ret["id"] << " AND producer_id = " << producerId;

	Query(query.str());
	row = FetchRow();

	if (!row.empty())
	{
		ret["univ_id"] = row[0];
		ret["scale"] = row[1];
		ret["base"] = row[2];
	}
	else
	{
		ret["univ_id"] = "";
		ret["scale"] = "1";
		ret["base"] = "0";
	}

	paramdbinfo[key] = ret;
	return ret;
}

map<string, string> NFmiRadonDB::GetParameterFromGrib1(long producerId, long tableVersion, long paramId,
                                                       long timeRangeIndicator, long levelId, double levelValue)
{
	string key = to_string(producerId) + "_" + to_string(tableVersion) + "_" +
		to_string(paramId) + "_" + to_string(timeRangeIndicator) + "_" +
		to_string(levelId) + to_string(levelValue);

	if (paramgrib1info.find(key) != paramgrib1info.end())
	{
		FMIDEBUG(cout << "DEBUG: ParameterFromGrib1() cache hit!" << endl);

		return paramgrib1info[key];
	}

	stringstream query;

	query << "SELECT p.id, p.name, p.version, u.name AS unit_name, "
	         "p.interpolation_id, i.name AS interpolation_name, "
	         "g.level_id "
	      << "FROM param_grib1 g, level_grib1 l, param p, param_unit u, interpolation_method i, "
	         "fmi_producer f "
	      << "WHERE g.param_id = p.id AND p.unit_id = u.id AND "
	         "p.interpolation_id = i.id AND f.id = g.producer_id "
	      << " AND f.id = " << producerId << " AND table_version = " << tableVersion << " AND number = " << paramId
	      << " AND timerange_indicator = " << timeRangeIndicator
	      << " AND (g.level_id IS NULL OR (g.level_id = l.level_id AND l.grib_level_id = " << levelId << "))"
	      << " AND (level_value IS NULL OR level_value = " << levelValue << ")"
	      << " ORDER BY g.level_id NULLS LAST, level_value NULLS LAST LIMIT 1";

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
		ret["grib1_table_version"] = to_string(tableVersion);
		ret["grib1_number"] = to_string(paramId);
		ret["interpolation_method"] = row[4];
		ret["level_id"] = row[5];
		ret["level_value"] = row[6];

		paramgrib1info[key] = ret;
	}

	return ret;
}

map<string, string> NFmiRadonDB::GetParameterFromGrib2(long producerId, long discipline, long category, long paramId,
                                                       long levelId, double levelValue)
{
	string key = to_string(producerId) + "_" + to_string(discipline) + "_" +
		to_string(category) + "_" + to_string(paramId) + "_" +
		to_string(levelId) + "_" + to_string(levelValue);

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
		      << "t.number = " << paramId;

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

	paramgrib2info[key] = ret;

	return ret;
}

map<string, string> NFmiRadonDB::GetParameterFromNetCDF(long producerId, const string &paramName, long levelId,
                                                        double levelValue)
{
	const string key = to_string(producerId) + "_" + paramName + "_" + to_string(levelId) + "_" +
		to_string(levelValue);

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

map<string, string> NFmiRadonDB::GetLevelFromDatabaseName(const std::string &name)
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
	const string key = to_string(producerId) + "_" + to_string(levelNumber) + "_" +
	             to_string(edition);

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

vector<vector<string>> NFmiRadonDB::GetGridGeoms(const string &ref_prod, const string &analtime,
                                                 const string &geom_name)
{
	const string key = ref_prod + "_" + analtime + "_" + geom_name;
	if (gridgeoms.count(key) > 0)
	{
		FMIDEBUG(cout << "DEBUG: GetGridGeoms() cache hit!" << endl);

		return gridgeoms[key];
	}

	stringstream query;

	query << "SELECT g.geometry_id, a.table_name, a.id, "
	         "g.geom_name"
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

map<string, string> NFmiRadonDB::GetGeometryDefinition(const string &geom_name)
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

	ret["geom_id"] = row[0];
	ret["prjn_name"] = row[1];
	ret["prjn_id"] = row[2];  // deprecated
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

			if (row.empty()) return map<string, string>();

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

			if (row.empty()) return map<string, string>();

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

			if (row.empty()) return map<string, string>();

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

			if (row.empty()) return map<string, string>();

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
			query << "SELECT nj, first_lat, first_lon, last_lat, last_lon"
			         "n, scanning_mode, points_along_parallels FROM "
			         "geom_reduced_gaussian_v WHERE geometry_id = "
			      << row[0];
			Query(query.str());
			row = FetchRow();

			if (row.empty()) return map<string, string>();

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
	string key = to_string(ni) + "_" + to_string(nj) + "_" +
	             to_string(lat) + "_" + to_string(lon) + "_" +
	             to_string(di) + "_" + to_string(dj) + "_" +
	             to_string(gribedition) + "_" + to_string(gridtype);

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

	int projection_id = std::stoi(row[0]);

	// TODO: for projections other than latlon, extra properties should be checked,
	// such as south pole, orientation etc.

	switch (projection_id)
	{
		case 1:
			query << "SELECT geometry_id, geometry_name FROM geom_latitude_longitude_v "
			      << "WHERE nj = " << nj << " AND ni = " << ni << " AND first_lon = " << lon
			      << " AND first_lat = " << lat << " AND di = " << di << " AND dj = " << dj;
			break;
		case 2:
			query << "SELECT geometry_id, geometry_name FROM geom_stereographic_v "
			      << "WHERE nj = " << nj << " AND ni = " << ni << " AND first_lon = " << lon
			      << " AND first_lat = " << lat << " AND di = " << di << " AND dj = " << dj;
			break;
		case 4:
			query << "SELECT geometry_id, geometry_name FROM geom_rotated_latitude_longitude_v "
			      << "WHERE nj = " << nj << " AND ni = " << ni << " AND first_lon = " << lon
			      << " AND first_lat = " << lat << " AND di = " << di << " AND dj = " << dj;
			break;
		case 5:
			query << "SELECT geometry_id, geometry_name FROM geom_lambert_conformal_v "
			      << "WHERE nj = " << nj << " AND ni = " << ni << " AND first_lon = " << lon
			      << " AND first_lat = " << lat << " AND di = " << di << " AND dj = " << dj;
			break;
		case 6:
			query << "SELECT geometry_id, geometry_name FROM geom_reduced_gaussian_v "
			      << "WHERE nj = " << nj << " AND ni = " << ni << " AND first_lon = " << lon
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

map<string, string> NFmiRadonDB::GetProducerDefinition(const string &producer_name)
{
	stringstream query;

	query << "SELECT id "
	      << "FROM fmi_producer"
	      << " WHERE name = '" << producer_name << "'";

	Query(query.str());

	vector<string> row = FetchRow();

	unsigned long int producer_id = std::stoul(row[0]);

	if (producerinfo.find(producer_id) != producerinfo.end()) return producerinfo[producer_id];

	return GetProducerDefinition(row[0]);
}

string NFmiRadonDB::GetLatestTime(const std::string &ref_prod, const std::string &geom_name, unsigned int offset)
{
	stringstream query;

	query << "SELECT min_analysis_time::timestamp, max_analysis_time::timestamp, partition_name "
	      << "FROM as_grid_v"
	      << " WHERE producer_name = '" << ref_prod << "' AND record_count > 0 ";

	if (!geom_name.empty())
	{
		query << " AND geometry_name = '" << geom_name << "'";
	}

	query << " GROUP BY min_analysis_time, max_analysis_time, partition_name ORDER BY max_analysis_time DESC LIMIT 1 "
	         "OFFSET "
	      << offset;

	Query(query.str());

	vector<string> row = FetchRow();

	if (row.size() == 0)
	{
		return "";
	}

	if (row[0] == row[1])
	{
		// analysis time partitioning
		return row[0];
	}

	query.str("");
	query << "SELECT max(analysis_time) FROM " << row[2];

	Query(query.str());

	row = FetchRow();

	if (row.empty())
	{
		return "";
	}

	return row[0];
}

map<string, string> NFmiRadonDB::GetStationDefinition(FmiRadonStationNetwork networkType, unsigned long stationId,
                                                      bool aggressive_cache)
{
	assert(!aggressive_cache);  // not supported yet

	string key = to_string(static_cast<int>(networkType)) + "_" + to_string(stationId);

	if (stationinfo.find(key) != stationinfo.end()) return stationinfo[key];

	stringstream query;

	map<string, string> ret;

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
	         "fs.station_id AND fs.network_id = 5) ";

	switch (networkType)
	{
		case kWMONetwork:
		case kICAONetwork:
		case kLPNNNetwork:
		case kRoadWeatherNetwork:
		default:
			throw runtime_error("Unsupported station network type: " + to_string(static_cast<int>(networkType)));
			break;
		case kFmiSIDNetwork:
			query << "JOIN station_network_mapping m ON (s.id = m.station_id AND "
			         "m.network_id = 5 AND "
			         "m.local_station_id = '"
			      << stationId << "')";
			break;
	}

	Query(query.str());

	auto row = FetchRow();

	if (row.empty())
	{
		return ret;
	}

	ret["id"] = row[0];
	ret["station_name"] = row[1];
	ret["longitude"] = row[2];
	ret["latitude"] = row[3];
	ret["altitude"] = row[4];
	ret["wmoid"] = row[5];
	ret["icaoid"] = row[6];
	ret["lpnn"] = row[7];
	ret["rwid"] = row[8];
	ret["fmisid"] = row[9];

	stationinfo[key] = ret;

	return ret;
}

std::map<string, string> NFmiRadonDB::GetLevelTransform(long producer_id, long param_id, long fmi_level_id,
                                                        double fmi_level_value)
{
	string key = to_string(producer_id) + "_" + to_string(param_id) + "_" +
	             to_string(fmi_level_id) + "_" + to_string(fmi_level_value);
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

std::string NFmiRadonDB::GetProducerMetaData(long producer_id, const string &attribute)
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

double NFmiRadonDB::GetProbabilityLimitForStation(long stationId, const std::string& paramName)
{
	std::stringstream ss;
	ss << "SELECT probability_limit FROM station_probability_limit_v WHERE station_id = " 
	   << stationId << " AND param_name = '" << paramName << "'";

	Query(ss.str());

	auto row = FetchRow();

	if (row.empty())
	{
		return kFloatMissing;
	}

	return stod(row[0]);
}

NFmiRadonDBPool *NFmiRadonDBPool::itsInstance = NULL;

NFmiRadonDBPool *NFmiRadonDBPool::Instance()
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

NFmiRadonDB *NFmiRadonDBPool::GetConnection()
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

void NFmiRadonDBPool::Release(NFmiRadonDB *theWorker)
{
	theWorker->Rollback();
	itsWorkingList[theWorker->Id()] = 0;

	FMIDEBUG(cout << "DEBUG: Worker released for id " << theWorker->Id() << endl);
}

void NFmiRadonDBPool::MaxWorkers(int theMaxWorkers)
{
	if (theMaxWorkers == itsMaxWorkers) return;

	// Making pool smaller is not supported

	if (theMaxWorkers < itsMaxWorkers)
		throw runtime_error("Making RadonDB pool size smaller is not supported (" +
		                    to_string(itsMaxWorkers) + " to " +
		                    to_string(theMaxWorkers) + ")");

	itsMaxWorkers = theMaxWorkers;

	itsWorkingList.resize(itsMaxWorkers, -1);
	itsWorkerList.resize(itsMaxWorkers, NULL);
}
