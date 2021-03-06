#include "NFmiNeonsDB.h"

#include <boost/format.hpp>
#include <algorithm>

using namespace std;

NFmiNeonsDB& NFmiNeonsDB::Instance()
{
	static NFmiNeonsDB instance_;
	return instance_;
}

NFmiNeonsDB::NFmiNeonsDB(short theId) : NFmiOracle(), itsId(theId)
{
	connected_ = false;
	user_ = "neons_client";
	database_ = "neons";

	const auto pw = getenv("NEONS_NEONSCLIENT_PASSWORD");
	if (pw)
	{
		password_ = string(pw);
	}
	else
	{
		throw std::runtime_error("Environment variable NEONS_NEONSCLIENT_PASSWORD not defined");
	}
}

NFmiNeonsDB::~NFmiNeonsDB() { Disconnect(); }
void NFmiNeonsDB::Connect() { return Connect(0); }
void NFmiNeonsDB::Connect(const int threadedMode)
{
	NFmiOracle::Connect(threadedMode);
	DateFormat("YYYYMMDDHH24MISS");
	Verbose(true);
}

void NFmiNeonsDB::Connect(const std::string& user, const std::string& password, const std::string& database,
                          const int threadedMode)
{
	NFmiOracle::Connect(user, password, database, threadedMode);
	DateFormat("YYYYMMDDHH24MISS");
	Verbose(true);
}

string NFmiNeonsDB::GetLatestTime(const std::string& ref_prod, const std::string& geom_name, unsigned int offset)
{
	// offset 0 suggests that we take the first (there is no offset); that does not
	// work in Oracle SQL though, there the first result is picked up with offset 1

	offset++;
	stringstream query;

	query << "SELECT rank, base_date "
	      << "FROM ("
	      << "SELECT to_char(base_date,'YYYYMMDDHH24MI') AS base_date, row_number() OVER (ORDER BY base_date DESC) AS "
	         "rank "
	      << "FROM as_grid "
	      << "WHERE model_type = '" << ref_prod << "' "
	      << "AND rec_cnt_dset > 0";

	if (!geom_name.empty())
	{
		query << " AND geom_name = '" << geom_name << "'";
	}

	query << " GROUP BY base_date) WHERE rank BETWEEN " << offset << " AND " << offset;

	Query(query.str());

	vector<string> row = FetchRow();

	if (row.size() == 0)
	{
		return "";
	}

	assert(std::stoul(row[0]) == offset);

	return row[1];
}

map<string, string> NFmiNeonsDB::GetGridDatasetInfo(long centre, long process, const string& geomName,
                                                    const string& baseDate)
{
	string key = to_string(centre) + "_" + to_string(process) + "_" + geomName +
	             "_" + baseDate;

	if (datasetinfo.find(key) != datasetinfo.end()) return datasetinfo[key];

	stringstream query;

	query << "SELECT "
	      << "dset_id, "
	      << "table_name "
	      << "FROM as_grid a, "
	      << "grid_num_model_grib nu, "
	      << "grid_model m, "
	      << "grid_model_name na "
	      << "WHERE nu.model_id = " << process << " AND nu.ident_id = " << centre << " AND m.flag_mod = 0 "
	      << " AND nu.model_name = na.model_name "
	      << " AND m.model_name = na.model_name "
	      << " AND m.model_type = a.model_type "
	      << " AND geom_name = '" << geomName << "'"
	      << " AND dset_name = 'AF'"
	      << " AND base_date = '" << baseDate << "'";

	Query(query.str());

	auto row = FetchRow();

	map<string, string> ret;

	if (row.empty())
	{
		return ret;
	}

	ret["dset_id"] = row[0];
	ret["table_name"] = row[1];

	datasetinfo[key] = ret;

	return ret;
}

/*
 * GetGridLevelName(string, long, long, long)
 *
 * GetGridLevellName will convert the given InLvlId on the given
 * InCodeTableVer to the correct lvl_type on the given OutCodeTableVer
 * for the given neons param.
 *
 */

string NFmiNeonsDB::GetGridLevelName(const std::string& parm_name, long InLvlId, long InCodeTableVer,
                                     long OutCodeTableVer)
{
	string lvl_name;
	string univ_id;
	string lvl_id = to_string(InLvlId);
	string no_vers = to_string(InCodeTableVer);
	string no_vers2 = to_string(OutCodeTableVer);

	// Implement caching since this function is quite expensive
	assert(!parm_name.empty());

	string key = parm_name + "_" + lvl_id + "_" + no_vers + "_" + no_vers2;

	if (levelinfo.find(key) != levelinfo.end()) return levelinfo[key];

	string query =
	    "SELECT lvl_type "
	    "FROM grid_lvl_grib "
	    "WHERE lvl_id = " +
	    lvl_id +
	    " "
	    "AND no_vers2 = " +
	    no_vers;

	Query(query);
	vector<string> row = FetchRow();

	if (!row.empty())
	{
		lvl_name = row[0];
	}
	else
	{
		levelinfo[key] = "";
	}

	query =
	    "SELECT univ_id "
	    "FROM grid_lvl_xref "
	    "WHERE lvl_type like '" +
	    lvl_name + "'";
	"AND no_vers2 = " + no_vers;

	Query(query);
	vector<string> id = FetchRow();

	if (!id.empty())
	{
		univ_id = id[0];
	}
	else
	{
		levelinfo[key] = "";
		return "";
	}

	query =
	    "SELECT lvl_type "
	    "FROM grid_lvl_xref "
	    "WHERE parm_name = '" +
	    parm_name +
	    "' "
	    "AND no_vers2 = " +
	    no_vers2 +
	    " "
	    "AND univ_id = " +
	    univ_id;

	Query(query);
	vector<string> prod_class = FetchRow();
	string pclass;

	if (!prod_class.empty())
	{
		lvl_name = prod_class[0];
	}
	else
	{
		query =
		    "SELECT lvl_type "
		    "FROM grid_lvl_xref "
		    "WHERE parm_name = 'ALL_OTHERS' "
		    "AND no_vers2 = " +
		    no_vers2 +
		    " "
		    "AND univ_id = " +
		    univ_id;
		Query(query);
		vector<string> lvltype = FetchRow();

		if (!lvltype.empty())
		{
			lvl_name = lvltype[0];
		}
	}

	levelinfo[key] = lvl_name;
	return levelinfo[key];
}

/*
 * GetGridLevelName(long, long, long, ,long)
 *
 * Replaces old proC function GetGridLvlNameFromNeons.
 *
 * GetGridLevellName will convert the given InLvlId on the given
 * InCodeTableVer to the correct lvl_type on the given OutCodeTableVer
 * for the given parameter InParmId.
 *
 */

string NFmiNeonsDB::GetGridLevelName(long InParmId, long InLvlId, long InCodeTableVer, long OutCodeTableVer)
{
	string parm_name = GetGridParameterName(InParmId, InCodeTableVer, OutCodeTableVer);
	assert(!parm_name.empty());
	return GetGridLevelName(parm_name, InLvlId, InCodeTableVer, OutCodeTableVer);
}

/*
 * GetGridLevelName(long,long)
 *
 * Replaces old proC function GetGridLvlNameForGRIB2.
 * Parameter InProducerId refers to the generatingProcessIdentifier found
 * inside grib.
 */

string NFmiNeonsDB::GetGridLevelName(long InLvlId, long InProducerId)
{
	string lvl_name;
	string lvl_id = to_string(InLvlId);
	string producer_id = to_string(InProducerId);

	// Implement caching since this function is quite expensive

	string key = lvl_id + "_" + producer_id;

	if (levelinfo.find(key) != levelinfo.end()) return levelinfo[key];

	string query =
	    "SELECT l.lvltype_name "
	    "FROM grid_lvltype_grib2 l, grid_num_model_grib g "
	    "WHERE l.lvltype = " +
	    lvl_id + " AND g.model_id = " + producer_id + " AND l.producer = g.ident_id";

	Query(query);
	vector<string> row = FetchRow();

	if (!row.empty())
	{
		lvl_name = row[0];

		levelinfo[key] = lvl_name;
		return lvl_name;
	}

	// Try "universal" producer 9999

	query =
	    "SELECT lvltype_name "
	    "FROM grid_lvltype_grib2 "
	    "WHERE lvltype = " +
	    lvl_id +
	    " "
	    "AND producer = 9999";

	Query(query);
	row = FetchRow();

	if (row.empty())
	{
		return "";
	}

	lvl_name = row[0];

	levelinfo[key] = lvl_name;
	return lvl_name;
}

/*
 * GetGridParameterId()
 *
 * Return the parm_id that matches given no_vers and parameter name.
 *
 */

long NFmiNeonsDB::GetGridParameterId(long no_vers, const std::string& name)
{
	string no_vers_str = to_string(no_vers);

	string key = name + "_" + no_vers_str;

	if (gridparamid.find(key) != gridparamid.end())
	{
		FMIDEBUG(cout << "DEBUG: GetGridParameterId() cache hit!" << endl);
		
		return gridparamid[key];
	}
	string query =
	    "SELECT parm_id FROM grid_param_grib WHERE no_vers = " + no_vers_str + " AND parm_name = '" + name + "'";

	Query(query);

	vector<string> row = FetchRow();

	if (row.empty())
	{
		gridparamid[key] = -1;
	}
	else
	{
		gridparamid[key] = std::stol(row[0]);
	}

	return gridparamid[key];
}

/*
 * GetGridParameterName(long, long, long, long)
 *
 * Replaces old proC function GetGridParNameFromNeons.
 *
 * GetGridParameterName will convert the InParmId on the given
 * InCodeTableVer to the correct ParName on the given OutCodeTableVer.
 *
 */

string NFmiNeonsDB::GetGridParameterName(long InParmId, long InCodeTableVer, long OutCodeTableVer,
                                         long timeRangeIndicator, long levelType)
{
	// Implement some sort of caching: this function is quite expensive

	string key = to_string(InParmId) + "_" + to_string(InCodeTableVer) + "_" +
	             to_string(OutCodeTableVer) + "_" + to_string(timeRangeIndicator) +
	             "_" + to_string(levelType);

	if (gridparameterinfo.find(key) != gridparameterinfo.end())
	{
		FMIDEBUG(cout << "GetGridParameterName() cache hit!" << endl);

		return gridparameterinfo[key];
	}

	stringstream query;

	if (InCodeTableVer != OutCodeTableVer)
	{
		assert(OutCodeTableVer > 0);

		query << "SELECT x2.parm_name "
		      << "FROM "
		      << "grid_param_grib g1, "
		      << "grid_param_xref x1, "
		      << "grid_param_xref x2 "
		      << "WHERE "
		      << "g1.parm_id = " << InParmId << " AND g1.no_vers = " << InCodeTableVer
		      << " AND g1.parm_name = x1.parm_name"
		      << " AND x1.no_vers = " << InCodeTableVer << " AND x1.univ_id = " << InParmId
		      << " AND x2.no_vers = " << OutCodeTableVer << " AND x1.univ_id = x2.univ_id";
	}
	else
	{
		query << "SELECT parm_name FROM grid_param_grib "
		      << " WHERE parm_id = " << InParmId << " AND no_vers = " << InCodeTableVer
		      << " AND timerange_ind = " << timeRangeIndicator << " AND (lvl_type IS NULL OR lvl_type = " << levelType
		      << ") "
		      << "ORDER BY lvl_type NULLS LAST";
	}

	Query(query.str());
	vector<string> row = FetchRow();

	if (row.empty())
	{
		return "";
	}

	string parm_name = row[0];

	query.str("");

	// This query is wrong and will return not work if two different producer classes use same code table
	query << "SELECT max(producer_class) FROM fmi_producers WHERE no_vers = " << OutCodeTableVer;

	Query(query.str());
	vector<string> prod_class = FetchRow();

	if (!prod_class.empty())
	{
		string pclass = prod_class[0];
		/* Switch the -'s to _'s for point producers */
		if ((pclass == "2") || (pclass == "3"))
		{
			parm_name.replace(parm_name.find("-"), 1, "_");
		}
	}

	gridparameterinfo[key] = parm_name;
	return gridparameterinfo[key];

#if 0		  
  string query = "SELECT parm_name "
                 "FROM grid_param_grib "
                 "WHERE parm_id = " + parm_id + " "
                 "AND no_vers = " + no_vers + " "
                 "AND timerange_ind = " + trInd + " "
                 "AND (lvl_type IS NULL OR lvl_type = " + levType + ") "
                 "ORDER BY lvl_type NULLS LAST";

  Query(query);
  vector<string> row = FetchRow();

  if (!row.empty())
    parm_name = row[0];

  else
    return "";

  if (InCodeTableVer == OutCodeTableVer) {
    gridparameterinfo[key] = parm_name;
    return gridparameterinfo[key];
  }
  else {
    /* We do some further digging */
    query = "SELECT univ_id "
            "FROM grid_param_xref "
            "WHERE parm_name like '" +parm_name + "'"
            " AND no_vers = " +no_vers;

    Query(query);
    vector<string> id = FetchRow();

    if (!id.empty()) {
      univ_id = id[0];
    }

    /* Finally dig out the parm_name on OutCodeTableVer */
    query = "SELECT parm_name "
            "FROM grid_param_xref "
            "WHERE univ_id = " + univ_id + " "
            "AND no_vers = "+ to_string(OutCodeTableVer);

    Query(query);
    vector<string> param = FetchRow();

    if (!param.empty()) {
      parm_name = param[0];
    }
#endif
}

/*
 * GetGridParameterNameForGrib2(long, long, long, long)
 *
 * Replaces old proC function GetGridParNameForGrib2.
 *
 */

string NFmiNeonsDB::GetGridParameterNameForGrib2(long InParmId, long InCategory, long InDiscipline, long InProducerId)
{
	string parm_name;
	string parm_id = to_string(InParmId);
	string category = to_string(InCategory);
	string discipline = to_string(InDiscipline);
	string producer_id = to_string(InProducerId);

	// Implement some sort of caching: this function is quite expensive

	string key = parm_id + "_" + category + "_" + discipline + "_" + producer_id;

	if (gridparameterinfo.find(key) != gridparameterinfo.end()) return gridparameterinfo[key];

	// First try to fetch the parm_name with the actual producer id

	string query =
	    "SELECT g.parm_name "
	    "FROM grid_param_grib2 g, grid_num_model_grib gg "
	    "WHERE discipline = " +
	    discipline +
	    " "
	    "AND g.producer = gg.ident_id "
	    "AND g.category = " +
	    category +
	    " "
	    "AND gg.model_id = " +
	    producer_id +
	    " "
	    "AND g.param = " +
	    parm_id;

	Query(query);
	vector<string> row = FetchRow();

	if (!row.empty())
	{
		parm_name = row[0];

		gridparameterinfo[key] = parm_name;
		return parm_name;
	}

	// If parm_name was not found, try to fetch the parm_name using the general
	// producer_id ( = 9999)

	query =
	    "SELECT parm_name "
	    "FROM grid_param_grib2 "
	    "WHERE discipline = " +
	    discipline +
	    " "
	    "AND category = " +
	    category +
	    " "
	    "AND producer = 9999 "
	    "AND param = " +
	    parm_id;

	Query(query);
	row = FetchRow();

	if (row.empty()) return "";

	parm_name = row[0];

	gridparameterinfo[key] = parm_name;
	return gridparameterinfo[key];
}

/*
 * GetGrib2Parameter(long, long, long)
 *
 * Get GRIB 2 parameter id from newbase id and producer id.
 *
 * TODO: caching
 *
 */

pair<int, int> NFmiNeonsDB::GetGrib2Parameter(unsigned long producerId, unsigned long parameterId)
{
	/* string query = "SELECT category, param "
	                  "FROM grid_param_grib2 g, grid_param_xref x, fmi_producers f, grid_num_model_grib n "
	                  "WHERE g.parm_name = x.parm_name AND f.no_vers = x.no_vers AND f.producer_id = n.model_id "
	                  "AND n.model_id = f.producer_id AND (n.ident_id = g.producer OR g.producer = 9999) "
	                  "AND x.univ_id = " + to_string (parameterId) + " "
	                  "AND f.producer_id = " + to_string (producerId);*/

	pair<int, int> p = make_pair(-1, -1);

	string query = "SELECT g.parm_name FROM grid_param_xref g, fmi_producers f WHERE g.univ_id = " +
	               to_string(parameterId) + " AND f.no_vers = g.no_vers AND f.producer_id = " +
	               to_string(producerId);

	Query(query);

	vector<string> row = FetchRow();

	if (row.empty()) return p;

	string parm_name = row[0];

	query = "SELECT category, param FROM grid_param_grib2 WHERE parm_name = '" + parm_name +
	        "' AND (producer = 9999 OR producer = "
	        "(SELECT ident_id FROM grid_num_model_grib g, fmi_producers f, grid_model m "
	        "WHERE f.producer_id = " +
	        to_string(producerId) +
	        "AND f.ref_prod = m.model_type AND m.model_name = g.model_name)) ORDER BY producer";

	Query(query);

	row = FetchRow();

	if (row.empty()) return p;

	p = make_pair(std::stoi(row[0]), std::stoi(row[1]));

	return p;
}

/*
 * GetUnivId(long, string)
 *
 * Get universal parameter id from producer/parameter name combination.
 * This function is used with non-grib data types (most notably with netcdf).
 *
 * http://cf-pcmdi.llnl.gov/documents/cf-standard-names/ecmwf-grib-mapping
 *
 * Parameter name refers to the Neons parameter name!
 */

std::string NFmiNeonsDB::GetGribParameterNameFromNetCDF(unsigned long producerId, const std::string& nc_param)
{
	string query =
	    "SELECT producer_id, parm_name, nc_name "
	    "FROM grid_param_nc "
	    "WHERE nc_name = '" +
	    nc_param +
	    "' "
	    "AND producer_id = " +
	    to_string(producerId);

	Query(query);

	vector<string> row = FetchRow();

	if (row.empty()) return "";

	return row[1];
}

map<string, string> NFmiNeonsDB::GetParameterDefinition(unsigned long producer_id, const string& parm_name)
{
	map<string, string> producer_info = GetProducerDefinition(producer_id);

	string query = "SELECT univ_id FROM grid_param_xref WHERE no_vers = " + producer_info["no_vers"] +
	               " AND parm_name = '" + parm_name + "'";

	Query(query);

	vector<string> row = FetchRow();

	if (row.empty())
	{
		map<string, string> ret;
		return ret;
	}

	return GetParameterDefinition(producer_id, std::stoul(row[0]));
}

/*
 * GetParameterDefinition()
 *
 * Retrieves parameter definition from NEONS meta-tables.
 *
 * Replaces pro*c function
 *
 * int CheckColumnFromParameter( DBParamDef *ParamDef, DBProducerDef *ProdDef)
 *
 * defined at objdb/pcsource/CheckColumnFromParameter.pc
 *
 */

map<string, string> NFmiNeonsDB::GetParameterDefinition(unsigned long producer_id, unsigned long universal_id)
{
	if (parameterinfo.find(producer_id) != parameterinfo.end())
	{
		if (parameterinfo[producer_id].find(universal_id) != parameterinfo[producer_id].end())
		{
			FMIDEBUG(cout << "DEBUG: GetParameterDefinition() cache hit!" << endl);

			return parameterinfo[producer_id][universal_id];
		}
	}

	map<string, string> ret;

	string prod_id = to_string(producer_id);
	string univ_id = to_string(universal_id);

	map<string, string> producer_info = GetProducerDefinition(producer_id);

	if (producer_info.empty()) return ret;

	string forecast_type = producer_info["seq_type_prfx"];
	int producer_class = std::stoi(producer_info["producer_class"]);
	string no_vers = producer_info["no_vers"];
	int dbclass_id = std::stoi(producer_info["dbclass_id"]);

	string query =
	    "SELECT "
	    "x.parm_name, "
	    "x.base, "
	    "x.scale, "
	    "u.unit_name, "
	    "nvl(g.parm_desc,'No Description') AS parm_desc, "
	    "nvl(u.unit_desc,'No Description') AS unit_desc, "
	    "replace(x.parm_name,'-','_') AS col_name, "
	    "x.univ_id "
	    "FROM grid_param g, grid_unit u, grid_param_xref x "
	    "WHERE u.unit_id = g.unit_id AND x.parm_name = g.parm_name"
	    " AND x.univ_id = " +
	    univ_id + " AND x.no_vers = " + no_vers;

	Query(query);

	vector<string> row = FetchRow();

	if (row.empty()) return ret;

	ret["parm_name"] = row[0];
	ret["base"] = row[1];
	ret["scale"] = row[2];
	ret["unit_name"] = row[3];
	ret["parm_desc"] = row[4];
	ret["unit_desc"] = row[5];
	ret["col_name"] = row[6];
	ret["univ_id"] = row[7];  // Fetch univ_id in cased called from GetParameterDefinition(ulong, string)

	switch (producer_class)
	{
		case 1:
			/* grid */
			break;

		case 2:
			/* lltbufr */

			if (dbclass_id == 11)

				/* For Climatic database we dont have the table descriptions.
				 * For Climatic database-producer DBCLASS_ID is 11 in the
				 * in the FMI_PRODUCER-table
				 */

				break;

			if (producer_id == 1005 || producer_id == 1032 || producer_id == 1034)

				/* For temp-data there is no openened columns in the database. */
				/* So we can't make the parameter name and database column name conversion */

				break;

			query = "SELECT 1 FROM lltbufr_seq_col WHERE col_name = replace('" + ret["parm_name"] +
			        "','-','_') AND seq_type = '" + forecast_type + "'";

			Query(query);

			row = FetchRow();

			if (row.empty())
			{
				ret.clear();
			}

			break;

		case 3:
			/* previ */

			query = "SELECT 1 FROM previ_col WHERE col_name = replace( '" + ret["parm_name"] +
			        "','-','_') AND previ_type = '" + forecast_type + "'";

			Query(query);

			row = FetchRow();

			if (row.empty())
			{
				ret.clear();
			}

			break;
	}

	parameterinfo[producer_id][universal_id] = ret;

	return ret;
}

/*
 * GetProducerDefinition(int)
 *
 * Retrieves producer definition from neons meta-tables.
 *
 * Replaces pro*c function
 *
 * int GetDBProducerDef( DBProducerDef *ProdDef)
 *
 * defined at fmidbu/pcsource/getdbproducerdef.pc
 *
 */

map<string, string> NFmiNeonsDB::GetProducerDefinition(unsigned long producer_id)
{
	if (producerinfo.count(producer_id) > 0)
	{
		FMIDEBUG(cout << "DEBUG: GetProducerDefinition() cache hit!" << endl);

		return producerinfo[producer_id];
	}

	string query =
	    "SELECT"
	    " producer_id,"
	    " ref_prod,"
	    " seq_type_prfx,"
	    " producer_class,"
	    " no_vers,"
	    " dbclass_id,"
	    " nvl(hours_for_latest,24) "
	    "FROM fmi_producers "
	    "WHERE producer_id = " +
	    to_string(producer_id);

	map<string, string> ret;

	Query(query);

	vector<string> row = FetchRow();

	if (!row.empty())
	{
		ret["producer_id"] = row[0];
		ret["ref_prod"] = row[1];
		ret["seq_type_prfx"] = row[2];
		ret["producer_class"] = row[3];
		ret["no_vers"] = row[4];
		ret["dbclass_id"] = row[5];
		ret["hours_for_latest"] = row[6];

		producerinfo[producer_id] = ret;
	}
	return ret;
}

/*
 * GetProducerDefinition(string)
 *
 * Retrieves producer definition from neons meta-tables.
 *
 * Replaces pro*c function
 *
 * int GetDBProducerDef( DBProducerDef *ProdDef)
 *
 * defined at fmidbu/pcsource/getdbproducerdef.pc
 *
 */

map<string, string> NFmiNeonsDB::GetProducerDefinition(const string& producer_name)
{
	string query =
	    "SELECT"
	    " producer_id "
	    "FROM fmi_producers "
	    "WHERE ref_prod = '" +
	    producer_name + "'";

	Query(query);

	vector<string> row = FetchRow();

	unsigned long int producer_id = std::stoul(row[0]);

	if (producerinfo.find(producer_id) != producerinfo.end()) return producerinfo[producer_id];

	return GetProducerDefinition(producer_id);
}

/*
 * GetGridGeoms()
 *
 * Retrieve geometries defined for producer and origintime
 */

vector<vector<string> > NFmiNeonsDB::GetGridGeoms(const string& ref_prod, const string& analtime,
                                                  const string& geom_name)
{
	string key = ref_prod + "_" + analtime + "_" + geom_name;
	if (gridgeoms.count(key) > 0)
	{
		FMIDEBUG(cout << "DEBUG: GetGridGeoms() cache hit!" << endl);

		return gridgeoms[key];
	}
	string query =
	    "SELECT geom_name, table_name, dset_id "
	    "FROM as_grid "
	    "WHERE rec_cnt_dset > 0 "
	    "AND model_type like '" +
	    ref_prod +
	    "' "
	    "AND base_date = '" +
	    analtime + "'";

	if (!geom_name.empty())
	{
		query += " AND geom_name = '" + geom_name + "'";
	}

	Query(query);

	vector<vector<string> > ret;

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

/*
 * GetGridModelDefinition(int)
 *
 * GetGridModelDefinition will pick up the model information for
 * the given model in the Database.
 *
 */

map<string, string> NFmiNeonsDB::GetGridModelDefinition(unsigned long producer_id)
{
	if (gridmodeldefinition.count(producer_id) > 0)
	{
		FMIDEBUG(cout << "DEBUG: GetGridModelDefinition() cache hit!" << endl);

		return gridmodeldefinition[producer_id];
	}

	map<string, string> ret;

	string query =
	    "SELECT fmi_producers.ref_prod, "
	    "fmi_producers.no_vers, "
	    "grid_model.model_name, "
	    "grid_model.flag_mod, "
	    "grid_model_name.model_desc, "
	    "grid_num_model_grib.model_id, "
	    "grid_model_ident.ident_name, "
	    "grid_model_ident.ident_id, "
	    "grid_model.model_type "
	    "FROM fmi_producers, grid_model, grid_model_name, grid_num_model_grib, grid_model_ident "
	    "WHERE fmi_producers.producer_id = " +
	    to_string(producer_id) +
	    " "
	    "AND fmi_producers.producer_class = 1 "
	    "AND fmi_producers.ref_prod = grid_model.model_type "
	    "AND grid_model_name.model_name = grid_model.model_name "
	    "AND grid_num_model_grib.model_name = grid_model.model_name "
	    "AND grid_model_ident.ident_id = grid_num_model_grib.ident_id";

	Query(query);

	vector<string> row = FetchRow();

	if (!row.empty())
	{
		ret["ref_prod"] = row[0];
		ret["no_vers"] = row[1];
		ret["model_name"] = row[2];
		ret["flag_mod"] = row[3];
		ret["model_desc"] = row[4];
		ret["model_id"] = row[5];
		ret["ident_name"] = row[6];
		ret["ident_id"] = row[7];
		ret["model_type"] = row[8];

		gridmodeldefinition[producer_id] = ret;
	}

	return ret;
}

/*
 * GetNeonsTables(string, string, string)
 *
 * In Neons one table contains data for one day only. If a query spans multiple
 * days, we must make a UNION over all the tables in our sql query. Function
 * GetNeonsTables() return all intermediate tables when given start and end
 * date and producer name.
 */

vector<string> NFmiNeonsDB::GetNeonsTables(const string& start_time, const string& end_time,
                                           const string& producer_name)
{
	vector<string> ret;

	string query = "SELECT tbl_name FROM as_lltbufr WHERE min_dat <= '" + end_time + "' AND max_dat >= '" + start_time +
	               "' AND seq_type = '" + producer_name + "'";

	Query(query);

	while (true)
	{
		vector<string> row = FetchRow();

		if (row.empty()) break;

		ret.push_back(row[0]);
	}

	return ret;
}

map<string, string> NFmiNeonsDB::GetGeometryDefinition(size_t ni, size_t nj, double lat, double lon, double di,
                                                       double dj)
{
	string key = to_string(ni) + "_" + to_string(nj) + "_" +
	             to_string(lat) + "_" + to_string(lon) + "_" +
	             to_string(di) + "_" + to_string(dj);

	if (geometryinfo_fromarea.find(key) != geometryinfo_fromarea.end())
	{
		FMIDEBUG(cout << "DEBUG: GetGeometryDefinition() cache hit!" << endl);

		return geometryinfo_fromarea[key];
	}

	string query =
	    "SELECT "
	    " prjn_name,"
	    " row_cnt,"
	    " col_cnt,"
	    " lat_orig,"
	    " long_orig,"
	    " orig_row_num,"
	    " orig_col_num,"
	    " pas_longitude,"
	    " pas_latitude,"
	    " geom_parm_1,"
	    " geom_parm_2,"
	    " geom_parm_3,"
	    " stor_desc, "
	    " gr.geom_name "
	    "FROM grid_reg_geom gr, grid_geom gm "
	    "WHERE row_cnt = " +
	    to_string(nj) + " AND col_cnt = " + to_string(ni) + " AND lat_orig = " +
	    boost::str(boost::format("%.5f") % lat) + " AND long_orig = " + boost::str(boost::format("%.5f") % lon) +
	    " AND pas_latitude = " + boost::str(boost::format("%.5f") % dj) + " AND pas_longitude = " +
	    boost::str(boost::format("%.5f") % di) + " AND gr.geom_name = gm.geom_name";

	map<string, string> ret;

	Query(query);

	vector<string> row = FetchRow();

	if (!row.empty())
	{
		ret["prjn_name"] = row[0];
		ret["row_cnt"] = row[1];
		ret["col_cnt"] = row[2];
		ret["lat_orig"] = row[3];
		ret["long_orig"] = row[4];
		ret["orig_row_num"] = row[5];
		ret["orig_col_num"] = row[6];
		ret["pas_longitude"] = row[7];
		ret["pas_latitude"] = row[8];
		ret["geom_parm_1"] = row[9];
		ret["geom_parm_2"] = row[10];
		ret["geom_parm_3"] = row[11];
		ret["stor_desc"] = row[12];
		ret["prjn_id"] = "";  // compatibility with radon
		ret["geom_name"] = row[13];

		geometryinfo_fromarea[key] = ret;
	}

	return ret;
}

/*
 * GetGeometryDefinition(string)
 *
 * Retrieves geometry definition from neons meta-tables.
 *
 * Replaces pro*c function
 *
 * int GetDBGeomDef(DBGribGeomDef *GeomDef)
 *
 * defined at fmidbu/pcsource/getdbgeomdef.pc
 *
 */

map<string, string> NFmiNeonsDB::GetGeometryDefinition(const string& geometry_name)
{
	if (geometryinfo.find(geometry_name) != geometryinfo.end())
	{
		FMIDEBUG(cout << "DEBUG: GetGeometryDefinition() cache hit!" << endl);

		return geometryinfo[geometry_name];
	}

	string query =
	    "SELECT"
	    " prjn_name,"
	    " row_cnt,"
	    " col_cnt,"
	    " lat_orig,"
	    " long_orig,"
	    " orig_row_num,"
	    " orig_col_num,"
	    " pas_longitude,"
	    " pas_latitude,"
	    " geom_parm_1,"
	    " geom_parm_2,"
	    " geom_parm_3,"
	    " stor_desc "
	    "FROM grid_reg_geom gr, grid_geom gm "
	    "WHERE gr.geom_name = '" +
	    geometry_name + "' AND gr.geom_name = gm.geom_name";

	map<string, string> ret;

	Query(query);

	vector<string> row = FetchRow();

	if (!row.empty())
	{
		ret["prjn_name"] = row[0];
		ret["row_cnt"] = row[1];
		ret["col_cnt"] = row[2];
		ret["lat_orig"] = row[3];
		ret["long_orig"] = row[4];
		ret["orig_row_num"] = row[5];
		ret["orig_col_num"] = row[6];
		ret["pas_longitude"] = row[7];
		ret["pas_latitude"] = row[8];
		ret["geom_parm_1"] = row[9];
		ret["geom_parm_2"] = row[10];
		ret["geom_parm_3"] = row[11];
		ret["stor_desc"] = row[12];
		ret["prjn_id"] = "";  // compatibility with radon

		geometryinfo[geometry_name] = ret;
	}

	return ret;
}

/*
 * GetStationInfo(int)
 *
 * Retrieves station information from Neons table station.
 *
 * By default this function uses aggressive caching: when first station
 * is requested, all stations are read to memory. As more often that not a
 * large number of stations are requested, this will reduce the amount
 * of SQL queries from N to 1. On the other hand when only a few stations
 * are requested, it will be an overkill. This behaviour can be disabled
 * by setting input argument aggressive_caching to false.
 *
 * Results are placed in an STL map, with currently three mandatory fields:
 * - station name field must be present and named "station_name"
 * - latitude must be present and named "latitude"
 * - longitude must be present and named "longitude"
 *
 * Replaces function
 *
 * int GetDbStationDef(DBStation*)
 *
 * defined at
 *
 * fmidbu/pcsource/getdbstationdef.pc
 *
 */

map<string, string> NFmiNeonsDB::GetStationInfo(unsigned long wmo_id, bool aggressive_cache)
{
	if (stationinfo.find(wmo_id) != stationinfo.end()) return stationinfo[wmo_id];

	string query =
	    "SELECT "
	    "indicatif_omm, "
	    "nom_station, "
	    "round(lat/100000, 4) as lat, "
	    "round(lon/100000, 4) as lon, "
	    "lpnn, "
	    "aws_id, "
	    "country_id, "
	    "CASE WHEN elevation_hp IS NOT NULL THEN elevation_hp "
	    "WHEN elevation_ha IS NOT NULL THEN elevation_ha "
	    "ELSE NULL END AS elevation ,"
	    "indicatif_oaci, "
	    "indicatif_ship, "
	    "niveau_pression, "
	    "elevation_sondage1, "
	    "elevation_sondage2, "
	    "elevation_hp, "
	    "elevation_ha, "
	    "numero_region_mto, "
	    "station_principale, "
	    "obs00, "
	    "obs03, "
	    "obs06, "
	    "obs09, "
	    "obs12, "
	    "obs15, "
	    "obs18, "
	    "obs21, "
	    "obsalti00, "
	    "obsalti06, "
	    "obsalti12, "
	    "obsalti18, "
	    "heure00, "
	    "heure06, "
	    "heure12, "
	    "heure18 "
	    "FROM "
	    "station "
	    "WHERE "
	    "indicatif_omm IS NOT NULL";

	/*
	 * If aggressive_cache is not set, query only for the individual station.
	 * Also, if aggressive_cache is set and map stationinfo is already populated
	 * do not fetch again all stations. This condition will happen when a station
	 * requested does not exist.
	 */

	if (!aggressive_cache || (aggressive_cache && stationinfo.size() > 0))
		query += " AND indicatif_omm = " + to_string(wmo_id);

	Query(query);

	while (true)
	{
		vector<string> values = FetchRow();

		map<string, string> station;

		if (values.empty()) break;

		int wid = std::stoi(values[0]);

		station["indicatif_omm"] = values[0];
		station["station_name"] = values[1];
		station["latitude"] = values[2];
		station["longitude"] = values[3];
		station["lpnn"] = values[4];
		station["aws_id"] = values[5];
		station["country_id"] = values[6];
		station["elevation"] = values[7];
		// legacy
		station["indicatif_oaci"] = values[8];
		station["indicatif_ship"] = values[9];
		station["niveau_pression"] = values[10];
		station["elevation_sondage1"] = values[11];
		station["elevation_sondage2"] = values[12];
		station["elevation_hp"] = values[13];
		station["elevation_ha"] = values[14];
		station["numero_region_mto"] = values[15];
		station["station_principale"] = values[16];
		station["obs00"] = values[17];
		station["obs03"] = values[18];
		station["obs06"] = values[19];
		station["obs09"] = values[20];
		station["obs12"] = values[21];
		station["obs15"] = values[22];
		station["obs18"] = values[23];
		station["obs21"] = values[24];
		station["obsalti00"] = values[25];
		station["obsalti06"] = values[26];
		station["obsalti12"] = values[27];
		station["obsalti18"] = values[28];
		station["heure00"] = values[29];
		station["heure06"] = values[30];
		station["heure12"] = values[31];
		station["heure18"] = values[32];

		stationinfo[wid] = station;
	}

	map<string, string> ret;

	if (stationinfo.find(wmo_id) != stationinfo.end())
		ret = stationinfo[wmo_id];
	else
		// If station does not exist, place empty map as a placeholder
		stationinfo[wmo_id] = ret;

	return ret;
}

/*
 * GetStationListForArea(float,float,float,float,bool)
 *
 * Retrieves a list of stations from Neons table station based on
 * given max/min lat/lon coordinates. If last argument (bool temp)
 * is false (default), retrieve all stations. If it is true, retrieve
 * only sounding stations.
 *
 * Results are placed in an STL map, with currently three mandatory fields:
 * - station name field must be present and named "station_name"
 * - latitude must be present and named "latitude"
 * - longitude must be present and named "longitude"
 *
 * Replaces functions
 *
 * int GetDbStationList(DBStationList*)
 *
 * defined at
 *
 * fmidbu/pcsource/getdbstationlist.pc
 *
 * and
 *
 * int GetDBStationListForTemp(DBStationList*)
 *
 * defined at
 *
 * fmidbu/pcsource/getdbstationlistfortemp.pc
 *
 */

map<int, map<string, string> > NFmiNeonsDB::GetStationListForArea(double max_latitude, double min_latitude,
                                                                  double max_longitude, double min_longitude, bool temp)
{
	map<int, map<string, string> > stationlist;

	string query =
	    "SELECT "
	    "indicatif_omm, "
	    "indicatif_oaci, "
	    "indicatif_ship, "
	    "indicatif_insee, "
	    "nom_station, "
	    "lat/100000, "
	    "lon/100000, "
	    "lpnn, "
	    "aws_id, "
	    "country_id, "
	    "CASE WHEN elevation_hp IS NOT NULL THEN elevation_hp "
	    "WHEN elevation_ha IS NOT NULL THEN elevation_ha "
	    "ELSE NULL END AS elevation "
	    "FROM "
	    "station "
	    "WHERE "
	    "indicatif_omm IS NOT NULL "
	    "AND "
	    "lat/100000 BETWEEN " +
	    to_string(min_latitude) + " AND " + to_string(max_latitude) +
	    " AND "
	    "lon/100000 BETWEEN " +
	    to_string(min_longitude) + " AND " + to_string(max_longitude);

	if (temp)
		query +=
		    " AND ( "
		    "(OBSALTI00 LIKE '%W%' OR OBSALTI00  LIKE '%P%')"
		    "OR (OBSALTI06  LIKE '%W%' OR OBSALTI06  LIKE '%P%')"
		    "OR (OBSALTI12  LIKE '%W%' OR OBSALTI12  LIKE '%P%')"
		    "OR (OBSALTI18  LIKE '%W%' OR OBSALTI18  LIKE '%P%'))";

	query += " ORDER BY lat DESC, lon";

	Query(query);

	while (true)
	{
		vector<string> values = FetchRow();

		map<string, string> station;

		if (values.empty()) break;

		int wid = std::stoi(values[0]);

		station["indicatif_omm"] = values[0];
		station["station_name"] = values[4];
		station["latitude"] = values[5];
		station["longitude"] = values[6];
		station["lpnn"] = values[7];
		station["aws_id"] = values[8];
		station["country_id"] = values[9];
		station["elevation"] = values[10];

		stationlist[wid] = station;

		/*
		 * Fill stationinfo also. This implies that when later on
		 * GetStationInfo() is called, it will not fetch the station list
		 * but uses this information.
		 *
		 * This should not be a problem since when querying data for an area
		 * only include stations that are inside that area. This could be a
		 * problem if in one par we would have an area query and that query
		 * would contain stations outside the area, but AFAIK that is impossible
		 * since parfile can only contain EITHER station id OR coordinates, not both.
		 *
		 */

		stationinfo[wid] = station;
	}

	return stationlist;
}

void NFmiNeonsDB::SQLDateMask(const std::string& theDateMask)
{
	/*
	 * Set date mask to class variable, not to database.
	 * This is used in connection pool to store the required
	 * date mask value. The date mask is set only when it is actually
	 * required (ie. during the first Query() because lots of NFmiNeonsDB
	 * functionality is just to return values from cache and connecting
	 * to database every time is time consuming.
	 */

	date_mask_sql_ = theDateMask;
}

NFmiNeonsDBPool* NFmiNeonsDBPool::itsInstance = NULL;

NFmiNeonsDBPool* NFmiNeonsDBPool::Instance()
{
	if (!itsInstance)
	{
		itsInstance = new NFmiNeonsDBPool();
	}

	return itsInstance;
}

NFmiNeonsDBPool::NFmiNeonsDBPool()
    : itsMaxWorkers(2),
      itsWorkingList(itsMaxWorkers, -1),
      itsWorkerList(itsMaxWorkers, NULL),
      itsExternalAuthentication(false),
      itsReadWriteTransaction(false),
      itsUsername(""),
      itsPassword(""),
      itsDatabase("")
{
}

NFmiNeonsDBPool::~NFmiNeonsDBPool()
{
	for (unsigned int i = 0; i < itsWorkerList.size(); i++)
	{
		itsWorkerList[i]->Detach();
		delete itsWorkerList[i];
	}
	itsWorkerList.clear();
	itsWorkingList.clear();
	delete itsInstance;
}
/*
 * GetConnection()
 *
 * Returns a read-only connection to Neons. When calling program has
 * finished, it should return the connection to the pool.
 *
 * TODO: smart pointers ?
 *
 */

NFmiNeonsDB* NFmiNeonsDBPool::GetConnection()
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
				assert(itsWorkingList[i]);

				itsWorkerList[i]->SQLDateMask("YYYYMMDDHH24MISS");

				FMIDEBUG(cout << "DEBUG: Worker returned with id " << itsWorkerList[i]->Id() << endl);

				return itsWorkerList[i];
			}
			else if (itsWorkingList[i] == -1)
			{
				// Create new connection
				try
				{
					itsWorkerList[i] = new NFmiNeonsDB(i);
					itsWorkerList[i]->PooledConnection(true);

					if (itsExternalAuthentication)
					{
						itsWorkerList[i]->user_ = "";
						itsWorkerList[i]->password_ = "";
					}
					else if (itsUsername != "" && itsPassword != "")
					{
						itsWorkerList[i]->user_ = itsUsername;
						itsWorkerList[i]->password_ = itsPassword;
					}

					if (itsDatabase != "")
					{
						itsWorkerList[i]->database_ = itsDatabase;
					}

					itsWorkerList[i]->Verbose(true);
					itsWorkerList[i]->Attach();
					itsWorkerList[i]->SQLDateMask("YYYYMMDDHH24MISS");

					itsWorkingList[i] = 1;

					FMIDEBUG(cout << "DEBUG: Worker returned with id " << itsWorkerList[i]->Id() << endl);

					return itsWorkerList[i];
				}
				catch (int e)
				{
					throw e;
				}
			}
		}

// All threads active
#ifdef DEBUG
		cout << "DEBUG: Waiting for worker release" << endl;
#endif

		usleep(100000);  // 100 ms
	}

	throw runtime_error("Impossible error at NFmiNeonsDBPool::GetConnection()");
}

/*
 * Release()
 *
 * Clears the database connection (does not disconnect!) and returns it
 * to pool.
 */

void NFmiNeonsDBPool::Release(NFmiNeonsDB* theWorker)
{
	lock_guard<mutex> lock(itsReleaseMutex);

	theWorker->Rollback();
	theWorker->EndSession();
	itsWorkingList[theWorker->Id()] = 0;

	FMIDEBUG(cout << "DEBUG: Worker released for id " << theWorker->Id() << endl);
}

void NFmiNeonsDBPool::MaxWorkers(int theMaxWorkers)
{
	if (theMaxWorkers == itsMaxWorkers) return;

	// Making pool smaller is not supported

	if (theMaxWorkers < itsMaxWorkers)
		throw runtime_error("Making NeonsDB pool size smaller is not supported (" +
		                    to_string(itsMaxWorkers) + " to " +
		                    to_string(theMaxWorkers) + ")");

	itsMaxWorkers = theMaxWorkers;

	itsWorkingList.resize(itsMaxWorkers, -1);
	itsWorkerList.resize(itsMaxWorkers, NULL);
}
