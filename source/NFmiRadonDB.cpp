#include <NFmiRadonDB.h>
#include <NFmiODBC.h>
#include <boost/lexical_cast.hpp>
#include <stdexcept>
#include <algorithm>

using namespace std;

#pragma GCC diagnostic ignored "-Wwrite-strings"

NFmiRadonDB& NFmiRadonDB::Instance() {
  static NFmiRadonDB instance_;
  return instance_; 
}

NFmiRadonDB::NFmiRadonDB(short theId) : NFmiODBC("neons_client", "kikka8si", "radon"), itsId(theId) {}

NFmiRadonDB::~NFmiRadonDB() {
  Disconnect();              
}

void NFmiRadonDB::Connect(const int threadedMode) {
  NFmiODBC::Connect(threadedMode);

/*  try
  {
    putenv("TZ=utc"); // for sqllite
    Execute("SELECT load_extension('libspatialite.so.5')");
    Execute("SELECT load_extension('libsqlitefunctions')");
  }
  catch (...)
  {}
*/
} 

void NFmiRadonDB::Connect(const std::string & user, const std::string & password, const std::string & database, const int threadedMode) {
  NFmiODBC::Connect(user,password,database,threadedMode);
/*
  try
  {
    putenv("TZ=utc"); // for sqllite
    Execute("SELECT load_extension('libspatialite.so.5')");
    Execute("SELECT load_extension('libsqlitefunctions')");
  }
  catch (...)
  {}
*/
}


map<string, string> NFmiRadonDB::GetProducerFromGrib(long centre, long process)
{
	using boost::lexical_cast;

	string key = lexical_cast<string> (centre) + "_" + lexical_cast<string> (process);

	if (gribproducerinfo.find(key) != gribproducerinfo.end())
	{
#ifdef DEBUG
		cout << "DEBUG: GetProducerFromGrib() cache hit!" << endl;
#endif
		return gribproducerinfo[key];
	}

	stringstream query;

	query << "SELECT f.id, f.name, f.class_id FROM fmi_producer f, producer_grib p "
			<< "WHERE f.id = p.producer_id AND p.centre = " << centre
			<< " AND p.ident = " << process;


	Query(query.str());

	vector<string> row = FetchRow();

	map<string,string> ret;

	if (row.empty())
	{
		//gridparamid[key] = -1;
#ifdef DEBUG
		cout << "DEBUG Producer not found\n";
#endif
	}
	else
	{
		ret["id"] = row[0];
		ret["name"] = row[1];
		ret["class_id"] = row[2];
		ret["centre"] = lexical_cast<string> (centre);
		ret["ident"] = lexical_cast<string> (process);

		gribproducerinfo[key] = ret;

	}

	return ret;
}

map<string, string> NFmiRadonDB::GetParameterFromNewbaseId(unsigned long producer_id, unsigned long universal_id)
{
  string key = boost::lexical_cast<string> (producer_id) + "_" + boost::lexical_cast<string> (universal_id);

  if (paramnewbaseinfo.find(key) != paramnewbaseinfo.end()) {
#ifdef DEBUG
    cout << "DEBUG: GetParameterFromNewbaseId() cache hit!" << endl;
#endif
    return paramnewbaseinfo[key];
  }
  
  map <string, string> ret;
  
  string prod_id = boost::lexical_cast<string> (producer_id);
  string univ_id = boost::lexical_cast<string> (universal_id);
  
  map <string, string> producer_info = GetProducerDefinition(producer_id);

  if (producer_info.empty())
	  return ret;


  stringstream query;
  
  query << "SELECT "
		<<"p.name, "
		<< "g.base, "
		<< "g.scale, "
		<< "g.univ_id "
		<< "FROM param_newbase g, param p "
		<< "WHERE x.producer_id = f.producer_id"
		<< " AND p.id = g.param_id "
		<< " AND g.univ_id = " << univ_id
		<< " AND g.producer_id = " << producer_id;

  Query(query.str());
   
  vector<string> row = FetchRow();
  
  if (row.empty())
    return ret;
    
  ret["parm_name"] = row[0];
  ret["base"] = row[1];
  ret["scale"] = row[2];
  ret["univ_id"] = row[3];
 // ret["parm_desc"] = row[4];
 // ret["unit_desc"] = row[5];
 // ret["col_name"] = row[6];

  paramnewbaseinfo[key] = ret;
  
  return ret;
  
}

map<string,string> NFmiRadonDB::GetParameterFromDatabaseName(long producerId, const string& parameterName)
{
	string key = boost::lexical_cast<string> (producerId) + "_" + parameterName;
	
	if (paramdbinfo.find(key) != paramdbinfo.end()) {
#ifdef DEBUG
	   cout << "DEBUG: GetParameterFromDatabaseName() cache hit!" << endl;
	   return paramdbinfo[key];
#endif
	}
	
	stringstream query;
	
	map<string,string> ret;
	
	query << "SELECT param_id,param_name,param_version,grib1_table_version,grib1_number,grib2_discipline,grib2_category,grib2_number FROM producer_param_v WHERE producer_id = " << producerId
			<< " AND param_name = '" << parameterName << "'";
	
	Query(query.str());
	
	auto row = FetchRow();
	
	if (row.empty()) return ret;
	
	ret["id"] = row[0];
	ret["name"] = row[1];
	ret["version"] = row[2];
	ret["grib1_table_version"] = row[3];
	ret["grib1_number"] = row[4];
	ret["grib2_discipline"] = row[5];
	ret["grib2_category"] = row[6];
	ret["grib2_number"] = row[7];
	
	paramdbinfo[key] = ret;
	return ret;
	
}
/*
string NFmiRadonDB::GetGridParameterName(long InParmId, long InCodeTableVer, long OutCodeTableVer, long timeRangeIndicator, long levelType)
{

  string parm_name;
  string univ_id;
  string parm_id = boost::lexical_cast<string>(InParmId);
  string no_vers = boost::lexical_cast<string>(InCodeTableVer);
  string no_vers2 = boost::lexical_cast<string>(OutCodeTableVer);
  string trInd = boost::lexical_cast<string>(timeRangeIndicator);
  string levType = boost::lexical_cast<string> (levelType);

  // Implement some sort of caching: this function is quite expensive

  string key = parm_id + "_" + no_vers + "_" + no_vers2 + "_" + trInd + "_" + levType;

  if (gridparameterinfo.find(key) != gridparameterinfo.end())
    return gridparameterinfo[key];

  stringstream query;
  
        query << "SELECT x.param_name "
               <<  "FROM param_grib1_v x, param_newbase y"
               <<  " WHERE y.univ_id = " << parm_id 
               <<  " AND x.param_id = y.param_id"
               <<  " AND x.table_version = " << no_vers2
               <<  " AND x.timerange_indicator = " << trInd 
               <<  " AND x.level_id IS NULL OR x.level_id = " << levType
               <<  " ORDER BY x.level_id NULLS LAST";

  Query(query.str());
  vector<string> row = FetchRow();

  if (!row.empty())
    parm_name = row[0];

  else
    return "";

  gridparameterinfo[key] = parm_name;
  return gridparameterinfo[key];
}
*/


map<string, string> NFmiRadonDB::GetParameterFromGrib1(long producerId, long tableVersion, long paramId, long timeRangeIndicator, long levelId, double levelValue)
{

	using boost::lexical_cast;

	string key = lexical_cast<string> (producerId) + "_" + lexical_cast<string> (tableVersion) + "_" +
		lexical_cast<string> (paramId) + "_" + lexical_cast<string> (timeRangeIndicator) + "_" + lexical_cast<string> (levelId) + lexical_cast<string> (levelValue) ;

	if (paramgrib1info.find(key) != paramgrib1info.end())
	{
#ifdef DEBUG
	   cout << "DEBUG: ParameterFromGrib1() cache hit!" << endl;
#endif
		return paramgrib1info[key];
	}

	stringstream query;

	query << "SELECT p.id, p.name, p.version, u.name AS unit_name, p.interpolation_id, i.name AS interpolation_name, g.level_id "
			<< "FROM param_grib1 g, param p, param_unit u, interpolation_method i, fmi_producer f "
			<< "WHERE g.param_id = p.id AND p.unit_id = u.id AND p.interpolation_id = i.id AND f.id = g.producer_id "
			<< " AND f.id = " << producerId
			<< " AND table_version = " << tableVersion
			<< " AND number = " << paramId
			<< " AND timerange_indicator = " << timeRangeIndicator
			<< " AND (level_id IS NULL OR level_id = " << levelId << ")"
			<< " AND (level_value IS NULL OR level_value = " << levelValue << ")"
			<< " ORDER BY level_id NULLS LAST, level_value NULLS LAST LIMIT 1"
		;

	Query(query.str());

	vector<string> row = FetchRow();

	map<string,string> ret;

	if (row.empty())
	{
#ifdef DEBUG
		cout << "DEBUG Parameter not found\n";
#endif
	}
	else
	{
		ret["id"] = row[0];
		ret["name"] = row[1];
		ret["version"] = row[2];
		ret["grib1_table_version"] = lexical_cast<string> (tableVersion);
		ret["grib1_number"] = lexical_cast<string> (paramId);
		ret["interpolation_method"] = row[4];
		ret["level_id"] = row[5];
		ret["level_value"] = row[6];

		paramgrib1info[key] = ret;

	}

	return ret;
}

map<string, string> NFmiRadonDB::GetParameterFromGrib2(long producerId, long discipline, long category, long paramId, long levelId, double levelValue)
{
	using boost::lexical_cast;

	string key = lexical_cast<string> (producerId) + "_" + lexical_cast<string> (discipline) + "_" + 
		lexical_cast<string> (category) + "_" + lexical_cast<string> (paramId) + "_" + lexical_cast<string> (levelId) + "_" + lexical_cast<string> (levelValue) ;

	if (paramgrib2info.find(key) != paramgrib2info.end())
	{
#ifdef DEBUG
	   cout << "DEBUG: ParameterFromGrib2() cache hit!" << endl;
#endif
           return paramgrib2info[key];
	}

	stringstream query;

	query << "SELECT p.id, p.name, p.version, u.name AS unit_name, p.interpolation_id, i.name AS interpolation_name, g.level_id "
			<< "FROM param_grib2 g, param p, param_unit u, interpolation_method i, fmi_producer f "
			<< "WHERE g.param_id = p.id AND p.unit_id = u.id AND p.interpolation_id = i.id AND f.id = g.producer_id "
			<< " AND f.id = " << producerId
			<< " AND discipline = " << discipline
			<< " AND category = " << category
			<< " AND number = " << paramId
			<< " AND (level_id IS NULL OR level_id = " << levelId << ")"
			<< " AND (level_value IS NULL OR level_value = " << levelValue << ")"
			<< " ORDER BY level_id NULLS LAST, level_value NULLS LAST LIMIT 1"
		;

	Query(query.str());

	vector<string> row = FetchRow();

	map<string,string> ret;
	
	if (row.empty())
	{
#ifdef DEBUG
		cout << "DEBUG Parameter not found\n";
#endif
	}
	else
	{
		ret["id"] = row[0];
		ret["name"] = row[1];
		ret["version"] = row[2];
		ret["grib2_discipline"] = lexical_cast<string> (discipline);
		ret["grib2_category"] = lexical_cast<string> (category);
		ret["grib2_number"] = lexical_cast<string> (paramId);
		ret["interpolation_method"] = row[4];
		ret["level_id"] = row[5];
		ret["level_value"] = row[6];

		paramgrib2info[key] = ret;
	}

	return ret;
}

map<string, string> NFmiRadonDB::GetParameterFromNetCDF(long producerId, const string& paramName, long levelId, double levelValue)
{
	using boost::lexical_cast;

	string key = lexical_cast<string> (producerId) + "_" + paramName + "_" + lexical_cast<string> (levelId) + "_" + lexical_cast<string> (levelValue) ;

	if (paramnetcdfinfo.find(key) != paramnetcdfinfo.end())
	{
#ifdef DEBUG
	   cout << "DEBUG: GetParameterFromNetCDF() cache hit!" << endl;
#endif
		return paramnetcdfinfo[key];
	}

	stringstream query;

	query << "SELECT p.id, p.name, p.version, u.name AS unit_name, p.interpolation_id, i.name AS interpolation_name, g.level_id, g.level_value "
			<< "FROM param_netcdf g, param p, param_unit u, interpolation_method i, fmi_producer f "
			<< "WHERE g.param_id = p.id AND p.unit_id = u.id AND p.interpolation_id = i.id AND f.id = g.producer_id "
			<< " AND f.id = " << producerId
			<< " AND g.netcdf_name = '" << paramName << "'"
			<< " AND (level_id IS NULL OR level_id = " << levelId << ")"
			<< " AND (level_value IS NULL OR level_value = " << levelValue << ")"
			<< " ORDER BY level_id NULLS LAST, level_value NULLS LAST LIMIT 1"
		;

	Query(query.str());

	vector<string> row = FetchRow();

	map<string,string> ret;

	if (row.empty())
	{
#ifdef DEBUG
		cout << "DEBUG Parameter not found\n";
#endif
	}
	else
	{
		ret["id"] = row[0];
		ret["name"] = row[1];
		ret["version"] = row[2];
		ret["netcdf_name"] = lexical_cast<string> (paramName);
		ret["interpolation_method"] = row[4];
		ret["level_id"] = row[5];
		ret["level_value"] = row[6];

		paramnetcdfinfo[key] = ret;
	}

	return ret;
}

map<string, string> NFmiRadonDB::GetLevelFromGrib(long producerId, long levelNumber, long edition)
{
	using boost::lexical_cast;

	string key = lexical_cast<string> (producerId) + "_" + lexical_cast<string> (levelNumber) + "_" + lexical_cast<string> (edition);

	if (levelinfo.find(key) != levelinfo.end())
	{
#ifdef DEBUG
	   cout << "DEBUG: GetLevelFromGrib() cache hit!" << endl;
#endif
		return levelinfo[key];
	}
		
	stringstream query;

	query << "SELECT id, name "
			<< "FROM level l, " << (edition == 2 ? "level_grib2 g " : "level_grib1 g")
			<< " WHERE l.id = g.level_id "
			<< " AND g.producer_id = " << producerId
			<< " AND g.grib_level_id = " << levelNumber
	;

	Query(query.str());

	vector<string> row = FetchRow();

	map<string,string> ret;

	if (row.empty())
	{
#ifdef DEBUG
		cout << "DEBUG Level not found\n";
#endif
	}
	else
	{
		ret["id"] = row[0];
		ret["name"] = row[1];
		ret["grib1Number"] = lexical_cast<string> (levelNumber);

		//param_grib1[key] = p
		//gridparamid[key] = boost::lexical_cast<long> (row[0]);

		levelinfo[key] = ret;
	}

	return ret;


}

vector<vector<string> > NFmiRadonDB::GetGridGeoms(const string& ref_prod, const string& analtime, const string& geom_name) 
{

  string key = ref_prod + "_" + analtime + "_" + geom_name;
  if (gridgeoms.count(key) > 0) {
#ifdef DEBUG
  cout << "DEBUG: GetGridGeoms() cache hit!" << endl;
#endif
    return gridgeoms[key];
  }
  
  stringstream query;
  
        query << "SELECT as_grid.geometry_id, as_grid.table_name, as_grid.id"
	        <<  " FROM as_grid, fmi_producer, geom_v"
	        <<  " WHERE as_grid.record_count > 0"
	        <<  " AND fmi_producer.name like '" << ref_prod << "'"
			<<  " AND as_grid.producer_id = fmi_producer.id"
	        <<  " AND as_grid.analysis_time = '" << analtime << "'"
			<<  " AND as_grid.geometry_id = geom_v.geom_id";

  if (!geom_name.empty())
  {
	  query << " AND geom_v.geom_name = '" << geom_name << "'";
  }

  Query(query.str());

  vector<vector<string> > ret;

  while (true) {
	vector<string> values = FetchRow();

	if (values.empty()) {
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
#ifdef DEBUG
    cout << "DEBUG: GetGeometryDefinition() cache hit!" << endl;
#endif

    return geometryinfo[geom_name];
  }

  // find geom name corresponding id

  stringstream query;

  query.str("");
   
  query << "SELECT"
	  <<   " projection_id,"
	  <<   " projection_name,"
	  <<   " ni,"
	  <<   " nj,"
	  <<   " first_lat,"
	  <<   " first_lon,"
	  <<   " di/1e5,"
	  <<   " dj/1e5, "
	  <<   " geom_parm_1,"
	  <<   " geom_parm_2,"
	  <<   " geom_parm_3,"
	  <<   " scanning_mode "
	  <<   "FROM geom_v, projection p "
	  <<   "WHERE geom_name = '" << geom_name << "'";

  map <string, string> ret;

  Query(query.str());

  vector<string> row = FetchRow();

  if (!row.empty()) {
    ret["prjn_id"] = row[0];
    ret["prjn_name"] = row[1];
    ret["row_cnt"] = row[3];
    ret["col_cnt"] = row[2];
    ret["lat_orig"] = row[4];
    ret["long_orig"] = row[5];
    ret["pas_longitude"] = row[6];
    ret["pas_latitude"] = row[7];
	ret["geom_parm_1"] = row[8];
    ret["geom_parm_2"] = row[9];
    ret["geom_parm_3"] = row[10];
	ret["stor_desc"] = row[11];
	
    geometryinfo[geom_name] = ret;
  }

  return ret;

}

map<string, string> NFmiRadonDB::GetGeometryDefinition(size_t ni, size_t nj, double lat, double lon, double di, double dj, int gribedition, int gridtype) {

	string key = boost::lexical_cast<string> (ni) + "_" +
			boost::lexical_cast<string> (nj) + "_" +
			boost::lexical_cast<string> (lat) + "_" +
			boost::lexical_cast<string> (lon) + "_" +
			boost::lexical_cast<string> (di) + "_" +
			boost::lexical_cast<string> (dj) + "_" +
			boost::lexical_cast<string> (gribedition) + "_" +
			boost::lexical_cast<string> (gridtype);
			
  if (geometryinfo_fromarea.find(key) != geometryinfo_fromarea.end())
  {
#ifdef DEBUG
    cout << "DEBUG: GetGeometryDefinition() cache hit!" << endl;
#endif

    return geometryinfo_fromarea[key];
  }

  stringstream query;
	
  query << "SELECT g.id,g.name FROM geom g, projection p "
            << "WHERE g.projection_id = p.id"
            << " AND nj = " << nj
            << " AND ni = " << ni
            << " AND 1000 * st_x(first_point) = " << lon
            << " AND 1000 * st_y(first_point) = " << lat
            << " AND 1000 * di = " << di
            << " AND 1000 * dj = " << dj
            << " AND p.grib" << gribedition << "_number = " << gridtype;

  map <string, string> ret;

  Query(query.str());

  vector<string> row = FetchRow();

  if (!row.empty()) {
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
#ifdef DEBUG
    cout << "DEBUG: GetProducerDefinition() cache hit!" << endl;
#endif
    return producerinfo[producer_id];
  }
  
  stringstream query;
  
  query << "SELECT f.id, f.name, f.class_id, g.centre, g.ident "
        << "FROM fmi_producer f, producer_grib g "
        << "WHERE f.id = " << producer_id
        << " AND f.id = g.producer_id";

  map <string, string> ret;
  
  Query(query.str());

  vector<string> row = FetchRow();

  if (!row.empty()) {
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
        << "FROM fmi_producers"
        << " WHERE name = '" << producer_name << "'";

  Query(query.str());
  
  vector<string> row = FetchRow();
  
  unsigned long int producer_id = boost::lexical_cast<unsigned long> (row[0]);
  
  if (producerinfo.find(producer_id) != producerinfo.end())
    return producerinfo[producer_id];
 
  return GetProducerDefinition(row[0]);
    
}

string NFmiRadonDB::GetLatestTime(const std::string& ref_prod, const std::string& geom_name, unsigned int offset)
{

  stringstream query;
  
  query <<  "SELECT analysis_time "
 	<<  "FROM as_grid_v"
	<<  " WHERE producer_name = '" << ref_prod
	<<  "' AND record_count > 0 ";

  if (!geom_name.empty())
  {
    query << " AND geometry_name = '" << geom_name << "'";
  }
  
  query << " ORDER BY analysis_time DESC LIMIT 1 OFFSET " << offset;
  
  Query(query.str());

  vector<string> row = FetchRow();

  if (row.size() == 0)
  {
    return "";
  }

  return row[0];
}

map<string, string> NFmiRadonDB::GetStationDefinition(FmiRadonStationNetwork networkType, unsigned long stationId, bool aggressive_cache)
{

  assert(!aggressive_cache); // not supported yet
  
  string key = boost::lexical_cast<string> (networkType) + "_" + boost::lexical_cast<string> (stationId);
  
  if (stationinfo.find(key) != stationinfo.end())
    return stationinfo[key];

  stringstream query;
  
  map<string,string> ret;
  
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
		  << "LEFT OUTER JOIN station_network_mapping wmo ON (s.id = wmo.station_id AND wmo.network_id = 1) "
		  << "LEFT OUTER JOIN station_network_mapping icao ON (s.id = icao.station_id AND icao.network_id = 2) "
		  << "LEFT OUTER JOIN station_network_mapping lpnn ON (s.id = lpnn.station_id AND lpnn.network_id = 3) "
		  << "LEFT OUTER JOIN station_network_mapping rw ON (s.id = rw.station_id AND rw.network_id = 4) "
		  << "LEFT OUTER JOIN station_network_mapping fs ON (s.id = fs.station_id AND fs.network_id = 5) ";
  
  switch (networkType)
  {
	  case kWMONetwork:
	  case kICAONetwork:
	  case kLPNNNetwork:
	  case kRoadWeatherNetwork:
	  default:
		  throw runtime_error("Unsupported station network type: " + boost::lexical_cast<string> (networkType));
		  break;
	  case kFmiSIDNetwork:
		  query << "JOIN station_network_mapping m ON (s.id = m.station_id AND m.network_id = 5 AND m.local_station_id = '" << stationId << "')";
		  break;
  }
		  
  Query(query.str());
  cout << query.str() << endl;
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
  : itsMaxWorkers(2)
  , itsWorkingList(itsMaxWorkers, -1)
  , itsWorkerList(itsMaxWorkers, NULL)
  , itsUsername("")
  , itsPassword("")
  , itsDatabase("")
{}

NFmiRadonDBPool::~NFmiRadonDBPool()
{
  for (unsigned int i = 0; i < itsWorkerList.size(); i++) {
    if (itsWorkerList[i]) {   
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

NFmiRadonDB * NFmiRadonDBPool::GetConnection() {
 
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

  while (true) {

    for (unsigned int i = 0; i < itsWorkingList.size(); i++) {
      // Return connection that has been initialized but is idle
      if (itsWorkingList[i] == 0) {
        itsWorkingList[i] = 1;


#ifdef DEBUG
        cout << "DEBUG: Idle worker returned with id " << itsWorkerList[i]->Id() << endl;
#endif
        return itsWorkerList[i];
      
      } 
	  else if (itsWorkingList[i] == -1) {
        // Create new connection
        itsWorkerList[i] = new NFmiRadonDB(i);

        if (itsUsername != "" && itsPassword != "")
        {
      	  itsWorkerList[i]->user_ = itsUsername;
       	  itsWorkerList[i]->password_ = itsPassword;
        }

        if (itsDatabase != "")
        {
      	  itsWorkerList[i]->database_ = itsDatabase;
        }

        itsWorkerList[i]->Connect(1);  

        itsWorkingList[i] = 1;

#ifdef DEBUG
        cout << "DEBUG: New worker returned with id " << itsWorkerList[i]->Id() << endl;
#endif
        return itsWorkerList[i];
    
	  }
    }

    // All threads active
#ifdef DEBUG
    cout << "DEBUG: Waiting for worker release" << endl;
#endif

    usleep(100000); // 100 ms  

  }   
}

/*
 * Release()
 * 
 * Clears the database connection (does not disconnect!) and returns it
 * to pool.
 */

void NFmiRadonDBPool::Release(NFmiRadonDB *theWorker) {
  
  theWorker->Rollback();
  itsWorkingList[theWorker->Id()] = 0;

#ifdef DEBUG
  cout << "DEBUG: Worker released for id " << theWorker->Id() << endl;
#endif

}

void NFmiRadonDBPool::MaxWorkers(int theMaxWorkers) {
  
  if (theMaxWorkers == itsMaxWorkers)
    return;

  // Making pool smaller is not supported
  
  if (theMaxWorkers < itsMaxWorkers)
    throw runtime_error("Making RadonDB pool size smaller is not supported (" + boost::lexical_cast<string> (itsMaxWorkers) + " to " + boost::lexical_cast<string> (theMaxWorkers) + ")");
  
  itsMaxWorkers = theMaxWorkers;

  itsWorkingList.resize(itsMaxWorkers, -1);
  itsWorkerList.resize(itsMaxWorkers, NULL);

}

