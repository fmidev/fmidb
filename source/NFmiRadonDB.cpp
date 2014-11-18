#include <NFmiRadonDB.h>
#include <NFmiODBC.h>
#include <boost/lexical_cast.hpp>
#include <stdexcept>
#include <algorithm>

using namespace std;

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
//  DateFormat("YYYYMMDDHH24MISS");
//  Verbose(true);
} 

void NFmiRadonDB::Connect(const std::string & user, const std::string & password, const std::string & database, const int threadedMode) {
  NFmiODBC::Connect(user,password,database,threadedMode);
 // DateFormat("YYYYMMDDHH24MISS");
 // Verbose(true);
}


map<string, string> NFmiRadonDB::ProducerFromGrib(long centre, long process)
{
	using boost::lexical_cast;

	string key = lexical_cast<string> (centre) + "_" + lexical_cast<string> (process);

	if (producerinfo.find(key) != producerinfo.end())
	{
#ifdef DEBUG
	   cout << "DEBUG: ProducerFromGrib() cache hit!" << endl;
#endif
		return producerinfo[key];
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

		producerinfo[key] = ret;

	}

	return ret;
}

map<string, string> NFmiRadonDB::GetNewbaseParameterDefinition(unsigned long producer_id, unsigned long universal_id)
{
/*
  if (parameterinfo.find(producer_id) != parameterinfo.end()) {
    if (parameterinfo[producer_id].find(universal_id) != parameterinfo[producer_id].end()) {
#ifdef DEBUG
    cout << "DEBUG: GetParameterDefinition() cache hit!" << endl;
#endif
      return parameterinfo[producer_id][universal_id];
    }
  }
  
  */
  
  map <string, string> ret;
  
  string prod_id = boost::lexical_cast<string> (producer_id);
  string univ_id = boost::lexical_cast<string> (universal_id);
  
  map <string, string> producer_info = GetProducerDefinition(producer_id);

  if (producer_info.empty())
	  return ret;

  string no_vers = producer_info["no_vers"];

  stringstream query;
  
        query << "SELECT "
                <<"x.param_name, "
                << "g.base, "
                << "g.scale, "
                << "g.univ_id "
                << "FROM param_newbase g, producer_param_v x "
                << "WHERE x.producer_id = g.producer_id"
                << " AND g.univ_id = " << univ_id
                << " AND x.newbase_id = " << univ_id 
                << " AND x.grib1_table_version = " << no_vers;

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
 // ret["univ_id"] = row[7]; // Fetch univ_id in cased called from GetParameterDefinition(ulong, string)

  newbaseinfo[producer_id][universal_id] = ret;
  
  return ret;
  
}

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
/*
  if (InCodeTableVer == OutCodeTableVer) {
    gridparameterinfo[key] = parm_name;
    return gridparameterinfo[key];
  }
  else 
  {
  
    query << "SELECT univ_id "
          <<  "FROM param_grib1_v "
          <<  "WHERE parm_name like '" << parm_name 
          <<  " AND table_version = " << no_vers;

    Query(query.str());
    vector<string> id = FetchRow();

    if (!id.empty()) 
    {
      univ_id = id[0];
    }
*/
    /* Finally dig out the parm_name on OutCodeTableVer */
    /*
    query <<  "SELECT parm_name "
          <<  "FROM param_grib1_v "
          <<  "WHERE univ_id = " << univ_id 
          <<  "AND table_version = " << boost::lexical_cast<string>(OutCodeTableVer);

    Query(query.str());
    vector<string> param = FetchRow();

    if (!param.empty()) 
    {
      parm_name = param[0];
    }
*/
 /*   query = "SELECT max(producer_class) "
            "FROM fmi_producers "
            "WHERE no_vers = " +boost::lexical_cast<string>(OutCodeTableVer);

    Query(query);
    vector<string> prod_class = FetchRow();
    string pclass;

    if (!prod_class.empty()) {
      pclass = prod_class[0];
      // Switch the -'s to _'s for point producers 
      if( (pclass == "2") | (pclass == "3") ) {
        parm_name.replace(parm_name.find("-"), 1, "_");
      }
    
 
    } 
  }*/

  gridparameterinfo[key] = parm_name;
  return gridparameterinfo[key];
}



map<string, string> NFmiRadonDB::ParameterFromGrib1(long producerId, long tableVersion, long paramId, long timeRangeIndicator, long levelId, double levelValue)
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

map<string, string> NFmiRadonDB::ParameterFromGrib2(long producerId, long discipline, long category, long paramId, long levelId, double levelValue)
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

map<string, string> NFmiRadonDB::ParameterFromNetCDF(long producerId, const string& paramName, long levelId, double levelValue)
{
	using boost::lexical_cast;

	string key = lexical_cast<string> (producerId) + "_" + paramName + "_" + lexical_cast<string> (levelId) + "_" + lexical_cast<string> (levelValue) ;

	if (paramnetcdfinfo.find(key) != paramnetcdfinfo.end())
	{
#ifdef DEBUG
	   cout << "DEBUG: ParameterFromNetCDF() cache hit!" << endl;
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

map<string, string> NFmiRadonDB::LevelFromGrib(long producerId, long levelNumber, long edition)
{
	using boost::lexical_cast;

	string key = lexical_cast<string> (producerId) + "_" + lexical_cast<string> (levelNumber) + "_" + lexical_cast<string> (edition);

	if (levelinfo.find(key) != levelinfo.end())
	{
#ifdef DEBUG
	   cout << "DEBUG: LevelFromGrib() cache hit!" << endl;
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
  
        query << "SELECT geometry_id, table_name, id "
	        <<  "FROM as_grid "
	        <<  "WHERE record_count > 0"
	        <<  " AND model_type like " << ref_prod
	        <<  " AND last_updated = " << analtime;

  if (!geom_name.empty())
  {
	  query << " AND geometry_id = " << geom_name;
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
/*
    
  if (geometryinfo.find(geometry_id) != geometryinfo.end())
  {
#ifdef DEBUG
    cout << "DEBUG: GetGeometryDefinition() cache hit!" << endl;
#endif

    return geometryinfo[geometry_name];
  }
  
  */
  // find geom name corresponding id

  stringstream query;
 /* string geom_name;
  
  query << "SELECT name from geom where id = " << geometry_id;
  
  Query(query.str());
  vector<string> row = FetchRow();

  if (!row.empty()) {
    geom_name = row[0];
  }
  else {
    geom_name = "";
  }  */
  
  query.str("");
   
        query << "SELECT"
              <<   " geom_name,"
              <<   " ni,"
              <<   " nj,"
              <<   " first_lat,"
              <<   " first_lon,"
              <<   " di,"
              <<   " dj "
              <<   "FROM geom_v "
              <<   "WHERE geom_name = '" << geom_name << "'";

  map <string, string> ret;

  Query(query.str());

  vector<string> row = FetchRow();

  if (!row.empty()) {
    ret["prjn_name"] = row[0];
    ret["row_cnt"] = row[1];
    ret["col_cnt"] = row[2];
    ret["lat_orig"] = row[3];
    ret["long_orig"] = row[4];
    ret["pas_longitude"] = row[5];
    ret["pas_latitude"] = row[6];
 //   ret["orig_row_num"] = row[5];
  //  ret["orig_col_num"] = row[6];
 /*   ret["pas_longitude"] = row[7];
    ret["pas_latitude"] = row[8];
    ret["geom_parm_1"] = row[9];
    ret["geom_parm_2"] = row[10];
    ret["geom_parm_3"] = row[11];
    ret["stor_desc"] = row[12];
   */ 

    geometryinfo[geom_name] = ret;
  }

  return ret;

}


map<string, string> NFmiRadonDB::GetProducerDefinition(unsigned long producer_id) 
{
/*
  if (producerinfo.count(producer_id) > 0)
  {
#ifdef DEBUG
    cout << "DEBUG: GetProducerDefinition() cache hit!" << endl;
#endif
    return producerinfo[producer_id];
  }  */
  using boost::lexical_cast;

  string id = lexical_cast<string> (producer_id);
  stringstream query;
  
        query << "SELECT f.id,f.name,f.class_id,f.last_updated,x.grib1_table_version, x.producer_name "
              <<   "FROM fmi_producer f, producer_param_v x"
              <<   " WHERE f.id = " << producer_id
              <<   " AND x.producer_id = " << producer_id;  

  map <string, string> ret;
  
  Query(query.str());

  vector<string> row = FetchRow();

  if (!row.empty()) {
    ret["producer_id"] = row[0];
    ret["ref_prod"] = row[1];
    ret["producer_class"] = row[2];
    ret["hours_for_latest"] = row[3];
    ret["no_vers"] = row[4];
    ret["seq_type_prfx"] = row[5];
  
    producerinfo[id] = ret;
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
              << " WHERE name = " << producer_name;

  Query(query.str());
  
  vector<string> row = FetchRow();
  
 // unsigned long int producer_id = boost::lexical_cast<unsigned long> (row[0]);
  string producer_id = row[0];
  
  if (producerinfo.find(producer_id) != producerinfo.end())
    return producerinfo[producer_id];
  
  return GetProducerDefinition(producer_id);
    
}

string NFmiRadonDB::GetLatestTime(const std::string& ref_prod, const std::string& geom_name)
{

  stringstream query;
  
  query <<  "SELECT analysis_time "
 	<<  "FROM as_grid_v"
	<<  " WHERE producer_name = '" << ref_prod
   //     <<  " AND geometry_name = " << geom_name
	<<  "' AND record_count > 0 ";
  
  Query(query.str());

  vector<string> row = FetchRow();

  if (row.size() == 0)
  {
    return "";
  }

  return row[0];
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
  , itsExternalAuthentication(false)
  , itsReadWriteTransaction(false)
  , itsUsername("")
  , itsPassword("")
  , itsDatabase("")
{}

NFmiRadonDBPool::~NFmiRadonDBPool()
{
  for (unsigned int i = 0; i < itsWorkerList.size(); i++) {
        itsWorkerList[i]->Disconnect();
    delete itsWorkerList[i];
  }
  itsWorkerList.clear(); itsWorkingList.clear();
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
		  cout << "DEBUG: Worker returned with id " << itsWorkerList[i]->Id() << endl;
#endif
        return itsWorkerList[i];
      
      } else if (itsWorkingList[i] == -1) {
        // Create new connection
    	try {
          itsWorkerList[i] = new NFmiRadonDB(i);

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

          itsWorkerList[i]->Connect();  

          itsWorkingList[i] = 1;

#ifdef DEBUG
		  cout << "DEBUG: Worker returned with id " << itsWorkerList[i]->Id() << endl;
#endif
          return itsWorkerList[i];
    	} catch (int e) {
    	  throw e;
    	}
      }
    }

    // All threads active
#ifdef DEBUG
    cout << "DEBUG: Waiting for worker release" << endl;
#endif

#ifdef _MSC_VER
    Sleep(100); // 100 ms
#else
    usleep(100000); // 100 ms  
#endif

  }

  throw runtime_error("Impossible error at NFmiRadonDBPool::GetConnection()");
   
}

/*
 * Release()
 * 
 * Clears the database connection (does not disconnect!) and returns it
 * to pool.
 */

void NFmiRadonDBPool::Release(NFmiRadonDB *theWorker) {
  
  lock_guard<mutex> lock(itsReleaseMutex);

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

