#include <NFmiNeon2DB.h>
#include <boost/lexical_cast.hpp>
#include <stdexcept>
#include <algorithm>

using namespace std;

NFmiNeon2DB& NFmiNeon2DB::Instance() {
  static NFmiNeon2DB instance_;
  return instance_; 
}

NFmiNeon2DB::NFmiNeon2DB(short theId) : NFmiODBC("neons_client", "kikka8si", "neon2"), itsId(theId) {}

NFmiNeon2DB::~NFmiNeon2DB() {
  Disconnect();              
}

/*void NFmiNeon2DB::Connect(const int threadedMode) {
  NFmiOracle::Connect(threadedMode);
  DateFormat("YYYYMMDDHH24MISS");
  Verbose(true);
}

void NFmiNeonsDB::Connect(const std::string & user, const std::string & password, const std::string & database, const int threadedMode) {
  NFmiOracle::Connect(user,password,database,threadedMode);
  DateFormat("YYYYMMDDHH24MISS");
  Verbose(true);
}*/


map<string, string> NFmiNeon2DB::ProducerFromGrib(long centre, long process)
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
map<string, string> NFmiNeon2DB::ParameterFromGrib1(long producerId, long tableVersion, long paramId, long timeRangeIndicator, long levelId, double levelValue)
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

	// always include level_id = 1 (ANYLEVEL) in query since that's the general case
	// only in some exception we have a parameter that has a different meaning
	// depending on the level it is found (for example harmonie and precipitation/mixing ratio)

	query << "SELECT p.id, p.name, p.version, u.name AS unit_name, p.interpolation_id, i.name AS interpolation_name, g.level_id "
			<< "FROM param_grib1 g, param p, param_unit u, interpolation_method i, fmi_producer f "
			<< "WHERE g.param_id = p.id AND p.unit_id = u.id AND p.interpolation_id = i.id AND f.id = g.producer_id "
			<< " AND f.id = " << producerId
			<< " AND table_version = " << tableVersion
			<< " AND number = " << paramId
			<< " AND timerange_indicator = " << timeRangeIndicator
			<< " AND level_id IN (1, " << levelId << ")"
			<< " AND (level_value IS NULL OR level_value = " << levelValue << ")"
			<< " ORDER BY CASE level_id WHEN 1 THEN 2 ELSE 1 END, level_value NULLS LAST LIMIT 1"
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
		ret["grib1_table_version"] = lexical_cast<string> (tableVersion);
		ret["grib1_number"] = lexical_cast<string> (paramId);
		ret["interpolation_method"] = row[5];
		ret["level_id"] = row[6];

		paramgrib1info[key] = ret;

	}

	return ret;
}

map<string, string> NFmiNeon2DB::ParameterFromGrib2(long producerId, long discipline, long category, long paramId, long levelId, double levelValue)
{
	using boost::lexical_cast;

	string key = lexical_cast<string> (producerId) + "_" + lexical_cast<string> (discipline) + "_" + 
		lexical_cast<string> (category) + "_" + lexical_cast<string> (paramId) + "_" + lexical_cast<string> (levelId) + lexical_cast<string> (levelValue) ;

	if (paramgrib2info.find(key) != paramgrib2info.end())
	{
#ifdef DEBUG
	   cout << "DEBUG: ParameterFromGrib2() cache hit!" << endl;
#endif
		return paramgrib2info[key];
	}

	stringstream query;

	// always include level_id = 1 (ANYLEVEL) in query since that's the general case
	// only in some exception we have a parameter that has a different meaning
	// depending on the level it is found (for example harmonie and precipitation/mixing ratio)

	query << "SELECT p.id, p.name, p.version, u.name AS unit_name, p.interpolation_id, i.name AS interpolation_name, g.level_id "
			<< "FROM param_grib2 g, param p, param_unit u, interpolation_method i, fmi_producer f "
			<< "WHERE g.param_id = p.id AND p.unit_id = u.id AND p.interpolation_id = i.id AND f.id = g.producer_id "
			<< " AND f.id = " << producerId
			<< " AND discipline = " << discipline
			<< " AND category = " << category
			<< " AND number = " << paramId
			<< " AND level_id IN (1, " << levelId << ")"
			<< " AND (level_value IS NULL OR level_value = " << levelValue << ")"
			<< " ORDER BY CASE level_id WHEN 1 THEN 2 ELSE 1 END, level_value NULLS LAST LIMIT 1"
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
		ret["grib2_discipline"] = lexical_cast<string> (discipline);
		ret["grib2_category"] = lexical_cast<string> (category);
		ret["grib2_number"] = lexical_cast<string> (paramId);
		ret["interpolationMethod"] = row[5];
		ret["level_id"] = row[6];

		paramgrib2info[key] = ret;
	}

	return ret;
}

map<string, string> NFmiNeon2DB::LevelFromGrib(long producerId, long levelNumber, long edition)
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
			<< "FROM level l, " << (edition == 1 ? "level_grib1 g " : "level_grib2 g")
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

#if 0

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
  : itsMaxWorkers(2)
  , itsWorkingList(itsMaxWorkers, -1)
  , itsWorkerList(itsMaxWorkers, NULL)
  , itsExternalAuthentication(false)
  , itsReadWriteTransaction(false)
  , itsUsername("")
  , itsPassword("")
  , itsDatabase("")
{}

NFmiNeonsDBPool::~NFmiNeonsDBPool()
{
  for (unsigned int i = 0; i < itsWorkerList.size(); i++) {
	  itsWorkerList[i]->Detach();
    delete itsWorkerList[i];
  }
  itsWorkerList.clear(); itsWorkingList.clear();
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

NFmiNeonsDB * NFmiNeonsDBPool::GetConnection() {
 
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

        itsWorkerList[i]->SQLDateMask("YYYYMMDDHH24MISS");

#ifdef DEBUG
		  cout << "DEBUG: Worker returned with id " << itsWorkerList[i]->Id() << endl;
#endif
        return itsWorkerList[i];
      
      } else if (itsWorkingList[i] == -1) {
        // Create new connection
    	try {
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

  throw runtime_error("Impossible error at NFmiNeonsDBPool::GetConnection()");
   
}

/*
 * Release()
 * 
 * Clears the database connection (does not disconnect!) and returns it
 * to pool.
 */

void NFmiNeonsDBPool::Release(NFmiNeonsDB *theWorker) {
  
  lock_guard<mutex> lock(itsReleaseMutex);

  theWorker->Rollback();
  theWorker->EndSession();
  itsWorkingList[theWorker->Id()] = 0;

#ifdef DEBUG
  cout << "DEBUG: Worker released for id " << theWorker->Id() << endl;
#endif

}

void NFmiNeonsDBPool::MaxWorkers(int theMaxWorkers) {
  
  if (theMaxWorkers == itsMaxWorkers)
    return;

  // Making pool smaller is not supported
  
  if (theMaxWorkers < itsMaxWorkers)
    throw runtime_error("Making NeonsDB pool size smaller is not supported (" + boost::lexical_cast<string> (itsMaxWorkers) + " to " + boost::lexical_cast<string> (theMaxWorkers) + ")");
  
  itsMaxWorkers = theMaxWorkers;

  itsWorkingList.resize(itsMaxWorkers, -1);
  itsWorkerList.resize(itsMaxWorkers, NULL);

}
#endif
