#ifndef RADONDB_H
#define RADONDB_H

#include <string>
#include <vector>
#include <map>
#include "NFmiODBC.h"
#include <mutex>


// from radon table 'network'
enum FmiRadonStationNetwork
{
	kUnknownNetwork = 0,
	kWMONetwork,
	kICAONetwork,
	kLPNNNetwork,
	kRoadWeatherNetwork,
	kFmiSIDNetwork
};

class NFmiRadonDBPool;

class NFmiRadonDB : public NFmiODBC {

public:

  friend class NFmiRadonDBPool;

  NFmiRadonDB(short theId = 0);
  ~NFmiRadonDB();

  void Connect(const int threadedMode = 0);

  void Connect(const std::string & user,
                const std::string & password,
                const std::string & database,
                const int threadedMode = 0);

  static NFmiRadonDB & Instance();

  std::map<std::string, std::string> GetProducerFromGrib(long centre, long process);
  std::map<std::string, std::string> GetParameterFromNewbaseId(unsigned long producer_id, unsigned long universal_id);
  //std::string GetGridParameterName(long InParmId,long InCodeTableVer,long OutCodeTableVer, long timeRangeIndicator = 0, long levelType = 0); // GRIB 1  
  std::map<std::string, std::string> GetParameterFromGrib1(long producerId, long tableVersion, long paramId, long timeRangeIndicator, long levelId, double levelValue);
  std::map<std::string, std::string> GetParameterFromGrib2(long producerId, long discipline, long category, long paramId, long levelId, double levelValue);
  std::map<std::string, std::string> GetParameterFromNetCDF(long producerId, const std::string& paramName, long levelId, double levelValue);
  std::map<std::string, std::string> GetParameterFromDatabaseName(long producerId, const std::string& paramName);
  
  std::map<std::string, std::string> GetProducerDefinition(unsigned long producer_id);
  std::map<std::string, std::string> GetProducerDefinition(const std::string &producer_name);
  std::map<std::string, std::string> GetLevelFromGrib(long producerId, long levelId, long edition);
  std::vector<std::vector<std::string> > GetGridGeoms(const std::string& ref_prod, const std::string& analtime, const std::string& geom_name = "");
  std::map<std::string, std::string> GetGeometryDefinition(const std::string& geom_name);
  std::string GetLatestTime(const std::string& ref_prod, const std::string& geom_name = "", unsigned int offset = 0);
  std::map<std::string, std::string> GetStationDefinition(FmiRadonStationNetwork networkType, unsigned long stationId, bool aggressive_cache = false);
  std::map<std::string, std::string> GetStationDefinition(FmiRadonStationNetwork networkType, const std::string& stationId, bool aggressive_cache = false); // overload for icao
  
  short Id() { return itsId; }
  
private:

  // These maps are used for caching

  std::map<std::string, std::map<std::string, std::string > > gribproducerinfo;
  std::map<unsigned long, std::map<std::string, std::string > > producerinfo;
  std::map<std::string, std::map<std::string, std::string> > levelinfo;
  std::map<std::string, std::map<std::string, std::string>> paramdbinfo;
  std::map<std::string, std::map<std::string, std::string>> paramgrib1info;
  std::map<std::string, std::map<std::string, std::string>> paramgrib2info;
  std::map<std::string, std::map<std::string, std::string>> paramnetcdfinfo;
  std::map<std::string, std::map<std::string, std::string>> paramnewbaseinfo;
  std::map<std::string, std::map<std::string, std::string>> geometryinfo;
  std::map<std::string, std::vector<std::vector<std::string>>> gridgeoms;
  std::map<std::string, std::map<std::string, std::string>> stationinfo;

  short itsId; // Only for connection pooling

};

class NFmiRadonDBPool {
	
	public:
	
	  static NFmiRadonDBPool* Instance();

	  ~NFmiRadonDBPool();

	  NFmiRadonDB * GetConnection();
	  void Release(NFmiRadonDB *theWorker);
	  void MaxWorkers(int theMaxWorkers);
	  int MaxWorkers() const { return itsMaxWorkers; }

	  void Username(const std::string& theUsername) { itsUsername = theUsername; }
	  void Password(const std::string& thePassword) { itsPassword = thePassword; }
	  void Database(const std::string& theDatabase) { itsDatabase = theDatabase; }

	private:

	  // Default to two workers

	  NFmiRadonDBPool();

	  static NFmiRadonDBPool* itsInstance;

	  int itsMaxWorkers;
	  std::vector<int> itsWorkingList;
	  std::vector<NFmiRadonDB *> itsWorkerList;

	  std::mutex itsGetMutex;
          
      std::string itsUsername;
	  std::string itsPassword;
	  std::string itsDatabase;
};

#endif
