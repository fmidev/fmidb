#ifndef NEONSDB_H
#define NEONSDB_H

#include <string>
#include <vector>
#include <map>
#include "NFmiOracle.h"
#include <mutex>

class NFmiNeonsDBPool;

class NFmiNeonsDB : public NFmiOracle {

public:

  friend class NFmiNeonsDBPool;

  NFmiNeonsDB(short theId = 0);
  ~NFmiNeonsDB();

  void Connect(const int threadedMode = 0);

  void Connect(const std::string & user,
                const std::string & password,
                const std::string & database,
                const int threadedMode = 0);

  static NFmiNeonsDB & Instance();

  std::string GetGridLevelName(const std::string& parm_name, long InLvlId, long InCodeTableVer,long OutCodeTableVer);
  std::string GetGridLevelName(long InParmId, long InLvlId, long InCodeTableVer,long OutCodeTableVer); // GRIB 1
  std::string GetGridLevelName(long InLvlId, long InProducerId); // GRIB 2
  std::string GetGridParameterName(long InParmId,long InCodeTableVer,long OutCodeTableVer, long timeRangeIndicator = 0); // GRIB 1
  std::string GetGridParameterNameForGrib2(long InParmId,long InCategory,long InDiscipline, long InProducerId); // GRIB 2
  std::string GetGribParameterNameFromNetCDF(unsigned long producerId, const std::string &param);

  std::map<std::string, std::string> GetParameterDefinition(unsigned long producer_id, const std::string &parm_name);
  std::map<std::string, std::string> GetParameterDefinition(unsigned long producer_id, unsigned long universal_id);
  std::map<std::string, std::string> GetProducerDefinition(unsigned long producer_id);
  std::map<std::string, std::string> GetProducerDefinition(const std::string &producer_name);
  std::map<std::string, std::string> GetGeometryDefinition(const std::string &geometry_name);
  std::map<std::string, std::string> GetGridModelDefinition(unsigned long producer_id);
  std::pair<int, int> GetGrib2Parameter(unsigned long producerId, unsigned long parameterId);

  std::map<std::string, std::string> GetStationInfo(unsigned long wmo_id, bool aggressive_cache = true);
  std::map<int, std::map<std::string, std::string> > GetStationListForArea(double max_latitude, double min_latitude, double max_longitude, double min_longitude, bool temp = false);
  
  std::vector<std::string> GetNeonsTables(const std::string &start_time, const std::string &end_time, const std::string &producer_name);
  std::vector<std::vector<std::string> > GetGridGeoms(const std::string& ref_prod, const std::string& analtime, const std::string& geom_name = "");

  long GetGridParameterId(long no_vers, const std::string& name);

  std::string GetLatestTime(const std::string& ref_prod, const std::string& geom_name = "");

  short Id() { return itsId; }
  
private:

  // These maps are used for caching

  std::map<unsigned long, std::map<std::string, std::string > > producerinfo;
  std::map<unsigned long, std::map<unsigned long, std::map<std::string, std::string > > > parameterinfo;
  std::map<std::string, std::map<std::string, std::string > > geometryinfo;
  std::map<unsigned long, std::map<std::string, std::string> > stationinfo;  
  std::map<std::string, std::string> levelinfo;
  std::map<std::string, std::string> gridparameterinfo;
  std::map<std::string, std::vector<std::vector<std::string> > > gridgeoms;
  std::map<std::string, long> gridparamid;
  std::map<unsigned long, std::map<std::string, std::string> > gridmodeldefinition;

  short itsId; // Only for connection pooling

};

class NFmiNeonsDBPool {
	
	public:
	
	  static NFmiNeonsDBPool* Instance();

	  ~NFmiNeonsDBPool() { delete itsInstance; }

	  NFmiNeonsDB * GetConnection();
	  void Release(NFmiNeonsDB *theWorker);
	  void MaxWorkers(int theMaxWorkers);
	  int MaxWorkers() const { return itsMaxWorkers; }

	  void ExternalAuthentication(bool theExternalAuthentication) { itsExternalAuthentication = theExternalAuthentication; }
	  bool ExternalAuthentication() const { return itsExternalAuthentication; }

	  void ReadWriteTransaction(bool theReadWriteTransaction) { itsReadWriteTransaction = theReadWriteTransaction; }
	  bool ReadWriteTransaction() const { return itsReadWriteTransaction; }

	  void Username(const std::string& theUsername) { itsUsername = theUsername; }
	  void Password(const std::string& thePassword) { itsPassword = thePassword; }
	  void Database(const std::string& theDatabase) { itsDatabase = theDatabase; }

	private:

	  // Default to two workers

	  NFmiNeonsDBPool();

	  static NFmiNeonsDBPool* itsInstance;

	  int itsMaxWorkers;
	  std::vector<int> itsWorkingList;
	  std::vector<NFmiNeonsDB *> itsWorkerList;

	  std::mutex itsGetMutex;
	  std::mutex itsReleaseMutex;

	  bool itsExternalAuthentication;
	  bool itsReadWriteTransaction;

	  std::string itsUsername;
	  std::string itsPassword;
	  std::string itsDatabase;
};

#endif
