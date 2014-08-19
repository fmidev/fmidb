#ifndef NEON2DB_H
#define NEON2DB_H

#include <string>
#include <vector>
#include <map>
#include "NFmiODBC.h"
#include <mutex>

#define kFloatMissing 32700.f

class NFmiNeon2DBPool;

class NFmiNeon2DB : public NFmiODBC {

public:

  friend class NFmiNeon2DBPool;

  NFmiNeon2DB(short theId = 0);
  ~NFmiNeon2DB();
/*
  void Connect(const int threadedMode = 0);

  void Connect(const std::string & user,
                const std::string & password,
                const std::string & database,
                const int threadedMode = 0);*/

  static NFmiNeon2DB & Instance();

  std::map<std::string, std::string> ProducerFromGrib1(long centre, long process);
  std::map<std::string, std::string> ParameterFromGrib1(long producerId, long tableVersion, long paramId, long timeRangeIndicator, long levelId = kFloatMissing, double levelValue = kFloatMissing); // GRIB 1
  std::map<std::string, std::string> LevelFromGrib1(long producerId, long levelId);

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
  void SQLDateMask(const std::string& theDateMask);
  
private:

  // These maps are used for caching

  std::map<unsigned long, std::map<std::string, std::string > > producerinfo;
  std::map<unsigned long, std::map<unsigned long, std::map<std::string, std::string > > > parameterinfo;
  std::map<std::string, std::map<std::string, std::string > > geometryinfo;
  std::map<unsigned long, std::map<std::string, std::string> > stationinfo;  
  std::map<std::string, std::string> levelinfo;
  std::map<std::string, std::string> levelinfosimple;
  std::map<std::string, std::string> gridparameterinfo;
  std::map<std::string, std::vector<std::vector<std::string> > > gridgeoms;
  std::map<std::string, long> gridparamid;
  std::map<unsigned long, std::map<std::string, std::string> > gridmodeldefinition;

  short itsId; // Only for connection pooling

};

class NFmiNeon2DBPool {
	
	public:
	
	  static NFmiNeon2DBPool* Instance();

	  ~NFmiNeon2DBPool();

	  NFmiNeon2DB * GetConnection();
	  void Release(NFmiNeon2DB *theWorker);
	  void MaxWorkers(int theMaxWorkers);
	  int MaxWorkers() const { return itsMaxWorkers; }

	  void ReadWriteTransaction(bool theReadWriteTransaction) { itsReadWriteTransaction = theReadWriteTransaction; }
	  bool ReadWriteTransaction() const { return itsReadWriteTransaction; }

	 /* void Username(const std::string& theUsername) { itsUsername = theUsername; }
	  void Password(const std::string& thePassword) { itsPassword = thePassword; }
	  void Database(const std::string& theDatabase) { itsDatabase = theDatabase; }
*/
	private:

	  // Default to two workers

	  NFmiNeon2DBPool();

	  static NFmiNeon2DBPool* itsInstance;

	  int itsMaxWorkers;
	  std::vector<int> itsWorkingList;
	  std::vector<NFmiNeon2DB *> itsWorkerList;

	  std::mutex itsGetMutex;
	  std::mutex itsReleaseMutex;

	  bool itsExternalAuthentication;
	  bool itsReadWriteTransaction;
};

#endif
