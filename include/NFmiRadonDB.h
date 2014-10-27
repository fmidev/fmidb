#ifndef RADONDB_H
#define RADONDB_H

#include <string>
#include <vector>
#include <map>
#include "NFmiODBC.h"
#include <mutex>

// #define kFloatMissing 32700.f

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

  std::map<std::string, std::string> ProducerFromGrib(long centre, long process);
  std::map<std::string, std::string> GetNewbaseParameterDefinition(unsigned long producer_id, unsigned long universal_id);
  std::map<std::string, std::string> ParameterFromGrib1(long producerId, long tableVersion, long paramId, long timeRangeIndicator, long levelId, double levelValue);
  std::map<std::string, std::string> ParameterFromGrib2(long producerId, long discipline, long category, long paramId, long levelId, double levelValue);
  std::map<std::string, std::string> ParameterFromNetCDF(long producerId, const std::string& paramName, long levelId, double levelValue);

  std::map<std::string, std::string> GetProducerDefinition(unsigned long producer_id);
  std::map<std::string, std::string> GetProducerDefinition(const std::string &producer_name);
  std::map<std::string, std::string> LevelFromGrib(long producerId, long levelId, long edition);
  std::vector<std::vector<std::string> > GetGridGeoms(const std::string& ref_prod, const std::string& analtime, const std::string& geom_name = "");
  std::map<std::string, std::string> GetGeometryDefinition(unsigned long geometry_id);
  std::string GetLatestTime(const std::string& ref_prod, const std::string& geom_name = "");
  
  short Id() { return itsId; }
  // void SQLDateMask(const std::string& theDateMask);
  
private:

  // These maps are used for caching

  //std::map<unsigned long, std::map<std::string, std::string > > producerinfo;
  std::map<std::string, std::map<std::string, std::string > > producerinfo;
  std::map<std::string, std::map<std::string, std::string> > levelinfo;
  std::map<unsigned long, std::map<unsigned long, std::map<std::string, std::string > > > newbaseinfo;
  std::map<std::string, std::map<std::string, std::string> > paramgrib1info;
  std::map<std::string, std::map<std::string, std::string> > paramgrib2info;
  std::map<std::string, std::map<std::string, std::string> > paramnetcdfinfo;
  std::map<std::string, std::map<std::string, std::string > > geometryinfo;
  std::map<std::string, std::vector<std::vector<std::string> > > gridgeoms;

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

	  void ReadWriteTransaction(bool theReadWriteTransaction) { itsReadWriteTransaction = theReadWriteTransaction; }
	  bool ReadWriteTransaction() const { return itsReadWriteTransaction; }

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
	  std::mutex itsReleaseMutex;

	  bool itsExternalAuthentication;
	  bool itsReadWriteTransaction;
          
          std::string itsUsername;
	  std::string itsPassword;
	  std::string itsDatabase;
};

#endif
