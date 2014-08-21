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

  std::map<std::string, std::string> ProducerFromGrib(long centre, long process);
  std::map<std::string, std::string> ParameterFromGrib1(long producerId, long tableVersion, long paramId, long timeRangeIndicator, long levelId, double levelValue);
  std::map<std::string, std::string> ParameterFromGrib2(long producerId, long discipline, long category, long paramId, long levelId, double levelValue);

  std::map<std::string, std::string> LevelFromGrib(long producerId, long levelId, long edition);

  short Id() { return itsId; }
  void SQLDateMask(const std::string& theDateMask);
  
private:

  // These maps are used for caching

  std::map<std::string, std::map<std::string, std::string > > producerinfo;
  std::map<std::string, std::map<std::string, std::string> > levelinfo;
  std::map<std::string, std::map<std::string, std::string> > paramgrib1info;
  std::map<std::string, std::map<std::string, std::string> > paramgrib2info;

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
