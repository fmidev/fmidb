#pragma once

#include "NFmiPostgreSQL.h"

#include <map>
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

class NFmiRadonDB : public NFmiPostgreSQL
{
   public:
	friend class NFmiRadonDBPool;

	NFmiRadonDB(short theId = 0);
	~NFmiRadonDB();

	void Connect();

	void Connect(const std::string& user, const std::string& password, const std::string& database,
	             const std::string& hostname, const int port = 5432);

	std::string ClassName() const
	{
		return "NFmiRadonDB";
	}
	static NFmiRadonDB& Instance();

	std::map<std::string, std::string> GetProducerFromGrib(long centre, long process, long type);
	std::vector<std::map<std::string, std::string>> GetProducerFromGrib(long centre, long process);

	std::map<std::string, std::string> GetParameterFromNewbaseId(unsigned long producer_id, unsigned long universal_id);
	void WarmGrib1ParameterCache(long producerId);
	void WarmGrib2ParameterCache(long producerId);
	std::map<std::string, std::string> GetParameterFromGrib1(long producerId, long tableVersion, long paramId,
	                                                         long timeRangeIndicator, long levelId, double levelValue);
	std::map<std::string, std::string> GetParameterFromGrib2(long producerId, long discipline, long category,
	                                                         long paramId, long levelId, double levelValue,
	                                                         long typeOfStatisticalProcessing = -1);
	std::map<std::string, std::string> GetParameterFromNetCDF(long producerId, const std::string& paramName,
	                                                          long levelId, double levelValue);
	std::map<std::string, std::string> GetParameterFromDatabaseName(long producerId, const std::string& paramName,
	                                                                int levelId = -1, double levelValue = 32700.f);
	std::map<std::string, std::string> GetParameterPrecision(const std::string& paramName);
	std::string GetNewbaseNameFromUnivId(unsigned long univ_id);

	std::map<std::string, std::string> GetLevelFromGrib(long producerId, long levelId, long edition);
	std::map<std::string, std::string> GetLevelFromDatabaseName(const std::string& name);

	std::map<std::string, std::string> GetProducerDefinition(unsigned long producer_id);
	std::map<std::string, std::string> GetProducerDefinition(const std::string& producer_name);
	std::vector<std::vector<std::string>> GetGridGeoms(const std::string& ref_prod, const std::string& analtime,
	                                                   const std::string& geom_name = "");
	std::map<std::string, std::string> GetGeometryDefinition(const std::string& geom_name);
	std::map<std::string, std::string> GetGeometryDefinition(size_t ni, size_t nj, double lat, double lon, double di,
	                                                         double dj, int gribedition, int gridtype);
	std::string GetLatestTime(int producer_id, const std::string& geom_name = "", unsigned int offset = 0);
	std::string GetLatestTime(const std::string& ref_prod, const std::string& geom_name = "", unsigned int offset = 0);
	std::map<std::string, std::string> GetStationDefinition(FmiRadonStationNetwork networkType, unsigned long stationId,
	                                                        bool aggressive_cache = false);
	std::map<std::string, std::string> GetStationDefinition(FmiRadonStationNetwork networkType,
	                                                        const std::string& stationId,
	                                                        bool aggressive_cache = false);  // overload for icao
	std::map<std::string, std::string> GetLevelTransform(long producer_id, long paramId, long source_level_id,
	                                                     double source_level_value);
	double GetProbabilityLimitForStation(long stationId, const std::string& paramName);
	std::string GetProducerMetaData(long producer_id, const std::string& attribute);
	std::map<std::string, std::string> GetTableName(long producerId, const std::string& analysisTime,
	                                                const std::string& geomName);

	short Id()
	{
		return itsId;
	}

   private:
	// These maps are used for caching

	std::map<std::string, std::map<std::string, std::string>> gribproducerinfo;
	std::map<std::string, std::vector<std::map<std::string, std::string>>> gribproducerinfolist;
	std::map<unsigned long, std::map<std::string, std::string>> producerinfo;
	std::map<std::string, std::map<std::string, std::string>> levelinfo;
	std::map<std::string, std::map<std::string, std::string>> levelnameinfo;
	std::map<std::string, std::map<std::string, std::string>> paramdbinfo;
	std::map<std::string, std::map<std::string, std::string>> paramgrib1info;
	std::map<std::string, std::map<std::string, std::string>> paramgrib2info;
	std::map<std::string, std::map<std::string, std::string>> paramnetcdfinfo;
	std::map<std::string, std::map<std::string, std::string>> paramnewbaseinfo;
	std::map<std::string, std::map<std::string, std::string>> geometryinfo;
	std::map<std::string, std::map<std::string, std::string>> geometryinfo_fromarea;
	std::map<std::string, std::vector<std::vector<std::string>>> gridgeoms;
	std::map<std::string, std::map<std::string, std::string>> stationinfo;
	std::map<std::string, std::map<std::string, std::string>> leveltransforminfo;
	std::map<std::string, std::string> producermetadatainfo;
	std::map<std::string, std::map<std::string, std::string>> tablenameinfo;

	short itsId;  // Only for connection pooling
};

class NFmiRadonDBPool
{
   public:
	static NFmiRadonDBPool* Instance();

	~NFmiRadonDBPool();

	NFmiRadonDB* GetConnection();
	void Release(NFmiRadonDB* theWorker);
	void MaxWorkers(int theMaxWorkers);
	int MaxWorkers() const
	{
		return itsMaxWorkers;
	}
	void Username(const std::string& theUsername)
	{
		itsUsername = theUsername;
	}
	void Password(const std::string& thePassword)
	{
		itsPassword = thePassword;
	}
	void Database(const std::string& theDatabase)
	{
		itsDatabase = theDatabase;
	}
	void Hostname(const std::string& theHostname)
	{
		itsHostname = theHostname;
	}
	void Port(int thePort)
	{
		itsPort = thePort;
	}

   private:
	NFmiRadonDBPool();

	static NFmiRadonDBPool* itsInstance;

	int itsMaxWorkers;
	std::vector<int> itsWorkingList;
	std::vector<NFmiRadonDB*> itsWorkerList;

	std::mutex itsGetMutex;

	std::string itsUsername;
	std::string itsPassword;
	std::string itsDatabase;
	std::string itsHostname;
	int itsPort;
};
