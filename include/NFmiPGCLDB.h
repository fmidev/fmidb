#pragma once

#include "NFmiPostgreSQL.h"
//#include "NFmiOracle.h"

#include <map>

class NFmiPGCLDB : public NFmiPostgreSQL 
{
   public:
	//NFmiCLDB();
	NFmiPGCLDB(short theId = 0);
	~NFmiPGCLDB();

	//void Connect(const int threadedMode = 0);
	void Connect();

	//void Connect(const std::string& user, const std::string& password, const std::string& database,
	//             const int threadedMode = 0);
	void Connect(const std::string& user, const std::string& password, const std::string& database,
                             const std::string& hostname, int port = 5432);

	std::string ClassName() const { return "NFmiPGCLDB"; }
	static NFmiPGCLDB& Instance(); // ?

	std::map<std::string, std::string> GetStationInfo(unsigned long producer_id, unsigned long station_id,
	                                                  bool aggressive_cache = true);
	std::map<int, std::map<std::string, std::string>> GetStationListForArea(unsigned long producer_id,
	                                                                        double max_latitude, double min_latitude,
	                                                                        double max_longitude, double min_longitude);

	std::map<std::string, std::string> GetParameterDefinition(unsigned long producer_id, unsigned long universal_id);
	std::map<std::string, std::string> GetProducerDefinition(unsigned long producer_id);
	std::vector<std::map<std::string, std::string>> GetParameterMapping(unsigned long producer_id,
	                                                                    unsigned long universal_id);
	// ? 
	short Id()
        {
		return itsId;
        }

   private:
	std::map<std::string, std::string> GetSwedishRoadStationInfo(unsigned long station_id, bool aggressive_cache);
	std::map<std::string, std::string> GetRoadStationInfo(unsigned long station_id, bool aggressive_cache);
	std::map<std::string, std::string> GetFMIStationInfo(unsigned long producer_id, unsigned long station_id,
	                                                     bool aggressive_cache);
	std::map<std::string, std::string> GetExtSynopStationInfo(unsigned long station_id, bool aggressive_cache);

	std::map<std::string, std::vector<std::map<std::string, std::string>>> parametermapping;
	std::map<unsigned long, std::map<unsigned long, std::map<std::string, std::string>>> parameterinfo;
	std::map<unsigned long, std::map<std::string, std::string>> road_weather_stations;
	std::map<unsigned long, std::map<std::string, std::string>> swedish_road_weather_stations;
	std::map<unsigned long, std::map<std::string, std::string>> extsynop_stations;
	std::map<std::string, std::map<std::string, std::string>> fmi_stations;
	std::map<unsigned long, std::map<std::string, std::string>> producerinfo;
	short itsId;
};
