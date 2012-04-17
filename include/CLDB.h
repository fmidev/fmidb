#include <string>
#include <vector>
#include <map>
#include "Oracle.h"

class CLDB : public Oracle {

public:

  CLDB();
  ~CLDB();
       
  static CLDB & Instance();
  
  std::map<std::string, std::string> GetStationInfo(unsigned long producer_id, unsigned long station_id, bool aggressive_cache = true);
  std::map<int, std::map<std::string, std::string> > GetStationListForArea(unsigned long producer_id, 
                                                                           double max_latitude, 
                                                                           double min_latitude, 
                                                                           double max_longitude, 
                                                                           double min_longitude);
  
  std::map<std::string, std::string> GetParameterDefinition(unsigned long producer_id, unsigned long universal_id);
  std::map<std::string, std::string> GetProducerDefinition(unsigned long producer_id);
  

private:

  std::map<std::string, std::string> GetSwedishRoadStationInfo(unsigned long station_id, bool aggressive_cache);
  std::map<std::string, std::string> GetRoadStationInfo(unsigned long station_id, bool aggressive_cache);
  std::map<std::string, std::string> GetFMIStationInfo(unsigned long station_id, bool aggressive_cache);
  
  std::map<unsigned long, std::map<unsigned long, std::map<std::string, std::string> > > parameterinfo;
  std::map<unsigned long, std::map<std::string, std::string> > road_weather_stations;
  std::map<unsigned long, std::map<std::string, std::string> > swedish_road_weather_stations;
  std::map<unsigned long, std::map<std::string, std::string> > fmi_stations;
  std::map<unsigned long, std::map<std::string, std::string> > producerinfo;
  
};
