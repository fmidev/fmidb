#ifndef NEONSDB_H
#define NEONSDB_H

#include <string>
#include <vector>
#include <map>
#include "Oracle.h"

class NeonsDB : public Oracle {

public:

  NeonsDB();
  ~NeonsDB();
        
  static NeonsDB & Instance();

  std::string GetGridLevelName(long InParmId, long InLvlId, long InCodeTableVer,long OutCodeTableVer);
  std::string GetGridParameterName(long InParmId,long InCodeTableVer,long OutCodeTableVer);
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
  
private:

  // These maps are used for caching

  std::map<unsigned long, std::map<std::string, std::string > > producerinfo;
  std::map<unsigned long, std::map<unsigned long, std::map<std::string, std::string > > > parameterinfo;
  std::map<std::string, std::map<std::string, std::string > > geometryinfo;
  std::map<unsigned long, std::map<std::string, std::string> > stationinfo;  
  std::map<std::string, std::string> levelinfo;
  std::map<std::string, std::string> gridparameterinfo;

};
#endif
