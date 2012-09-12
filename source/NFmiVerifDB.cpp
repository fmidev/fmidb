#include <NFmiVerifDB.h>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

#include <stdexcept>

using namespace std;

NFmiVerifDB & NFmiVerifDB::Instance() {
  static NFmiVerifDB instance_;

  return instance_; 
}

NFmiVerifDB::NFmiVerifDB() : NFmiOracle() {
  connected_ = false;
  user_ = "verifimport";
  password_ = "08y8abei";
  database_ = "verif";
}

NFmiVerifDB::~NFmiVerifDB() {
  Disconnect();              
}


/*
 * GetStationInfo(int, int)
 * 
 * Overcoat function to return station information
 * depending on producer.
 * 
 * By default producer-based functions use aggressive caching: when first station 
 * is requested, all stations are read to memory. As more often that not a
 * large number of stations are requested, this will reduce the amount
 * of SQL queries from N to 1. On the other hand when only a few stations
 * are requested, it will be an overkill. This behaviour can be disabled 
 * by setting input argument aggressive_cache to false.
 *
 * Results are placed in an STL map, with currently three mandatory fields:
 * - station name field must be present and named "station_name"
 * - latitude must be present and named "latitude"
 * - longitude must be present and named "longitude"
 *
 */

map<string, string> NFmiVerifDB::GetStationInfo(unsigned long station_id, bool aggressive_cache) {
  
  map<string, string> ret;

  if (stations.find(station_id) != stations.end())
      return stations[station_id];

  string query = "SELECT "
                 "target_id,"
                 "fmisid,"
                 "wmon,"
                 "station_id,"
                 "name,"
                 "latitude, "
                 "longitude, "
                 "x,"
                 "y,"
                 "external_info "
                 "FROM "
                 "locations ";

  /*
   * If aggressive_cache is not set, query only for the individual station.
   * Also, if aggressive_cache is set and map stationinfo is already populated
   * do not fetch again all stations. This condition will happen when a station
   * requested does not exist.
   */

  if (!aggressive_cache || (aggressive_cache && stations.size() > 0))
    query += " AND fmisid = " + boost::lexical_cast<string> (station_id);

  Query(query);

  while (true) {
    vector <string> values = FetchRow();

    map <string, string> station;

    if (values.empty())
      break;

    int sid = boost::lexical_cast<int> (values[0]);

    station["target_id"] = values[0];
    station["fmisid"] = values[1];
    station["wmon"] = values[2];
    station["station_id"] = values[3];
    station["name"] = values[4];
    station["latitude"] = values[5];
    station["longitude"] = values[6];
        
    stations[sid] = station;

  }
    
  if (stations.find(station_id) != stations.end())
    ret = stations[station_id];
  else
    // If station does not exist, place empty map as a placeholder
    stations[station_id] = ret;
  
  return ret;
  
}

/*
 * GetProducerDefinition(string)
 * 
 * Retrieves producer definition from table producers
 * 
 */

map<string, string> NFmiVerifDB::GetProducerDefinition(const string &producer) {

  if (producerinfo.find(producer) != producerinfo.end())
    return producerinfo[producer];
 
  string query = "SELECT "
                 "id, "
                 "name "
                 "FROM verifng.producers "
                 "WHERE name = '" + producer + "'";

  map <string, string> ret;
  
  Query(query);

  vector<string> row = FetchRow();
  
  if (!row.empty()) {
    ret["id"] = row[0];
    ret["name"] = row[1];
  
    producerinfo[producer] = ret;
  }
  
  return ret;
  
}

/*
 * GetStationListForArea(unsigned long,float,float,float,float)
 * 
 * Retrieves a list of stations from CLDB station tables based on 
 * given max/min lat/lon coordinates and producer id.
 * 
 * Results are placed in an STL map, with currently three mandatory fields:
 * - station name field must be present and named "station_name"
 * - latitude must be present and named "latitude"
 * - longitude must be present and named "longitude"
 * 
 * Totally new function, no equivalent pro*c function exist.
 *  
 */

map<int, map<string, string> > NFmiVerifDB::GetStationListForArea(unsigned long producer_id,
                                                          double max_latitude, 
                                                          double min_latitude, 
                                                          double max_longitude, 
                                                          double min_longitude) {
  
  map<int, map<string, string> > stationlist;
  string
      query = "SELECT "
              "r.fmisid AS station_id, "
              "l.latitude, " 
              "l.longitude, "
              "r.station_formal_name AS station_name, "
              "r.fmisid, "
              "NULL AS lpnn, "
              "l.elevation "
              "FROM "
              "rw_stations r, "
              "locations l "
              "WHERE "
              "r.fmisid = l.fmisid "
              "AND "
              "l.latitude BETWEEN " +
              boost::lexical_cast<string> (min_latitude) +
              " AND " +
              boost::lexical_cast<string> (max_latitude) +
              " AND "
              "l.longitude BETWEEN " +
              boost::lexical_cast<string> (min_longitude) +
              " AND " +
              boost::lexical_cast<string> (max_longitude);
 

  Query(query);
  
  while (true) {
    vector <string> values = FetchRow();
      
    map <string, string> station;
        
    if (values.empty())
      break;

    int id = boost::lexical_cast<int> (values[0]);

    station["station_id"] = values[0];
    station["latitude"] = values[1];
    station["longitude"] = values[2];
    station["station_name"] = values[3];
    station["fmisid"] = values[4];
    station["lpnn"] = values[5];
    station["elevation"] = values[6];

    stationlist[id] = station;
      
    /*
     * Fill stationinfo also. This implies that when later on 
     * GetStationInfo() is called, it will not fetch the station list 
     * but uses this information. 
     * 
     * This should not be a problem since when querying data for an area 
     * only include stations that are inside that area. This could be a 
     * problem if in one par we would have an area query and that query 
     * would contain stations outside the area, but AFAIK that is impossible 
     * since parfile can only contain EITHER station id OR coordinates, not both. 
     * 
     */
      
  }  
  
  return stationlist;
}


int NFmiVerifDB::StatId(const string & theStat)
{
  if (!metadata.instantiated)
    Initialize();

  string stat = theStat;

  /*
   * stat "FAR" was renamed to "FARE" since key word FAR
   * was reserved elsewhere.
   *
   */

  if (stat == "FARE")
    stat = "FAR";

  if (metadata.statIds.find(theStat) != metadata.statIds.end())
      return metadata.statIds[theStat];

  throw runtime_error("could not find id for stat "+stat);

}

// ----------------------------------------------------------------------
/*!
 * \brief Get periodtype id
 */
// ----------------------------------------------------------------------

int NFmiVerifDB::PeriodTypeId(const string & thePeriod)
{
  if (!metadata.instantiated)
    Initialize();

  if (metadata.periodTypeIds.find(thePeriod) != metadata.periodTypeIds.end())
    return metadata.periodTypeIds[thePeriod];

  throw runtime_error("could not find id for period "+thePeriod);

}

// ----------------------------------------------------------------------
/*!
 * \brief Get period id
 */
// ----------------------------------------------------------------------

int NFmiVerifDB::PeriodId(const string & thePeriodName)
{
  if (!metadata.instantiated)
    Initialize();

  if (metadata.periodIds.find(thePeriodName) != metadata.periodIds.end())
    return metadata.periodIds[thePeriodName];

  // If no existing period id is found, a new period is inserted

  vector <string> parts;

  boost::split(parts, thePeriodName, boost::is_any_of(","));

  if(parts.size() != 3)
    throw runtime_error("The period name must be list of three comma-separated values (" + thePeriodName + ")");

  string sql = "insert into periods (period,start_date,end_date) values ('" + parts[0] + "','" + parts[1] + "','" + parts[2] + "')";

  Execute(sql);

  sql = "SELECT id FROM periods WHERE period = '" + parts[0] + "' AND start_date = '" + parts[1] + "' AND end_date = '" + parts[2] + "'";

  vector<string> row = FetchRow();

  if (row.empty())
    throw runtime_error("Unable to fetch just inserted period id");

  int periodId = boost::lexical_cast<int> (row[0]);

  metadata.periodIds[thePeriodName] = periodId;

  return periodId;
}


// ----------------------------------------------------------------------
/*!
 * \brief Get metadata
 */
// ----------------------------------------------------------------------

void NFmiVerifDB::Initialize(void)
{

  /* Retrieve metadata once from the database since its very static
   * so we don't need to retrieve it again every time we output something.
   */

  vector<string> row;

  /* stat is fetched from table estimators */

  Query("select id,name from verifng.estimators order by id");

  while (true)
  {
  	row = FetchRow();

  	if (row.empty())
      break;

  	int id = boost::lexical_cast<int> (row[0]);
  	string name = row[1];

  	metadata.statIds[name] = id;

  }

  metadata.periodTypeIds["annual"] = 1;
  metadata.periodTypeIds["monthly"] = 3;
  metadata.periodTypeIds["seasonal"] = 2;

  /* periodid is fetched from table periods */

  Query("SELECT id,period||','||to_char(start_date, 'yyyy-mm-dd')||','||to_char(end_date, 'yyyy-mm-dd') FROM verifng.periods");

  while (true)
  {
    vector<string> row = FetchRow();

    if(row.empty())
      break;

    int id = boost::lexical_cast<int> (row[0]);

    metadata.periodIds[row[1]] = id;
  }


  /* All metadata has been retrieved */

  metadata.instantiated = true;

}
