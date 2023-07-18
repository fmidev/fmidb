#include "NFmiCLDB.h"

#include <iomanip>

using namespace std;

NFmiCLDB& NFmiCLDB::Instance()
{
	static NFmiCLDB instance_;
	return instance_;
}

NFmiCLDB::NFmiCLDB() : NFmiOracle()
{
	connected_ = false;
	user_ = "neons_client";
	database_ = "CLDB";

	const auto pw = getenv("CLDB_NEONSCLIENT_PASSWORD");
	if (pw)
	{
		password_ = string(pw);
	}
	else
	{
		throw std::runtime_error("Environment variable CLDB_NEONSCLIENT_PASSWORD not defined");
	}
}

NFmiCLDB::~NFmiCLDB() { Disconnect(); }
void NFmiCLDB::Connect(const int threadedMode)
{
	NFmiOracle::Connect(threadedMode);
	DateFormat("YYYYMMDDHH24MISS");
	Verbose(true);
}

void NFmiCLDB::Connect(const std::string& user, const std::string& password, const std::string& database,
                       const int threadedMode)
{
	NFmiOracle::Connect(user, password, database, threadedMode);
	DateFormat("YYYYMMDDHH24MISS");
	Verbose(true);
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

map<string, string> NFmiCLDB::GetStationInfo(unsigned long producer_id, unsigned long station_id, bool aggressive_cache)
{
	map<string, string> ret;

	switch (producer_id)
	{
		case 20011:
			ret = GetExtSynopStationInfo(station_id, aggressive_cache);
			break;

		case 20013:
			ret = GetRoadStationInfo(station_id, aggressive_cache);
			break;

		case 20014:
			ret = GetSwedishRoadStationInfo(station_id, aggressive_cache);
			break;

		default:
			ret = GetFMIStationInfo(producer_id, station_id, aggressive_cache);
			break;
	}

	return ret;
}

/*
 * GetRoadStationInfo(int,bool)
 *
 * Retrieves station information from CLDB views rw_stations and locations.
 *
 * This function is needed since in the future road weather data (and road weather
 * station metadata) might not be stored at NEONS.
 *
 * Primary station identifier is FMISID.
 */

map<string, string> NFmiCLDB::GetRoadStationInfo(unsigned long station_id, bool aggressive_cache)
{
	if (road_weather_stations.find(station_id) != road_weather_stations.end()) return road_weather_stations[station_id];

	string query =
	    "WITH stations AS ("
	    "SELECT"
	    " rw.rw_station_id, rw.fmisid, rw.station_formal_name "
	    "FROM rw_stations rw "
	    "UNION ALL "
	    "SELECT"
	    "  to_number(member_code), station_id, NULL as name "
	    "FROM network_members_v1 "
	    "WHERE"
	    "  network_id = 31"
	    ") "
	    "SELECT "
	    "r.fmisid AS station_id, "
	    "l.latitude, "
	    "l.longitude, "
	    "r.station_formal_name, "
	    "l.elevation "
	    "FROM "
	    "stations r, "
	    "locations l "
	    "WHERE "
	    "r.fmisid = l.fmisid";

	/*
	 * If aggressive_cache is not set, query only for the individual station.
	 * Also, if aggressive_cache is set and map stationinfo is already populated
	 * do not fetch again all stations. This condition will happen when a station
	 * requested does not exist.
	 */

	if (!aggressive_cache || (aggressive_cache && road_weather_stations.size() > 0))
		query += " AND r.fmisid = " + to_string(station_id);

	Query(query);

	while (true)
	{
		vector<string> values = FetchRow();

		map<string, string> station;

		if (values.empty()) break;

		int sid = std::stoi(values[0]);

		station["station_id"] = sid;
		station["latitude"] = values[1];
		station["longitude"] = values[2];
		station["station_name"] = values[3];
		station["fmisid"] = sid;
		station["elevation"] = values[4];

		road_weather_stations[sid] = station;
	}

	map<string, string> ret;

	if (road_weather_stations.find(station_id) != road_weather_stations.end())
		ret = road_weather_stations[station_id];
	else
		// If station does not exist, place empty map as a placeholder
		road_weather_stations[station_id] = ret;

	return ret;
}

/*
 * GetSwedishRoadStationInfo(int,bool)
 *
 * Retrieves station information from CLDB views srw_stations and locations.
 *
 * Primary station identifier is FMISID.
 */

map<string, string> NFmiCLDB::GetSwedishRoadStationInfo(unsigned long station_id, bool aggressive_cache)
{
	if (swedish_road_weather_stations.find(station_id) != swedish_road_weather_stations.end())
		return swedish_road_weather_stations[station_id];

	/*  string query = "SELECT "
	                 "r.fmisid as station_id, "
	                 "l.latitude, "
	                 "l.longitude, "
	                 "r.station_formal_name, "
	                 "l.elevation "
	                 "FROM "
	                 "srw_stations r, "
	                 "locations l "
	                 "WHERE "
	                 "r.fmisid = l.fmisid";
	*/
	string query =
	    "SELECT "
	    "s.station_id as station_id, "
	    "round(s.station_geometry.sdo_point.y, 5) as latitude, "
	    "round(s.station_geometry.sdo_point.x, 5) as longitude, "
	    "s.station_name, "
	    "s.station_elevation "
	    "FROM stations_v1 s, network_members_v1 n "
	    "WHERE s.station_id = n.station_id "
	    "AND n.network_id IN (50,67,64) "
	    "AND n.membership_end = to_date('9999-12-31 00:00:00', 'yyyy-mm-dd hh24:mi:ss')";
	/*
	 * If aggressive_cache is not set, query only for the individual station.
	 * Also, if aggressive_cache is set and map stationinfo is already populated
	 * do not fetch again all stations. This condition will happen when a station
	 * requested does not exist.
	 */

	if (!aggressive_cache || (aggressive_cache && road_weather_stations.size() > 0))
		query += " AND s.station_id = " + to_string(station_id);

	Query(query);

	while (true)
	{
		vector<string> values = FetchRow();

		map<string, string> station;

		if (values.empty()) break;

		int sid = std::stoi(values[0]);

		station["station_id"] = sid;
		station["latitude"] = values[1];
		station["longitude"] = values[2];
		station["station_name"] = values[3];
		station["fmisid"] = sid;
		station["elevation"] = values[4];

		swedish_road_weather_stations[sid] = station;
	}

	map<string, string> ret;

	if (swedish_road_weather_stations.find(station_id) != road_weather_stations.end())
		ret = swedish_road_weather_stations[station_id];
	else
		// If station does not exist, place empty map as a placeholder
		swedish_road_weather_stations[station_id] = ret;

	return ret;
}

map<string, string> NFmiCLDB::GetExtSynopStationInfo(unsigned long station_id, bool aggressive_cache)
{
	if (extsynop_stations.find(station_id) != extsynop_stations.end()) return extsynop_stations[station_id];

	stringstream query;

	query << "SELECT "
	      << "r.wmon as station_id, "
	      << "r.lat as latitude, "
	      << "r.lon as longitude, "
	      << "r.station_name, "
	      << "r.fmisid, "
	      << "r.h as elevation "
	      << "FROM "
	      << "wmostations r "
	      << "WHERE "
	      << "membership_end = to_date('9999-12-31', 'yyyy-mm-dd') AND loc_end = to_date('9999-12-31', 'yyyy-mm-dd')";

	if (!aggressive_cache || (aggressive_cache && extsynop_stations.size() > 0))
		query << " AND r.wmon = '" << setw(5) << setfill('0') << station_id << "'";

	Query(query.str());

	while (true)
	{
		vector<string> values = FetchRow();

		map<string, string> station;

		if (values.empty()) break;

		int sid = std::stoi(values[0]);

		station["station_id"] = sid;
		station["latitude"] = values[1];
		station["longitude"] = values[2];
		station["station_name"] = values[3];
		station["fmisid"] = values[4];
		station["elevation"] = values[5];

		extsynop_stations[sid] = station;
	}

	map<string, string> ret;

	if (extsynop_stations.find(station_id) != extsynop_stations.end())
		ret = extsynop_stations[station_id];
	else
		// If station does not exist, place empty map as a placeholder
		extsynop_stations[station_id] = ret;

	return ret;
}

/*
 * GetFMIStationInfo(int,bool)
 *
 * Retrieves station information from CLDB table sreg.
 *
 * By default this function uses aggressive caching: when first station
 * is requested, all stations are read to memory. As more often that not a
 * large number of stations are requested, this will reduce the amount
 * of SQL queries from N to 1. On the other hand when only a few stations
 * are requested, it will be an overkill. This behaviour can be disabled
 * by setting input argument aggressive_caching to false.
 *
 * This function will fetch rain station information from table
 * neons_client.station, since that table has forged station numbers
 * for these rain stations (numbers are >= 110000).
 *
 * Results are placed in an STL map.
 *
 * This function is a part of a scheme to replace table neons_client.station
 * (at CLDB).
 *
 *
 */

map<string, string> NFmiCLDB::GetFMIStationInfo(unsigned long producer_id, unsigned long station_id,
                                                bool aggressive_cache)
{
	string producer_id_str = to_string(producer_id);
	string key = producer_id_str + (to_string(station_id).length() == 4 ? "_0" + to_string(station_id) : "_" + to_string(station_id));

	if (fmi_stations.find(key) != fmi_stations.end()) return fmi_stations[key];

	/*
	 * If aggressive_cache is not set, query only for the individual station.
	 * Also, if aggressive_cache is set and map stationinfo is already populated
	 * do not fetch again all stations. This condition will happen when a station
	 * requested does not exist.
	 */

	string query;

	if (producer_id != 20015 && producer_id != 20022)
	{
		query =
		    "SELECT n.member_code AS wmon, round(s.station_geometry.sdo_point.y, 5) AS latitude, "
		    "round(s.station_geometry.sdo_point.x, 5) AS longitude, "
		    "s.station_name, s.station_id, NULL, s.station_elevation FROM stations_v1 s LEFT OUTER JOIN "
		    "network_members_v1 n ON (s.station_id = n.station_id AND n.network_id = 20) ";

		if (!aggressive_cache || (aggressive_cache && fmi_stations.size() > 0))
			query += " WHERE (s.station_id = " + to_string(station_id) +
				 " OR to_number(n.member_code) = " + to_string(station_id) + ")";
/*		query =
		    "SELECT wmon, lat, lon, station_name, NULL as fmisid, lpnn, elevation "
		    "FROM sreg_view WHERE wmon IS NOT NULL AND lat IS NOT NULL AND lon IS NOT NULL";

		if (!aggressive_cache || (aggressive_cache && fmi_stations.size() > 0))
			query += " AND wmon = " + to_string(station_id);
*/
	}
	else if (producer_id == 20022) // icebuoy
	{
		query =
		    "SELECT NULL AS wmon, m.lat AS latitude, m.lon AS longitude, "
		    "s.station_name, s.station_id, NULL AS lpnn, m.elev as elevation "
		    "FROM stations_v1 s JOIN moving_locations_v1 m ON m.station_id = s.station_id "
		    "WHERE m.created = (select max(created) from moving_locations_v1 where station_id = " + to_string(station_id) + ")";

	}
	else
	{
		query =
		    "SELECT n.member_code AS wmon, round(s.station_geometry.sdo_point.y, 5) AS latitude, "
		    "round(s.station_geometry.sdo_point.x, 5) AS longitude, "
		    "s.station_name, s.station_id, NULL, s.station_elevation FROM stations_v1 s LEFT OUTER JOIN "
		    "network_members_v1 n ON (s.station_id = n.station_id AND n.network_id = 20) ";

		if (!aggressive_cache || (aggressive_cache && fmi_stations.size() > 0))
			query += " WHERE (s.station_id = " + to_string(station_id) +
			         " OR to_number(n.member_code) = " + to_string(station_id) + ")";
	}

	Query(query);

	map<string, string> station;

	while (true)
	{
		vector<string> values = FetchRow();

		if (values.empty()) break;

		station["wmon"] = values[0];
		station["latitude"] = values[1];
		station["longitude"] = values[2];
		station["station_name"] = values[3];
		station["fmisid"] = values[4];
		station["lpnn"] = values[5];
		station["elevation"] = values[6];

		// for 20015, 20022 use fmisid, else use wmo number
		string tempkey = producer_id_str + "_" + ((producer_id == 20015 || producer_id == 20022) ? station["fmisid"] : station["wmon"]);

		fmi_stations[tempkey] = station;

		station.clear();
	}

	map<string, string> ret;

	if (fmi_stations.find(key) != fmi_stations.end())
		ret = fmi_stations[key];
	else
		// If station does not exist, place empty map as a placeholder
		fmi_stations[key] = ret;

	return ret;
}

/*
 * GetParameterDefinition(unsigned long int, int)
 *
 * Retrieves parameter definition from CLDB meta-tables
 * (NOT database system tables!). Function uses aggressive
 * caching -- when first parameter is fetched, all parameters
 * for that producer are fetched.
 *
 */

map<string, string> NFmiCLDB::GetParameterDefinition(unsigned long producer_id, unsigned long universal_id)
{
	if (parameterinfo.find(producer_id) != parameterinfo.end())
		if (parameterinfo[producer_id].find(universal_id) != parameterinfo[producer_id].end())
			return parameterinfo[producer_id][universal_id];

	if (parameterinfo.find(producer_id) == parameterinfo.end() || parameterinfo[producer_id].size() == 0)
	{
		map<string, string> pinfo;

		// TODO: from where to read scale and base ??

		string query =
		    "SELECT "
		    "univ_id, "
		    "responding_col, "
		    "responding_sensor, "
		    "producer_no, "
		    "1 AS scale, "
		    "precision, "
		    "0 AS base, "
		    "data_type, "
		    "responding_id "
		    "FROM "
		    "clim_param_xref "
		    "WHERE producer_no = " +
		    to_string(producer_id);

		Query(query);

		while (true)
		{
			vector<string> values = FetchRow();

			if (values.empty()) break;

			int uid = std::stoi(values[0]);
			int pid = std::stoi(values[3]);

			pinfo["univ_id"] = values[0];
			pinfo["responding_col"] = values[1];
			pinfo["responding_sensor"] = values[2];
			pinfo["producer_no"] = values[3];
			pinfo["scale"] = values[4];
			pinfo["precision"] = values[5];
			pinfo["base"] = values[6];
			pinfo["data_type"] = values[7];
			pinfo["responding_id"] = values[8];

			parameterinfo[pid][uid] = pinfo;

			pinfo.clear();
		}
	}

	map<string, string> ret;

	if (parameterinfo.find(producer_id) != parameterinfo.end())
		if (parameterinfo[producer_id].find(universal_id) != parameterinfo[producer_id].end())
			ret = parameterinfo[producer_id][universal_id];

	return ret;
}

vector<map<string, string>> NFmiCLDB::GetParameterMapping(unsigned long producer_id, unsigned long universal_id)
{
	string key = to_string(producer_id) + "_" + to_string(universal_id);

	if (parametermapping.find(key) != parametermapping.end()) return parametermapping[key];

	vector<map<string, string>> ret;

	string query =
	    "SELECT "
	    "measurand_id, "
	    "sensor_no, "
	    "scale, "
	    "base "
	    "FROM "
	    "clim_param_xref_ng "
	    "WHERE producer_id = " +
	    to_string(producer_id) + " AND univ_id = " + to_string(universal_id);

	Query(query);

	while (true)
	{
		map<string, string> pinfo;

		vector<string> values = FetchRow();

		if (values.empty()) break;

		pinfo["measurand_id"] = values[0];
		pinfo["sensor_no"] = values[1];
		pinfo["scale"] = values[2];
		pinfo["base"] = values[3];

		ret.push_back(pinfo);
	}

	parametermapping[key] = ret;

	return ret;
}

/*
 * GetProducerDefinition(int)
 *
 * Retrieves producer definition from table neons_client.clim_producer.
 *
 */

map<string, string> NFmiCLDB::GetProducerDefinition(unsigned long producer_id)
{
	if (producerinfo.find(producer_id) != producerinfo.end()) return producerinfo[producer_id];

	string query =
	    "SELECT "
	    "producer_no, "
	    "producer_name, "
	    "table_name "
	    "FROM clim_producers "
	    "WHERE producer_no = " +
	    to_string(producer_id);

	map<string, string> ret;

	Query(query);

	vector<string> row = FetchRow();

	if (!row.empty())
	{
		ret["producer_no"] = row[0];
		ret["producer_name"] = row[1];
		ret["table_name"] = row[2];

		producerinfo[producer_id] = ret;
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

map<int, map<string, string>> NFmiCLDB::GetStationListForArea(unsigned long producer_id, double max_latitude,
                                                              double min_latitude, double max_longitude,
                                                              double min_longitude)
{
	map<int, map<string, string>> stationlist;
	string query;

	switch (producer_id)
	{
		case 20011:
			// ext synop

			query =
			    "SELECT "
			    "r.wmon AS station_id, "
			    "r.lat, "
			    "r.lon, "
			    "r.station_name, "
			    "r.fmisid, "
			    "NULL AS lpnn, "
			    "r.h "
			    "FROM "
			    "wmostations r "
			    "WHERE "
			    "r.lat BETWEEN " +
			    to_string(min_latitude) + " AND " + to_string(max_latitude) +
			    " AND "
			    "r.lon BETWEEN " +
			    to_string(min_longitude) + " AND " + to_string(max_longitude);
			break;

		case 20013:
			// Road weather

			query =
			    "WITH stations AS ("
			    "SELECT"
			    " rw.rw_station_id, rw.fmisid "
			    "FROM rw_stations rw "
			    "UNION ALL "
			    "SELECT"
			    " to_number(member_code), station_id "
			    "FROM network_members_v1 "
			    "WHERE"
			    " network_id = 31"
			    ")"
			    "SELECT "
			    "r.fmisid AS station_id, "
			    "l.latitude, "
			    "l.longitude, "
			    "s.station_name, "
			    "r.fmisid, "
			    "NULL AS lpnn, "
			    "l.elevation "
			    "FROM "
			    "stations r, "
			    "stations_v1 s, "
			    "locations l "
			    "WHERE "
			    "r.fmisid = l.fmisid "
			    "AND "
			    "s.station_id = l.fmisid "
			    "AND "
			    "l.latitude BETWEEN " +
			    to_string(min_latitude) + " AND " + to_string(max_latitude) +
			    " AND "
			    "l.longitude BETWEEN " +
			    to_string(min_longitude) + " AND " + to_string(max_longitude);
			break;

		case 20014:
			// Swedish road weather

			query =
			    "SELECT "
			    "s.station_id as station_id, "
			    "round(s.station_geometry.sdo_point.y, 5) as latitude, "
			    "round(s.station_geometry.sdo_point.x, 5) as longitude, "
			    "s.station_name, "
			    "s.station_id as fmisid, "
			    "NULL as lpnn, "
			    "s.station_elevation "
			    "FROM stations_v1 s, network_members_v1 n "
			    "WHERE s.station_id = n.station_id "
			    "AND n.network_id IN (50,67,64) "
			    "AND n.membership_end = to_date('9999-12-31 00:00:00', 'yyyy-mm-dd hh24:mi:ss')"
			    "AND round(s.station_geometry.sdo_point.y, 5) BETWEEN " +
			    to_string(min_latitude) + " AND " + to_string(max_latitude) +
			    " AND "
			    "round(s.station_geometry.sdo_point.x, 5) BETWEEN " +
			    to_string(min_longitude) + " AND " + to_string(max_longitude);
			;

			/*      query = "SELECT "
			              "r.fmisid AS station_id, "
			              "l.latitude, "
			              "l.longitude, "
			              "r.station_formal_name AS station_name, "
			              "r.fmisid, "
			              "NULL AS lpnn, "
			              "l.elevation "
			              "FROM "
			              "srw_stations r, "
			              "locations l "
			              "WHERE "
			              "r.fmisid = l.fmisid "
			              "AND "
			              "l.latitude BETWEEN " +
			              to_string (min_latitude) +
			              " AND " +
			              to_string (max_latitude) +
			              " AND "
			              "l.longitude BETWEEN " +
			              to_string (min_longitude) +
			              " AND " +
			              to_string (max_longitude);
			*/
			break;

		case 20015:
			throw runtime_error("Area-based queries not supported for producer 20015 yet");
			break;

		case 20016:
			query =
			    "SELECT w.wmon, "
			    "round(S.STATION_GEOMETRY.sdo_point.y, 5) as LATITUDE, "
			    "round(S.STATION_GEOMETRY.sdo_point.x, 5) as LONGITUDE, "
			    "s.station_name, "
			    "s.station_id AS fmisid,"
			    "NULL as lpnn, "
			    "s.station_elevation "
			    "FROM stations_v1 s, group_members_v1 gm, wmostations w "
			    "WHERE (gm.group_id = 125 OR gm.group_id = 127)"
			    "AND w.fmisid = s.station_id "
			    "AND gm.station_id = s.station_id "
			    "AND sysdate BETWEEN gm.valid_from AND gm.valid_to "
			    "AND gm.membership_on = 'Y' "
			    "AND round(S.STATION_GEOMETRY.sdo_point.x, 5) BETWEEN " +
			    to_string(min_longitude) + " AND " + to_string(max_longitude) +
			    " AND round(S.STATION_GEOMETRY.sdo_point.y, 5) BETWEEN " + to_string(min_latitude) +
			    " AND " + to_string(max_latitude);
			break;

		case 20018:
			query =
			    "SELECT w.wmon, "
			    "round(S.STATION_GEOMETRY.sdo_point.y, 5) as LATITUDE, "
			    "round(S.STATION_GEOMETRY.sdo_point.x, 5) as LONGITUDE, "
			    "s.station_name, "
			    "s.station_id as fmisid, "
			    "NULL as lpnn, "
			    "NULL as elevation "
			    "FROM stations_v1 s, group_members_v1 gm, wmostations w "
			    "WHERE s.station_id = w.fmisid "
			    "AND s.station_id = gm.station_id "
			    "AND s.country_id = 0 "
			    "AND gm.membership_on = 'Y' "
			    "AND sysdate BETWEEN gm.valid_from AND gm.valid_to "
			    "AND round(S.STATION_GEOMETRY.sdo_point.x, 5) BETWEEN " +
			    to_string(min_longitude) + " AND " + to_string(max_longitude) +
			    " AND round(S.STATION_GEOMETRY.sdo_point.y, 5) BETWEEN " + to_string(min_latitude) +
			    " AND " + to_string(max_latitude);
			break;

		default:
			/*
			 *  LPNN stations
			 *
			 * "Official" stations query by Sami Kiesiläinen, does not include
			 * precipitation stations (station id >= 110000)
			 */

			query =
			    "SELECT "
			    "s.station_id as station_id, "
			    "round(s.station_geometry.sdo_point.y, 5) as latitude, "
			    "round(s.station_geometry.sdo_point.x, 5) as longitude, "
			    "s.station_name, "
			    "s.station_id as fmisid, "
			    "NULL as lpnn, "
			    "s.station_elevation "
			    "FROM stations_v1 s, network_members_v1 n "
			    "WHERE s.station_id = n.station_id "
			    //"AND n.network_id IN (50,67,64) " // no network_id in default
			    "AND n.membership_end = to_date('9999-12-31 00:00:00', 'yyyy-mm-dd hh24:mi:ss')"
			    "AND round(s.station_geometry.sdo_point.y, 5) BETWEEN " +
			    to_string(min_latitude) + " AND " + to_string(max_latitude) +
			    " AND "
			    "round(s.station_geometry.sdo_point.x, 5) BETWEEN " +
			    to_string(min_longitude) + " AND " + to_string(max_longitude);
			;

		/*	query =
			    "SELECT "
			    "to_number(lpad(wmo_bloc, 2, 0) || lpad(wmon, 3, 0)) AS wmon, "
			    "floor(lat/100) + mod(lat,100)/60 + nvl(lat_sec, 0)/3600 AS latitude, "
			    "floor(lon/100) + mod(lon,100)/60 + nvl(lon_sec, 0)/3600 AS longitude, "
			    "name AS station_name, "
			    "NULL AS fmisid, "
			    "lpnn AS station_id, "
			    "elstat "
			    "FROM "
			    "sreg "
			    "WHERE "
			    "floor(lat/100) + mod(lat,100)/60 + nvl(lat_sec, 0)/3600 BETWEEN " +
			    to_string(min_latitude) + " AND " + to_string(max_latitude) +
			    " AND "
			    "floor(lon/100) + mod(lon,100)/60 + nvl(lon_sec, 0)/3600 BETWEEN " +
			    to_string(min_longitude) + " AND " + to_string(max_longitude) +
			    " AND wmon IS NOT NULL "
			    "AND (message IN ('M200','M200A','M500','M500H','HASY','NAWS','AFTN') OR lpnn IN (1019)) "
			    "AND enddate IS NULL "
			    "AND lpnn NOT IN (9950) ";*/

			break;
	}

	Query(query);

	while (true)
	{
		vector<string> values = FetchRow();
		map<string, string> station;

		if (values.empty()) break;

		int id = std::stoi(values[0]);

		station["station_id"] = id;
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

		switch (producer_id)
		{
			case 20013:
				road_weather_stations[id] = station;
				break;

			case 20014:
				swedish_road_weather_stations[id] = station;
				break;

			default:
				fmi_stations[to_string(producer_id) + "_" + to_string(id)] =
				    station;
				break;
		}
	}

	return stationlist;
}
