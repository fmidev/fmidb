#include <NFmiODBC.h>
#include <stdexcept>
#include <exception>
#include <sstream>
#include "boost/lexical_cast.hpp"

#ifdef DEBUG
#include <assert.h>
#endif

using namespace std;

/*
 * class NFmiODBC
 * 
 * c++ does not have native string-compliant NULL value, replace NULL with 
 * empty string.
 * 
 */  

NFmiODBC & NFmiODBC::Instance() {
  static NFmiODBC instance_;
  return instance_; 
}


NFmiODBC::NFmiODBC() : connected_(false){}

NFmiODBC::NFmiODBC(const std::string& user, const std::string& password, const std::string& database)
	: connected_(false)
	, user_(user)
	, password_(password)
	, database_(database)
{}

void NFmiODBC::Connect(const string & user,
                   const string & password,
                   const string & database) {

  if (connected_)
    return;
    
  odbc::otl_connect::otl_initialize(); // initialize OCI environment

  connection_string_ = user+"/"+password+"@"+database;

  try {
    db_.rlogon(connection_string_.c_str());
    connected_ = true;

#ifdef DEBUG
cout << "DEBUG: connected to ODBC " << database << endl;
#endif

  } catch(odbc::otl_exception& p) {
    cerr << "Unable to connect to ODBC:" << endl;
    cerr << p.msg << endl; // print out error message
    exit(1);
  }

}

void NFmiODBC::Connect() {

  if (connected_)
    return;

  odbc::otl_connect::otl_initialize(); // initialize NFmiODBC environment

  connection_string_ = user_+"/"+password_+"@"+database_;

  try {
    db_.rlogon(connection_string_.c_str());
    connected_ = true;
    
#ifdef DEBUG
cout << "DEBUG: connected to ODBC " << database_ << endl;
#endif

  } catch(odbc::otl_exception& p) {
    cerr << "Unable to connect to ODBC:" << endl;
    cerr << p.msg << endl; // print out error message
    exit(1);
  }    
}

void NFmiODBC::Query(const string & sql) {

  if (!connected_) {
  	cerr << "ERROR: must be connected before executing query" << endl;
  	exit(1);
  }
  
#ifdef DEBUG
cout << "DEBUG: " << sql.c_str() << endl;
#endif

  try {
   	
   	if (stream_.good())	{
  		// Stream is open but a new query needs to executed
  		rs_iterator_.detach();
  		stream_.close();
  	}
 
    stream_.open(20,sql.c_str(),db_);
    rs_iterator_.attach(stream_);
    
  } catch (odbc::otl_exception& p) {
  	cerr << p.msg;
    cerr << "Query: " << p.stm_text << endl;
    throw p.code;
  }
}

vector<string> NFmiODBC::FetchRow() {

  if(!connected_)
    throw runtime_error("NFmiODBC: Cannot perform SQL query before connected");

#ifdef DEBUG
  assert(stream_.good());
#endif

  vector<string> ret;

  if (!rs_iterator_.next_row()) {
    rs_iterator_.detach();
    stream_.close();
    return ret;
  }

  int desc_len;
  otl_column_desc* desc = stream_.describe_select(desc_len);
  
  otl_datetime tval;
  string sval;
  long int ival = 0;
  double dval = 0.0;


  for(int n=0;n<desc_len;++n) {

    // Short-circuit logic when value is null

    if (rs_iterator_.is_null(n+1)) {
      ret.push_back("");
      continue;
    }
    
    switch (desc[n].otl_var_dbtype) {
      case 1:
        // varchar
        rs_iterator_.get(n+1, sval);        
        ret.push_back(sval);
        
        break;

      case 4:
      case 5:
      case 6:
      case 20:
        // different sized integers
        rs_iterator_.get(n+1, ival);
        sval = boost::lexical_cast<string> (ival);
        ret.push_back(sval);
        
        break; 

      case 2:
      case 3:
        // double and float

        if (rs_iterator_.is_null(n+1)) {
          ret.push_back("");
        } else {
          rs_iterator_.get(n+1, dval);
          
          ival = static_cast<long>(dval);
          ostringstream ss;
          
          if (ival == dval) {
            // int
            ss << ival;
          } else {
          	ss << dval;
          }

          ret.push_back(ss.str());
          ss.str("");
        }
        
        break;

      case 8:
        // timestamp
        // Force format of timestamp to standard time
        
        rs_iterator_.get(n+1, tval);
        ret.push_back(MakeStandardDate(tval));
 
        break;

      case 9:
        // postgresql data type text maps to "oracle LONG VARCHAR"
        rs_iterator_.get(n+1, sval);
        ret.push_back(sval);
        
        break;

      default:
        cout << "Got unhandled data type: " << desc[n].otl_var_dbtype << endl;
        break;
    }
  }

  return ret;
}

/*
 * Execute(string)
 * 
 * Executes an SQL DML command.
 *
 */

void NFmiODBC::Execute(const string & sql) throw (int) { 

#ifdef DEBUG
cout << "DEBUG: " << sql << endl;
#endif

  try { 
    odbc::otl_cursor::direct_exec(
        db_,
        sql.c_str(),
        odbc::otl_exception::enabled);
  } catch(odbc::otl_exception& p) {
    // re-throw error code
    cerr << p.msg << endl;
    cerr << "Query: " << p.stm_text << endl;
    throw p.code;  
  }

}
 
NFmiODBC::~NFmiODBC() {
  Disconnect();              
}


void NFmiODBC::Disconnect() {
  db_.logoff();
  connected_ = false;        
}

void NFmiODBC::Commit() throw (int) {

#ifdef DEBUG
  cout << "DEBUG: COMMIT" << endl;
#endif

  try {
    db_.commit();
  } catch (odbc::otl_exception& p) {

    // Always print error if commit fails

    cerr << p.msg << endl;
    throw p.code;
  }
}

void NFmiODBC::Rollback() throw (int) {

#ifdef DEBUG
  cout << "DEBUG: ROLLBACK" << endl;
#endif

  try {

  //	db_.cancel();
    if (stream_.good()) {

      rs_iterator_.detach();
      stream_.close();

    }
    db_.rollback();
  } catch (odbc::otl_exception& p) {

    // Always print error if rollback fails (it should be impossible though)

    cerr << p.msg;
    throw p.code;
  }
}

/*
 * MakeStandardDate()
 * 
 * Format an OTL datetime to standard ISO format.
*/

string NFmiODBC::MakeStandardDate(const otl_datetime &time) {
  
  char date[20];
  sprintf(date, "%4d-%02d-%02d %02d:%02d:%02d", time.year, time.month, time.day, time.hour, time.minute, time.second);
  date[19] = '\0';
  
  return static_cast<string>(date);
  
}
