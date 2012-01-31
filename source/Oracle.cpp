#include <Oracle.h>
#include <stdexcept>
#include <exception>
#include <sstream>
#include <iomanip>

#include "boost/lexical_cast.hpp"

#ifdef DEBUG
#include <assert.h>
#endif

using namespace std;

/*
 * class Oracle
 * 
 * OTL does not read Oracle environment variables (NLS_DATE_FORMAT etc)
 * --> if special data formats are needed those queries should have 
 * explicit to_char() calls. By default dates are formatted to NEONS 
 * time (YYYYMMDDHH24MI).
 * 
 * c++ does not have native string-compliant NULL value, replace NULL with 
 * empty string.
 * 
 */  

Oracle & Oracle::Instance() {
  static Oracle instance_;
  return instance_; 
}

Oracle::Oracle() : test_mode_(false) {} ;


void Oracle::Connect(const string & user,
                     const string & password,
                     const string & database) {

  if (connected_)
    return;
    
  oracle::otl_connect::otl_initialize(); // initialize OCI environment

  connection_string_ = user+"/"+password+"@"+database;

  try {
    db_.rlogon(connection_string_.c_str(), 0);
    connected_ = true;

#ifdef DEBUG
cout << "DEBUG: connected to Oracle " << database << " as user " << user << endl;
#endif

    Execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYYMMDDHH24MISS'");

  } catch(oracle::otl_exception& p) {
    cerr << "Unable to connect to Oracle:" << endl;
    cerr << p.msg << endl; // print out error message
    exit(1);
  }

}

void Oracle::Connect() {

  if (connected_)
    return;

  oracle::otl_connect::otl_initialize(); // initialize OCI environment

  connection_string_ = user_+"/"+password_+"@"+database_;

  try {
    db_.rlogon(connection_string_.c_str(), 0); // connect to Oracle
    connected_ = true;

#ifdef DEBUG
cout << "DEBUG: connected to Oracle " << database_ << " as user " << user_ << endl;
#endif    

    Execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYYMMDDHH24MISS'");
    
  } catch(oracle::otl_exception& p) {
    cerr << "Unable to connect to Oracle:" << endl;
    cerr << p.msg << endl; // print out error message
    exit(1);
  }    
}

void Oracle::Query(const string & sql, const unsigned int buffer_size) {

  if (!connected_) {
    cerr << "ERROR: must be connected before executing query" << endl;
    exit(1);
  }

  if (TestMode())
    return;

#ifdef DEBUG
cout << "DEBUG: " << sql.c_str() << endl;
#endif

  try {
    if (stream_.eof() || stream_.good())  {
      // Stream is open but a new query needs to executed OR
                // stream is at eof
      rs_iterator_.detach();
      stream_.close();
    }
    
    stream_.open(buffer_size,sql.c_str(),db_);
    rs_iterator_.attach(stream_);

  } catch (oracle::otl_exception& p) {
    cerr << p.msg;
    cerr << "Query: " << p.stm_text << endl;
    throw p.code;
  }
}

/*
 * FetchRow()
 * 
 * Fetch a single row from stream iterator, cast all elements to 
 * string and return to calling function a vector.
 * 
 */
 
vector<string> Oracle::FetchRow() {

  if(!connected_)
    throw runtime_error("Oracle: Cannot perform SQL query before connected");

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
  const int long_max_size = 70000;
  otl_long_string long_raw(long_max_size);
  db_.set_max_long_size(long_max_size);
  ostringstream strs;

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
        // problem is that Oracle NUMBER can be int 
        // or float or double
        
        rs_iterator_.get(n+1, dval);

        ival = static_cast<long>(dval);
          
        if (ival == dval) {
          // int
          ret.push_back(boost::lexical_cast<string> (ival));
        } else {
          // Use ostringstream -- boost lexical cast does damage to
          // doubles -- 30.1033 is suddenly 30.103300000000001
          
          ostringstream strs;
          strs << dval;
          sval = strs.str();
          
          ret.push_back(sval);
        }

        break;

      case 8:
        // timestamp
        // Force format of timestamp to NEONS time
        
        rs_iterator_.get(n+1, tval);

        ret.push_back(MakeNEONSDate(tval));
        break;

      case 10:
      
        /*
         * Oracle LONG RAW (binary data)
         * 
         * Long raw comes as unsigned char from OTL -- format that to a hex string
         * and pass on to the calling function.
         * Truly an inefficient method to retrieve binary data from database, but we
         * should cope for now. Must be optimized later on.
         * 
         */ 
        
        rs_iterator_.get(n+1, long_raw);

        // Make a two "digit" hex from each char
        
        for (int i = 0; i < long_raw.len(); i++)
          strs << setw(2) << setfill('0') << hex << static_cast<int> (long_raw[i]);

        sval = strs.str();
        ret.push_back(sval);

        break;

      default:
        cerr << "OTL: Got unhandled data type: " << desc[n].otl_var_dbtype << endl;
        break;
    }
  }

  return ret;
}


/*
 * FetchRowFromCursor()
 * 
 * Fetch a single row from refcursor iterator, cast all elements to 
 * string and return to calling function a vector.
 * 
 */
 
vector<string> Oracle::FetchRowFromCursor() {

  if(!connected_)
    throw runtime_error("Oracle: Cannot perform SQL query before connected");

#ifdef DEBUG
  assert(stream_.good());
  assert(refcur_.good());
#endif

  vector<string> ret;

  if (!rc_iterator_.next_row()) {
    rc_iterator_.detach();
    refcur_.close();
    stream_.close();
    return ret;
  }

  int desc_len;
  otl_column_desc* desc = refcur_.describe_select(desc_len);

  otl_datetime tval;
  string sval;
  long int ival = 0;
  double dval = 0.0;
  const int long_max_size = 70000;
  otl_long_string long_raw(long_max_size);
  db_.set_max_long_size(long_max_size);
  ostringstream strs;

  for(int n=0;n<desc_len;++n) {

    // Short-circuit logic when value is null

    if (rc_iterator_.is_null(n+1)) {
      ret.push_back("");
      continue;
    }
    
    switch (desc[n].otl_var_dbtype) { 
      case 1:
        // varchar
        rc_iterator_.get(n+1, sval);
        ret.push_back(sval);
        
        break;

      case 4:
      case 5:
      case 6:
      case 20:
        // different sized integers
        rc_iterator_.get(n+1, ival);
        sval = boost::lexical_cast<string> (ival);
        ret.push_back(sval);
        
        break; 

      case 2:
      case 3:
        // double and float
        // problem is that Oracle NUMBER can be int 
        // or float or double
        
        rc_iterator_.get(n+1, dval);

        ival = static_cast<long>(dval);
          
        if (ival == dval) {
          // int
          ret.push_back(boost::lexical_cast<string> (ival));
        } else {
          // Use ostringstream -- boost lexical cast does damage to
          // doubles -- 30.1033 is suddenly 30.103300000000001
          
          ostringstream strs;
          strs << dval;
          sval = strs.str();
          
          ret.push_back(sval);
        }

        break;

      case 8:
        // timestamp
        // Force format of timestamp to NEONS time
        
        rc_iterator_.get(n+1, tval);
 
        ret.push_back(MakeNEONSDate(tval));
        break;

      case 10:
      
        /*
         * Oracle LONG RAW (binary data)
         * 
         * Long raw comes as unsigned char from OTL -- format that to a hex string
         * and pass on to the calling function.
         * Truly an inefficient method to retrieve binary data from database, but we
         * should cope for now. Must be optimized later on.
         * 
         */ 
        
        rc_iterator_.get(n+1, long_raw);

        // Make a two "digit" hex from each char
        
        for (int i = 0; i < long_raw.len(); i++)
          strs << setw(2) << setfill('0') << hex << static_cast<int> (long_raw[i]);

        sval = strs.str();
        ret.push_back(sval);

        break;

      default:
        cerr << "OTL: Got unhandled data type: " << desc[n].otl_var_dbtype << endl;
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

void Oracle::Execute(const string & sql) throw (int) { 

#ifdef DEBUG
cout << "DEBUG: " << sql.c_str() << endl;
#endif

  try {
    oracle::otl_cursor::direct_exec(
        db_,
        sql.c_str(),
        oracle::otl_exception::enabled);
  } catch(oracle::otl_exception& p) {
    // re-throw error code 
    cerr << p.msg;
    cerr << "Query: " << p.stm_text << endl;
    throw p.code;  
  }
}

/*
 * ExecuteProcedure(string)
 * 
 * Function to execute Oracle pl/sql packages (procedures).
 * 
 */ 
 
void Oracle::ExecuteProcedure(const string & sql) throw (int) { 

  if (!connected_) {
    cerr << "ERROR: must be connected before executing query" << endl;
    exit(1);
  }

  string temp_sql = "BEGIN\n:cur<refcur,out> := " + sql + ";\nEND;";
   
#ifdef DEBUG
cout << "DEBUG: " << temp_sql.c_str() << endl;
#endif

  try {
    if (stream_.good())  {
      // Stream is open but a new query needs to executed
      rc_iterator_.detach();
      stream_.close();
    }
 
    stream_.set_commit(0);
 
    stream_.open(1,temp_sql.c_str(),db_);
    
    stream_ >> refcur_;
    
    rc_iterator_.attach(refcur_);
  
  } catch (oracle::otl_exception& p) {
    cerr << p.msg;
    cerr << "Query: " << p.stm_text << endl;
    throw p.code;
  }
  
}

Oracle::~Oracle() {
  Disconnect();              
}


void Oracle::Disconnect() {
  db_.logoff(); // disconnect from Oracle
  connected_ = false;        
}

/*
 * MakeStandardDate()
 * 
 * Format an OTL datetime to standard ISO format.
*/

string Oracle::MakeStandardDate(const otl_datetime &time) {
  
  char date[20];
  sprintf(date, "%4d-%02d-%02d %02d:%02d:%02d", time.year, time.month, time.day, time.hour, time.minute, time.second);
  date[19] = '\0';
  
  return static_cast<string>(date);
  
}

/*
 * MakeNEONSDate()
 * 
 * Format an OTL datetime to NEONS time format (YYYYMMDDHH24MI).
*/

string Oracle::MakeNEONSDate(const otl_datetime &time) {
  char date[15];
  sprintf(date, "%4d%02d%02d%02d%02d", time.year, time.month, time.day, time.hour, time.minute);
  date[14] = '\0';

  return static_cast<string>(date);
}

/*
 * Commit()
 *
 * Commit transaction.
 */

void Oracle::Commit() throw (int) {

#ifdef DEBUG
cout << "DEBUG: COMMIT" << endl;
#endif

  try {
    db_.commit();
  } catch (oracle::otl_exception& p) {
    cerr << p.msg << endl;
    throw p.code;
  }
}

/*
 * Rollback()
 *
 * Rollback transaction.
 */

void Oracle::Rollback() throw (int) {

#ifdef DEBUG
cout << "DEBUG: ROLLBACK" << endl;
#endif

  try {
    db_.rollback();
  } catch (oracle::otl_exception& p) {
    cerr << p.msg;
    throw p.code;
  }
}
