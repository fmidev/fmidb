#include <NFmiOracle.h>
#include <stdexcept>
#include <exception>
#include <sstream>
#include <iomanip>
#include <algorithm>

#include "boost/lexical_cast.hpp"

#ifdef DEBUG
#include <assert.h>
#endif

using namespace std;

/*
 * class NFmiOracle
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

NFmiOracle & NFmiOracle::Instance() {
  static NFmiOracle instance_;
  return instance_; 
}

NFmiOracle::NFmiOracle() : test_mode_(false), verbose_(false) {} ;


void NFmiOracle::Connect(const string & user,
                         const string & password,
                         const string & database,
                         const int threadedMode) {

  if (connected_)
    return;
    
  oracle::otl_connect::otl_initialize(threadedMode); // initialize OCI environment

  connection_string_ = user+"/"+password+"@"+database;

  try {
    db_.rlogon(connection_string_.c_str(), 0);
    connected_ = true;

#ifdef DEBUG
cout << "DEBUG: connected to Oracle " << database << " as user " << user << endl;
#endif

  } catch(oracle::otl_exception& p) {
    cerr << "Unable to connect to Oracle with DSN " << user << "/*@" << database << endl;
    cerr << p.msg << endl; // print out error message
    exit(1);
  }

}

void NFmiOracle::Connect(const int threadedMode) {

  if (connected_)
    return;

  oracle::otl_connect::otl_initialize(threadedMode); // initialize OCI environment

  connection_string_ = user_+"/"+password_+"@"+database_;

  try {
    db_.rlogon(connection_string_.c_str(), 0); // connect to NFmiOracle
    connected_ = true;

#ifdef DEBUG
cout << "DEBUG: connected to Oracle " << database_ << " as user " << user_ << endl;
#endif    

    DateFormat("YYYYMMDDHH24MISS");
    
  } catch(oracle::otl_exception& p) {
    cerr << "Unable to connect to Oracle with DSN " << user_ << "/*@" << database_ << endl;
    cerr << p.msg << endl; // print out error message
    exit(1);
  }    
}

void NFmiOracle::Query(const string & sql, const unsigned int buffer_size) {

  if (!connected_)
    throw runtime_error("ERROR: must be connected before executing query");

  if (TestMode())
    return;

#ifdef DEBUG
cout << "DEBUG: " << sql.c_str() << endl;
#endif

  try {
    if (stream_.good() || stream_.eof())  {
      // Stream is open but a new query needs to executed OR
      // stream is at eof

      rs_iterator_.detach();
      stream_.close();

    }
    
    stream_.open(buffer_size,sql.c_str(),db_);

    rs_iterator_.attach(stream_);

  } catch (oracle::otl_exception& p) {

    if (verbose_) {
      cerr << p.msg;
      cerr << "Query: " << p.stm_text << endl;
    }

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
 
vector<string> NFmiOracle::FetchRow() {

  if(!connected_)
    throw runtime_error("Cannot perform SQL query before connected");

#ifdef DEBUG
  assert(stream_.good());
#endif

  vector<string> ret;

  if (!rs_iterator_.next_row()) {
    rs_iterator_.detach();

    stream_.flush();
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

        ret.push_back(MakeDate(tval));
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
 
vector<string> NFmiOracle::FetchRowFromCursor() {

  if(!connected_)
    throw runtime_error("NFmiOracle: Cannot perform SQL query before connected");

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
 
        ret.push_back(MakeDate(tval));
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

void NFmiOracle::Execute(const string & sql) throw (int) { 

  if (!connected_) {
    cerr << "ERROR: must be connected before executing query" << endl;
    exit(1);
  }

#ifdef DEBUG
  cout << "DEBUG: " << sql.c_str() << endl;
#endif

  try {
    oracle::otl_cursor::direct_exec(
        db_,
        sql.c_str(),
        oracle::otl_exception::enabled);
  } catch(oracle::otl_exception& p) {

    if (verbose_) {
      cerr << p.msg;
      cerr << "Query: " << p.stm_text << endl;
    }

    // re-throw error code
    throw p.code;  
  }
}

/*
 * ExecuteProcedure(string)
 * 
 * Function to execute NFmiOracle pl/sql packages (procedures).
 * 
 */ 
 
void NFmiOracle::ExecuteProcedure(const string & sql) throw (int) { 

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

    if (verbose_) {
      cerr << p.msg;
      cerr << "Query: " << p.stm_text << endl;
    }

    throw p.code;
  }
  
}

NFmiOracle::~NFmiOracle() {
  Disconnect();              
}


void NFmiOracle::Disconnect() {
  db_.logoff(); // disconnect from NFmiOracle
  connected_ = false;        
}

/*
 * MakeStandardDate()
 * 
 * Format an OTL datetime to string format.
*/

string NFmiOracle::MakeDate(const otl_datetime &time) {
  
  char date[date_mask_.size()+1];
  snprintf(date, date_mask_.size()+1, date_mask_.c_str(), time.year, time.month, time.day, time.hour, time.minute, time.second);
  
  return static_cast<string>(date);
  
}

/*
 * Commit()
 *
 * Commit transaction.
 */

void NFmiOracle::Commit() throw (int) {

#ifdef DEBUG
  cout << "DEBUG: COMMIT" << endl;
#endif

  try {
    db_.commit();
  } catch (oracle::otl_exception& p) {

    // Always print error if commit fails

    cerr << p.msg << endl;
    throw p.code;
  }
}

/*
 * Rollback()
 *
 * Rollback transaction.
 */

void NFmiOracle::Rollback() throw (int) {

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
  } catch (oracle::otl_exception& p) {

    // Always print error if rollback fails (it should be impossible though)

    cerr << p.msg;
    throw p.code;
  }
}

void NFmiOracle::TransactionIsolationLevel(const std::string &level) {
/*
 * This is supposed to be supported by OTL ?!

	if (level == "READ UNCOMMITTED")
	  db_.set_transaction_isolation_level(otl_tran_read_uncommitted);
	  
	else if (level == "READ COMMITTED")
	  db_.set_transaction_isolation_level(otl_tran_read_committed);
	
	else if (level == "REPEATABLE READ")
	  db_.set_transaction_isolation_level(otl_tran_repeatable_read);
	
	else if (level == "SERIALIZABLE")
	  db_.set_transaction_isolation_level(otl_tran_read_uncommitted);
	
	else if (level == "READ ONLY")
	  Execute("SET TRANSACTION TO READ ONLY");
	  
	else 
	  throw runtime_error("Invalid isolation level: " + level);  
*/
}

void NFmiOracle::Attach() {

	if (connected_)
    return;
    
  oracle::otl_connect::otl_initialize(1); // initialize OCI environment

  //connection_string_ = user_+"/"+password_+"@"+database_;

  try {
    db_.server_attach(database_.c_str());
    connected_ = true;

#ifdef DEBUG
cout << "DEBUG: attached to Oracle " << database_ << endl;
#endif

  } catch(oracle::otl_exception& p) {
    cerr << "Unable to attach to Oracle " << database_ << endl;
    cerr << p.msg << endl; // print out error message
    exit(1);
  }
}

void NFmiOracle::Detach() {
	
	if (!connected_)
    return;
  
  try {
    db_.server_detach();
    connected_ = false;

#ifdef DEBUG
cout << "DEBUG: detached from Oracle " << database_ << endl;
#endif

  } catch(oracle::otl_exception& p) {
    cerr << "Unable to detach from Oracle " << database_ << endl;
    cerr << p.msg << endl; // print out error message
    exit(1);
  }
}

void NFmiOracle::BeginSession() {
	
	if (!connected_)
	  throw runtime_error("Cannot begin session before connected");
		
  try {
    db_.session_begin(user_.c_str() , password_.c_str()); // 0 --> auto commit off
    Execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYYMMDDHH24MISS'");

#ifdef DEBUG
cout << "DEBUG: session started as " << user_ << "/***" << endl;
#endif

  } catch(oracle::otl_exception& p) {
    cerr << "Unable to begin session as user " << user_ << endl;
    cerr << p.msg << endl; // print out error message
    exit(1);
  }
}

void NFmiOracle::EndSession() {
	
	if (!connected_)
	  throw runtime_error("Cannot end session if not connected");
		
  try {
    db_.session_end();

#ifdef DEBUG
cout << "DEBUG: session ended" << endl;
#endif

  } catch(oracle::otl_exception& p) {
    cerr << "Unable to end session" << endl;
    cerr << p.msg << endl; // print out error message
    exit(1);
  }
}

void NFmiOracle::DateFormat(const string &dateFormat) {
  Execute("ALTER SESSION SET NLS_DATE_FORMAT = '" + dateFormat + "'");

  // set date mask; only two common cases supported for now

  string tempFormat = dateFormat; // input being const

  transform(tempFormat.begin(), tempFormat.end(), tempFormat.begin(), ::toupper);

  if (tempFormat == "YYYYMMDDHH24MISS")
    date_mask_ = "%4d%02d%02d%02d%02d"; // Note: seconds missing
  else if (tempFormat == "YYYY-MM-DD HH24:MI:SS")
    date_mask_ ="%4d-%02d-%02d %02d:%02d:%02d";
  else
    throw runtime_error("Invalid date mask: " + dateFormat);
}
