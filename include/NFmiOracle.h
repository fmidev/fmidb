#ifndef __NFMIORACLE_H__
#define __NFMIORACLE_H__

#include <string>
#include <vector>
#include "otlsettings.h"

class NFmiOracle {

public:

  NFmiOracle();
  virtual ~NFmiOracle();
        
  static NFmiOracle & Instance();

  void Connect(const int threadedMode = 0);

  void Connect( const std::string & user,
                const std::string & password,
                const std::string & database,
                const int threadedMode = 0);
        
  void Disconnect (void);
        
  void Query(const std::string & sql, const unsigned int buffer_size = 50);
  std::vector<std::string> FetchRow(void);
  std::vector<std::string> FetchRowFromCursor(void);
  
  void Execute(const std::string & sql) throw (int);
  void ExecuteProcedure(const std::string & sql) throw (int);
  
  std::string MakeStandardDate(const otl_datetime &datetime);
  std::string MakeNEONSDate(const otl_datetime &datetime);

  void Commit() throw (int);
  void Rollback() throw (int);

  bool TestMode() { return test_mode_; }
  void TestMode(bool test_mode) { test_mode_ = test_mode; }
  
  void TransactionIsolationLevel(const std::string &level);

  // These function are used with connection pooling

  void Attach();
  void Detach();
  void BeginSession();
  void EndSession();
  
protected:

  oracle::otl_connect db_;
  oracle::otl_stream stream_;
  oracle::otl_refcur_stream refcur_;
  
  otl_stream_read_iterator<oracle::otl_stream, oracle::otl_exception, oracle::otl_lob_stream> rs_iterator_;
  otl_stream_read_iterator<oracle::otl_refcur_stream, oracle::otl_exception, oracle::otl_lob_stream> rc_iterator_;

  bool connected_;

  std::string user_;
  std::string password_;
  std::string database_;
   
  std::string connection_string_;

  bool test_mode_;

};

#endif
