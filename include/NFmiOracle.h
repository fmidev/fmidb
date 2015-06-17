#pragma once

#include <string>
#include <vector>
#include "otlsettings.h"
#include "NFmiDatabase.h"

class NFmiOracle : public NFmiDatabase {

public:

  NFmiOracle();
  virtual ~NFmiOracle();
        
  static NFmiOracle & Instance();

  virtual void Connect();
  virtual void Connect(const int threadedMode);

  virtual void Connect( const std::string & user,
                const std::string & password,
                const std::string & database,
                const int threadedMode = 0);
        
  virtual void Disconnect (void) final;
  
  virtual void Query(const std::string & sql) final;
  virtual void Query(const std::string & sql, const unsigned int buffer_size) final;
  
  std::vector<std::string> FetchRow(void);
  std::vector<std::string> FetchRowFromCursor(void);
  
  void Execute(const std::string & sql) throw (int);
  void ExecuteProcedure(const std::string & sql) throw (int);
  
  std::string MakeDate(const otl_datetime &datetime);
  //std::string MakeNEONSDate(const otl_datetime &datetime);

  virtual void Commit() throw (int) final;
  virtual void Rollback() throw (int) final;

  bool TestMode() { return test_mode_; }
  void TestMode(bool test_mode) { test_mode_ = test_mode; }
  
  void TransactionIsolationLevel(const std::string &level);

  void DateFormat(const std::string &dateFormat);

  bool Verbose() { return verbose_; }
  void Verbose(bool verbose) { verbose_ = verbose; }

  // These function are used with connection pooling

  void Attach();
  void Detach();

  /*
   * This function can be called directly by the connection pool, or the library
   * can take care of calling it. If latter option is used, the session will only
   * be started if it is absolutely needed (ie. requested data is not found from
   * cache).
   */

  void BeginSession();

  /*
   * Connection pool should always call this function before "releasing" a used
   * connection back to pool.
   */

  void EndSession();

  void PooledConnection(bool pooled_connection);
  bool PooledConnection() const;

protected:

  oracle::otl_connect db_;
  oracle::otl_stream stream_;
  oracle::otl_refcur_stream refcur_;
  
  otl_stream_read_iterator<oracle::otl_stream, oracle::otl_exception, oracle::otl_lob_stream> rs_iterator_;
  otl_stream_read_iterator<oracle::otl_refcur_stream, oracle::otl_exception, oracle::otl_lob_stream> rc_iterator_;

  bool test_mode_;

  std::string date_mask_;
  std::string date_mask_sql_;

  bool verbose_;
  bool initialized_;
  bool pooled_connection_;
  bool credentials_set_;

};
