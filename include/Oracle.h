#ifndef ORACLE_H
#define ORACLE_H

#include <string>
#include <vector>
#include "otlsettings.h"

class Oracle {

public:

  Oracle();
  virtual ~Oracle();
        
  static Oracle & Instance();

  virtual void Connect(void);

  virtual void Connect( const std::string & user,
                const std::string & password,
                const std::string & database);
        
  virtual void Disconnect (void);
        
  virtual void Query(const std::string & sql, const unsigned int buffer_size = 50);
  virtual std::vector<std::string> FetchRow(void);
  virtual std::vector<std::string> FetchRowFromCursor(void);
  
  virtual void Execute(const std::string & sql) throw (int);
  virtual void ExecuteProcedure(const std::string & sql) throw (int);
  
  virtual std::string MakeStandardDate(const otl_datetime &datetime);
  virtual std::string MakeNEONSDate(const otl_datetime &datetime);

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

};

#endif
