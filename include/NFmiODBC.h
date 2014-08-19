#ifndef __NFMIODBC_H__
#define __NFMIODBC_H__

#include <string>
#include <vector>
#include "otlsettings.h"

class NFmiODBC {

public:

  NFmiODBC();
  NFmiODBC(const std::string& user, const std::string& password, const std::string& database);
  virtual ~NFmiODBC();
        
  static NFmiODBC & Instance();

  virtual void Connect(void);

  virtual void Connect( const std::string & user,
                const std::string & password,
                const std::string & database);
        
  virtual void Disconnect (void);
        
  virtual void Query(const std::string & sql);
  virtual std::vector<std::string> FetchRow(void);

  virtual void Execute(const std::string & sql) throw (int);

  virtual std::string MakeStandardDate(const otl_datetime &datetime);

  void Commit() throw (int);
  void Rollback() throw (int);

protected:

  odbc::otl_connect db_;
  odbc::otl_stream stream_;
  otl_stream_read_iterator<odbc::otl_stream, odbc::otl_exception, odbc::otl_lob_stream> rs_iterator_;
          
  bool connected_;

  std::string user_;
  std::string password_;
  std::string database_;
   
  std::string connection_string_;

};

#endif
