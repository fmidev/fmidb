#pragma once

class NFmiDatabase {

public:

  NFmiDatabase() : connected_(false), user_(""), password_(""), database_(""), connection_string_("") {};
  NFmiDatabase(const std::string& user, const std::string& password, const std::string& database) : connected_(false), user_(user), password_(password), database_(database), connection_string_("") {};
  
  virtual ~NFmiDatabase() {};
        
  virtual void Connect() = 0;
  virtual void Disconnect() = 0;
        
  virtual void Query(const std::string & sql) = 0;
  virtual std::vector<std::string> FetchRow(void) = 0;

  virtual void Execute(const std::string & sql) = 0;

  virtual void Commit() = 0;
  virtual void Rollback() = 0;

protected:

  bool connected_;

  std::string user_;
  std::string password_;
  std::string database_;

  std::string connection_string_;

};
