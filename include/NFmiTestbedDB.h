#include <string>
#include <vector>
#include "NFmiODBC.h"

class NFmiTestbedDB : public NFmiODBC
{

public:

  NFmiTestbedDB();
  ~NFmiTestbedDB();
  std::string ClassName() const { return "NFmiTestbedDB"; }
	
  static NFmiTestbedDB & Instance();

};
