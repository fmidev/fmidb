#include <string>
#include <vector>
#include "NFmiODBC.h"

class NFmiTestbedDB : public NFmiODBC
{

public:

  NFmiTestbedDB();
  ~NFmiTestbedDB();
        
  static NFmiTestbedDB & Instance();

};
