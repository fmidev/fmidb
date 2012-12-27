#define OTL_ODBC

#ifdef UNIX
#define OTL_ODBC_UNIX
#endif

#define OTL_ODBC_POSTGRESQL
#define OTL_ORA11G_R2 // Compile OTL 4.0/OCI11.2
#define OTL_EXPLICIT_NAMESPACES
#define OTL_STL
#define OTL_STREAM_READ_ITERATOR_ON

#pragma GCC push_options
#pragma GCC diagnostic ignored "-Wconversion"
#include <otlv4.h> // include the OTL 4.0 header file
#pragma GCC pop_options

