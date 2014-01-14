#define OTL_ODBC

#ifdef UNIX
#define OTL_ODBC_UNIX
#endif

#define OTL_ODBC_POSTGRESQL
#define OTL_ORA11G_R2 // Compile OTL 4.0/OCI11.2
#define OTL_EXPLICIT_NAMESPACES
#define OTL_STL
#define OTL_STREAM_READ_ITERATOR_ON

#if defined __GNUC__ && !defined __clang__

#pragma GCC push_options
#pragma GCC diagnostic ignored "-Wconversion"
#include <otlv4.h> // include the OTL 4.0 header file
#pragma GCC pop_options

#else
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wconversion"
#pragma clang diagnostic ignored "-Wunused-private-field"
#pragma clang diagnostic ignored "-Wunused-variable"


#include <otlv4.h> // include the OTL 4.0 header file

#pragma clang diagnostic pop
#endif
