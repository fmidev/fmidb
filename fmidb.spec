%define LIBNAME fmidb
Summary: fload utility
Name: %{LIBNAME}
Version: 13.10.1
Release: 1.el6.fmi
License: FMI
Group: Development/Tools
URL: http://www.fmi.fi
Source0: %{name}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot-%(%{__id_u} -n)
Provides: %{LIBNAME}
BuildRequires: oracle-instantclient-devel >= 11.2.0.3.0
BuildRequires: unixODBC-devel
BuildRequires: boost-devel >= 1.54
Requires: oracle-instantclient-basic >= 11.2.0.3.0

%description
FMI database library

%prep
rm -rf $RPM_BUILD_ROOT

%setup -q -n "%{LIBNAME}" 

%build
make %{_smp_mflags} 

%install
%makeinstall

%post
umask 007
/sbin/ldconfig > /dev/null 2>&1

%postun
umask 007
/sbin/ldconfig > /dev/null 2>&1

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,0644)
%{_libdir}/lib%{LIBNAME}.so
%{_libdir}/lib%{LIBNAME}.a

%changelog
* Tue Oct  1 2013 Mikko Partio <mikko.partio@fmi.fi> - 13.10.1-1.el6.fmi
- Initial build
