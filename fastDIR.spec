%define FastDIRServer fastDIR-server
%define FastDIRClient fastDIR-client
%define FastDIRClientDevel fastDIR-client-devel
%define FastDIRClientDebuginfo fastDIR-client-debuginfo
%define CommitVersion %(echo $COMMIT_VERSION)

Name: fastDIR
Version: 1.1.1
Release: 1%{?dist}
Summary: high performance distributed directory service
License: AGPL v3.0
Group: Arch/Tech
URL:  http://github.com/happyfish100/fastDIR/
Source: http://github.com/happyfish100/fastDIR/%{name}-%{version}.tar.gz

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n) 

BuildRequires: libfastcommon-devel >= 1.0.46
BuildRequires: libserverframe-devel >= 1.1.2
Requires: %__cp %__mv %__chmod %__grep %__mkdir %__install %__id
Requires: libfastcommon >= 1.0.46
Requires: libserverframe >= 1.1.2
Requires: %{FastDIRServer} = %{version}-%{release}
Requires: %{FastDIRClient} = %{version}-%{release}

%description
high performance distributed directory service
commit version: %{CommitVersion}

%package -n %{FastDIRServer}
Requires: libfastcommon >= 1.0.46
Requires: libserverframe >= 1.1.2
Summary: FastDIR server

%package -n %{FastDIRClient}
Requires: libfastcommon >= 1.0.46
Requires: libserverframe >= 1.1.2
Summary: FastDIR client library and tools

%package -n %{FastDIRClientDevel}
Requires: %{FastDIRClient} = %{version}-%{release}
Summary: Development header file of FastDIR client library

%description -n %{FastDIRServer}
FastDIR server
commit version: %{CommitVersion}

%description -n %{FastDIRClient}
FastDIR client library and tools
commit version: %{CommitVersion}

%description -n %{FastDIRClientDevel}
This package provides the header files of libfdirclient
commit version: %{CommitVersion}


%prep
%setup -q

%build
./make.sh clean && ./make.sh

%install
rm -rf %{buildroot}
DESTDIR=$RPM_BUILD_ROOT ./make.sh install

%post

%preun

%postun

%clean
rm -rf %{buildroot}

%files

%files -n %{FastDIRServer}
/usr/bin/fdir_serverd

%files -n %{FastDIRClient}
%defattr(-,root,root,-)
/usr/lib64/libfdirclient.so*
/usr/bin/fdir_cluster_stat
/usr/bin/fdir_list
/usr/bin/fdir_mkdir
/usr/bin/fdir_remove
/usr/bin/fdir_rename
/usr/bin/fdir_service_stat
/usr/bin/fdir_stat

%files -n %{FastDIRClientDevel}
%defattr(-,root,root,-)
/usr/include/fastdir/client/*

%changelog
* Fri Jan 1 2021 YuQing <384681@qq.com>
- first RPM release (1.0)
