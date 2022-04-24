%define FastDIRServer fastDIR-server
%define FastDIRClient fastDIR-client
%define FastDIRDevel  fastDIR-devel
%define FastDIRConfig fastDIR-config
%define CommitVersion %(echo $COMMIT_VERSION)

Name: fastDIR
Version: 3.3.0
Release: 1%{?dist}
Summary: high performance distributed directory service
License: AGPL v3.0
Group: Arch/Tech
URL:  http://github.com/happyfish100/fastDIR/
Source: http://github.com/happyfish100/fastDIR/%{name}-%{version}.tar.gz

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n) 

BuildRequires: libfastcommon-devel >= 1.0.57
BuildRequires: libserverframe-devel >= 1.1.14
BuildRequires: FastCFS-auth-devel >= 3.0.0
Requires: %__cp %__mv %__chmod %__grep %__mkdir %__install %__id
Requires: libfastcommon >= 1.0.57
Requires: libserverframe >= 1.1.14
Requires: libfdirstorage >= 1.0.1
Requires: FastCFS-auth-client >= 3.0.0
Requires: FastCFS-auth-config >= 2.0.0
Requires: %{FastDIRServer} = %{version}-%{release}
Requires: %{FastDIRClient} = %{version}-%{release}

%description
high performance distributed directory service
commit version: %{CommitVersion}

%package -n %{FastDIRServer}
Requires: libfastcommon >= 1.0.57
Requires: libserverframe >= 1.1.14
Requires: libfdirstorage >= 1.0.1
Requires: FastCFS-auth-client >= 3.0.0
Requires: FastCFS-auth-config >= 2.0.0
Requires: %{FastDIRConfig} >= 1.0.0
Summary: FastDIR server

%package -n %{FastDIRClient}
Requires: libfastcommon >= 1.0.57
Requires: libserverframe >= 1.1.14
Requires: FastCFS-auth-client >= 3.0.0
Requires: FastCFS-auth-config >= 2.0.0
Requires: %{FastDIRConfig} >= 1.0.0
Summary: FastDIR client library and tools

%package -n %{FastDIRDevel}
Requires: %{FastDIRClient} = %{version}-%{release}
Summary: header files of FastDIR client library

%package -n %{FastDIRConfig}
Summary: FastDIR config files for sample

%description -n %{FastDIRServer}
FastDIR server
commit version: %{CommitVersion}

%description -n %{FastDIRClient}
FastDIR client library and tools
commit version: %{CommitVersion}

%description -n %{FastDIRDevel}
This package provides the header files of libfdirclient
commit version: %{CommitVersion}

%description -n %{FastDIRConfig}
FastDIR config files for sample including server and client
commit version: %{CommitVersion}

%prep
%setup -q

%build
./make.sh clean && ./make.sh

%install
rm -rf %{buildroot}
DESTDIR=$RPM_BUILD_ROOT ./make.sh install
CONFDIR=%{buildroot}/etc/fastcfs/fdir/
SYSTEMDIR=%{buildroot}/usr/lib/systemd/system/
mkdir -p $CONFDIR
mkdir -p $SYSTEMDIR
cp conf/*.conf $CONFDIR
cp systemd/fastdir.service $SYSTEMDIR

%post

%preun

%postun
mkdir -p /opt/fastcfs/fdir

%clean
rm -rf %{buildroot}

%files

%post -n %{FastDIRServer}
mkdir -p /opt/fastcfs/fdir

%files -n %{FastDIRServer}
/usr/bin/fdir_serverd
%config(noreplace) /usr/lib/systemd/system/fastdir.service

%post -n %{FastDIRClient}
mkdir -p /opt/fastcfs/fdir

%files -n %{FastDIRClient}
%defattr(-,root,root,-)
/usr/lib64/libfdirclient.so*
/usr/bin/fdir_cluster_stat
/usr/bin/fdir_list
/usr/bin/fdir_mkdir
/usr/bin/fdir_remove
/usr/bin/fdir_rename
/usr/bin/fdir_getxattr
/usr/bin/fdir_setxattr
/usr/bin/fdir_service_stat
/usr/bin/fdir_stat
/usr/bin/fdir_list_servers

%files -n %{FastDIRDevel}
%defattr(-,root,root,-)
/usr/include/fastdir/client/*

%files -n %{FastDIRConfig}
%defattr(-,root,root,-)
%config(noreplace) /etc/fastcfs/fdir/*.conf

%changelog
* Fri Jan 1 2021 YuQing <384681@qq.com>
- first RPM release (1.0)
