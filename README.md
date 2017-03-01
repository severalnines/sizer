Basic Installation
===================
Check [sizer](https://severalnines.com/sizer-capacity-planning-tool) for the latest instructions.
in short:
* edit the Makefile and set MYSQL_BASEDIR
* create symblic links for mysql client libraries as follows:
  * $ cd $MYSQL_BASEDIR
  * $ sudo ln -s libmysqlclient.so libmysqlclient_r.so
  * $ sudo ln -s libmysqlclient.so libmysqlclient_r.so.18
  * $ sudo ln -s libmysqlclient.so libmysqlclient_r.so.18.0.0
* $ make
* $ sudo make install

To run the binary after creating it, use:
  LD_LIBRARY_PATH=$MYSQL_BASEDIR/lib ./sizer

Hint: If you followed the installation instructins from mysql-cluster guide
your MYSQL_BASEDIR should be /usr/local/mysql

Redhat/Centos/Suse using RPM:
You must install the MySQL-Cluster-devel (contains header files) package before running make:
   sudo rpm -Uvh mysqlcluster-72-rpm/cluster/repo/MySQL-Cluster-devel-gpl-7.2.8-1.el6.x86_64.rpm


Bugs
===================
https://github.com/severalnines/sizer/issues

Support and Pricing
===================
Contact sales@severalnines.com for more information
