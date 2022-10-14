install_dir="/temp"

cd $install_dir

wget https://downloads.yugabyte.com/releases/2.14.1.0/yugabyte-2.14.1.0-b36-linux-x86_64.tar.gz

tar xvfz yugabyte-2.14.1.0-b36-linux-x86_64.tar.gz && cd yugabyte-2.14.1.0/

./bin/post_install.sh
