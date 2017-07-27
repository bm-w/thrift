#!/bin/sh

#//Requires these

# brew install boost
# brew install libevent
# brew install libtool
# brew install cmake
# brew install bison
# brew install openssl

#first
./bootstrap.sh

#then (paths to boost and libevent may differ)
# don't necessarily need to exclude all these language libs, but provided for limited build scope
./configure --prefix=/usr/local/ --with-boost=/usr/local/Cellar/boost/1.64.0_1 --with-libevent=/usr/local/Cellar/libevent/2.1.8  --without-cpp --without-qt4 --without-qt5 --without-c_glib --without-erlang --without-perl --without-php --without-php_extension --without-ruby --without-haskell --without-go --without-d --without-openssl --without-nodejs --without-lua

#then
#cd compiler/cpp
#cmakebuild.sh