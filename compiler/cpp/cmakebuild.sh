#!/bin/sh

mkdir ~/thrift/build-xcode
cd ~/thrift/build-xcode
cmake -G Xcode ..
xcodebuild