#!/bin/bash

echo
echo "Building the HPG Pore's Java package"
echo
mvn install -DskipTests

echo
echo "Building the HPG Pore's dynamic library libhpgpore.so"
echo
cd src/main/native
./build-native-lib.sh
cd ../../..

echo
echo "Copying files to current directory"
echo
cp -v target/hpg-pore-0.1.0-jar-with-dependencies.jar .
cp -v src/main/native/libhpgpore.so .

