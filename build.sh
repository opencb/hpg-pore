#!/bin/bash

mvn install -DskipTests

cd src/main/native
./build-native-lib.sh
cd ../../..
