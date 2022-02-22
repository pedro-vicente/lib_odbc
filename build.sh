#!/bin/bash
mkdir -p build
pushd build
cmake .. 
cmake --build . --parallel 9 
popd