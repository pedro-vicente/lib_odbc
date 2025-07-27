#!/bin/bash
mkdir -p build
pushd build
cmake .. --fresh
cmake --build . --parallel 9 
popd