#!/usr/bin/env bash

OUTPUT=/tmp/out.res
FOLDER=cmake-build-release

cd ${FOLDER} && make -j && cd .. || exit 1
time -p ${FOLDER}/sort $1 ${OUTPUT} || exit 1
gensort/valsort ${OUTPUT} || exit 1
