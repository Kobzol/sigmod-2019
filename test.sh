#!/usr/bin/env bash

OUTPUT=/tmp/out.bin
FOLDER=cmake-build-release

rm -rf ${OUTPUT}
rm -rf ./out-*
cd ${FOLDER} && make -j && cd .. || exit 1
OMP_NESTED=TRUE time -p ${FOLDER}/sort $1 ${OUTPUT} || exit 1
gensort/valsort ${OUTPUT} || exit 1
