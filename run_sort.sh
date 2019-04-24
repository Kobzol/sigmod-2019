#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
OMP_NESTED=TRUE taskset -c 0-9,20-29 ${DIR}/build/sort $1 $2
# OMP_PLACES=sockets OMP_PROC_BIND=spread
