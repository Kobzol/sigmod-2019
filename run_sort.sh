#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
OMP_NESTED=TRUE ${DIR}/build/sort $1 $2
# OMP_PLACES=sockets OMP_PROC_BIND=spread
