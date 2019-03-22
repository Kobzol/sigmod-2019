#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

tar --exclude='test.sh' -czf submission.tar.gz src CMakeLists.txt README *.sh
