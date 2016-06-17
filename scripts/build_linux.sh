#!/bin/bash

set -x

TAG=$(git describe --abbrev=0 --tags)
if [ ! -z $1 ];then
   TAG=$1
fi

sed -i '' -e "s/version = \".*/version = \"${TAG}\"/" src/fullerite/main.go
PDIR=$(echo $(pwd) |sed -e 's/scripts//')
docker run -ti -v ${PDIR}:/data/ -w /data/ qnib/golang make
mv ${PDIR}/bin/fullerite ${PDIR}/bin/fullerite-${TAG}-Linux
rm -f bin/gom bin/beatit

docker run -ti -v ${PDIR}:/data/ -w /data/ qnib/alpn-go-dev make fullerite
mv ${PDIR}/bin/fullerite ${PDIR}/bin/fullerite-${TAG}-LinuxMusl
rm -f ${PDIR}/bin/gom bin/beatit

# Darwin
if [ "Darwin" == $(uname) ];then
    make fullerite
    mv ${PDIR}/bin/fullerite ${PDIR}/bin/fullerite-${TAG}-Darwin
    rm -f ${PDIR}/bin/gom bin/beatit
fi
git checkout src/fullerite/main.go
