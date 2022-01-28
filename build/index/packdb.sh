#!/bin/sh
DIR=$1
export BACKUP=${2:-.orig}
if [ -z "${BACKUPS}" ]; then BACKUPS=remove; fi
dbpack() {
    local listing=$(ls -l $1);
    echo "# "$(date +%T)" Packing $1";
    knodb pack COMPRESSION=zstd19 BACKUP=${BACKUP} $1 1> $1.packlog 2>&1;
    echo "# "$(date +%T)" Finished packing $1";
    echo "${listing}";
    ls -l $1;
}
flexpack() {
    echo "# "$(date +%T)" Packing $1";
    knodb flexpack BACKUP=${BACKUP} $1 1> $1.packlog 2>&1;
    echo "# "$(date +%T)" Finished packing $1";
}
export -f flexpack
export -f dbpack
pxargs(){
    nprocs=$1; shift;
    xargs -L 1 -P ${nprocs} -I_item $*;
}
cd ${DIR};
ls *.flexindex | pxargs 2 knodb flexpack _item BACKUP=${BACKUP} >> pack.log 2>&1 &
ls *.pool *.index | grep -v "\\.[[:digit:]]\\.index" | \
    pxargs 7 knodb pack COMPRESSION=zstd19 BACKUP=${BACKUP} _item >> pack.log 2>&1 &
wait && if [ "${BACKUP}" = ".orig" ]; then rm -f *.orig; fi;
