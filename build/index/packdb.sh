#!/bin/sh
DIR=$1
export BACKUP=${2:-.orig}
pxargs(){
    nprocs=$1; shift;
    xargs -L 1 -P ${nprocs} -I_item $*;
}
cd ${DIR};
ls *.flexindex | pxargs 2 knodb flexpack _item BACKUP=${BACKUP} >> pack.log 2>&1 &
ls *.pool pools/*.pool *.index 2> /dev/null | grep -v "\\.[[:digit:]]\\.index" | \
    pxargs 7 knodb pack COMPRESSION=zstd19 BACKUP=${BACKUP} _item >> pack.log 2>&1 &
wait && if [ "${BACKUP}" = ".orig" ]; then rm -f *.orig; fi;
