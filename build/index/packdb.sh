#!/bin/sh
DIR=$1
BACKUP=${2:-.orig}
if [ -z "${BACKUPS}" ]; then BACKUPS=remove; fi
pxargs(){
    nprocs=$1; shift;
    xargs -L 1 -P ${nprocs} -I_item $*;
}
(cd ${DIR}; ls *.flexindex | pxargs 4 knodb flexpack _item BACKUP=${BACKUP} ) &&
    (cd ${DIR}; ls *.pool *.index| pxargs 4 knodb pack COMPRESSION=zstd19 BACKUP=${BACKUP} _item ) &&
    (cd ${DIR}; if [ "${BACKUP}" = ".orig" ]; then rm -f *.orig; fi );
