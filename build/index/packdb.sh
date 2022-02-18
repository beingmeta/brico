#!/bin/sh
DIR=$1
BACKUPS=${BACKUPS:-$2}
pxargs(){
    nprocs=$1; shift;
    xargs -L 1 -P ${nprocs} -I_item $*;
}
(cd ${DIR}; ls *.pool | pxargs 4 pack-pool COMPRESSION=zstd19 _item; touch pools.done) &
(cd ${DIR}; ls *.index | pxargs 6 pack-index _item; touch indexes.done);
while [ ! -f ${DIR}/pools.done ] && [ ! -f ${DIR}/indexes.done ]; do sleep 1; done
if [ -z "${BACKUPS}" ]; then
    rm ${DIR}/*.bak;
else
    if [ ! -d ${BACKUPS} ]; then mkdir ${BACKUPS}; fi
    for dbfile in ${DIR}/*.bak; do
	target=$(basename ${dbfile} .bak)
	mv ${dbfile} ${BACKUPS}/${target}; done
fi;
