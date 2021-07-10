BZIP := $(shell which lbzip2 || which bzip2)

%.tar.xz: %.tar
	xz -z -v -f -T 0 $<

%.tar.bz2: %/*.pool %/*.index
	TARFILE=$(*F)-$(shell date +%Y%m%d).tar; \
	  rm -f $${TARFILE}.bz2 $${TARFILE}.bz2 && \
	  tar cf $${TARFILE} $^ && ${BZIP} -9 $${TARFILE} && \
	  if [ ! -f $@ ]; then ln -s $${TARFILE}.bz2 $@; \
	  elif [ -h $@ ]; then \
	    rm $@ && ln -s $${TARFILE}.bz2 $@; fi && \
	  ls -l $${TARFILE}.bz2 $@; 

%/packed: %/*.pool %/*.index
	cd $(@D); \
	  (ls *.pool | xargs -P 5 -I_item pack-pool COMPRESSION=zstd19 CODESLOTS=yes _item) && \
	  (ls *.index | xargs -P 5 -I_item pack-index _item) && \
	  date > packed;

#brico/%.index: brico_fresh/%.index
#wikid/%.index: wikid_fresh/%.index

wikid_core.tar: wikid/packed wikid/*.pool \
	wikid/core.index wikid/misc.index wikid/names.index wikid/latlong.index \
	wikid/en_words.index wikid/en_norms.index \
	wikid/en_indicators.index wikid/en_aliases.index \
	wikid/wikidrefs.index wikid/wikidprops.index \
	wikid/genls.index wikid/isa.index wikid/relations.index wikid/partof.index \
	wikid/termlogic.index
	tar cf $@ $^

wikid_extras.tar: wikid/packed wikid/lex_*.index \
	wikid/en_frags.index wikid/en_glosses.index
	tar cf $@ $^

.PRECIOUS: packed


