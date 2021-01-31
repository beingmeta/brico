%.tar.xz: %.tar
	xz -z -v -f -T 0 $<

%.tar: %/*.pool %/*.index %/packed
	tar cf $@ $^

%/packed: %/*.pool %/*.index
	cd $(@D); \
	  (ls *.pool | xargs -P 5 -I_item pack-pool COMPRESSION=zstd19 CODESLOTS=yes _item) && \
	  (ls *.index | xargs -P 5 -I_item pack-index _item) && \
	  date > packed;

brico/%.index: brico_fresh/%.index
wikid/%.index: wikid_fresh/%.index

wikid_core.tar: wikid/packed wikid/*.pool wikid/core.index wikid/misc.index \
	wikid/en_*.index wikid/names.index \
	wikid/wordnet.index wikid/wikidrefs.index wikid/wikidprops.index \
	wikid/latlong.index \
	wikid/genls.index wikid/isa.index \
	wikid/relations.index wikid/partof.index \
	wikid/termlogic.index
	tar cf $@ $^

wikid_xlex.tar: wikid/packed wikid/lex_*.index
	tar cf $@ $^

.PRECIOUS: packed


