(define brico-indexes
  `{
    #[name core path "core.index"]
    #[name latlong path "latlong.index"
      keyslot {lat long alt globe lat/long lat/long/alt lat/long/alt/globe}]
    #[name wikidprops path "wikidprops.index"]
    #[name wikidrefs path "wikidrefs.index" sizing 1.5 keyslot wikidref]
    #[name wordnet path "wordnet.index"]
    #[name lattice path "lattice.index"]
    #[name termlogic path "termlogic.index"]
    #[name en_words path "en_words.index" keyslot @1/2c1c7"English"]
    #[name en_norms path "en_norms.index" 
      keyslot @1/44896"English norms"]
    #[name en_etc path "en_etc.index" 
      keyslot {@1/2ac91"Aliases in English"
	       @1/44a40"English indices"}
      appendix #t]
    #[name en_frags path "en_frags.index" 
      keyslot @1/44bed"English fragments"
      appendix #t]
    #[name babel_words path "babel_words.index"
      keyslot ,(difference (?? 'type 'words 'type 'lexslot)
			   @1/2c1c7"English")]
    #[name babel_norms path "babel_norms.index" 
      keyslot ,(difference (?? 'type 'norms 'type 'lexslot)
			   @1/44896"English norms")]
    #[name babel_etc path "babel_etc.index" 
      keyslot ,(difference (?? 'type '{aliases indicators} 'type 'lexslot)
			   {@1/2ac91"Aliases in English"
			    @1/44a40"English indices"})
      appendix #t]
    #[name babel_frags path "babel_frags.index" 
      keyslot ,(difference (?? 'type 'fragments 'type 'lexslot)
			   @1/44bed"English fragments")
      appendix #t]

    })

