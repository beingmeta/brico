(in-module 'brico/build/index/setup)

(config! 'bricosource "./brico/")

(use-module 'brico)

(module-export! '{brico-indexes wikid-indexes})

(defambda (lex-specs type separated (sep-sizing 5.0) (babel-sizing 5.0) (appendix #f))
  {(for-choices (langid separated)
     (let* ((slotid (?? 'type type 'language (get language-map langid)))
	    (typename (if (ambiguous? type) "etc" type))
	    (name  (glom langid "_" typename)))
       (frame-create #f
	 'name (string->symbol name)
	 'tags type
	 'path (glom name ".index")
	 'sizing sep-sizing
	 'keyslot slotid
	 'appendix (tryif appendix appendix))))
   `#[name ,(string->symbol (glom "babel_" (if (ambiguous? type) "etc" type)))
      path ,(glom "babel_" (if (ambiguous? type) "etc" type) ".index")
      sizing ,babel-sizing]})
#|
(defambda (lex-specs type separated (sep-sizing 5.0) (appendix #f))
  (for-choices (langid separated)
    (let* ((slotid (?? 'type type 'language (get language-map langid)))
	   (typename (if (ambiguous? type) "etc" type))
	   (name  (glom langid "_" typename)))
      (frame-create #f
	'appendix (tryif appendix appendix)
	'name (string->symbol name)
	'tags type
	'path (glom name ".index")
	'sizing sep-sizing
	'keyslot slotid))))
|#

(define brico-indexes
  `{
    #[name core path "core.index"]
    #[name latlong path "latlong.index"
      keyslot {lat long alt globe lat/long lat/long/alt lat/long/alt/globe}]
    #[name wikidprops path "wikidprops.index"]
    #[name wikidrefs path "wikidrefs.index" sizing 1.5 keyslot wikidref]
    #[name wordnet path "wordnet.index" sizing 8.0]
    #[name lattice path "lattice.index" sizing 10.0]
    #[name termlogic path "termlogic.index" sizing 10.0]

    #[name en_words path "en_words.index" sizing 5.0 keyslot ,en]
    #[name en_norms path "en_norms.index" sizing 2.0 keyslot ,en_norms]
    #[name en_frags path "en_frags.index" sizing 10.0
      keyslot ,en_frags
      appendix #t]
    #[name en_glosses path "en_glosses.index" sizing 8.0
      keyslot ,en_glosses]
    #[name en_etc path "en_etc.index" sizing 3.0
      keyslot ,{en_aliases en_indicators}
      appendix #t]
    #[name names path "names.index" sizing 8.0]

    #[name babel_words path "babel_words.index" sizing 5.0 keyslot #f]
    #[name babel_norms path "babel_norms.index" sizing 4.0 keyslot #f]
    #[name babel_glosses path "babel_glosses.index" sizing 8.0 keyslot #f appendix #t]
    #[name babel_frags path "babel_frags.index" sizing 10.0 keyslot #f appendix #t]
    #[name babel_etc path "babel_etc.index" sizing 8.0 keyslot #f appendix #t]

    #[name relations path "relations.index" sizing 10.0]
    #[name properties path "properties.index" sizing 10.0]
    })

(define wikid-indexes
  `{
    #[name core path "core.index" ]
    #[name latlong path "latlong.index"
      keyslot {lat long alt globe lat/long lat/long/alt lat/long/alt/globe}]
    #[name wikidrefs path "wikidrefs.index" sizing 1.5 keyslot wikidref]
    #[name lattice path "lattice.index" sizing 10.0]
    #[name termlogic path "termlogic.index" sizing 10.0]

    #[name names path "names.index" sizing 8.0]

    #[name en_words path "en_words.index" sizing 6.0 keyslot ,en]
    #[name en_norms path "en_norms.index" sizing 6.0 keyslot ,en_norms]
    #[name en_glosses path "en_glosses.index" sizing 8.0
      keyslot ,en_glosses]
    #[name en_frags path "en_frags.index" sizing 10.0
      keyslot ,en_frags
      appendix #t]
    #[name en_etc path "en_etc.index" sizing 3.0
      keyslot ,{en_aliases en_indicators}
      appendix #t]

    #[name babel_words path "babel_words.index" sizing 5.0 keyslot #f]
    #[name babel_norms path "babel_norms.index" sizing 4.0 keyslot #f]
    #[name babel_glosses path "babel_glosses.index" sizing 8.0 keyslot #f appendix #t]
    #[name babel_frags path "babel_frags.index" sizing 10.0 keyslot #f appendix #t]
    #[name babel_etc path "babel_etc.index" sizing 8.0 keyslot #f appendix #t]

    ;; ,(lex-specs 'words separate-words 15_000_000 6.0)
    ;; ,(lex-specs 'norms separate-norms 15_000_000 6.0)
    ;; ,(lex-specs 'glosses separate-glosses 1_000_000 2.0)
    ;; ,(lex-specs 'fragments separate-fragments 10_000_000 12.0 #t)
    ;; ,(lex-specs '{aliases indicators} separate-etc 1_000_000 12.0 #t)

    #[name relations path "relations.index" sizing 10.0]
    #[name properties path "properties.index" sizing 10.0]
    })

;; (define separators
;;   '{id min ms is tr br hr pt af lt cs gl nds et cy eu sq da lb ro nn rm
;;     ca sco ga nb sc oc fur vi sv mt eo co ar fi hu gd sl el fr})
;; (define separate-words {separators '{}})
;; (define separate-norms {separators '{}})
;; (define separate-fragments {separate-words '{}})
;; (define separate-glosses {separators '{}})
;; (define separate-etc {separators '{}})

#|
(define wikid-indexes
  `{
    #[name core path "core.index" ]
    #[name latlong path "latlong.index"
      keyslot {lat long alt globe lat/long lat/long/alt lat/long/alt/globe}]
    #[name wikidrefs path "wikidrefs.index" sizing 1.5 keyslot wikidref]
    #[name lattice path "lattice.index" sizing 10.0]
    #[name termlogic path "termlogic.index" sizing 10.0]

    #[name names path "names.index" sizing 8.0]

    #[name en_words path "en_words.index" sizing 6.0 keyslot ,en]
    #[name en_norms path "en_norms.index" sizing 6.0 keyslot ,en_norms]
    #[name en_glosses path "en_glosses.index" sizing 8.0
      keyslot ,en_glosses]
    #[name en_frags path "en_frags.index" sizing 10.0
      keyslot ,en_frags
      appendix #t]
    #[name en_etc path "en_etc.index" sizing 3.0
      keyslot ,{en_aliases en_indicators}
      appendix #t]

    ,(lex-specs 'words separate-words 15_000_000)
    ,(lex-specs 'norms separate-norms 15_000_000)
    ,(lex-specs 'glosses separate-glosses 1_000_000)
    ,(lex-specs 'fragments separate-fragments 10_000_000 #t)
    ,(lex-specs 'aliases separate-aliases 1_000_000 #t)
    ,(lex-specs 'indicators separate-indicators 1_000_000 #t)

    #[name babel_words path "babel_words.index" sizing 25.0 keyslot #f]
    #[name babel_norms path "babel_norms.index" sizing 25.0 keyslot #f]
    #[name babel_frags path "babel_frags.index" sizing 25.0 keyslot #f appendix #t]
    #[name babel_glosses path "babel_glosses.index" sizing 10.0 keyslot #f appendix #t]
    #[name babel_indicators path "babel_indicators.index" sizing 4.0 keyslot #f appendix #t]
    #[name babel_aliases path "babel_aliases.index" sizing 4.0 keyslot #f appendix #t]

    #[name relations path "relations.index" sizing 10.0]
    #[name properties path "properties.index" sizing 10.0]
    })
|#

