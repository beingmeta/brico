#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/english)

(use-module '{texttools varconfig logger optimize text/stringfmts engine})
(use-module '{knodb knodb/search knodb/fuzz})
(use-module 'brico/build/index)
(use-module '{brico brico/indexing})

(define english @1/2c1c7)
(define engloss @1/2ffbd)

(define enorm (get norm-map english))
(define enindexes (get indicator-map english))
(define enaliases (get alias-map english))

(define frags (get frag-map english))
(define cues (get indicator-map english))
(define norm (get norm-map english))

(defambda (index-english f (batch-state #f))
  (prefetch-oids! f)
  (let* ((loop-state (get batch-state 'loop))
	 (core.index (getopt loop-state 'core.index))
	 (words.index (getopt loop-state 'words.index))
	 (frags.index (getopt loop-state 'frags.index))
	 (norms.index  (getopt loop-state 'norms.index))
	 (aliases.index  (getopt loop-state 'aliases.index))
	 (glosses.index  (getopt loop-state 'glosses.index))
	 (indicators.index  (getopt loop-state 'indicators.index))
	 (names.index (getopt loop-state 'names.index))
	 (word-count 0)
	 (name-count 0))
    (do-choices f
      (when (and (test f 'type) (not (test f 'source @1/1)))
	(let* ((words (choice (get f 'words) (get (get f '%words) 'en)))
	       (norms {(get f 'norms) (get (get f '%norms) 'en)})
	       (indicators (get (get f '%indicators) 'en))
	       (aliases (get (get f '%aliases) 'en))
	       (names {(get f 'names)
		       (pick norms capitalized?)
		       (pick words capitalized?)
		       (pick aliases capitalized?)
		       (pick (get f '{family lastname}) string?)})
	       (glosses (choice (get f 'gloss) (get (get f '%glosses) 'en))))
	  (knodb/index+! words.index f english 'terms #[language en] words)
	  (knodb/index+! norms.index f norm 'terms #[language en] norms)
	  (knodb/index+! aliases.index f enaliases 'terms #[language en] aliases)
	  (knodb/index+! indicators.index f cues 'terms #[language en] indicators)
	  (knodb/index+! frags.index f frags 'phrases #[language en fragsize 2] words)
	  (knodb/index+! names.index f 'names 'terms #[language en fragsize 2 normcase {lower default}]
			names)
	  (set! word-count (+ word-count (choice-size words)))
	  (set! name-count (+ name-count (choice-size names)))
	  (index-frame core.index f 'has
		       {(tryif (exists? words) @1/2c1c7"English")
			(tryif (exists? norms) @1/44896"Common English")
			(tryif (exists? indicators) @1/44a40"English indices")
			(tryif (exists? aliases) @1/2ac91"Aliases in English")
			(tryif (exists? glosses) @1/2ffbd"Gloss (English)")})
	  (do-choices (gloss glosses)
	    (knodb/index+! glosses.index f engloss 'text #[language en] gloss))
	  (index-string names.index f '{family lastname}))))
    (swapout f)))

(define (main . names)
  (config! 'appid (glom "index-" (basename (car names) ".pool") "-english"))
  (when (config 'optimize #t)
    (optimize! '{engine brico brico/indexing brico/lookup
		 knodb knodb/search 
		 knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms
		 knodb/fuzz/text knodb/fuzz/graph}))
  (let* ((pools (getdbpool (try (elts names) brico-pool-names)))
	 (nconcepts (max (reduce-choice + pools 0 pool-load) #mib))
	 (core.index (target-index "core.index" #f pools))
	 (words.index (target-index "en_words.index" [keyslot english] pools))
	 (frags.index (target-index "en_frags.index" [keyslot frags] pools))
	 (indicators.index 
	  (target-index "en_indicators.index" [keyslot cues] pools))
	 (norms.index (target-index "en_norms.index" [keyslot enorm] pools))
	 (aliases.index (target-index "en_aliases.index" [keyslot enaliases] pools))
	 (glosses.index (target-index "en_glosses.index" [keyslot engloss] pools))
	 (names.index (target-index "names.index" #f pools))
	 (oids (difference (pool-elts pools) (?? 'source @1/1) (?? 'status 'deleted))))
    (do-choices (pool pools)
      (dbctl pool 'metadata 'indexes
	     (choice (dbctl pool 'metadata 'indexes)
		     (glom "en_" {"words" "frags" "norms" "aliases" "indicators" "glosses"} ".index"))))
    (commit pools)
    (dbctl (pool/getindexes pools) 'readonly #f)
    (engine/run index-english oids
      `#[loop #[core.index ,core.index
		words.index ,words.index 
		frags.index ,frags.index
		indicators.index ,indicators.index
		norms.index ,norms.index
		aliases.index ,aliases.index
		glosses.index ,glosses.index
		names.index ,names.index]
	 branchindexes {core.index words.index frags.index indicators.index norms.index
			aliases.index glosses.index names.index}
	 counters {words names}
	 logcounters #(words names)
	 batchsize ,(config 'batchsize 5000)
	 logfreq ,(config 'logfreq 60)
	 checkfreq 15
	 checktests ,(engine/delta 'items 100000)
	 checkpoint ,{pools 
		      core.index words.index frags.index
		      indicators.index norms.index
		      aliases.index
		      glosses.index
		      names.index}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t
	 logfreq ,(config 'logfreq 50)])
    (commit)))
(module-export! 'main)

(when (config 'optimize #t config:boolean)
  (optimize! '{knodb knodb/branches knodb/search 
	       knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms knodb/fuzz/phrases
	       knodb/tinygis
	       fifo engine})
  (optimize! '{brico brico/indexing})
  (optimize-locals!))

