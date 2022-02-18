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
	(let* ((words (get f 'words))
	       (norms (get f 'norms))
	       (indicators (get f 'indicators))
	       (aliases (pickstrings (get f 'aliases)))
	       (names (get f 'names))
	       (glosses (get f 'gloss)))
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

(define (main poolname)
  (config! 'appid (glom "index-" (basename poolname ".pool") "-english"))
  (when (config 'optimize #t)
    (optimize! '{engine brico brico/indexing brico/lookup
		 knodb knodb/search 
		 knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms
		 knodb/fuzz/text knodb/fuzz/graph}))
  (let* ((pool (getdbpool poolname 'core))
	 (nconcepts (min (pool-load pool) #mib))
	 (core.index (pool/index/target pool 'name 'core))
	 (words.index (pool/index/target pool 'keyslot en))
	 (frags.index (pool/index/target pool 'keyslot frags))
	 (indicators.index (pool/index/target pool 'keyslot en_indicators))
	 (norms.index (pool/index/target pool 'keyslot en_norms))
	 (aliases.index (pool/index/target pool 'keyslot en_aliases))
	 (glosses.index (pool/index/target pool 'keyslot en_glosses))
	 (names.index (pool/index/target pool 'name 'names))
	 (oids (difference (pool-elts pool) (?? 'source @1/1) (?? 'status 'deleted))))
    (%watch core.index words.index frags.index indicators.index norms.index aliases.index glosses.index names.index)
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
	 checkpoint ,{pool
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

