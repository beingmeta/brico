#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/languages)

(use-module '{texttools varconfig logger optimize text/stringfmts engine})
(use-module '{knodb knodb/search knodb/fuzz})
(use-module '{brico brico/build/index})

(define english @1/2c1c7)

(define default-size 50)

(define lexslots '{%words %norms %glosses %glosses %indicators})

(define (get-index indexid batch-state loop-state)
  (try (get batch-state indexid) (get loop-state indexid)))

(defambda (index-words f (batch-state #f))
  (prefetch-oids! f)
  (let* ((loop-state (get batch-state 'loop))
	 (core.index (get-index 'core.index batch-state loop-state))
	 (words.index (get-index 'words.index batch-state loop-state))
	 (frags.index (get-index 'frags.index batch-state loop-state))
	 (norms.index  (get-index 'norms.index batch-state loop-state))
	 (aliases.index  (get-index 'aliases.index batch-state loop-state))
	 (indicators.index  (get-index 'indicators.index batch-state loop-state))
	 (glosses.index  (get-index 'glosses.index batch-state loop-state)))
    (do-choices f
      (when (and (test f 'type) (not (test f 'source @1/1)))
	(let* ((%words (get f '%words))
	       (%norms (get f '%norms))
	       (%aliases (get f '%aliases))
	       (%glosses (get f '%glosses))
	       (%indicators (get f '%indicators))
	       (langids (getkeys {%words %norms %glosses %indicators})))
	  (when (exists? %words)      (index-frame core.index f 'has '%words))
	  (when (exists? %norms)      (index-frame core.index f 'has '%norms))
	  (when (exists? %aliases)    (index-frame core.index f 'has '%aliases))
	  (when (exists? %glosses)    (index-frame core.index f 'has '%glosses))
	  (when (exists? %indicators) (index-frame core.index f 'has '%indicators))
	  (do-choices (langid (getkeys %words))
	    (let* ((words (get %words langid)))
	      (knodb/index+! words.index
			     f  (get language-map langid) 'terms
			     [language langid]
			     words)
	      (knodb/index+! frags.index
			     f (get frag-map langid) 'phrases
			     [language langid fragsize 2]
			     words)))
	  (do-choices (langid (getkeys %norms))
	    (knodb/index+! norms.index
			   f (get norm-map langid) 'terms 
			   [language langid]
			   (get %norms langid))
	    (index-frame core.index f 'has (get norm-map langid)))
	  (do-choices (langid (getkeys %aliases))
	    (knodb/index+! aliases.index
			   f (get alias-map langid) 'terms
			   [language langid]
			   (get %aliases langid))
	    (index-frame core.index f 'has (get alias-map langid)))
	  (do-choices (langid (getkeys %indicators))
	    (knodb/index+! indicators.index
			   f (get indicator-map langid) 'terms
			   [language langid]
			   (get %indicators langid))
	    (index-frame core.index f 'has (get gloss-map langid)))
	  (do-choices (langid (getkeys %glosses))
	    (knodb/index+! glosses.index
			   f (get gloss-map langid) 'text
			   [language langid]
			   (get %glosses langid))
	    (index-frame core.index f 'has (get gloss-map langid)))))
      (swapout f))))

(define default-partopts [maxload 0.8 partsize 4000000])

(define separate-languages '{EN NL ES IT DE SK PL DK})
(define separate-languages 'EN)
;; Not yet implemented
(define merged-languages {})

(define (main poolname)
  (config! 'appid (glom "index-" (basename poolname ".pool") "-multilingual"))
  (when (config 'optimize #t)
    (optimize! '{engine brico brico/indexing brico/lookup
		 knodb knodb/search 
		 knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms
		 knodb/fuzz/text knodb/fuzz/graph}))
  (let* ((pool (getdbpool poolname))
	 (nconcepts (max (pool-load pool) #mib))
	 (core.index (target-index "core.index" #f pool))
	 (words.index (lex-indexes 'words {#f separate-languages} default-partopts pool 10.0))
	 (norms.index (lex-index 'norms {#f separate-languages} default-partopts pool 10.0))
	 (frags.index (lex-index 'fragments {#f separate-languages} default-partopts pool 10.0))
	 (indicators.index (lex-index 'indicators {#f separate-languages} #f pool 10.0))
	 (glosses.index (lex-index 'glosses {#f separate-languages} #f pool 10.0))
	 (aliases.index (lex-index 'indicators {#f separate-languages} #f pool 10.0))
	 (oids (difference (pool-elts pool) (?? 'source @1/1) (?? 'status 'deleted))))
    (commit pool) ;; Save updated INDEXES metadata on pool
    (dbctl (pool/getindexes pool) 'readonly #f)
    (engine/run index-words oids
      `#[loop #[core.index ,core.index
		words.index ,words.index 
		frags.index ,frags.index
		indicators.index ,indicators.index
		glosses.index ,glosses.index
		aliases.index ,aliases.index
		norms.index ,norms.index]
	 branchindexes {core.index aliases.index words.index frags.index indicators.index norms.index glosses.index}
	 counters {words names}
	 logcounters #(words names)
	 batchsize ,(config 'batchsize 1000)
	 logfreq ,(config 'logfreq 50)
	 checkfreq 15
	 checktests ,{(engine/delta 'items 100000)
		      (engine/maxchanges 2000000)}
	 checkpoint ,{pool core.index words.index frags.index 
		      indicators.index aliases.index
		      norms.index glosses.index}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t])
    (commit)))

(when (config 'optimize #t config:boolean)
  (optimize! '{knodb knodb/branches knodb/search 
	       knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms knodb/fuzz/phrases
	       knodb/tinygis
	       fifo engine})
  (optimize! '{brico brico/indexing})
  (optimize-locals!))

(module-export! 'main)
