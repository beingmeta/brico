#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/languages)

(use-module '{texttools varconfig logger optimize text/stringfmts engine})
(use-module '{knodb knodb/search knodb/fuzz})
(use-module '{brico brico/build/index})

(define english @1/2c1c7)

(define lexslots '{%words %norms %glosses %glosses %indicators})

(defambda (index-words f (batch-state #f))
  (prefetch-oids! f)
  (let* ((loop-state (get batch-state 'loop))
	 (core.index (getopt loop-state 'core.index))
	 (words.index (getopt loop-state 'words.index))
	 (frags.index (getopt loop-state 'frags.index))
	 (norms.index  (getopt loop-state 'norms.index))
	 (aliases.index  (getopt loop-state 'aliases.index))
	 (indicators.index  (getopt loop-state 'indicators.index))
	 (glosses.index  (getopt loop-state 'glosses.index)))
    (do-choices f
      (when (and (test f 'type) (not (test f 'source @1/1)))
	(let* ((%words (get f '%words))
	       (%norms (get f '%norms))
	       (%aliases (get f '%aliases))
	       (%glosses (get f '%glosses))
	       (%indicators (get f '%indicators))
	       (langids (getkeys {%words %norms %glosses %indicators})))
	  (when (exists? %words)
	    (index-frame core.index f 'has '%words))
	  (when (exists? %norms)
	    (index-frame core.index f 'has '%norms))
	  (when (exists? %aliases)
	    (index-frame core.index f 'has '%aliases))
	  (when (exists? %glosses)
	    (index-frame core.index f 'has '%glosses))
	  (when (exists? %indicators)
	    (index-frame core.index f 'has '%indicators))
	  (do-choices (langid (difference langids 'en))
	    (when (test %words langid)
	      (let* ((words (get %words langid))
		     (phrases (pick words compound-string?) ))
		;;; Debug tracepoint
		(when (zero? (random 2048))
		  (logdebug |IndexWords|
		    "Indexing " (|| words) " words for " 
		    (get language-map langid) " (" langid ")" 
		    " of " f " in " words.index))
		(knodb/index+! words.index f  (get language-map langid) 'terms
			      [language langid fragsize 2]
			      (get %words langid))))
	    (when (test %norms langid)
	      (knodb/index+! norms.index f (get norm-map langid) 'terms 
			    [language langid fragsize 2]
			    (get %norms langid))
	      (index-frame core.index f 'has (get norm-map langid)))
	    (when (test %aliases langid)
	      (knodb/index+! aliases.index f (get alias-map langid) 'terms
			    [language langid frags 2]
			    (get %aliases langid))
	      (index-frame core.index f 'has (get alias-map langid)))
	    (when (test %indicators langid)
	      (knodb/index+! indicators.index f (get indicator-map langid) 'terms
			    [language langid frags #f]
			    (get %indicators langid))
	      (index-frame core.index f 'has (get gloss-map langid)))
	    (when (test %glosses langid)
	      (knodb/index+! indicators.index f (get gloss-map langid) 'text
			    [language langid frags #f]
			    (get %glosses langid))
	      (index-frame core.index f 'has (get gloss-map langid))))))
      (swapout f))))

(define (main . names)
  (config! 'appid "index-multilingual")
  (when (config 'optimize #t)
    (optimize! '{engine brico brico/indexing brico/lookup}))
  (let* ((pools (getdbpool (try (elts names) brico-pool-names)))
	 (nconcepts (max (reduce-choice + pools 0 pool-load) #mib))
	 (core.index (target-index "core.index" #f pools))
	 (words.index (target-index "lex_words.index" #f pools))
	 (frags.index (target-index "lex_frags.index" #f pools))
	 (indicators.index (target-index "lex_indicators.index" #f pools))
	 (glosses.index (target-index "lex_glosses.index" #f pools))
	 (norms.index (target-index "lex_norms.index" #f pools))
	 (aliases.index (target-index "lex_aliases.index" #f pools))
	 (oids (difference (pool-elts pools) (?? 'source @1/1) (?? 'status 'deleted))))
    (dbctl (pool/getindexes pools) 'readonly #f)
    (drop! core.index (cons 'has lexslots))
    (engine/run index-words oids
      `#[loop #[core.index ,core.index
		words.index ,words.index 
		frags.index ,frags.index
		indicators.index ,indicators.index
		glosses.index ,glosses.index
		aliases.index ,aliases.index
		norms.index ,norms.index]
	 counters {words names}
	 logcounters #(words names)
	 batchsize ,(config 'batchsize 5000)
	 logfreq ,(config 'logfreq 50)
	 checkfreq 15
	 checktests ,(engine/delta 'items 100000)
	 checkpoint ,{pools core.index words.index frags.index 
		      indicators.index aliases.index
		      norms.index glosses.index}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t])
    (commit)))

(when (config 'optimize #t config:boolean)
  (optimize! '{knodb knodb/branches knodb/search 
	       knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms
	       knodb/tinygis
	       fifo engine})
  (optimize! '{brico brico/indexing})
  (optimize-locals!))

(module-export! 'main)
