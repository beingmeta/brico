#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/languages)

(use-module '{texttools varconfig logger optimize text/stringfmts kno/threads engine})
;;; This module needs to go early because it (temporarily) disables the brico database
(use-module 'brico/build/index)

(use-module '{knodb knodb/search knodb/fuzz knodb/adjuncts knodb/flexindex knodb/flexpool})
(use-module '{brico})

(define english @1/2c1c7)

(define lexslots '{%words %norms %glosses %glosses %indicators})

(define (get-index indexid batch-state loop-state)
  (try (get batch-state indexid) (get loop-state indexid)))

(defambda (index-words f (batch-state #f))
  (let* ((loop-state (get batch-state 'loop))
	 (core.index (get-index 'core.index batch-state loop-state))
	 (words.index (get-index 'words.index batch-state loop-state))
	 (frags.index (get-index 'frags.index batch-state loop-state))
	 (norms.index  (get-index 'norms.index batch-state loop-state))
	 (aliases.index  (get-index 'aliases.index batch-state loop-state))
	 (indicators.index  (get-index 'indicators.index batch-state loop-state))
	 (glosses.index  (get-index 'glosses.index batch-state loop-state))
	 (words.adjunct (get loop-state 'words.adjunct))
	 (norms.adjunct (get loop-state 'norms.adjunct))
	 (glosses.adjunct (get loop-state 'glosses.adjunct))
	 (indicators.adjunct (get loop-state 'indicators.adjunct))
	 (aliases.adjunct (get loop-state 'aliases.adjunct)))
    (debug%watch "index-words"
      core.index "partitions" (dbctl core.index 'partitions)
      words.index "partitions" (dbctl words.index 'partitions)
      frags.index "partitions" (dbctl frags.index 'partitions)
      norms.index "partitions" (dbctl norms.index 'partitions)
      aliases.index "partitions" (dbctl aliases.index 'partitions)
      glosses.index "partitions" (dbctl glosses.index 'partitions)
      )
    (prefetch-oids! f)
    (pool-prefetch! words.adjunct f)
    (pool-prefetch! norms.adjunct f)
    (pool-prefetch! glosses.adjunct f)
    (pool-prefetch! indicators.adjunct f)
    (pool-prefetch! aliases.adjunct f)
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
	    (let* ((words (get %words langid))
		   (slotid (get language-map langid))
		   (fragslotid (get frag-map langid))
		   (phrases (pick words compound-string?)))
	      (knodb/index+! words.index f slotid 'terms [language langid] words)
	      (index-frame core.index f 'has slotid)
	      (when (exists? phrases)
		(knodb/index+! frags.index f fragslotid  'phrases
			       [language langid fragsize 2]
			       phrases)
		(index-frame core.index f 'has fragslotid))
	      (table-increment! batch-state 'words (|| words))
	      (table-increment! batch-state 'names (|| (pick words capitalized?)))
	      (table-increment! batch-state 'phrases (|| phrases))))
	  (do-choices (langid (getkeys %norms))
	    (let* ((norms (get %norms langid))
		   (slotid (get norm-map langid)))
	      (knodb/index+! norms.index f slotid 'terms [language langid] norms)
	      (index-frame core.index f 'has slotid)))
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
	    (let ((glosses (get %glosses langid)))
	      (knodb/index+! glosses.index
			     f (get gloss-map langid) 'text
			     [language langid]
			     glosses)
	      (index-frame core.index f 'has (get gloss-map langid))
	      (table-increment! batch-state 'glosses (|| glosses))))
	  ;; Special cases for english
	  (when (test f 'words)
	    (let* ((words (get f 'words))
		   (phrases (pick words compound-string?)))
	      ;; (knodb/index+! words.index f en 'terms #[language en] words)
	      ;; (index-frame core.index f 'has en)
	      (when (exists? phrases)
		(knodb/index+! frags.index f en_frags 'phrases
			       #[language en fragsize 2]
			       phrases)
		(index-frame core.index f 'has en_frags))
	      (table-increment! batch-state 'words (|| words))
	      (table-increment! batch-state 'names (|| (pick words capitalized?)))
	      (table-increment! batch-state 'phrases (|| phrases))
	      ))
	  (when (test f 'norms)
	    (let* ((norms (get f 'words)))
	      (knodb/index+! norms.index f en_norms 'terms #[language en] norms)
	      (index-frame core.index f 'has en_norms)))
	  (when (test f 'gloss)
	    (let* ((glosses (get f 'gloss)))
	      (knodb/index+! glosses.index
			     f en_glosses 'text
			     [language english]
			     glosses)
	      (index-frame core.index f 'has en_glosses)
	      (table-increment! batch-state 'glosses (|| glosses)))))))
    (swapout f)
    (swapout words.adjunct f)
    (swapout norms.adjunct f)
    (swapout glosses.adjunct f)
    (swapout indicators.adjunct f)
    (swapout aliases.adjunct f)))

(define default-partopts [maxload 0.8 partsize 4000000])

(define (get-lex-index pool lang lextype)
  (try (pool/index/find pool 'keyslot (?? 'type lextype 'language lang))
       (pool/index/find pool 'name (string->symbol (glom (get lang '%langid) "_etc")))
       (pool/index/find pool 'name (string->symbol (glom "babel_" lextype)))
       (pool/index/find pool 'name 'babel_etc)
       (make-lex-index pool lang lextype)))

(defslambda (make-lex-index pool lang lextype)
  (try (pool/index/target pool 'keyslot (?? 'type lextype 'language lang))
       (pool/index/target pool 'name (string->symbol (glom (get lang '%langid) "_etc")))
       (let ((spec (try (pool/index/spec pool 'name (string->symbol (glom "babel_" lextype)))
			(pool/index/spec pool 'name 'babel_etc)
			#[name babel_etc path "babel_etc.index" size #4mib])))
	 (knodb/ref (mkpath (dirname (pool-source pool))
			    (string-subst (get spec 'path) ".index" ".flexindex"))
		    (cons `#[type flexindex flexindex kindex size #4mib create #t]
			  spec)))))

(defambda (get-indexes pool lextypes (languages all-languages))
  (make-aggregate-index (mt/call #[] get-lex-index pool languages lextypes)))

(define (get-base-indexes pool)
  (let ((core.index (pool/index/target pool 'name 'core))
	(words.index (get-indexes pool 'words))
	(norms.index (get-indexes pool 'norms))
	(frags.index (get-indexes pool 'fragments))
	(glosses.index (get-indexes pool 'glosses))
	(indicators.index (get-indexes pool 'indicators))
	(aliases.index (get-indexes pool 'aliases)))
    `#[core.index ,core.index
       words.index ,words.index
       norms.index ,norms.index
       frags.index ,frags.index
       glosses.index ,glosses.index
       indicators.index ,indicators.index
       aliases.index ,aliases.index]))
      
(define (index-languages pool (indexes))
  (default! indexes (get-base-indexes pool))
  (let* ((nconcepts (max (pool-load pool) #mib))
	 (adjuncts (begin (adjuncts/init! pool) (poolctl pool 'adjuncts)))
	 (oids (difference (pool-elts pool) (?? 'source @1/1) (?? 'status 'deleted)))
	 (loop-init (frame-create indexes
		      'words.adjunct (get adjuncts '%words)
		      'norms.adjunct (get adjuncts '%norms)
		      'glosses.adjunct (get adjuncts '%glosses)
		      'indicators.adjunct (get adjuncts '%indicators)
		      'aliases.adjunct (get adjuncts '%aliases))))
    (%watch "index-languages"
      "\nindexes" indexes
      "\nloop-init" loop-init)
    (engine/run index-words oids
      `#[loop ,loop-init
	 branchindexes {core.index aliases.index words.index frags.index
			indicators.index norms.index glosses.index}
	 counters {words phrases glosses names}
	 logcounters #(words phrases glosses names)
	 batchsize ,(config 'batchsize 500)
	 logfreq ,(config 'logfreq 50)
	 checkfreq 15
	 checktests ,{(engine/delta 'items 100000)
		      (engine/maxchanges 2000000)}
	 checkpoint ,{pool (getvalues indexes)}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t])
    (commit)))

(define (main poolname)
  (config! 'appid (glom "index-" (basename poolname #t) "-terms"))
  (when (config 'optimize #t)
    (optimize! '{engine brico brico/indexing brico/lookup
		 knodb knodb/search
		 knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms
		 knodb/fuzz/text knodb/fuzz/graph}))
  (let ((pool (getdbpool poolname)))
    (if (flexpool? pool)
	(let ((indexes (get-base-indexes pool)))
	  (do-choices (p (dbctl pool 'partitions) i)
	    (config! 'appid (glom "index-" (basename poolname #t) "-terms." (1+ i)))
	    (index-languages p indexes)))
	(index-languages pool))))


(when (config 'optimize #t config:boolean)
  (optimize! '{knodb knodb/branches knodb/search 
	       knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms knodb/fuzz/phrases
	       knodb/tinygis
	       fifo engine})
  (optimize! '{brico brico/indexing})
  (optimize-locals!))

(module-export! 'main)
