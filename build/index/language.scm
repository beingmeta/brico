#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/languages)

(use-module '{texttools varconfig logger optimize text/stringfmts engine})
;;; This module needs to go early because it (temporarily) disables the brico database
(use-module 'brico/build/index)

(use-module '{knodb knodb/search knodb/fuzz knodb/adjuncts})
(use-module '{brico})

(define just-languages {})
(define skip-languages {})
(varconfig! brico:terms:justlangs just-languages)
(varconfig! brico:terms:skiplangs skip-languages)

(define english @1/2c1c7)

(define default-size 50)

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

(define separate-languages '{EN NL ES IT DE SK PL DK})
(define separate-languages 'EN)

(define (get-indexes pool lextype)
  (make-aggregate-index
   {(for-choices (lang all-languages)
      (pool/index/target pool 'keyslot (?? 'type lextype 'language lang)))
    (pool/index/target pool 'name (string->symbol (glom "babel_" lextype)))}))

(define (main poolname . languages)
  (config! 'appid (glom "index-" (basename poolname ".pool") "-multilingual"))
  (when (config 'optimize #t)
    (optimize! '{engine brico brico/indexing brico/lookup
		 knodb knodb/search 
		 knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms
		 knodb/fuzz/text knodb/fuzz/graph}))
  (let* ((pool (getdbpool poolname))
	 (nconcepts (max (pool-load pool) #mib))
	 (core.index (pool/index/target pool 'name 'core))
	 (words.index (get-indexes pool 'words))
	 (norms.index (get-indexes pool 'norms))
	 (frags.index (get-indexes pool 'fragments))
	 (indicators.index (get-indexes pool 'indicators))
	 (glosses.index (get-indexes pool 'glosses))
	 (aliases.index (get-indexes pool 'aliases))
	 (adjuncts (begin (adjuncts/init! pool) (poolctl pool 'adjuncts)))
	 (oids (difference (pool-elts pool) (?? 'source @1/1) (?? 'status 'deleted)))
	 (loop-init `#[core.index ,core.index
		       words.index ,words.index 
		       frags.index ,frags.index
		       indicators.index ,indicators.index
		       glosses.index ,glosses.index
		       aliases.index ,aliases.index
		       norms.index ,norms.index
		       words.adjunct ,(get adjuncts '%words)
		       norms.adjunct ,(get adjuncts '%norms)
		       glosses.adjunct ,(get adjuncts '%glosses)
		       indicators.adjunct ,(get adjuncts '%indicators)
		       aliases.adjunct ,(get adjuncts '%aliases)]))
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
