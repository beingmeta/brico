#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/core)
(use-module '{texttools varconfig logger optimize text/stringfmts knodb engine})
;; This module needs to go early because it (temporarily) disables the brico database
(use-module '{brico brico/indexing})

(use-module 'brico/build/index)

(use-module 'knodb/tinygis)

(define %volatile '{fixup})
(define %loglevel %notice%)

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (indexer/prefetch (qc oids))))

(define index-also '{ticker})

;; (define core-index (target-file "core.index"))
;; (define wikidprops-index (target-file "wikidprops.index"))
;; (define wikidrefs-index (target-file "wikidrefs.index"))
;; (define latlong-index (target-file "latlong.index"))
;; (define wordnet-index (target-file "wordnet.index"))
;; (define wikidprops-index (target-file "wikidprops.index"))

(define (index-latlong index f)
  (when (test f '{lat long})
    (index-frame index f 'lat (get-degree-keys (picknums (get f 'lat))))
    (index-frame index f 'long (get-degree-keys (picknums (get f 'long)))))
  (when (test f 'lat/long)
    (index-frame index f 'lat (get-degree-keys (first (get f 'lat/long))))
    (index-frame index f 'long (get-degree-keys (second (get f 'lat/long))))))

(define (reporterror ex) (message "Error in indexcore: " ex))

(define fixup #f)

(define wn-sources
  {@1/0{WN16} @1/46074"Wordnet 3.0, Copyright 2006 Princeton University"
   @1/94d47"Wordnet 3.1, Copyright 2011 Princeton University"})

(define index-sources
  '{@1/0{WN16}
    @1/46074"Wordnet 3.0, Copyright 2006 Princeton University"
    @1/94d47"Wordnet 3.1, Copyright 2011 Princeton University"
    wikidata})

(define wordnet-slotids
  (choice '{type source has words hypernym hyponym sensecat
	    sensekeys synsets verb-frames pertainym
	    lex-fileno}
	  '{has gloss %sensekeys}))

(defambda (core-indexer frames batch-state loop-state task-state)
  (let ((core.index (get batch-state 'core.index))
	(wikidprops.index (get batch-state 'wikidprops.index))
	(latlong.index (get batch-state 'latlong.index))
	(wikidrefs.index (get batch-state 'wikidrefs.index))
	(wordnet.index (get batch-state 'wordnet.index)))
    (info%watch "CORE-INDEXER/thread"
      core.index wikidprops.index latlong.index wordnet.index)
    (prefetch-oids! frames)
    (do-choices (f frames)
      (when fixup (fixup f))
      (cond ((or (empty? (oid-value f)) (test f 'type 'deleted) (test f 'deleted))
	     (index-frame core.index f 'status 'deleted))
	    ((test f 'source index-sources)
	     (when (exists? wordnet.index)
	       (index-frame wordnet.index
		   f '{type source has words hypernym hyponym sensecat
		       sensekeys synsets verb-frames pertainym
		       lex-fileno})
	       (index-frame wordnet.index f '%sensekeys (getvalues (get f '%sensekeys)))
	       (index-frame wordnet.index f 'has (getkeys f))
	       (index-gloss wordnet.index f 'gloss #default 'en))
	     (when (and (exists? wikidprops.index) (test f 'type 'wikidprop))
	       (index-frame wikidprops.index f 'has (getkeys f))
	       (index-frame wikidprops.index f '{wikid wikidref type wikidtype})
	       (index-frame wikidprops.index f 'wikid
			    {(upcase (get f 'wikid)) (downcase (get f 'wikid))})
	       (index-frame wikidprops.index f 'wikidref
			    {(upcase (get f 'wikidref)) (downcase (get f 'wikidref))}))
	     (index-brico core.index f)
	     (index-latlong latlong.index f)
	     (index-frame core.index f index-also)
	     (index-wikid wikidprops.index f)
	     (when (test f 'wikidref) (index-frame wikidrefs.index f 'wikidref)))
	    ((test f 'type '{slot language lexslot kbsource})
	     (index-brico core.index f)
	     (index-frame core.index f index-also))
	    ((test f 'source) (index-frame core.index f 'source))
	    (else (index-frame core.index f 'status 'deleted))))
  (swapout frames)))

(define (main poolname (outdir #f))
  (config! 'appid  "brico:index-core")
  (let* ((pool (knodb/ref poolname #f))
	 (core.index (pool/index/target pool outdir 'name 'core))
	 (latlong.index (pool/index/target pool outdir 'name 'latlong))
	 (wikidrefs.index (pool/index/target pool outdir 'keyslot 'wikidref))
	 (index (make-aggregate-index {core.index latlong.index wikidrefs.index}
				      [register #t]))
	 (wordnet.index (tryif (overlaps? (pool-base pool) @1/0)
			  (pool/index/target pool outdir 'name 'wordnet)))
	 (wikidprops.index (tryif (overlaps? (pool-base pool) @1/0)
			     (pool/index/target pool outdir 'name 'wikidprops))))
    (info%watch "MAIN" pool core.index wikidprops.index latlong.index wordnet.index)
    (prog1
        (engine/run core-indexer (pool-elts pool)
          `#[loop #[core.index ,core.index
		    wordnet.index ,wordnet.index
		    latlong.index ,latlong.index
		    wikidprops.index ,wikidprops.index
		    wikidrefs.index ,wikidrefs.index]
	     branchindexes {core.index wordnet.index wikidprops.index
			    wikidrefs.index latlong.index}
	     batchsize ,(config 'batchsize 10000)


	     ;; checktests ,{(tryif (config 'savefreq)
	     ;; 		(engine/interval (getopt opts 'savefreq (config 'savefreq 60))))
	     ;; 	      (engine/maxchanges (getopt opts 'maxchanges 1_000_000))}

	     checktests ,(engine/interval (config 'savefreq 60))
	     checkpoint ,{pool core.index wikidprops.index wordnet.index
		          wikidrefs.index latlong.index}
	     logfns {,engine/log ,engine/logrusage}
	     logfreq ,(config 'logfreq 50)
	     logchecks #t])
      (commit)
      (swapout)
      (let ((wordforms (find-frames core.index 'type 'wordform)))
        (lognotice |Wordforms|
	  "Indexing " ($count (choice-size wordforms) "wordform"))
        (prefetch-oids! wordforms)
        (do-choices (f wordforms)
	  (index-frame (try wordnet.index core.index) f '{word of sensenum language rank type}))
        (commit {wordnet.index core.index}))
      (commit))))
(module-export! 'main)

(when (config 'optimize #t config:boolean)
  (optimize! '{knodb knodb/branches knodb/search
	       knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms
	       knodb/tinygis
	       fifo engine})
  (optimize! '{brico brico/indexing})
  (optimize-locals!))

