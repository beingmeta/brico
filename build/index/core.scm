#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/core)

(use-module '{texttools varconfig logger optimize text/stringfmts knodb engine})
(use-module 'brico/build/index)
(use-module '{brico brico/indexing})

(poolctl brico.pool 'readonly #f)

(use-module 'knodb/tinygis)

(define %volatile '{fixup})

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (indexer/prefetch (qc oids))))

(define index-also '{ticker})

(define core-index (target-file "core.index"))
(define wikidprops-index (target-file "wikidprops.index"))
(define wikidrefs-index (target-file "wikidrefs.index"))
(define latlong-index (target-file "latlong.index"))
(define wordnet-index (target-file "wordnet.index"))

(define (index-latlong index f)
  (when (test f '{lat long})
    (index-frame index f 'lat (get-degree-keys (picknums (get f 'lat))))
    (index-frame index f 'long (get-degree-keys (picknums (get f 'long)))))
  (when (test f 'lat/long)
    (index-frame index f 'lat (get-degree-keys (first (get f 'lat/long))))
    (index-frame index f 'long (get-degree-keys (second (get f 'lat/long))))))

(define (reporterror ex) (message "Error in indexcore: " ex))

(define (get-derived-slots f)
  {(get language-map (car (pick (get f '%words) pair?)))
   (get norm-map (car (pick (get f '%norm) pair?)))
   (get indicator-map (car (pick (get f '%signs) pair?)))
   (get gloss-map (car (pick (get f '%gloss) pair?)))
   (get alias-map (car (pick (get f '%aliases) pair?)))
   (get language-map (getkeys (pick (get f '%words) slotmap?)))
   (get norm-map (getkeys (pick (get f '%norm) slotmap?)))
   (get indicator-map (getkeys (pick (get f '%signs) slotmap?)))
   (get gloss-map (getkeys (pick (get f '%gloss) slotmap?)))
   (get alias-map (getkeys (pick (get f '%aliases) slotmap?)))})

(define fixup #f)
(define (indicator-fixup concept)
  (when (test concept '%index)
    (let ((indicators (pick (get concept '%index) pair?)))
      (when (exists? indicators) 
	(add! concept '%indicators indicators)
	(drop! concept '%index indicators)))))

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
      (cond ((or (test f 'type 'deleted) (test f 'deleted))
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
	     (index-brico core.index f)
	     (index-latlong latlong.index f)
	     (index-frame core.index f index-also)
	     (when (or (test f 'source 'wikidata) (test f 'type 'wikidprop))
	       (index-brico wikidprops.index f)
	       (index-frame wikidprops.index f index-also))
	     (index-wikid wikidprops.index f)
	     (when (test f 'wikidref) (index-frame wikidrefs.index f 'wikidref)))
	    ((test f 'type '{slot language lexslot kbsource})
	     (index-brico core.index f)
	     (index-frame core.index f index-also))
	    ((test f 'source) (index-frame core.index f 'source))
	    (else (index-frame core.index f 'status 'deleted))))
  (swapout frames)))

(define (main . names)
  (config! 'appid  "index-core")
  (let* ((pools (getdbpool (try (elts names) brico-pool-names)))
	 (core.index (target-index core-index [size #8mib] pools))
	 (latlong.index (target-index latlong-index #[keyslot {lat long}] pools))
	 (wordnet.index (tryif (overlaps? (pool-base pools) @1/0)
			  (target-index wordnet-index [keyslot wordnet-slotids] pools)))
	 (wikidrefs.index (target-index wikidrefs-index #[keyslot wikidref size 2] pools))
	 (wikidprops.index (target-index wikidprops-index [size 8] pools))
	 (index (make-aggregate-index {core.index latlong.index wikidrefs.index}
				      [register #t])))
    (commit pools) ;; Save updated INDEXES metadata on pools
    (info%watch "MAIN" core.index wikidprops.index latlong.index wordnet.index)
    (engine/run core-indexer (pool-elts pools)
      `#[loop #[core.index ,core.index
		wordnet.index ,wordnet.index
		latlong.index ,latlong.index
		wikidprops.index ,wikidprops.index
		wikidrefs.index ,wikidrefs.index]
	 branchindexes {core.index wordnet.index wikidprops.index
			wikidrefs.index 
			latlong.index}
	 batchsize ,(config 'batchsize 10000) batchrange 4
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pools core.index wikidprops.index wordnet.index 
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
    (commit)))

(when (config 'optimize #t config:boolean)
  (optimize! '{brico brico/indexing knodb/tinygis fifo engine})
  (optimize-locals!))

