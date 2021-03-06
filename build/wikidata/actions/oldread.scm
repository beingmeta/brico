#!/usr/bin/knox
;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'brico/build/wikidata/actions/read)

(when (file-exists? "read.cfg") (load-config "read.cfg"))

(use-module '{webtools archivetools texttools logger varconfig})
(use-module '{io/filestream text/stringfmts optimize})
(use-module '{kno/reflect kno/profile kno/mttools engine})

(use-module '{knodb knodb/branches knodb/typeindex knodb/flexindex})
(use-module '{brico brico/build/wikidata})

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(config! 'thread:logexit #f)
(config! 'filestream:inbufsize (* 50 #mib))
(config! 'filestream:bufsize (* 50 #mib))
(config! 'dbloglevel %warn%)
(config! 'xprofiling #t)

(define (read-wikidata-db-init)
  (dbctl brico.pool 'readonly #f))
(config! 'brico:onload read-wikidata-db-init)

(define inbufsize (* 1024 1024 3))
(varconfig! inbufsize inbufsize)

(define %loglevel %notice%)

(define dochain #f)
(varconfig! chain dochain config:boolean)

;;; Reading data

(define (skip-file-start port)
  "This skips the opening [ character."
  (getline port))

(define filestream-opts
  [startfn skip-file-start
   readfn getline])

;;; Parsing the data

(define (convert-lexslot slotval (new (frame-create #f)))
  (do-choices (key (getkeys slotval))
    (let ((v (get slotval key)))
      (if (vector? v)
	  (add! new key (get (elts v) 'value))
	  (add! new key (get v 'value)))))
  new)

(define (convert-sitelinks slotval (new (frame-create #f)))
  (do-choices (key (getkeys slotval))
    (let ((v (get slotval key)))
      (if (vector? v)
	  (doseq (elt v)
	    (add! new key (frame-create #f
			    'title (get v 'title) 'site (get v 'site)
			    'badges (elts (get v 'badges)))))
	  (add! new key (frame-create #f
			  'title (get v 'title) 'site (get v 'site)
			  'badges (elts (get v 'badges)))))))
  new)

(define typeslots '{type datatype rank snaktype type})
(define refslots '{type datatype rank snaktype type})

(define (convert-claims v)
  (cond ((symbol? v) (try (get propmaps.table (upcase v)) v))
	((string? v)
	 (if (has-prefix v {"p" "P"})
	     (try (get propmaps.table (upcase v)) v)
	     v))
	((vector? v) (convert-claims (elts v)))
	((table? v)
	 (when (test v 'references) (drop! v 'references))
	 (try
	  (tryif (test v 'datatype "wikibase-item")
	    (get-wikidref (get (get (get v 'datavalue) 'value) 'id)))
	  (tryif (test v 'entity-type "item")
	    (get-wikidref (get v 'id)))
	  (tryif (test v 'datatype "wikibase-entityid")
	    (get-wikidref (get (get v 'value) 'id)))
	  (let ((converted (frame-create #f))
		(keys (getkeys v)))
	    (do-choices (key keys)
	      (let* ((vals (get v key))
		     (slot (try (get propmaps.table (upcase key)) key)))
		(when (vector? vals) (set! vals (elts vals)))
		(cond ((overlaps? slot typeslots)
		       (add! converted slot (symbolize vals)))
		      (else (add! converted slot (convert-claims vals))))))
	    converted)))
	(else v)))

(define (import-wikid-item item index has.index)
  (let* ((id (get item 'id))
	 (ref (get-wikidref id))
	 (type (get item 'type))
	 (needs-init (fail? (oid-value ref))))
    ;;(info%watch "IMPORT-WIKID-ITEM" id ref)
    (unless (test ref 'lastrevid (get item 'lastrevid))
      (store! ref 'wikid id)
      (store! ref 'type type)
      (let ((moment (timestamp)))
	(store! ref 'modified moment)
	(unless (test ref 'created) (store! ref 'created moment)))
      (store! ref 'lastrevid (get item 'lastrevid))
      (store! ref 'labels (convert-lexslot (get item 'labels)))
      (store! ref 'aliases (convert-lexslot (get item 'aliases)))
      (store! ref 'descriptions (convert-lexslot (get item 'descriptions)))
      (store! ref 'sitelinks (convert-sitelinks (get item 'sitelinks)))
      (let* ((norms (get (get ref 'labels) 'en))
	     (words {norms (get (get ref 'aliases) 'en)})
	     (snorms (stdstring norms))
	     (swords (stdstring words))
	     (fnorms (for-choices (compound (pick {norms snorms} compound-string?))
		       (list (elts (words->vector compound)))))
	     (fwords (for-choices (compound (pick {words swords} compound-string?))
		       (list (elts (words->vector compound))))))
	(store! ref 'norms norms)
	(store! ref 'words words)
	(index-frame index ref 'wikid id)
	(index-frame index ref 'words {words swords fwords})
	(index-frame index ref 'norms {norms snorms fnorms}))
      (let* ((claims (convert-claims (get item 'claims)))
	     (props (getkeys claims)))
	(index-frame has.index ref 'has (list props))
	(store! ref 'claims claims)
	(do-choices (prop props)
	  (let* ((main (get (get claims prop) 'mainsnak))
		 (vals {(pickoids main) (pickstrings main) (picknums main)
			(get (get (pickmaps main) 'datavalue) 'value)}))
	    (add! ref prop vals)
	    (index-frame index ref prop (pickoids vals)))))
      (index-frame has.index ref 'has (getkeys ref))
      (index-frame index ref 'type)
      (unless (in-pool? ref brico.pool)
	(store! ref '%id (wikidata/makeid ref id)))
      (loginfo |Imported| id " ==> " ref))
    ref))

(define (import-enginefn batch batch-state loop-state task-state)
  (local index (try (get batch-state 'index) (get loop-state 'index)))
  (local newprops (get loop-state 'newprops.index))
  (local has.index (get loop-state 'has.index))
  (doseq (line batch)
    (let* ((json (onerror (jsonparse line 'symbolize) #f))
	   (id (and json (get json 'id))))
      (if json
	  (import-wikid-item
	   json
	   (if (has-prefix id {"P" "p"}) newprops index)
	   has.index)
	  (logwarn |BadJSON| line)))))

(define (setup)
  (when (file-exists? "read.cfg") (load-config "read.cfg"))
  (config-default! 'bricosource "brico")
  (when (file-directory? "wikidata")
    (config-default! 'wikidata "wikidata")
    ;; Should this be conditional on something like wikidata.pool being writable?
    (config! 'wikidata:build #t))
  (unless (config 'wikidata) (config! 'wikidata "wikidata"))
  (dbctl brico.pool 'readonly #f))

(define (main (maxitems #f)
	      (threadcount (mt/threadcount (config 'nthreads #t)))
	      (file (CONFIG 'INPUTFILE "latest-wikidata.json")))
  (setup)
  (let* ((in (filestream/open file filestream-opts))
	 (fillfn (slambda (n) (filestream/read/n in n)))
	 (started (elapsed-time)))
    (filestream/log! in)
    (engine/run import-enginefn fillfn
      `#[statefile ,(if wikidata.dir (mkpath wikidata.dir "read.state") "read.state")

	 loop #[index ,wikidata.index
		newprops.index ,newprops.index
		has.index ,has.index]
	 branchindexes index
	 maxitems ,maxitems

	 batchcall #t
	 nthreads ,threadcount
	 batchsize ,(config 'batchsize 1000)
	 queuesize ,(config 'queuesize 10000)
	 fillsize ,(config 'fillsize 5000)
	 fillstep ,(config 'fillstep (if threadcount (* threadcount 15)))

	 checkfreq ,(config 'checkfreq 60)
	 checktests ,(engine/delta 'items (config 'checkcount 100000))
	 checkpoint ,{wikidata.pool wikidata.index newprops.index has.index}
	 stopfns
	 ,(engine/test 'memusage (config 'maxmem {} config:bytes)
		       'items (or maxitems {})
		       'elapsed (config 'maxtime {} config:number))

	 loopdump ,(config 'loopdump #f)
	 logchecks #t
	 logfns {,engine/log ,engine/logrusage 
		 ,(lambda () (filestream/log! in))}
	 logfreq ,(config 'logfreq 30)])
    (filestream/save! in)))
  
(when (config 'optimized #t)
  (optimize! '{knodb knodb/flexpool knodb/flexindex knodb/adjuncts 
	       knodb/branches knodb/typeindex
	       brico brico/indexing brico/build/wikidata
	       io/filestream})
  (logwarn |Optimized| 
    "Modules " '{knodb knodb/flexpool knodb/flexindex knodb/adjuncts 
		 knodb/branches knodb/typeindex
		 brico brico/indexing brico/build/wikidata
		 io/filestream})
  (optimize-locals!)
  (logwarn |Optimized| (get-source)))

(when (config 'profiling #f)
  (config! 'profiled filestream/read)
  (config! 'profiled 
	   {get-wikidref probe-wikidref get-wikidprop
	    import-wikid-item 
	    convert-claims convert-lexslot
	    convert-sitelinks
	    filestream/read}))

(comment
 (begin
   (define in (filestream/open "latest-wikidata.json")) (filestream/read in)
   (define (read-item in) (jsonparse (filestream/read in)))
   (define last-input #f)
   (define (import (in in))
     (import-wikid-item (read-item in) wikidata.index has.index))))
