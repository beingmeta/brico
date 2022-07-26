#!/usr/bin/knox
;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'brico/build/wikidata/actions/read)

(use-module '{webtools archivetools texttools logger varconfig})
(use-module '{io/filestream text/stringfmts optimize})
(use-module '{kno/reflect kno/profile kno/profiling kno/mttools kno/statefiles})
(use-module '{engine engine/readfile})

(use-module '{knodb knodb/branches knodb/typeindex regex knodb/flexindex})
(use-module '{brico brico/build/wikidata})

(module-export! '{main})
(module-export! '{import-wikid-item convert-claims})

(define engine (get-module 'engine))
(define branches (get-module 'knodb/branches))
(define flexindex (get-module 'knodb/flexindex))

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(config! 'thread:logexit #f)
(config! 'filestream:inbufsize (* 50 #mib))
(config! 'filestream:bufsize (* 50 #mib))
(config! 'dbloglevel %warn%)
(config! 'xprofiling #t)

(define-init all-threads {})

(define wikidread-batchsize 1000)
(varconfig! wikidread:batchsize wikidread-batchsize)
(define wikidread-queuesize 10000)
(varconfig! wikidread:queuesize wikidread-queuesize)
(define wikidread-fillsize 5000)
(varconfig! wikidread:fillsize wikidread-fillsize)
(define wikidread-fillstep-perthread 100)
(varconfig! wikidread:fillstep wikidread-fillstep-perthread)

(define (read-wikidata-db-init)
  (dbctl brico.pool 'readonly #f))
(config! 'brico:onload read-wikidata-db-init)

(define inbufsize (* 1024 1024 3))
(varconfig! inbufsize inbufsize)

(define %loglevel %notice%)

(define dochain #f)
(varconfig! chain dochain config:boolean)

(define %optmods
 '{knodb knodb/flexpool knodb/flexindex knodb/adjuncts 
   knodb/branches knodb/typeindex
   brico brico/indexing brico/build/wikidata
   engine engine/readfile})

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

(define known-slotids (choice typeslots refslots '{property precision id}))
(define-init add-known-slotid! 
  (slambda (slot) 
    (set+! known-slotids slot)
    ;; This should normalize it
    (set! known-slotids known-slotids)))

(define (convert-claims v)
  (cond ((symbol? v) (try (get propmaps.table (upcase v)) v))
	((string? v)
	 (try (get propmaps.table v)
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
		     (slot (try (get propmaps.table key)
				(tryif (and (string? key) (has-prefix key {"p" "P" "Q" "q"}))
				  (get-wikidref key))
				key)))
		(cond ((oid? slot))
		      ((overlaps? slot known-slotids))
		      ((and (string? key) (regex/match #/p[[:digit:]]+/i key))
		       (->wikidprop key))
		      ((and (symbol? key)
			    (regex/match #/p[[:digit:]]+/i (symbol->string key)))
		       (->wikidprop key))
		      (else (add-known-slotid! slot)))
		(when (vector? vals) (set! vals (elts vals)))
		(cond ((overlaps? slot typeslots)
		       (add! converted slot (symbolize vals)))
		      (else (add! converted slot (convert-claims vals))))))
	    converted)))
	(else v)))

(define lattice-props {wikid-instanceof wikid-subclassof})

(define (import-wikid-item item pos index base.index wikids.index)
  (let* ((id (get item 'id))
	 (ref (get-wikidref id))
	 (type (get item 'type))
	 (needs-init (fail? (oid-value ref))))
    ;;(info%watch "IMPORT-WIKID-ITEM" id ref)
    (unless (test ref 'lastrevid (get item 'lastrevid))
      (store! ref 'wikid id)
      (store! ref 'wikidtype type)
      (let ((moment (timestamp)))
	(store! ref 'modified moment)
	(unless (test ref 'created) (store! ref 'created moment)))
      (when pos (store! ref 'filepos pos))
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
	(index-frame wikids.index ref 'wikid id)
	(index-frame index ref 'words {words swords fwords})
	(index-frame index ref 'norms {norms snorms fnorms}))
      (let* ((claims (convert-claims (get item 'claims)))
	     (props (getkeys claims)))
	;; Index claim props under a different composite key `(list prop)`
	(index-frame base.index ref 'has (list props))
	(store! ref 'claims claims)
	(do-choices (prop props)
	  (let* ((main (get (get claims prop) 'mainsnak))
		 (vals {(pickoids main) (pickstrings main) (picknums main)
			(get (get (pickmaps main) 'datavalue) 'value)})
		 (oidvals (pickoids vals)))
	    (add! ref prop vals)
	    (index-frame base.index ref 'has prop)
	    (when (overlaps? prop lattice-props)
	      (index-frame index ref prop (list oidvals))
	      (when (eq? prop wikid-subclassof)
		(add! ref 'type 'wikidclass)
		(index-frame base.index ref 'type 'wikidclass)))
	    (index-frame index ref prop oidvals)
	    (index-frame index oidvals 'refs ref))))
      (index-frame base.index ref 'has (getkeys ref))
      (index-frame base.index ref '{type wikidtype})
      (unless (in-pool? ref brico.pool)
	(store! ref '%id (wikidata/makeid ref id)))
      (loginfo |Imported| id " ==> " ref))
    ref))

(define (import-enginefn batch batch-state loop-state task-state)
  (local index (try (get batch-state 'index) (get loop-state 'index)))
  (local wikidprops (get loop-state 'wikidprops.index))
  (local base.index (get loop-state 'base.index))
  (local wikids.index (get loop-state 'wikids.index))
  (doseq (entry batch)
    (let* ((line (if (pair? entry) (cdr entry) entry))
	   (json (onerror (jsonparse line 'symbolize) #f))
	   (id (and json (get json 'id))))
      (if json
	  (import-wikid-item
	   json
	   (and (pair? entry) (car entry))
	   (qc index (tryif (has-prefix id {"P" "p"}) wikidprops))
	   (qc base.index (tryif (has-prefix id {"P" "p"}) wikidprops))
	   (qc wikids.index (tryif (has-prefix id {"P" "p"}) wikidprops)))
	  (logwarn |BadJSON| line)))))

(define (setup . ignored)
  (config-default! 'bricosource "brico")
  (when (file-directory? "wikidata")
    (config-default! 'wikidata "wikidata")
    ;; Should this be conditional on something like wikidata.pool being writable?
    (config! 'wikidata:build #t))
  (unless (config 'wikidata) (config! 'wikidata "wikidata"))
  (dbctl brico.pool 'readonly #f)
  (dbctl wikidprops.index 'readonly #f))

(define (open-wikidata-file file)
  (let ((in (open-input-file file)))
    (getline in)
    in))

(define (runloop (maxitems (config 'maxitems #f))
		 (threadcount (mt/threadcount (config 'nthreads #t)))
		 (file (CONFIG 'INFILE "latest-wikidata.json")))
  (let* ((statefile (if wikidata.dir (mkpath wikidata.dir "read.state") "read.state"))
	 (state (tryif (file-exists? statefile) (statefile/read statefile)))
	 (jobid (config 'JOBID (or (getenv "U8_JOBID") "readwikidata")))
	 (started (elapsed-time)))
    (config! 'appid (glom jobid "#" (1+ (try (get state 'cycles) 0))))
    (engine/run import-enginefn engine/readfile/fillfn
      `#[statefile ,statefile
	 stopfile ,(glom jobid ".stop")
	 donefile ,(or (getenv "ENGINE_DONEFILE") (getenv "U8_DONEFILE") (glom jobid ".done"))
	 loop #[index ,wikidbuild.index
		wikidprops.index ,wikidprops.index
		base.index ,base.index
		wikids.index ,wikids.index]
	 infile ,(config 'infile "latest-all.json")
	 posfn  #t
	 openfn ,open-wikidata-file
	 branchindexes index
	 maxitems ,maxitems

	 batchcall #t
	 nthreads ,threadcount
	 batchsize ,(config 'batchsize 1000)
	 queuesize ,(config 'queuesize 10000)
	 fillsize ,(config 'fillsize 5000)
	 fillstep ,(config 'fillstep 
			   (if threadcount
			       (* threadcount wikidread-fillstep-perthread)
			       wikidread-fillstep-perthread))

	 checkfreq ,(config 'checkfreq 60)
	 checktests ,(engine/delta 'items (config 'checkcount 100000))
	 checkpoint ,{wikidata.pool brico.pool wikidbuild.index wikidprops.index base.index wikids.index}
	 stopfns
	 ,(engine/test 'memusage (config 'maxmem {} config:bytes)
		       'items (or maxitems {})
		       'elapsed (config 'maxtime {} config:number))

	 loopdump ,(config 'loopdump #f)
	 logchecks #t
	 logfns {,engine/log ,engine/logrusage ,engine/readfile/log}
	 logfreq ,(config 'logfreq 30)])))

(define (main (maxitems (CONFIG 'MAXITEMS #f))
	      (threadcount (mt/threadcount (config 'nthreads #t)))
	      (file (CONFIG 'INFILE "latest-wikidata.json")))
  (unless (file-exists? file) (irritant file |MissingInputFile|))
  (setup)
  (unless (and maxitems (= maxitems 0))
    (let ((state (runloop maxitems threadcount file)))
      (logwarn |Engine/Threads/Done|
	(do-choices (thread (get state 'threads))
	  (lineout "  " (thread-id thread) "\t" thread)))
      (logwarn |RunDone| "Trying redundant commit")
      (commit)
      (lineout (listdata (get state 'taskstate)))
      (when (or (config 'profiling #f) (and (exists? (config 'profiled)) (config 'profiled)))
        (profile/report import-enginefn #default
                        profile/time profile/utime profile/stime
			profile/runsecs profile/idlesecs
                        profile/run% profile/idle%
                        profile/ncalls profile/nitems
                        profile/waits profile/pauses profile/faults)
        (let ((table (profiles->table))
              (filename (runfile ".profile.xtype")))
	  (store! table 'taskstate (get state 'taskstate))
	  (store! table 'summary
	    (frame-create #f
	      'name (get state 'name)
	      'started (get state 'started)
	      'batchsize (get state 'batchsize)
	      'stopfile (get state 'stopfile)
	      'donefile (get state 'donefile)
	      'filltime (get state 'filltime)
	      'threadtime (get state 'threadtime)
	      'clocktime (get state 'clocktime)
	      'nthreads (get state 'nthreads)
	      'maxitems (get state 'maxitems)
	      'stopval (get state 'stopval)
	      'items (get state 'items)
	      'cycles (get state 'cycles)))
	  (store! table 'opts (get state 'opts))
          (write-xtype (profiles->table) (extend-byte-output filename))
          (logwarn |ProfilesUpdated| filename)))
      (config! 'fastexit #t)
      (logwarn |RunDone|
	"Everything is saved, exiting (main)"))))
  
(when (config 'profiling #f)
  ;; (config! 'profiled import-enginefn)
  (config! 'profiled filestream/read)
  (config! 'profiled {oid-value index-frame index/save! index/merge! flexindex/front})
  (config! 'profiled {commit
                      (get branches 'branch/commit!)
                      (get branches 'index/branch)
                      (get flexindex 'getfront)
                      (get flexindex 'make-front)})
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
     (import-wikid-item (read-item in) wikidbuild.index base.index wikids.index))))

