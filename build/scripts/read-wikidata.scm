#!/usr/bin/knox
;;; -*- Mode: Scheme; -*-

(config! 'bricosource "brico")
(when (file-directory? "wikidata")
  (config-default! 'wikidata (abspath "wikidata"))
  ;; Should this be conditional on something like wikidata.pool being writable?
  (config! 'wikidata:build #t))

(use-module '{logger webtools varconfig libarchive texttools
	      io/filestream brico text/stringfmts optimize
	      kno/reflect kno/profile
	      kno/mttools})
(use-module '{knodb knodb/branches knodb/typeindex knodb/flexindex})
(use-module 'brico/build/wikidata)

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(config! 'thread:logexit #f)
(config! 'filestream:inbufsize (* 50 #mib))
(config! 'filestream:bufsize (* 50 #mib))
(config! 'dbloglevel %warn%)
(config! 'xprofiling #t)

(dbctl brico.pool 'readonly #f)

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

(define (checkpoint in)
  (wikidata/save!)
  (filestream/save! in))

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

;;; Reporting

(define (runstats (usage (rusage)))
  (stringout "CPU: " (inexact->string (get usage 'cpu%) 2) "%"
    ", load: " (doseq (v (get usage 'loadavg) i)
	       (printout (if (> i 0) " ") (inexact->string v 2)))
    ", resident: " ($bytes (get usage 'memusage))
    ", virtual: " ($bytes (get usage 'vmemusage))))

(define (read-loop in (duration 60) (index wikidata.index) (has.index has.index))
  (let ((branch (index/branch index))
	(started (elapsed-time))
	(saved (elapsed-time)))
    (while (< (elapsed-time started) duration)
      (let* ((line (filestream/read in))
	     (item (and (satisfied? line) (string? line)
			(jsonparse line 'symbolize))))
	(debug%watch "READ-LOOP" line item)
	(when item
	  (import-wikid-item item branch has.index)
	  (when  (and (> (elapsed-time saved) 10)
		      (zero? (random 500)))
	    (branch/commit! branch)
	    (set! saved (elapsed-time)))))
      (when (filestream/done? in) (break)))
    (branch/commit! branch)))

(define (thread-loop in (threadcount #t) (duration 60) 
		     (index wikidata.index) (has.index has.index))
  (let ((threads {}))
    (dotimes (i (mt/threadcount threadcount))
      (set+! threads (thread/call read-loop in duration index has.index)))
    (thread/join threads)))

(define (dobatch in (threadcount #t) (duration 120) 
		 (index wikidata.index) (has.index has.index))
  (let ((start-count (filestream-itemcount in))
	(start-time (elapsed-time))
	(before (and (reflect/profiled? filestream/read)
		     (profile/getcalls filestream/read))))
    (if threadcount
	(thread-loop in threadcount duration index has.index)
	(let ((started (elapsed-time))
	      (saved (elapsed-time)))
	  (while (< (elapsed-time started) duration)
	    (let* ((entry (filestream/read in #t))
		   (line (cdr entry))
		   (count (car entry))
		   (item (and (satisfied? line) (string? line)
			      (jsonparse line 'symbolize))))
	      (when item
		(import-wikid-item item index has.index)))
	    (when (filestream/done? in) (break)))))
    (let ((cur-count (filestream-itemcount in)))
      (lognotice |Saving|
	"wikidata after processing "
	($count (- cur-count start-count) "item") " in "
	(secs->string (elapsed-time start-time)) 
	" (" ($rate (- cur-count start-count) (elapsed-time start-time))
	" items/sec) -- "
	(runstats)))
    (when before
      (let ((cur (profile/getcalls filestream/read)))
	(lognotice |ReadProfile|
	  "After " (secs->string (elapsed-time start-time)) 
	  " filestream/read took "
	  (secs->string (- (profile/time cur) (profile/time before)))
	  " (clock), "
	  (secs->string (- (profile/utime cur) (profile/utime before)))
	  " (user)" 
	  (secs->string (- (profile/stime cur) (profile/stime before)))
	  " (system) across "
	  ($count (- (profile/ncalls cur) (profile/ncalls before)) "call")
	  " interrupted by "
	  ($count (- (profile/waits cur) (profile/waits before)) "wait") )))
    (checkpoint in)
    (clearcaches)
    (filestream/log! in '(overall))))

(define (main (file "latest-wikidata.json") 
	      (secs (config 'cycletime 120))
	      (cycles (config 'cycles 10))
	      (threadcount (mt/threadcount (config 'nthreads #t))))
  (unless (config 'wikidata) (config! 'wikidata (abspath "wikidata")))
  (let ((in (filestream/open file filestream-opts))
	(dochain (and (bound? chain) (config 'chain #t config:boolean)))
	(started (elapsed-time)))
    (filestream/log! in)
    (dotimes (i cycles)
      (lognotice |Cycle| "Starting #" (1+ i) " of " cycles ": " (runstats))
      (dobatch in threadcount secs)
      (lognotice |Cycle| "Finished #" (1+ i) "/" cycles " cycles, "
		 "processed " ($count (filestream-itemcount in) "item") " in "
		 (secs->string (elapsed-time started)))
      (filestream/log! in '(overall)))
    (checkpoint in)
    (if (or (not dochain) (unbound? chain) (not (applicable? chain)))
	(logwarn |NoChain| "Just exiting")
	(unless (or (file-exists? "read-wikidata.stop") (filestream/done? in))
	  (if chain
	      (chain file secs cycles threadcount)
	      (logcrit |NoChain| "Just exiting"))))))
  
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
  (optimize!)
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
