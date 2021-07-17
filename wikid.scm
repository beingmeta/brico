;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'brico/wikid)

(use-module '{texttools reflection logger varconfig text/stringfmts 
	      knodb knodb/config})

(define-init %loglevel %notify%)
;;(set! %loglevel %debug%)

(module-export! '{wikid.pool wikid.index wikid.source})
(define %nosubst '{wikid.source wikid.pool wikid.index wikid.background})
(define %optmods '{brico logger knodb})

(define-init wikid.source #f)
(define-init wikid.pool #f)
(define-init wikid.index #f)
(define-init wikid.indexes #f)
(define-init wikid.opts #[background #t readonly #t basename "wikid.pool"])
(define-init wikid:readonly #t)

;;; Read/write config

(define (set-wikid:readonly! flag)
  (knodb/readonly! wikid.pool flag)
  (knodb/readonly! wikid.index flag)
  (set! wikid:readonly flag))
(config-def! 'wikid:readonly
  (lambda (var (val))
    (cond ((unbound? val) wikid:readonly)
	  ((and val wikid:readonly) #f)
	  ((not (or val wikid:readonly)) #f)
	  ((and wikid.pool wikid.index) (set-wikid:readonly! val))
	  ((or wikid.pool wikid.index) 
	   (logwarn |IncompleteWikiDB|
	     "Can't set readonly to " val " for wikid.pool=" wikid.pool " wikid.index=" wikid.index))
	  (else (set! wikid:readonly val)))))

;;; WIKID setup

(define (setup-wikid pool (opts wikid.opts))
  (and (pool? pool) (eq? (pool-base pool) @1/8000000)
       (let ((indexes (pool/getindexes pool)))
	 (lognotice |WikidConfig| pool)
	 (set! wikid.pool pool)
	 (set! wikid.index (pool/getindex pool))
	 (set! wikid.indexes indexes)
	 (set! wikid.source (pool-source pool))
	 (set-wikid:readonly! wikid:readonly)
	 pool)))

(propconfig! 'wikid:background wikid.opts 'background)

(define-init wikidsource-configfn
  (knodb/configfn setup-wikid wikid.opts))

(config-def! 'wikid:source wikidsource-configfn)
(config-def! 'wikidsource wikidsource-configfn)


