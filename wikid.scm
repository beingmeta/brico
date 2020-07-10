;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

(in-module 'brico/wikid)

(use-module '{texttools reflection logger varconfig stringfmts 
	      knodb knodb/config})

(define-init %loglevel %notify%)
;;(set! %loglevel %debug%)

(module-export! '{wikid.pool wikid.index wikid.source})
(define %nosubst '{wikid.source wikid.pool wikid.index wikid.background})

(define-init wikid.source #f)
(define-init wikid.pool #f)
(define-init wikid.index #f)
(define-init wikid.indexes #f)
(define-init wikid.opts #[background #t readonly #t basename "wikid.pool"])

(define (setup-wikid pool (opts wikid.opts))
  (and (pool? pool) (eq? (pool-base pool) @1/8000000)
       (let ((indexes (pool/getindexes pool)))
	 (lognotice |WikidConfig| pool)
	 (set! wikid.pool pool)
	 (set! wikid.index (pool/getindex pool))
	 (set! wikid.indexes indexes)
	 (set! wikid.source (pool-source pool))
	 pool)))

(define-init wikidsource-configfn
  (knodb/configfn setup-wikid wikid.opts))

(propconfig! 'wikid:background wikid.opts 'background)
(propconfig! 'wikid:readonly wikid.opts 'readonly)

(config-def! 'wikid:source wikidsource-configfn)
(config-def! 'wikidsource wikidsource-configfn)


