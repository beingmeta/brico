;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu).

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
(define-init wikid.setup #f)
(define-init wikid.index #f)
(define-init wikid.indexes #f)
(define-init wikid.opts #[background #f readonly #t basename "wikid.pool"])

(define-init wikid.opts #[readonly #t basename "wikid.pool"])

(define-init wikid:readonly #t)

;;; WIKID setup/config

(define (setup-wikid pool (opts wikid.opts))
  (and (pool? pool) (eq? (pool-base pool) @1/8000000)
       (not (config 'brico:disabled))
       (not (eq? wikid.setup pool))
       (let ((indexes (pool/getindexes pool opts)))
	 (lognotice |WIKID|
	   "Configured from " (pool-source pool) " with " (|| indexes) " indexes"
	   (if (getopt opts 'background) " (in background)")
	   (printout "\n    " pool))
	 (set! wikid.pool pool)
	 (set! wikid.index (pool/getindex pool))
	 (when (getopt opts 'background) (use-index wikid.index))
	 (set! wikid.indexes indexes)
	 (set! wikid.source (pool-source pool))
	 (set-wikid:readonly! wikid:readonly)
	 (set! wikid.setup pool)
	 pool)))

(define (set-wikid:readonly! flag)
  (knodb/readonly! wikid.pool flag)
  (knodb/readonly! wikid.index flag)
  (set! wikid:readonly flag))

;;; Configs

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

(config-def! 'wikid:disabled
  (slambda (var (val))
    (if (unbound? val) (getopt wikid.opts 'disabled)
	(let ((enabling (not val))
	      (disabled (getopt wikid.opts 'disabled)))
	  (store! wikid.opts 'disabled val)
	  (cond (wikid.setup
		 (unless enabling
		   (logwarn |WIKID|
		     "The database is already setup from " wikid.setup
		     "\n Disabling it has no effect (sorry).")))
		((and enabling disabled)
		 ;; This is the case where we are enabling the database and
		 ;;  will set it up if required
		 (if (config 'wikid:source)
		     (config! 'wikid:source (config 'wikid:source))
		     (lognotice |WIKID| "Enabling future database configuration"))))))))

(config-def! 'wikid:background
  (slambda (var (val))
    (cond ((unbound? val) (getopt wikid.opts 'background))
	  ((not wikid.setup) (store! wikid.opts 'background val) val)
	  ((getopt wikid.opts 'background)
	   (unless val
	     (logwarn |WIKID| "Clearing config, but cannot remove WIKID from the background"))
	   (getopt wikid.opts 'background))
	  (else (use-index wikid.index)
		(store! wikid.opts 'background val)
		#t))))

(define-init wikidsource-configfn
  (knodb/configfn setup-wikid wikid.opts))

(config-def! 'wikid:source wikidsource-configfn)
(config-def! 'wikidsource wikidsource-configfn)


