;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index)

(use-module '{logger texttools varconfig fifo engine knodb text/stringfmts ezrecords})
(use-module '{knodb knodb/search knodb/fuzz knodb/branches knodb/flexindex})

(module-export! '{target-index lex-index lex-indexes
		  target-file getdbpool
		  brico-pool-names get-index-size get-pool-size})

(config! 'cachelevel 2)
(config! 'thread:logexit #f)

;; We disable BRICO setup until we have used our pool (which might be BRICO)
;;(config! 'brico:disabled #t)

(define indir (config 'indir (abspath "brico/")))
(define outdir (config 'outdir (abspath "fresh/")))
(define bugjar (abspath "bugjar/"))

(unless (file-directory? bugjar) (mkdir bugjar))
(config! 'engine:bugjar bugjar)

;;(config! 'engine:threads 1)

(define brico-pool-names "brico.pool")

(define (check-dirs)
  (unless (file-directory? indir)
    (logpanic |BadInputDir|
      "The specified input path " (write indir) " isn't a directory"))
  (unless (file-directory? outdir)
    (logpanic |BadOutputDir|
      "The specified output path " (write outdir) " isn't a directory")))
(when (config 'checkdirs #t config:boolean) (check-dirs))

(define (getdbpool arg (indexes 'core))
  (let ((pool (knodb/ref arg)))
    (when (eq? (pool-base pool) @1/0)
      (config! 'bricosource pool))
    (config! 'brico:disabled #f)
    (when indexes
      (set+! indexes 'core)
      (do-choices (name indexes)
	(use-index (pool/index/open pool 'name name))))
    (do-choices (pool pool)
      (dbctl pool 'metadata 'readonly #f)
      (dbctl pool 'readonly #f))
    pool))

;;(define misc-slotids (file->dtype (mkpath data-dir "miscslots.dtype")))

;;(config! 'bricosource indir)
;;(use-pool (mkpath indir "brico.pool"))

(use-module '{brico brico/indexing})
(use-module '{brico brico/indexing kno/mttools trackrefs optimize knodb/tinygis})
(use-module '{logger varconfig})

(defrecord (langinfo)
  language id norm aliases frag cue gloss)

(define lang-slots
  (let ((table (make-hashtable)))
    (do-choices (lang (pickoids all-languages))
      (store! table {lang (get lang 'key)}
	      (cons-langinfo lang (get lang 'key)
			     (get norm-map lang)
			     (get alias-map lang)
			     (get frag-map lang)
			     (get indicator-map lang)
			     (get gloss-map lang))))))

;; (define (make-threadindex base)
;;   (let ((tmp (frame-create #f)))
;;     (do-choices (slotid (getkeys base))
;;       (store! tmp slotid (make-hashtable)))
;;     tmp))

;; (define (threadindex/merge! into from)
;;   (do-choices (slotid (getkeys from))
;;     (index/merge! (try (get into slotid) (get into '%default))
;; 		  (get from slotid))))

(define dontindex (choice (?? 'source @1/1)))

(define (target-file name) (mkpath outdir name))

(define-init flexindex-threshold #f) ;; 20000000
;;(define-init flexindex-partsize  10_000_000) ;; 20000000
(define-init flexindex-partsize  1_000_000) ;; 20000000

(define (writable-index . args)
  (let ((ix (apply open-index args)))
    (indexctl ix 'readonly #f)
    ix))

(defambda (access-index filename opts size keyslot)
  (unless (has-suffix filename {".index" ".fileindex"})
    (cond ((and flexindex-threshold (> size flexindex-threshold))
	   (set! filename (glom filename ".flexindex"))
	   (set! size flexindex-partsize))
	  (else (set! filename (glom filename ".index")))))
  (cond ((has-suffix filename ".flexindex")
	 (flex/open-index filename (cons (frame-create #f
					   'create #t
					   'partsize (getopt opts 'partsize size)
					   'keyslot (tryif keyslot keyslot))
					 opts)))
	((and (file-exists? filename) (not (config 'REBUILD #f config:boolean)))
	 (writable-index filename
			 `(#[register ,(getopt opts 'register #t)]
			   . ,opts)))
	(else (when (file-exists? filename)
		(logwarn |ReplacingFile|
		  (write filename) ", backup in "
		  (write  (glom filename ".bak")))
		(move-file filename (glom filename ".bak")))
	      (make-index filename
		`(#[type kindex size ,size keyslot ,keyslot] . ,opts))
	      (lognotice |NewIndex| "Making new file index " filename)
	      (writable-index filename
			      `(#[register ,(getopt opts 'register #t)]
				. ,opts)))))

(defambda (get-pool-size pools (minval #mib))
  (let ((total-oids 0))
    (when pools
      (do-choices (pool pools)
	(set! total-oids (+ total-oids (pool-load pool)))))
    (max total-oids minval)))

(define (get-index-size (poolsize #1mib) (indexsize 8.0))
  (if (inexact? indexsize)
      (->exact (ceiling (* indexsize poolsize)))
      (if (< indexsize 100)
	  (max (* 3 poolsize) indexsize)
	  indexsize)))

;; (define (target-index filename (opts #f) (pool #f) (indexsize) (keyslot))
;;   (default! indexsize (getopt opts 'indexsize 10.0))
;;   (default! keyslot (getopt opts 'keyslot (config 'keyslot #f)))
;;   (local poolsize (if pool (pool-load pool) #1mib))
;;   (unless (search "/" filename)
;;     (set! filename (mkpath outdir filename)))
;;   (unless (file-directory? (dirname filename))
;;     (mkdirs (dirname filename)))
;;   (let ((index (access-index filename opts (get-index-size poolsize indexsize) keyslot)))
;;     (when pool
;;       (let* ((indexes (or (poolctl pool 'metadata 'indexes) {}))
;; 	     (root-dir (getopt opts 'indexroot))
;; 	     (index-path (if root-dir
;; 			     (strip-prefix (index-source index) root-dir)
;; 			     (basename (index-source index)))))
;; 	(if (has-prefix index-path "/")
;; 	    (logerr |NoRelative|
;; 	      "Can't find relative reference to " (index-source index)
;; 	      " for the pool " pool)
;; 	    (if (overlaps? index-path indexes)
;; 		(loginfo |ExistingIndex|
;; 		  "Index path " (write index-path) " is already configured for "
;; 		  pool)
;; 		(begin (poolctl pool 'metadata 'indexes {index-path indexes})
;; 		  (logwarn |AddIndexPath| (write index-path) " to " pool))))))
;;     index))

(define (lex-index type language (opts #f) (pool #f) (indexsize) (keyslot) (filename) (langid))
  (default! indexsize (getopt opts 'size 10.0))
  (default! langid (and language (if (symbol? language) language (get language '%langid))))
  (default! keyslot
    (try
     (cond ((and type language) (?? '%langid langid 'type type))
	   (type (?? 'type 'lexslot 'type type))
	   (language (?? 'type 'lexslot '%langid langid))
	   (else #f))
     #f))
  (default! filename
    (cond ((and type language) (glom langid "_" type))
		(language (glom langid "_terms"))
		(type (glom "lex_" type))
		(else "terms")))
  (target-index filename opts pool indexsize (qc keyslot)))

(defambda (lex-indexes types languages (opts #f) (pool #f) (indexsize))
  (default! indexsize (getopt opts 'size #default))
  (let ((indexes (lex-index types languages opts pool indexsize)))
    (if (ambiguous? indexes)
	(make-aggregate-index indexes)
	indexes)))

