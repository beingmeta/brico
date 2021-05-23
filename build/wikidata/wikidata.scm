;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'brico/build/wikidata)

(use-module '{webtools libarchive texttools})
(use-module '{logger varconfig text/stringfmts})
(use-module '{knodb knodb/branches knodb/typeindex 
	      knodb/flexindex})
(use-module '{brico})

(module-export! '{wikidata.dir
		  wikidata.pool wikidata.index
		  words.index norms.index has.index props.index wikidata.props
		  propmaps.table
		  wikidata/ref wikid/ref
		  wikidata/find wikid/find
		  wikidata/makeid
		  get-wikidref
		  probe-wikidref
		  get-wikidprop})

(module-export! '{wikidata/save!})

(define %loglevel %notice%)

(define-init wikidata.dir #f)

(define-init wikidata.pool #f)
(define-init wikidata.index #f)

(define-init words.index #f)
(define-init norms.index #f)
(define-init has.index #f)
(define-init props.index #f)

(define-init propmaps.table (make-hashtable))

(define-init wikidata-readonly #f)
(define-init wikidata-build #f)

(define-init set-wikidata-dir!
  (slambda (dir) (setup-wikidata dir)))

(define (setup-wikidata dir)
  (logwarn |SetupWikidata| dir)

  (set! wikidata.dir (realpath dir))

  (set! wikidata.pool
    (knodb/make (mkpath dir "wikidata.flexpool")
		[create #t type 'flexpool
		 base @31c1/0 capacity (* 128 1024 1024)
		 partsize (* 1024 1024) pooltype 'kpool
		 prefix "pools/"
		 adjuncts #[labels #[pool "labels"]
			    aliases #[pool "aliases"]
			    claims #[pool "claims"]
			    sitelinks #[pool "sitelinks"]]
		 prealloc #t
		 reserve 120]))

  (set! words.index
    (if (and (file-exists? (mkpath dir "words.index"))
	     (file-exists? (mkpath dir "rare/words.flexindex")))
	{(knodb/ref (mkpath dir "words.index") [readonly #t keyslot 'words register #t])
	 (knodb/ref (mkpath dir "rare/words.flexindex") [readonly #t keyslot 'words register #t])}
	(flex/open-index (mkpath dir "words.flexindex")
			 [indextype 'kindex size (* 6 1024 1024) create #t
			  readonly (not (config 'wikidata:build))
			  justfront (config 'wikidata:build)
			  keyslot 'words register #t
			  maxkeys (* 4 1024 1024)])))

  (set! norms.index
    (if (and (file-exists? (mkpath dir "norms.index"))
	     (file-exists? (mkpath dir "rare/norms.flexindex")))
	{(knodb/ref (mkpath dir "norms.index") [readonly #t keyslot 'norms register #t])
	 (knodb/ref (mkpath dir "rare/norms.flexindex") [readonly #t keyslot 'norms register #t])}
	(flex/open-index (mkpath dir "norms.flexindex")
			 [indextype 'kindex size (* 6 1024 1024) create #t
			  readonly (not (config 'wikidata:build))
			  justfront (config 'wikidata:build)
			  keyslot 'norms register #t
			  maxkeys (* 4 1024 1024)])))

  (set! props.index
    (if (and (file-exists? (mkpath dir "props.index"))
	     (file-exists? (mkpath dir "rare/props.flexindex")))
	{(knodb/ref (mkpath dir "props.index") [readonly #t keyslot 'props register #t])
	 (knodb/ref (mkpath dir "rare/props.flexindex") [readonly #t keyslot 'props register #t])}
	(flex/open-index (mkpath dir "props.flexindex")
			 [indextype 'kindex size (* 6 1024 1024) create #t
			  readonly (not (config 'wikidata:build))
			  justfront (config 'wikidata:build)
			  maxkeys (* 4 1024 1024)
			  register #t])))

  (set! has.index
    (knodb/make (mkpath dir "hasprops.index")
		[indextype 'kindex create #t keyslot 'has register #t]))

  ;; This is used to index newly created properties
  (unless wikidprops.index
    (let ((new (knodb/ref (mkpath (or (config 'brico:dir) dir) "wikidprops.index")
			  [indextype 'kindex size #mib create #t
			   readonly (not (config 'wikidata:build))
			   background #t
			   maxload 0.8
			   register #t])))
      (config! 'brico:wikidprops new)))
  
  (let ((props {(find-frames core.index 'type 'wikidprop) 
		(find-frames wikidprops.index 'type 'wikidprop)})
	(table propmaps.table))
    (prefetch-oids! props)
    (do-choices (prop props) 
      (store! table (get prop '{wikid wikidref}) prop)
      (store! table (upcase (get prop '{wikid wikidref})) prop)))

  (set! wikidata.index
    (make-aggregate-index
     {words.index norms.index has.index props.index wikidprops.index}
     #[register #t])))

(define (config-wikidata-source var (val #f))
  (cond ((not val) wikidata.dir)
	  ((not (string? val)) (irritant val |NotADirectoryPath|))
	  ((and wikidata.dir
		(equal? (realpath wikidata.dir) (realpath val)))
	   wikidata.dir)
	  (wikidata.dir (irritant wikidata.dir |WikidataAlreadyConfigured|))
	  ((not (file-directory? val))
	   (irritant val |NotADirectoryPath|))
	  (else (set-wikidata-dir! val))))
;; Synonyms
(config-def! 'wikidata:source config-wikidata-source)
(config-def! 'wikidatasource config-wikidata-source)
(config-def! 'wikidata config-wikidata-source)

(config-def! 'wikidata:build
  (lambda (var (val))
    (cond ((not (bound? val)) wikidata-build)
	  ((not val) (set! wikidata-build #f))
	  ((not wikidata.dir)
	   (set! wikidata-build #t))
	  (wikidata-build wikidata-build)
	  (else (error |WikidataAlreadyConfigured|)))))
(varconfig! wikidata:readonly wikidata-readonly)

(define (wikidata/save!)
  (knodb/commit! {wikidata.pool wikidata.index brico.pool
		  words.index norms.index has.index props.index
		  wikidprops.index}))

;;; Wikidata refs

(define (get-wikidref id)
  (if (has-prefix id "Q")
      (oid-plus @31c1/0 (string->number (slice id 1)))
      (try (get propmaps.table (upcase id))
	   (make-new-prop (upcase id)))))

(define (probe-wikidref id)
  (if (has-prefix id "Q")
      (oid-plus @31c1/0 (string->number (slice id 1)))
      (get propmaps.table id)))

(define (get-wikidprop id)
  (if (or (symbol? id) (string? id)) 
      (get propmaps.table (upcase id))
      id))

;;; Use define-init to avoid redefining with a new lock
(define-init make-new-prop
  (defsync (make-new-prop id)
    (try (find-frames wikidprops.index 'wikid id)
	 (find-frames brico.index 'wikid id)
	 (?? 'wikid id)
	 (alloc-new-prop id))))

(define (alloc-new-prop id)
  (let ((f (frame-create brico.pool
	     'type '{wikidprop property slot}
	     'wikidtype 'property
	     'wikid id 'wikidref id
	     'source 'wikidata
	     '%id (list 'WIKIDPROP id))))
    (index-frame wikidprops.index f '{type wikidtype wikid wikidref source})
    (index-frame wikidprops.index f 'has (getkeys f))
    (store! propmaps.table id f)
    f))

;;; Generating %id slots

(define (wikidata/makeid wf (id))
  (default! id (try (get wf 'id) "??"))
  `(wikidata ,id ,@(if (singleton? (get wf 'norms))
		       `(norm ,(get wf 'norms))
		       (if (singleton? (get wf 'words))
			   `(words ,(get wf 'words))
			   '()))
	     ,@(if (singleton? (get wf 'gloss))
		   `(gloss ,(get wf 'gloss))
		   '())))

(define (wikidata/ref arg . ignored)
  (cond ((not wikidata.pool) (error |WikdataNotConfigured|))
	((oid? arg)
	 (if (or (in-pool? arg wikidata.pool) (test arg 'wikitype) (test arg 'wikid))
	     arg
	     (and (test arg 'wikidref)
		  (get-wikidref (get arg 'wikidref)))))
	((string? arg)
	 (try (probe-wikidref (upcase arg)) #f))
	((symbol? arg)
	 (try (probe-wikidref (upcase (symbol->string arg))) #f))
	(else {})))

(define wikid/ref wikidata/ref)

(defambda (wikidata/find . specs)
  (apply find-frames wikidata.index specs))

(define wikid/find wikidata/find)

