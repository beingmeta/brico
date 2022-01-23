;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu).

(in-module 'brico/build/wikidata)

(use-module '{webtools archivetools texttools})
(use-module '{logger varconfig text/stringfmts})
(use-module '{knodb knodb/branches knodb/typeindex 
	      knodb/flexindex})
(use-module '{brico})
(define %optmods '{brico logger knodb knodb/branches knodb/typeindex knodb/flexindex})

(module-export! '{wikidata.dir wikidata-build
		  wikidata.pool wikidata.index
		  words.index norms.index has.index props.index refs.index
		  subclassof.index instanceof.index
		  ->wikidprop
		  propmaps.table
		  wikidata/ref wikid/ref
		  wikidata/find wikid/find
		  wikidata/makeid
		  wikidata/class!
		  get-wikidref
		  probe-wikidref
		  get-wikidprop
		  wikid/brico/ref
		  brico/ref
		  wikid/gloss})

(module-export! 'table/top)

(module-export! '{wikidata/save!})

(module-export! '{wikid/getrefs wikid/countrefs wikid/toprefs wikid/refscores
		  wikid/genls wikid/genls* wikid/specls wikid/specls*})

(module-export! '{wikid-isa
		  wikid-instanceof
		  wikid-genls
		  wikid-subclassof
		  wikid-occupation
		  wikid-equivalent
		  wikid-opposite
		  wikid-memberof
		  wikid-partof
		  wikid-different
		  wikid-territory
		  wikid-instance-slotids
		  wikid-classes})

(module-export! '{get-wikidata.json})

(define %loglevel %notice%)

(define-init wikidata.dir #f)

(define-init wikidata.pool #f)
(define-init wikidata.index #f)

(define-init words.index #f)
(define-init norms.index #f)
(define-init has.index #f)
(define-init refs.index #f)
(define-init props.index #f)
(define-init instanceof.index #f)
(define-init subclassof.index #f)

(define-init wikid-classes {})

(define-init propmaps.table (make-hashtable))

(define-init wikidata-readonly #f)
(define-init wikidata-build #f)

(define wikid-instance-slotids
  {@1/44f50("publication date" wikid "P577")
   ;; @1/44dd9("inception" wikid "P571")
   @1/451b4("date of official opening" wikid "P1619")
   ;; @1/44fb9("creator" wikid "P170")
   ;;  @1/450f9("duration" wikid "P2047")
   @1/44dfe("coordinate location" wikid "P625")})

(define wikid-isa @1/1f("instance of" wikid "P31"))
(define wikid-instanceof @1/1f("instance of" wikid "P31"))
(define wikid-genls @1/20("subclass of" wikid "P279"))
(define wikid-subclassof @1/20("subclass of" wikid "P279"))
(define wikid-occupation @1/44e51("occupation" wikid "P106"))
(define wikid-equivalent @1/21("equivalent class" wikid "P1709"))
(define wikid-opposite @1/22("opposite of" wikid "P461"))
(define wikid-memberof @1/23("member of" wikid "P463"))
(define wikid-partof @1/24("part of" wikid "P361"))
(define wikid-different @1/25("different from" wikid "P1889"))
(define wikid-territory @1/26("located in the administrative territorial entity" wikid "P131"))

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
	     (file-exists? (mkpath dir "words_tail.index")))
	{(knodb/ref (mkpath dir "words.index") [readonly #t keyslot 'words register #t])
	 (knodb/ref (mkpath dir "words_tail.index") [readonly #t keyslot 'words register #t])}
	(flex/open-index (mkpath dir "words.flexindex")
			 [indextype 'kindex size (* 6 1024 1024) create #t
			  readonly (not (config 'wikidata:build))
			  justfront (config 'wikidata:build)
			  keyslot 'words register #t
			  maxkeys (* 4 1024 1024)])))

  (set! norms.index
    (if (and (file-exists? (mkpath dir "norms.index"))
	     (file-exists? (mkpath dir "norms_tail.index")))
	{(knodb/ref (mkpath dir "norms.index") [readonly #t keyslot 'norms register #t])
	 (knodb/ref (mkpath dir "norms_tail.index") [readonly #t keyslot 'norms register #t])}
	(flex/open-index (mkpath dir "norms.flexindex")
			 [indextype 'kindex size (* 6 1024 1024) create #t
			  readonly (not (config 'wikidata:build))
			  justfront (config 'wikidata:build)
			  keyslot 'norms register #t
			  maxkeys (* 4 1024 1024)])))

  (set! props.index
    (if (and (file-exists? (mkpath dir "props.index"))
	     (file-exists? (mkpath dir "props_tail.index")))
	{(knodb/ref (mkpath dir "props.index") [readonly #t register #t])
	 (knodb/ref (mkpath dir "props_tail.index") [readonly #t register #t])}
	(flex/open-index (mkpath dir "props.flexindex")
			 [indextype 'kindex size (* 6 1024 1024) create #t
			  readonly (not (config 'wikidata:build))
			  justfront (config 'wikidata:build)
			  maxkeys (* 4 1024 1024)
			  register #t])))

  (set! refs.index
    (if (and (file-exists? (mkpath dir "refs.index"))
	     (file-exists? (mkpath dir "refs_tail.index")))
	{(knodb/ref (mkpath dir "refs.index") [readonly #t keyslot 'refs register #t])
	 (knodb/ref (mkpath dir "refs_tail.index") [readonly #t keyslot 'refs register #t])}
	(flex/open-index (mkpath dir "refs.flexindex")
			 [indextype 'kindex size (* 6 1024 1024) create #t
			  readonly (not (config 'wikidata:build))
			  justfront (config 'wikidata:build)
			  maxkeys (* 4 1024 1024)
			  keyslot 'refs
			  register #t])))

  (set! instanceof.index
    (knodb/make (mkpath dir "instanceof.index")
		[indextype 'kindex create #t keyslot wikid-instanceof
		 size #8mib register #t]))

  (set! subclassof.index
    (knodb/make (mkpath dir "subclassof.index")
		[indextype 'kindex create #t keyslot wikid-subclassof
		 size #8mib register #t]))

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
      (let ((refstrings (get prop '{wikid wikidref})))
	(store! table refstrings prop)
	(store! table (upcase refstrings) prop)
	(store! table (downcase refstrings) prop)
	(store! table (string->symbol (downcase refstrings)) prop))))

  (set! wikid-classes (find-frames props.index 'type 'wikidclass))

  (set! wikidata.index
    (make-aggregate-index
     {words.index norms.index has.index refs.index
      props.index wikidprops.index
      subclassof.index instanceof.index}
     #[register #t])))

(config-def! 'wikidata:build
  (lambda (var (val))
    (cond ((not (bound? val)) wikidata-build)
	  ((not val) (set! wikidata-build #f))
	  ((not wikidata.dir)
	   (set! wikidata-build #t))
	  (wikidata-build wikidata-build)
	  (else (error |WikidataAlreadyConfigured| |ConfigWikidataBuild|)))))
(varconfig! wikidata:readonly wikidata-readonly)

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

(define (wikidata/save!)
  (knodb/commit! {wikidata.pool wikidata.index brico.pool
		  words.index norms.index refs.index has.index props.index
		  wikidprops.index}))

(define (wikid/brico/ref arg)
  (if (or (string? arg) (symbol? arg))
      (?? 'wikidref (upcase arg))
      (tryif (and (oid? arg) (test arg 'wikid))
	(?? 'wikidref (get arg 'wikid)))))
(define (brico/ref arg) (wikid/brico/ref arg))

;;; Wikidata refs

(define (get-wikidref id)
  (when (symbol? id) (set! id (symbol->string id)))
  (if (has-prefix id "Q")
      (oid-plus @31c1/0 (string->number (slice id 1)))
      (try (get propmaps.table id)
	   (get propmaps.table (upcase id))
	   (make-new-prop (upcase id)))))

(define (probe-wikidref id)
  (if (has-prefix id "Q")
      (oid-plus @31c1/0 (string->number (slice id 1)))
      (tryif (has-prefix id {"p" "P"}) 
	(get propmaps.table id))))

(define (get-wikidprop id)
  (if (or (symbol? id) (string? id)) 
      (get propmaps.table (upcase id))
      id))

;;; Use define-init to avoid redefining with a new lock
(define-init make-new-prop
  (defsync (make-new-prop id)
    (unless (uppercase? id) (set! id (upcase id)))
    (try (find-frames wikidprops.index 'wikid id)
	 (find-frames brico.index 'wikid id)
	 (find-frames brico.index 'wikidref id)
	 (?? 'wikid id)
	 (?? 'wikidref id)
	 (alloc-new-prop id))))

(define (alloc-new-prop id)
  (let ((f (frame-create brico.pool
	     'type '{wikidprop property slot}
	     'wikidtype 'property
	     'wikid id 'wikidref id
	     'source 'wikidata
	     '%id (list 'WIKIDPROP id))))
    (logwarn |NewWikiDProp| id)
    (index-frame wikidprops.index f '{type wikidtype wikid wikidref source})
    (index-frame wikidprops.index f '{wikid wikidref} {(upcase id) (downcase id)})
    (index-frame wikidprops.index f 'has (getkeys f))
    (store! propmaps.table id f)
    (store! propmaps.table {(upcase id) (downcase id)} f)
    (unless (symbol? id) (store! propmaps.table (string->symbol id) f))
    f))

(define (->wikidprop id)
  (try (get propmaps.table id)
       (get propmaps.table (downcase id))
       (find-frames wikidprops.index 'wikid id)
       (find-frames brico.index 'wikid id)
       (find-frames brico.index 'wikid (downcase id))
       (find-frames brico.index 'wikidref id)
       (find-frames brico.index 'wikidref (downcase id))
       (?? 'wikid id)
       (?? 'wikidref id)
       (alloc-new-prop id)))

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
(define ->wikidata wikidata/ref)

(defambda (wikidata/find . specs)
  (apply find-frames wikidata.index specs))

(define wikid/find wikidata/find)

;;; Getting a wikidata ref off of the web

(define (get-wikidata.json id)
  (get (jsonparse (get (urlget (glom "https://wikidata.org/entity/" (upcase id) ".json") #[follow #t])
		       '%content))
       'entities))

;;; Semantic functions

(define (wikid/getrefs x)
  (find-frames refs.index 'refs x))
(define (wikid/countrefs x)
  (|| (find-frames refs.index 'refs x)))
(defambda (wikid/refscores items)
  (let ((table (make-hashtable)))
    (do-choices (item items)
      (store! table item (|| (find-frames refs.index 'refs item))))
    table))
(defambda (wikid/toprefs items (n 3))
  (let ((table (make-hashtable)))
    (do-choices (item items)
      (store! table item (|| (find-frames refs.index 'refs item))))
    (max/sorted items n table)))

(define (table/top table (thresh 0.9))
  (let* ((maxval (table-maxval table))
	 (threshval (* thresh maxval))
	 (candidates (table-skim table threshval))
	 (strata (rsorted (get table candidates)))
	 (result candidates))
    (doseq (stratum strata)
      (let* ((next (pick candidates table >= stratum))
	     (sum (reduce-choice + next 0 table))
	     (mean (/~ sum (|| next))))
	(if (> mean threshval)
	    (set! result next)
	    (break))))
    result))

(define (wikid/gloss x (lang 'en))
  (get (get x 'descriptions) lang))

(define (wikid/genls x)
  (get x wikid-genls))
(define (wikid/genls* x)
  (get* x wikid-genls))
(define (wikid/specls x)
  (find-frames wikidata.index wikid-genls x))
(define (wikid/specls* x)
  (let ((found (make-hashset))
	(roots x)
	(next {}))
    (while (exists? roots)
      (do-choices (root roots)
	(unless (hashset-get found root)
	  (hashset-add! found root)
	  (set+! next (reject (find-frames wikidata.index wikid-genls root) found))))
      (set! roots next)
      (set! next {}))
    (hashset-elts found)))

(define (wikidata/class! frame)
  (unless wikidata.pool (error |NoWikidataInit| wikidata/class!))
  (unless (test frame 'type 'wikidclass)
    (add! frame 'type 'wikidclass)
    (index-frame props.index frame 'type 'wikidclass)
    (set+! wikid-classes frame)))

(define has-map
  [place {wikid-partof wikid-territory}
   person {@1/44ecc("date of birth" wikid "P569")
	   @1/44e4b("father" wikid "P22")
	   @1/44e4d("mother" wikid "P25")}])

(define (get-wikid-matches f type (thresh 0.8) (max #f))
  (let* ((possible (wikid/find 'words (get f 'words) 'has (get has-map type)))
	 (refscores (wikid/refscores possible))
	 (top (table/top refscores thresh)))
    (tryif (and (exists? top) (or (not max) (< (|| top) max))) top)))

(define (wikid-partof* x) (get* x {wikid-territory wikid-partof}))

(define (wikid/listmaps bf (count 5))
  (let* ((wordmatches (wikid/find 'words (get bf 'words)))
	 (toprefs (wikid/toprefs wordmatches count)))
    (append
     (choice->vector (get bf @?engloss))
     (forseq (f toprefs)
       (vector `(wikidmap! ,bf ,f)
	       (qc (pick-one (get (get f 'descriptions) 'en)))
	       (try (?? 'wikidref (get f 'wikid)) #f)))))) 

(module-export! '{get-wikid-matches wikid/listmaps})

