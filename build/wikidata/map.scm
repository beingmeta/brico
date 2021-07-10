;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'brico/build/wikidata/map)

(use-module '{logger webtools varconfig archivetools texttools text/stringfmts})
(use-module '{defmacro optimize})
(use-module '{knodb knodb/branches knodb/typeindex knodb/flexindex})
(use-module '{brico brico/indexing brico/wikid brico/build/wikidata})

(module-export! '{wikidata->brico wikid/brico wikid/src
		  wikidmap wikidmatch wikid/getmap wikidmap!
		  wikid/import! wikid/copy! wikid/update!
		  wikidata/import/enginefn
		  wikid/drop!})

(define %loglevel %notice%)

(define-init skip-index #t)
(varconfig! wikidata:skipindex skip-index config:boolean)
(define-init always-index '{wikidref wikid pending type})

(define-init wikidmap (make-hashtable))
(define-init wikid.update {})
(varconfig! wikid:update wikid.update #f choice)

(define (get-index f)
  (cond ((in-pool? f brico.pool) brico.index)
	((in-pool? f wikid.pool) wikid.index)
	(else (irritant f |UnknownBricoPool|))))

(defambda (unindex! index frame slot value)
  (unless skip-index (set! slot (intersection slot always-index)))
  (when (and (exists? slot) (exists? value))
    (do-choices (ix index)
      (if (aggregate-index? ix)
	  (unindex! (indexctl ix 'partitions) frame slot value)
	  (if (eq? (indexctl ix 'keyslot) slot)
	      (drop! ix value frame)
	      (drop! ix (cons slot value) frame))))))

(defambda (wikidmap! brico wikidata (opts #f) (copy) (index))
  (default! copy (getopt opts 'copy #t))
  (default! index (get-index brico))
  (let* ((curmap (get brico 'wikidref))
	 (wikidref (if (string? wikidata) wikidata (get wikidata 'wikid)))
	 (wikidata (if (string? wikidata) (wikid/ref wikidata) wikidata))
	 (disowned (?? 'wikidref wikidref)))
    (when (and index (exists? curmap))
      (unindex! index brico 'wikidref curmap))
    (when (and index (exists? disowned)) 
      (unindex! (get-index disowned) disowned 'wikidref wikidref))
    (drop! wikidmap {curmap (wikid/ref curmap) wikidref wikidata})
    (add! wikidmap {wikidata wikidref} brico)
    (store! brico 'wikidref wikidref)
    (drop! disowned 'wikidref wikidref)
    (do-choices (p (pick (pickoids (getkeys brico)) 'type 'wikidprop))
      (let ((v (get brico p))) 
	(drop! brico p v)
	(when index (unindex! index brico p v))))
    (when (exists? disowned)
      (do-choices (p (pick (pickoids (getkeys disowned)) 'type 'wikidprop))
	(let ((v (get disowned p))) 
	  (drop! disowned p v)
	  (when index (unindex! index disowned p v)))))
    (when index (index-frame index brico 'wikidref wikidref))
    (when copy (wikid/copy! wikidata brico index))))

(define (wikid/drop! brico (index))
  (when (in-pool? brico wikidata.pool) (set! brico (wikid/brico/ref brico)))
  (default! index (get-index brico))
  (drop! brico @1/2c27e{isa} (get brico @1/1f("instance of" wikid "P31")))
  (drop! brico @1/2c272{genls} (get brico @1/1f("instance of" wikid "P31")))
  (let* ((prev (wikid/ref (get brico 'wikidref)))
	 (labels (get prev 'labels))
	 (aliases (get prev 'aliases))
	 (descs (get prev 'descriptions)))
    (do-choices (lang (getkeys labels))
      (retract! brico (get norm-map lang)
	{(get labels lang) (downcase (get labels lang))})
      (retract! brico (get language-map lang)
	{(get labels lang) (downcase (get labels lang))
	 (get aliases lang) (downcase (get aliases lang))})
      (retract! brico (get gloss-map lang)
	{(get descs lang) (downcase (get descs lang))})))
  (drop! brico 'wikidref)
  (do-choices (p (pick (pickoids (getkeys brico)) 'type 'wikidprop))
    (let ((v (get brico p))) 
      (drop! brico p v)
      (when index (unindex! index brico p v)))))

(define (wikidata->brico wikid (wikidstring) (cached))
  (default! wikidstring (if (string? wikid) wikid (get wikid 'wikid)))
  (unless (oid? wikid) (set! wikid (or (wikidata/ref wikidstring) wikid)))
  (when (fail? wikid) (irritant wikidstring |InvalidWikidRef|))
  (set! cached (try (get wikidmap wikid) (get wikidmap wikidstring)))
  (if (exists? cached)
      (if cached cached {})
      (let ((found (try (?? 'wikidref wikidstring) #f)))
	(store! wikidmap wikidstring found)
	(store! wikidmap wikid found)
	(or found (fail)))))
(define wikid/brico (fcn/alias wikidata->brico))

(define (wikid/src f)
  (wikid/ref (get f 'wikidref)))

(define (getmap.norms wikidata (spec #f) (opts #f))
  (let* ((lower (getopt opts 'lower #f))
	 (words (get wikidata 'norms))
	 (candidates (difference (?? en_norms {words (tryif lower (downcase words))})
				 (?? 'skipwikid {#t (get wikidata 'wikid)}))))
    (when spec 
      (do-choices (slotid (getkeys spec))
	(set! candidates (intersection candidates 
				       (?? slotid (get spec slotid))))))
    candidates))
(define (getmap.words wikidata (spec #f) (opts #f))
  (let* ((lower (getopt opts 'lower #f))
	 (words (get wikidata 'norms))
	 (candidates (difference (?? en_norms {words (tryif lower (downcase words))})
				 (?? 'skipwikid {#t (get wikidata 'wikid)}))))
    (when spec 
      (do-choices (slotid (getkeys spec))
	(set! candidates (intersection candidates 
				       (?? slotid (get spec slotid))))))
    candidates))

(define (wikid/getmap wikidata (spec #f) (opts #f))
  (try (getmap.norms wikidata spec opts)
       (getmap.words wikidata spec opts)))

;; (defambda (wikid/getmap wikidframes (spec #f) (opts #f))
;;   (for-choices (wikidframe {(pickoids wikidframes)
;; 			    (wikid/ref (pickstrings wikidframes))})
;;     (try (?? 'wikidref (get wikidframe 'wikid))
;; 	 (let ((candidate (for-choices (spec spec)
;; 			     (wikidmatch wikidframe spec opts))))
;; 	   (when (singleton? candidate)
;; 	     (lognotice |WikidMap| 
;; 	       "Found existing " candidate " for " wikidframe)
;; 	     (wikidmap! candidate wikidframe opts))
;; 	   (cond ((singleton? candidate) candidate)
;; 		 ((and (fail? candidate) (getopt opts 'import #f))
;; 		  (wikid/import! wikidframes opts))
;; 		 (else candidate))))))

(define (copy-lexslots wikid brico index opts)
  ;; We should use 'index' here
  (let* ((wds #[]) (norms #[]) (glosses #[])
	 (labels (get wikid 'labels))
	 (aliases (get wikid 'aliases))
	 (descriptions (get wikid 'descriptions))
	 (lower (getopt opts 'lower #f)))
    (do-choices (alias aliases)
      (do-choices (langid (getkeys alias))
	(let ((v (get alias langid)))
	  (add! wds langid v)
	  (when lower (add! wds langid (downcase v))))))
    (do-choices (label labels)
      (do-choices (langid (getkeys label))
	(add! {wds norms} langid (get label langid))
	(when lower
	  (add! {wds norms} langid (downcase (get label langid))))))
    (do-choices (description descriptions)
      (do-choices (langid (getkeys description))
	(add! glosses langid (get description langid))))
    (let ((cur-wds (get brico '%norms))
	  (cur-norms (get brico '%words))
	  (cur-glosses (get brico '%glosses))
	  (langs (getkeys {wds norms glosses})))
      (store! brico '%words wds)
      (store! brico '%norms norms)
      (store! brico '%glosses glosses)
      ;; (add! brico 'words (get wds 'en))
      ;; (add! brico 'norms (get norms 'en))
      (when (and index (not skip-index))
	(do-choices (lang langs)
	  (let ((addwds (difference (get wds lang) (get cur-wds lang))))
	    (when (exists? addwds)
	      (index-string wikid.index brico 
			    (get language-map (downcase lang))
			    {addwds (downcase addwds)})))
	  (let ((addnorms (difference (get norms lang) (get cur-norms lang))))
	    (when (exists? addnorms)
	      (index-string wikid.index brico 
			    (get norm-map (downcase lang))
			    addnorms))))))))

(define import-slotids
  [@1/2c27e{isa} {@1/1f("instance of" wikid "P31")
		  @1/44e51("occupation" wikid "P106")
		  @1/44ee9("position held" wikid "P39")
		  @1/45056("noble title" wikid "P97")
		  @1/45a48("animal breed" wikid "P4743")}
   @1/2c272{genls} @1/20("subclass of" wikid "P279")
   @1/2c274{part-of} {@1/4500a("location" wikid "P276")
		      @1/450f1("watershed" wikid "P4614")
		      @1/44dbf("country" wikid "P17")
		      @1/44dd7("located in time zone" wikid "P421")
		      @1/44dc1("continent" wikid "P30")
		      @1/45262("series" wikid "P179")}
   @1/2c275{parts} {@1/44e17("has part" wikid "P527")
		    @1/44dc8("contains administrative territorial entity" wikid "P150")   }
   @1/2c276{made-of}   {@1/44fbd("material used" wikid "P186")}
   @1/2c279{member-of} {@1/23("member of" wikid "P463") 	;;=#6.1
			@1/44ebc("member of political party" wikid "P102")
			@1/4525c("parliamentary group" wikid "P4100")
			@1/4526d("astronaut mission" wikid "P450")}])

(define (wikid/copy! wikid frame (opts #f) (index wikid.index) (template #f) (unmapped {}))
  (local dropvals (in-pool? frame wikid.pool))
  (cond ((index? opts) (set! index opts) (set! opts #f))
	(else (default! index (getopt opts 'index))))
  (copy-lexslots wikid frame index opts)
  (when index (index-core index frame))
  (do-choices (prop (pickoids (getkeys wikid)))
    (let* ((wikidvals (get wikid prop))
	   (refvals (pickoids wikidvals))
	   (litvals (difference wikidvals refvals))
	   (newvals (difference (choice (get refvals 'wikid)
					(wikidata->brico refvals)
					litvals)
				#f))
	   (cur (get frame prop))
	   (toadd (difference newvals cur))
	   (todrop (difference cur newvals)))
      (add! frame prop toadd)
      (when dropvals (drop! frame prop todrop))
      (set+! unmapped (get (reject refvals wikidmap) 'wikid))
      (when (and index (not skip-index) (exists? toadd) (exists? refvals))
	(index-frame index frame prop (pickoids toadd)))))
  (do-choices (import (getkeys import-slotids))
    (let* ((cur (get frame import))
	   (new (wikidata->brico (get wikid (get import-slotids import))))
	   (add (difference new cur))
	   (drop (difference cur new)))
      (add! frame import add)
      (when dropvals (drop! frame import drop))
      (when (and index (not skip-index))
	(index-frame index frame import add))))
  (when (and index (exists? unmapped))
    (add! frame 'pending unmapped)
    (index-frame index frame 'pending unmapped))
  (when template
    (do-choices (key (getkeys template))
      (let* ((cur (get frame key))
	     (new (get template key))
	     (add (difference new cur))
	     (drop (difference cur new)))
	(add! frame key add)
	(when dropvals (drop! frame key drop))
	(when (and index (not skip-index))
	  (index-frame wikid.index frame key add)))))
  (when index
    (adjust-pending index wikid frame)
    (adjust-pending wikid.index wikid frame)
    (adjust-pending brico.index wikid frame))
  (cond ((%test frame 'sensecat))
	((%test frame @?genls)
	 (add! frame 'type (get (%get frame @?genls) 'type))
	 (add! frame 'sensecat (get (%get frame @?genls) 'sensecat))
	 (when index (index-frame index frame '{sensecat type}))))
  (when (and index (not skip-index))
    (index-lattice index frame)
    (index-relations index frame)
    (index-analytics index frame)))

(define (wikid/update! frame (index #f) (opts #f))
  (let* ((wikidref (get frame 'wikidref))
	 (wf (wikid/ref wikidref)))
    (if (fail? wf)
	"No mapping"
	(wikid/copy! wf frame #f index))))

(defambda (adjust-pending index wikid frame)
  (let ((adjust (find-frames index 'pending wikid))
	(dropvals (in-pool? frame wikid.pool)))
    (lock-oids! adjust)
    (prefetch-oids! adjust)
    (do-choices (adj adjust)
      (do-choices (slot (getkeys adj))
	(when (test adj slot wikid)
	  (add! adj slot frame)
	  (when dropvals (drop! adj slot wikid))
	  (when (and index (not skip-index))
	    (index-frame index adj slot frame)))))))

(define (wikid/import! wikid (opts #f) (into #f) (template) (wikidstring))
  (default! template (getopt opts 'template #f))
  (default! wikidstring (if (string? wikid) wikid (get wikid 'wikid)))
  (unless (oid? wikid) (set! wikid (or (wikidata/ref wikidstring) wikid)))
  (when (fail? wikid) (irritant wikidstring |InvalidWikidRef|))
  (let* ((cached (or (try (get wikidmap wikid) (get wikidmap wikidstring)
			  (wikidata->brico wikidstring))
		     #f))
	 (label (try (pick-one (get wikid 'norms)) (pick-one (get wikid 'words))))
	 (pool (getopt opts 'pool wikid.pool))
	 (frame (or cached
		    (frame-create pool
		      '%id `(WIKID ,label ,wikidstring)
		      'type {'wikid (getopt opts 'type '{noun thing})}
		      'source 'wikidata
		      'wikidref wikidstring
		      'imported (config 'sessionid)
		      'imported_at (timestamp)
		      'wikidata_rev (get wikid 'lastrevid))))
	 (unmapped {}))
    (when (or (not cached) (in-pool? frame wikid.pool)
	      (exists in-pool? frame wikid.update))
      (store! wikidmap {wikid wikidstring} frame)
      (wikid/copy! wikid frame (get-index frame) template))
    frame))

(defambda (wikidata/import/enginefn batch)
  (prefetch-oids! batch)
  (prefetch-keys! wikid.index (cons 'wikidref (get batch 'wikid)))
  (wikid/import! batch))

