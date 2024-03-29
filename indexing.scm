;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu).

(in-module 'brico/indexing)
;;; Functions for generating BRICO indexes

(use-module '{knodb brico texttools varconfig logger})

(define %nosubst '{indexinfer default-frag-window})
(define %optmods '{knodb brico logger})

(define fix-wikid #t)
(varconfig! 'brico:wikid:fix fix-wikid)
(define wikid-fixers {})
(varconfig! 'brico:wikid:fixers wikid-fixers)

;; When true, this assumes that the lattice has been indexed,
;;  so that closures can be computed based on their inverses
;;  (e.g. (get x genls*)== (?? @?specls* x))
(define indexinfer #t)

(define indexinfer-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) indexinfer)
	  ((equal? val indexinfer)
	   indexinfer)
	  (else
	   (set! indexinfer val)))))
(config-def! 'indexinfer indexinfer-config)

;;; Indexing functions

(defambda (index-relation index frame slotids (usevals) (inverse #f) (genslot #f) (oidvals #f))
  (do-choices (slotid slotids)
    (let* ((values (or usevals (get frame slotid)))
	   (oidvals (pickoids values))
	   (inverse (and inverse
			 (try (pick inverse slotid?)
			      (get slotid 'inverse)
			      #f)))
	   (genls+ (and genslot (try (difference genslot genls*) #f)))
	   (dogenls (or (not genslot) (overlaps? genslot genls*))))
      ;; (when (eq? slotid '%id) (%watch "INDEX-RELATION" index frame slotid values))
      (index-frame index frame slotid values)
      (when inverse 
	(index-frame index oidvals inverse frame))
      (when (or genls+ dogenls)
	(index-frame index frame slotid (list oidvals)))
      (when dogenls
	;; This is a faster way to get genls*
	(index-frame index frame slotid (?? specls* oidvals)))
      (when genls+
	(index-frame index frame slotid (pickoids (get oidvals genls+)))))))

(defambda (unindex frame slot value (index #f))
  (do-choices (slot slot)
    (do-choices (value value)
      (if (not index)
	  (do-choices (index (config 'background))
	    (when (%test index (cons slot value) frame)
	      (message "Dropping references from " index)
	      (drop! index  (cons slot value) frame)))
	  (when (%test index (cons slot value) frame)
	    (drop! index  (cons slot value) frame))))))

;;; Slot indexing functions

(define (stem-compound string)
  (seq->phrase (map porter-stem (words->vector string))))

(define (dedash string)
  (tryif (position #\- string)
    (choice (string-subst string "-" " " )
	    (string-subst string "-" ""))))

(defambda (index-string index frame slot (value #default)
			(opts #f) (soundslike) (frag))
  (when (and opts (not (opts? opts)))
    (set! frag opts)
    (set! opts #f))
  (default! soundslike (getopt opts 'soundslike #t))
  (default! frag (getopt opts 'frag #f))
  (default! value (get frame slot))
  (do-choices slot
    (let* ((values (stdspace (pickstrings value)))
	   (expvalues (choice values (basestring values)))
	   (normcaps (capitalize (downcase (pick expvalues somecap?))))
	   (lowered (downcase values)))
      ;; By default, we index strings under their direct values, under
      ;;  their values without diacritics, and under versions with normalized
      ;;  capitalization.  Normalizing capitalization makes all elements of a
      ;;  compound be uppercase and makes oddly capitalized terms (e.g. iTunes)
      ;;  be lowercased.
      (index-frame index frame slot 
		   {expvalues normcaps lowered
		    (dedash expvalues)
		    (dedash (difference lowered expvalues))
		    (dedash (difference normcaps lowered expvalues))})
      (when soundslike
	(index-frame index frame slot
		     (choice (pick (metaphone (choice values normcaps) #t) length>1)
			     (pick (metaphone+ (choice values normcaps) #t) length>1)))
	(index-frame index frame slot (pick (soundex (choice values normcaps) #t) length>1)))
      (when frag (index-frags index frame slot values 1 #f)))))

(defambda (index-string/keys value (opts #f) (soundslike) (frag))
  (when (and opts (not (opts? opts)))
    (set! frag opts)
    (set! opts #f))
  (default! soundslike (getopt opts 'soundslike #t))
  (default! expand (getopt opts 'expand #t))
  (default! frag (getopt opts 'frag #f))
  (let* ((values (stdspace (pickstrings value)))
	 (expvalues (choice values (basestring values)))
	 (normcaps (capitalize (downcase (pick expvalues somecap?))))
	 (lowered (downcase values)))
    ;; By default, we index strings under their direct values, under
    ;;  their values without diacritics, and under versions with normalized
    ;;  capitalization.  Normalizing capitalization makes all elements of a
    ;;  compound be uppercase and makes oddly capitalized terms (e.g. iTunes)
    ;;  be lowercased.
    (choice expvalues normcaps
	    (tryif soundslike
	      (choice
	       (pick (metaphone (choice values normcaps) #t) length>1)
	       (pick (soundex (pick (reject (choice values normcaps) compound-string?)
				length < 11)
			      #t)
		 length>1))))))

(defambda (index-name index frame slot (value) (window default-frag-window))
  (default! value (get frame slot))
  (let* ((values (downcase (stdspace value)))
	 (expvalues (choice values (basestring values))))
    (index-frame index frame slot expvalues)
    (when window
      (index-frags index frame slot expvalues window))))

(define (index-frame* index frame slot base (inverse #f) (v))
  (default! v (get frame base))
  (index-relation index frame base v (try (get base 'inverse) #f))
  (do ((g v (difference (get g base) seen))
       (seen frame (choice g seen)))
      ((empty? g))
    (index-relation index frame slot g inverse)))

(define (index-gloss index frame slotid (value) (language 'en) (stem))
  (default! value (get frame slotid))
  (default! stem (overlaps? language 'en))
  (let* ((wordlist (getwords value))
	 (gloss-words (filter-choices (word (elts wordlist))
			(< 2 (length word) 16))))
    (index-frame index frame slotid
		 (choice gloss-words 
			 (tryif stem (string->packet (porter-stem gloss-words)))))))

;;; Indexing string fragments

(define default-frag-window #f)

(define fragwindow-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) default-frag-window)
	  ((equal? val default-frag-window) default-frag-window)
	  ((not val) (set! default-frag-window #f))
	  ((and (number? val) (exact? val) (> val 0))
	   (set! default-frag-window #f))
	  (else (warning "Invalid fragment window" val)))))
(config-def! 'fragwindow fragwindow-config)

(define (metaphone1 w)
  (pick (tryif (and (not (uppercase? w)) (> (length w) 3))
	  (metaphone w #t))
    length>1))
(define (metaphone2 w)
  (pick (tryif (and (not (uppercase? w)) (> (length w) 5))
	  (metaphone (porter-stem w) #t))
    length>1))

(define (trimwordvec v)
  (if (or (position "" v)  (position #"" v) (position #{} v))
      {}
      v))

(defambda (index-frags index frame slot values window (phonetic #f) (compounds))
  (set! compounds (pick values compound-string?))
  (when (exists? compounds)
    (let* ((stdcompounds (basestring compounds))
	   (wordv (words->vector compounds))
	   (swordv (words->vector stdcompounds)))
      (index-frame index frame slot
		   (vector->frags (trimwordvec (choice wordv swordv)) window))
      (when phonetic
	(index-frame index frame slot
		     (vector->frags (trimwordvec (map metaphone1 wordv))))
	(index-frame index frame slot
		     (vector->frags (trimwordvec (map metaphone2 wordv))))))))

(defambda (index-frags/keys value (window 1) (phonetic #t))
  (let* ((values (stdstring value))
	 (compounds (pick values compound-string?))
	 (stdcompounds (basestring compounds))
	 (wordv (words->vector compounds))
	 (swordv (words->vector stdcompounds)))
    (choice (vector->frags (trimwordvec (choice wordv swordv)) window)
	    (tryif phonetic (vector->frags (trimwordvec (map metaphone1 wordv))))
	    (tryif phonetic (vector->frags (trimwordvec (map metaphone2 wordv)))))))

;;; Frame indexing functions

;; These are various other slotids which are useful to index
;;(define misc-slotids '{PERTAINYM REGION COUNTRY @1/2c27e{IMPLIES}})

(define (index-core index frame)
  (index-frame index frame
    '{type sensecat source
      topic_domain region_domain usage_domain
      derivations language
      fips-code dsg 
      wikid wikidref wikidef
      has})
  (index-frame index frame 'has (getkeys frame))
  (when (and (or (%test frame 'words) (%test frame '%words))
	     (ambiguous? (get frame 'sensecat)))
    (index-frame index frame 'sensecat 'ambiguous))
  (when (and (test frame '{%words words word type})
	     (not (test frame 'sensecat)))
    (index-frame index frame 'sensecat 'senseless))
  (when (test frame '%index)
    (index-frame index frame (pick (get frame '%index) {oid? symbol?})))
  (when (test frame '{%mnemonics wikid %ids})
    (let* ((v (get frame '{%mnemonics wikid %ids}))
	   (stringy {(pickstrings v) (picksyms v)}))
      (index-frame index frame '%id v)
      (index-frame index frame '%id (downcase stringy))
      (index-frame index frame '%id (string->symbol (downcase stringy)))))
  (when (and (test frame '%id)
	     (or (test frame 'type 'slot)
		 (test frame 'type 'get-methods)
		 (symbol? (get frame '%id))))
    (let* ((id (get frame '%id))
	   (stringy  {(pickstrings id) (picksyms id)}))
      (index-frame index frame '%id id)
      (index-frame index frame '%id (downcase stringy))))
  (index-frame index frame '%mnemonics)
  (index-frame index frame '{key through slots inverse @1/2c27a}))

(define wordform-slotids '{word of language rank type sensenum})

(define (index-wordform index frame)
  (when (test frame 'type 'wordform)
    (index-frame index frame wordform-slotids)
    (let ((derivations (get frame 'derivations)))
      (index-frame index frame 
	'derivations
	{derivations 
	 (get derivations 'of) 
	 (get (get derivations 'of) 'of) 
	 (get derivations 'word)
	 (for-choices (d derivations)
	   (cons (get d 'word) (get d 'type)))}))))

;;; Older indexing methods

(define (index-concept index concept)
  (index-brico index concept)  
  (when (test concept 'wikidref) (index-wikid index concept))  
  (index-words index concept)
  (index-relations index concept)
  (index-refterms index concept)
  (index-lattice index concept)
  (index-analytics index concept))

(define (index-brico index frame)
  (cond ((empty? (getkeys frame))
	 (index-frame index frame 'status 'deleted))
	((test frame 'type 'deleted)
	 (index-frame index frame 'type)
	 (index-frame index frame 'status 'deleted))
	((test frame 'source @1/1)
	 ;; We minimally index the Roget frames, since they tend
	 ;;  to mostly get in the way and often be archaic
	 (index-frame index frame '{type source}))
	(else
	 (index-core index frame)
	 (when (test frame 'type 'language)
	   (index-frame index frame
	     '{langid language iso639/1 iso639/B iso639/T}))
	 (when (test frame '{get-methods test-methods add-effects drop-effects})
	   (index-frame index frame
	     '{get-methods test-methods add-effects drop-effects
	       key through derivation inverse closure-of slots
	       primary-slot index %id})))))

(define (index-words index concept)
  (index-string index concept english (get concept 'words))
  (index-name index concept 'names (get concept 'names))
  (index-name index concept 'names
	      (pick (cdr (pick (get concept '%words) pair?))
		capitalized?))
  (index-name index concept 'names
	      (pick (getvalues (pick (get concept '%words) slotmap?))
		capitalized?))
  (do-choices (xlation (pick (get concept '%words) pair?))
    (let ((lang (get language-map (car xlation))))
      (index-string index concept lang (cdr xlation))))
  (do-choices (xlation (pick (get concept '%words) slotmap?))
    (do-choices (langid (getkeys xlation))
      (let ((lang (get language-map langid)))
	(index-string index concept lang (get xlation langid)))))
  (do-choices (xlation (pick (get concept '%norms) pair?))
    (let ((lang (get norm-map (car xlation))))
      (index-string index concept lang (cdr xlation))))
  (do-choices (xlation (pick (get concept '%norms) slotmap?))
    (do-choices (langid (getkeys xlation))
      (let ((lang (get norm-map langid)))
	(index-string index concept lang (get xlation langid)))))
  (do-choices (xlation (pick (get concept '%indicators) pair?))
    (let ((lang (get indicator-map (car xlation))))
      (index-string index concept lang (cdr xlation))))
  (do-choices (xlation (pick (get concept '%indicators) slotmap?))
    (do-choices (langid (getkeys xlation))
      (let ((lang (get indicator-map langid)))
	(index-string index concept lang (get xlation langid)))))
  (do-choices (xlation (pick (get concept '%aliases) slotmap?))
    (do-choices (langid (getkeys xlation))
      (let ((lang (get alias-map langid)))
	(index-string index concept lang (get xlation langid))))))

(define (get-timekeys value)
  (let* ((stamp (get value 'time))
	 (date (slice stamp 0 (position #\T stamp)))
	 (time (and (has-prefix stamp {"+20" "+197" "+198" "+199"})
		    (onerror (timestamp (slice stamp 1)) #f))))
    {(slice date 0 (textmatcher '(+ (GREEDY #((ispunct) (isdigit+)))) date))
     (gather {#("T" (isdigit+)) #("T" (isdigit+) ":" (isdigit+))} stamp)
     (tryif time (get time 'timekeys))}))

(define (parse-unit val)
  (cond ((not (string? val)) (fail))
	((equal? val "1") 'count)
	((has-prefix val "http://www.wikidata.org/entity/")
	 (?? 'wikidref (slice val 31)))
	(else val)))

(define (parse-globe val)
  (cond ((not (string? val)) (fail))
	((has-prefix val "http://www.wikidata.org/entity/")
	 (?? 'wikidref (slice val 31)))
	(else (fail))))

(define (index-wikid index concept)
  (when (and fix-wikid (exists? wikid-fixers))
    ((pick wikid-fixers applicable? concept) concept))
  (do-choices (slot (pick (pickoids (getkeys concept)) 'source 'wikidata))
    (when fix-wikid
      (if (%test concept slot #f) (drop! concept slot #f))
      ((get wikid-fixers slot) concept))
    (do-choices (value (get concept slot))
      (cond ((string? value)
	     (index-frame index concept slot value))
	    ((oid? value)
	     (index-frame index concept slot {value (list value)}))
	    ((timestamp? value)
	     (index-frame index concept slot (get value 'timekeys)))
	    ((not (table? value)))
	    ((test value 'time)
	     (index-frame index concept slot (get-timekeys value)))
	    ((test value 'amount)
	     (let ((amount (string->number (get value 'amount)))
		   (unit (parse-unit (get value 'unit))))
	       (when amount
		 (if (exists? unit)
		     (index-frame index concept slot (cons amount unit))
		     (index-frame index concept slot amount)))))
	    ((test value 'text)
	     (let* ((text (get value 'text))
		    (lang  (get value 'language))
		    (langid (try (get language-map lang)
				 {(tryif (position #\_ lang)
				    (get language-map (slice lang 0 (position #\_ lang))))
				  lang}))
		    (stringvals {(stdstring text) (stdspace text)}))
	       (index-frame index concept slot stringvals)
	       (index-frame index concept slot (cons stringvals langid))))
	    ;; For lat/long
	    ((test value 'globe))
	    (else)))))

(define (getallvalues concept slotids)
  (choice (%get concept slotids)
	  (%get concept (get* (pickoids slotids) 'slots))))

(defambda (index-genl-values index frame slot (values))
  (do-choices (slot slot)
    (let ((v (if (bound? values) values (getallvalues frame slot))))
      (when (exists? v)
	;; We index (v) to indicate that v is a stored value
	(index-relation index frame slot (choice v (list v)))
	(index-relation index frame slot (?? specls* v))))))

(define lattice-slotids
  {@1/2c272{GENLS}
   @1/2c273{SPECLS}
   @1/2c274{PART-OF}
   @1/2c275{PARTS}
   @1/2c276{MADE-OF}
   @1/2c277{STUFF-OF}
   @1/2c278{MEMBERS}
   @1/2c279{MEMBER-OF}
   @1/2c27b{GENLS*}
   @1/2c27c{KINDS*}
   @1/2c281{PART-OF*}
   @1/2c282{PARTS*}
   @1/2c283{MEMBER-OF*}
   @1/2c284{MEMBERS*}
   @1/2c285{STUFF-OF*}
   @1/2c286{INGREDIENTS*}
   })

;; These are slotids whose indexed values should include implications
(define generalize-slotids
  '{@1/2c274{PART-OF}
    @1/2c275{PARTS}
    @1/2c276{MADE-OF}
    @1/2c277{STUFF-OF}
    @1/2c278{MEMBERS}
    @1/2c279{MEMBER-OF}
    @1/2c27e{ISA}})

(define termlogic-slotids
  {@1/4{ALWAYS}
   @1/5{SOMETIMES}
   @1/6{NEVER}
   @1/8{/ALWAYS}
   @1/a{COMMONLY}
   @1/b{RARELY}
   @1/e{RELTERMS}})

;; These are slotids whose inverses should also be indexed
(define invert-slotids
  {@1/10{DIFFTERMS}
   @1/3f65f{ENTAILS}
   @1/2c27d{DISJOINT}
   @1/2d9e9{=IS=}})

(define special-relationships
  {termlogic-slotids lattice-slotids})

(module-export! '{lattice-slotids generalize-slotids termlogic-slotids invert-slotids})

(define (index-relations index concept (slots))
  (default! slots
    (reject (reject (pick (difference (pickoids (getkeys concept)) special-relationships)
		      'type 'relation)
	      'type 'termlogic)
      'type 'wikidprop))
  (do-choices (slotid slots)
    (index-relation index concept slotid (getallvalues concept slotid)
		    (or (overlaps? slotid invert-slotids)
			(test slotid 'type 'indexinverse))
		    (or (overlaps? slotid generalize-slotids)
			(test slotid 'type 'indexgenls)))))

(define (index-refterms index concept)
  (index-relation index concept refterms (%get concept refterms) #f)
  (index-relation index concept sumterms (%get concept sumterms) /sumterms)
  ;; This handles the case of explicit inverse pointers.
  ;;  If we want to add a pointer R from X to Y and
  ;;   we can't or don't want to modify X, we store
  ;;   an explicit inverse ((inv R) Y)=X which will be
  ;;   found by the inverse inference methods.
  ;; Otherwise, we don't index the @?defines and @?referenced
  ;;  slots because it is easier to just get @?sumterms
  ;;  and @?refterms.
  (when (%test concept /sumterms)
    (index-relation index (%get concept /sumterms) sumterms concept)
    (index-relation index concept /sumterms (%get concept /sumterms)))
  (when (%test concept /refterms)
    (index-relation index (%get concept references) refterms concept)
    (index-relation index concept references (%get concept references))))

(define (index-lattice index concept)
  (index-frame* index concept genls* genls specls*)
  (index-frame* index concept partof* partof parts*)
  (index-frame* index concept memberof* memberof members*)
  (index-frame* index concept ingredientof* ingredientof ingredients*))

(define (index-analytics index concept)
  ;; ALWAYS is transitive
  (index-frame* index concept always always /always)
  ;; SOMETIMES is symmetric
  (index-frame index concept sometimes)
  (index-frame index (get concept sometimes) sometimes concept)
  ;; NEVER is symmetric
  (index-frame index concept never)
  (index-frame index (get concept never) never concept)
  (index-frame index concept somenot)
  (index-frame index (get concept somenot) /somenot concept)
  (index-frame index concept commonly)
  (index-frame index concept rarely)
  ;; index some inferred values, relying on the lattice relations
  ;; which were indexed above
  (let ((s (get concept sometimes))
	(c (get concept commonly))
	(n (get concept never)))
    (when (exists? s)
      (index-frame index concept sometimes (list s))
      (index-frame index s sometimes (list concept))
      (index-frame index concept
	sometimes (find-frames index /always s))
      (unless (test concept 'type 'individual)
	(index-frame index (find-frames index /always s)
	  sometimes concept)))
    (when (exists? n)
      (index-frame index concept never (list n))
      (index-frame index n never (list concept))
      (index-frame index (find-frames index /always n)
	never concept))
    (when (exists? c)
      ;; This indexes concept as commonly being all of the always of c
      (index-frame index concept
	commonly (find-frames index /always c)))))

(define (indexer index concept (slotids) (values))
  (if (bound? slotids)
      (if (bound? values)
	  (index-relation index concept slotids values)
	  (index-relation index concept slotids))
      (index-concept index concept)))

(define (next-expansion expansions visited)
  (let ((oids (get expansions (getkeys expansions))))
    (prefetch-oids! oids)
    (hashset-add! visited oids)
    (let ((table (make-hashtable)))
      (do-choices (slotid (getkeys expansions))
	(prefetch-keys! (cons (get slotid inverse) (get expansions slotid)))
	(let ((next (reject (get (get expansions slotid) slotid)
		      visited)))
	  (when (exists? next)
	    (add! table slotid next))))
      (if (exists? (getkeys table)) table (fail)))))

(define (prefetch-expansions oids slotids)
  (let ((visited (choice->hashset oids))
	(next (make-hashtable)))
    (prefetch-oids! oids)
    (prefetch-keys! (cons (get (pick slotids oid?) 'inverse) oids))
    (do-choices (slotid slotids)
      (add! next slotid (get oids slotid)))
    (do ((scan next (next-expansion (qc scan) visited)))
	((fail? scan)))))

(define (indexer-prefetch-slotids!)
  (prefetch-oids! (choice generalize-slotids invert-slotids
			  (get language-map (getkeys language-map))
			  (get norm-map (getkeys norm-map))
			  (get indicator-map (getkeys indicator-map))
			  (get gloss-map (getkeys gloss-map)))))

(define (indexer/prefetch oids)
  (prefetch-oids! oids)
  (prefetch-keys! (cons (choice refterms /refterms) oids))
  (let ((kovalues (%get oids generalize-slotids)))
    (prefetch-expansions (qc kovalues) genls)))

(define (index-lattice/prefetch oids)
  (prefetch-oids! oids)
  (prefetch-expansions
   (qc oids) (qc genls partof memberof ingredientof)))

;;; Endpoint for indexing BRICO+ frames

(define (brico/index! f)
  (index-concept (knodb/getindex f) f))

;; (define (brico/index-slot! f slotid (opts #f) (relations) (fields))
;;   (default! relations (getopt opts 'relations.index (getopt opts 'index)))
;;   (default! fields (getopt opts 'fields.index (getopt opts 'index)))
;;   (default! isrelation
;;     (or (overlaps? slotid invert-slotids)
;; 	(overlaps? slotid generalize-slotids)
;; 	(and (oid? slotid)
;; 	     (test slotid 'type '{relation indexinverse indexgenls}))))
;;   (let* ((values (get f slotid))
;; 	 (oids (pickoids values))
;; 	 (notoids (difference values oids))
;; 	 (all (getallvalues oids slotid))
;; 	 (alloids (pickoids all))
;; 	 (other {notoids (difference all alloids)}))
;;     (when (exists? oids)
;;       (if isrelation
;; 	  (index-relation relations f slotid (choice alloids oids)
;; 			  (or (overlaps? slotid invert-slotids)
;; 			      (test slotid 'type 'indexinverse))
;; 			  (or (overlaps? slotid generalize-slotids)
;; 			      (test slotid 'type 'indexgenls)))
;; 	  (index-frame relations f slotid (choice alloids oids))))
;;     (when (exists? other)
;;       (index-frame fields f slotid {other (list notoids)}))))

;;; EXPORTS

;;; These are helpful for indexing data BRICO-style or indexing
;;;  data which use BRICO.
(module-export!
 '{
   brico/index-slot!
   index-relation
   indexer unindex
   index-string index-name index-frags index-frame*
   index-string/keys index-frags/keys index-gloss})

;;; These all support indexing BRICO itself
(module-export!
 '{index-brico
   index-core index-wikid index-brico index-wordform
   index-words index-relations index-lattice
   index-analytics
   index-refterms
   index-concept
   indexer/prefetch
   index-lattice/prefetch
   indexer-prefetch-slotids!})

