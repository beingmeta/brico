;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, LLC

(in-module 'brico)
;;; Using the BRICO database from Kno

(define %nosubst '{bricosource brico.source
		   brico.pool brico.index brico.indexes
		   freqfns use-wordforms})

(use-module '{texttools kno/reflect logger varconfig knodb knodb/config binio})
;; For custom methods
(use-module 'kno/rulesets)

(define-init %loglevel %notify%)
;;(set! %loglevel %debug%)

(define (get-keyslot ix) 
  (tryif (index? ix) (indexctl ix 'keyslot)))

;;; Configuring bricosource

(define-init brico.source #f)
(define-init brico.pool {})
(define-init brico.index {})
(define-init brico.indexes {})
(define brico.opts #[background #t readonly #t basename "brico.pool"])
(define-init brico:readonly #t)

(define wikidref.index {})
(define wordnet.index #f)
(define core.index {})
(define termlogic.index #f)

(define en.index #f)
(define en_norms.index #f)
(define en_aliases.index #f)

(define words.index #f)
(define norms.index #f)
(define aliases.index #f)

(define wordnet.adjunct #f)
(define attic.adjunct #f)

(module-export!
 '{en.index en_norms.index en_aliases.index
   words.index norms.index aliases.index 
   names.index})

(module-export! '{core.index wikidref.index wordnet.index
		  lattice.index termlogic.index})

(module-export! '{wordnet.adjunct attic.adjunct})

(define absfreqs {})

(define host-string '(+ {(isalnum) "-"}))
(define host-name `#((* #(,host-string ".")) ,host-string))

(defambda (pick-indexes indexes slots)
  (filter-choices (index indexes)
    (and (index? index)
	 (if (dbctl index 'keyslot)
	     (overlaps? (dbctl index 'keyslot) slots)
	     (and (dbctl index 'slots)
		  (exists? (intersection (elts (dbctl index 'slots)) slots)))))))

(define (config-absfreqs var (val))
  (cond ((not (bound? val)) absfreqs)
	((and (string? val)
	      (or (position #\@ val)
		  (has-suffix val ".index")))
	 (set+! absfreqs (open-index val)))
	((string? val)
	 (set+! absfreqs (file->dtype val)))
	((table? val) (set+! absfreqs val))
	(else (error 'typeerror "Not a table or index" val))))
(config-def! 'absfreqs config-absfreqs)

;;; Common tables

(define-init all-languages 
  (read-xtype (get-component "data/languages.xtype")))
(define-init language-map
  (read-xtype (get-component "data/langmap.table")))
(define-init norm-map
  (read-xtype (get-component "data/normmap.table")))
(define-init alias-map
  (read-xtype (get-component "data/aliasmap.table")))
(define-init gloss-map
  (read-xtype (get-component "data/glossmap.table")))
(define-init indicator-map
  (read-xtype (get-component "data/indicatormap.table")))
(define-init frag-map
  (read-xtype (get-component "data/fragmap.table")))
(define-init ids-table
  (read-xtype (get-component "data/ids.table")))

(define index-map indicator-map)

(define-init all-languages (get language-map (getkeys language-map)))
(define-init all-norms (get norm-map (getkeys norm-map)))
(define-init all-glosses (get gloss-map (getkeys gloss-map)))

(define-init builtin-index
  (let ((tempindex (make-index "BRICOIDS" #[type tempindex register #t keyslot %id])))
    (do-choices (id (getkeys ids-table))
      (add! tempindex id (get ids-table id)))
    (when (config 'BRICO:BUILTINS #t config:boolean)
      (use-index tempindex))
    tempindex))

;;; This is how these tables where generated
(comment
 (do-choices (l (?? 'type 'language))
   (store! language-map (get l 'key) l)
   (store! language-map (get l 'langid) l))
 (do-choices (l (?? 'type 'gloss))
   (store! gloss-map (get l 'key) l))
 (do-choices (l (?? 'type 'norm))
   (store! norm-map (get l 'key) l))
 (do-choices (l (?? 'type 'indexes))
   (store! indicator-map (get l 'key) l))
 (do-choices (l (?? 'type 'fragments))
   (store! frag-map (get l 'key) l)))

;;; Building the idtable

(define (get-idtable (core.index core.index) (table (make-hashtable)))
  (do-choices (mkeys (pick (getkeys core.index) '%mnemonics))
    (do-choices (f (get core.index mkeys))
      (let ((ids (picksyms (get f '%mnemonics))))
	(do-choices (id ids)
	  (when (and (string? id) (position #\; id))
	    (set+! ids (trim-spaces (elts (segment id ";"))))))
	(add! table ids f)
	(add! table (downcase ids) f))))
  (do-choices (key (getkeys table))
    (when (ambiguous? (get table key))
      (logwarn |AmbiguousID|
	(write key) " maps to "
	(do-choices (v (get table key))
	  (printout "\n\t" v)))
      (if (singleton? (reject (get table key) 'type 'lexslot))
	  (store! table key (reject (get table key) 'type 'lexslot))
	  (drop! table key))))
  table)

;;; Common BRICO frames

(define english @1/2c1c7"English")
(define en @1/2c1c7"English")
(define en_norms @1/44896)
(define en_aliases @1/2ac91"Aliases in English")
(define en_frags @1/44bed)
(define english-gloss @1/2ffbd"Gloss (English)")
(define english-norm @1/44896)
(define spanish @1/2c1fc"Spanish")
(define spanish-norms @1/448cb"Common Spanish")
(define spanish-aliases @1/2ace7"Aliases in Spanish")
(define spanish-glosses @1/32cd2"Gloss (Spanish)")
(define french @1/2c122"French")

(define wordnet-source @1/0"WordNet 1.6 Copyright 1997 by Princeton University.  All rights reserved.")
(define roget-source @1/1"Derived from 1911 Roget Thesaurus")
(define brico-source @1/2d9ea{BRICO10})

;; Due to a range of factors, including the roots of WordNet, English is the most complete language, 
;;  so we make it the default.
(define default-language english)

(define always @1/4{ALWAYS})
(define sometimes @1/5{SOMETIMES})
(define never @1/6{NEVER})
(define somenot @1/7{SOMENOT})
(define optional @1/7{OPTIONAL})
(define commonly @1/a{commonly})
(define rarely @1/b{rarely})

;;; Inverses
(define /always @1/8{%ALWAYS})
(define /somenot @1/9{%SOMENOT})
(define /commonly @1/c{/commonly})
(define /rarely @1/d{/RARELY})

(define elements @1/e{ELEMENTS})
(define elementof @1/f{ELEMENTOF})

(define %always /always)
(define %somenot /somenot)

(define probably commonly)
(define optional somenot)
(define /optional /somenot)
(define %optional /somenot)

(define genls @1/2c272{GENLS})
(define genls* @1/2c27b{GENLS*})
(define kindof @1/2c272{GENLS})
(define kindof* @1/2c27b{GENLS*})
(define specls @1/2c273{SPECLS})
(define specls* @1/2c27c{SPECLS*})
(define isa @1/2c27e{ISA})
(define implies @1/2c27e{ISA})
(define implies* @1/2f201{IMPLIES*})
(define impliedby @1/2c27f{IMPLIEDBY})
(define impliedby* @1/44c99{IMPLIEDBY*})

(define parts @1/2c275{PARTS})
(define parts* @1/2c282{PARTS*})
(define partof @1/2c274{PARTOF})
(define partof* @1/2c281{PARTOF*})
(define members @1/2c278{MEMBERS})
(define members* @1/2c284{MEMBERS*})
(define memberof @1/2c279{MEMBER-OF})
(define memberof* @1/2c283{MEMBER-OF*})
(define ingredients @1/2c276{INGREDIENTS})
(define ingredients* @1/2c286{INGREDIENTS*})
(define ingredientof @1/2c277{INGREDIENTOF})
(define ingredientof* @1/2c285{INGREDIENTOF*})
(define entails @1/3f65f{ENTAILS})
(define entailedby @1/2b74c{ENTAILEDBY})
(define entails* @1/44c9a{ENTAILS*})
(define entailedby* @1/44c9b{ENTAILEDBY*})

(define sumterms @1/2ab4d{SUMTERMS})
(define /sumterms @1/2ab55{/SUMTERMS})
(define refterms @1/2ab57{REFTERMS})
(define /refterms @1/2ab5a{/REFTERMS})
(define references @1/2ab5a{/REFTERMS})
(define =is= @1/2d9e9{=IS=})
(define sameas @1/2d9e9{SAMEAS})
(define inverse @1/2c27a{INVERSE})
(define disjoint @1/2c27d{DISJOINT})
(define relterms @1/e{RELTERMS})
(define /relterms @1/f{/RELTERMS})
(define diffterms @1/10{DIFFTERMS})
(define differentiae diffterms)
(define /diffterms @1/11{/DIFFTERMS})
(define differentiates /diffterms)

(define brico-slotids
  (choice always sometimes never somenot commonly rarely
	  /always /somenot /commonly /rarely
	  genls genls* kindof kindof* specls specls*
	  parts parts* partof partof*
	  members members* memberof memberof*
	  ingredients ingredients* ingredientof ingredientof*
	  =is= sameas inverse disjoint 
	  refterms /refterms sumterms /sumterms diffterms /diffterms
	  relterms /relterms))

;;;; Getting ids from frames

(define (getid concept (language default-language))
  (tryif (oid? concept)
	 (try (get (get concept '%ids) language)
	      (get-norm (get concept 'brico) language)
	      (get-norm concept language)
	      (let ((id (get concept '%id)))
		(if (string? id) id
		    (if (symbol? id) (symbol->string id)
			(oid->string concept)))))))

(define (computeid oid language)
  (getid oid
	 (if (oid? language) language
	     (try (get language-map language) language))))

(define (translator item language)
  (pick-one
   (tryif (oid? item)
	  (if (or (test item 'type 'slot) (test item '%ids))
	      (cachecall computeid item language)
	      (tryif (test item '{words %words})
		     (get-norm item
			       (if (oid? language) language
				   (try (get language-map language) language))
			       #t))))))

(config! 'i18n/translators translator)

;;; Getting norms, glosses, etc.

(define (get-norm concept (language default-language) (tryhard #t) (langid))
  "Gets the 'normal' word for a concept in a given language, \
   going to English or other languages if necessary"
  (default! langid (if (oid? language) (get language 'key) language))
  (try (tryif (eq? language english) (pick-one (largest (get concept 'norms) length)))
       (pick-one (largest (get (get concept '%norms) langid) length))
       (pick-one (largest (get concept language)))
       (tryif (and tryhard (not (eq? language english)))
	 (try (pick-one (largest (get-norm concept english #f langid)))
	      (pick-one (largest (cdr (get concept '%words))))))
       (tryif (exists? (get concept '%norms))
	 (pick-one (largest (getvalues (get concept '%norms)))))
       (tryif (exists? (get concept '%words))
	 (pick-one (largest (getvalues (get concept '%words)))))))

(define (%get-norm concept (language default-language) (langid))
  "Gets the 'normal' word for a concept in a given language, \
   skipping custom overrides and not looking in other languages."
  (default! langid (if (oid? language) (get language 'key) language))
  (try (tryif (eq? language english) (pick-one (largest (get concept 'norms) length)))
       (pick-one (largest (get (get concept '%norms) language) length))
       (tryif (eq? language english) (pick-one (largest (get concept 'words) length)))
       (pick-one (largest (get (get concept '%words) langid) length))
       (pick-one (largest (cdr (get concept '%words)) length))))

(define (get-normterm concept (language default-language))
  (try (get-norm concept language #f)
       (string-append "en$" (get-norm concept english))))

(define (get-gloss concept (language default-language) (gloss-slot))
  (unless (oid? language) (set! language (get language-map language)))
  (default! gloss-slot (try (get gloss-map language) #f))
  (try (tryif gloss-slot (get concept gloss-slot))
       (tryif (and (exists? language) (oid? language))
	 (get (get concept '%glosses) language))
       (tryif (test concept '%glosses)
	 (let ((lang (pick-one (getkeys (get concept '%glosses)))))
	   (string-append "(" (getid lang 'id) ") " (get-gloss concept lang))))
       (if (test concept 'gloss)
	   (string-append "(en) " (get concept 'gloss))
	   "no gloss")))

(define (get-single-gloss concept (language default-language))
  (pick-one (get-gloss concept language)))
(define (get-short-gloss concept (language #f))
  (do ((gloss (get-single-gloss concept language))
       (pbreak 0 (textsearch #{#("\n" (spaces) "\n") "\&para;" ";"}
			     gloss (1+ pbreak))))
      ((if pbreak (> pbreak 8) #t)
       (if pbreak (subseq gloss 0 pbreak) gloss))))

(define (get-doc concept (language default-language))
  (tryif (oid? concept)
	 (try (get (get concept '%docs) language)
	      (get (get concept '%glosses) language)
	      (get concept '%doc)
	      (get (get concept '%docs) english)
	      (get concept 'gloss))))

;;; Displaying glosses

(define (gloss f (slotid english-gloss))
  (if (applicable? slotid)
      (lineout f "\t" (slotid f))
      (lineout f "\t" (get f slotid))))

;;; Capitalizing word entries

(define (capup string)
  (if (or (capitalized? string) (uppercase? string)) string
      (capitalize string)))

(define (cap%wds e (cautious #f))
  (if (pair? e)
      (if (or (capitalized? (cdr e)) (uppercase? (cdr e)))
	  e
	  (if cautious
	      (cons (car e) (choice (cdr e) (capitalize (cdr e))))
	      (cons (car e) (capitalize (cdr e)))))
      e))

(define (cap%frame! f (cautious #f))
  (let* ((words (get f '%words))
	 (norm (get f '%norms))
	 (cwords (cap%wds words cautious))
	 (cnorm (cap%wds norm cautious))
	 (changed #f))
    (unless (identical? words cwords)
      (store! f '%words cwords)
      (set! changed #t))
    (unless (identical? norm cnorm)
      (store! f '%norms cnorm)
      (set! changed #t))
    (unless cautious
      (when (test f 'words)
	(store! f 'words (capup (get f 'words))))
      (when (test f 'ranked)
	(store! f 'ranked (map capup (get f 'ranked)))))
    changed))


(define (capdown string)
  (if (uppercase? string) string (downcase string)))

(define (low%wds e (cautious #f))
  (if (pair? e)
      (if (lowercase? (cdr e)) e
	  (if cautious
	      (cons (car e) (choice (cdr e) (capdown (cdr e))))
	      (cons (car e) (capdown (cdr e)))))
      e))

(define (low%frame! f (cautious #f))
  (let* ((words (get f '%words))
	 (norm (get f '%norms))
	 (cwords (low%wds words cautious))
	 (cnorm (low%wds norm cautious))
	 (changed #f))
    (unless (identical? words cwords)
      (store! f '%words cwords)
      (set! changed #t))
    (unless (identical? norm cnorm)
      (store! f '%norms cnorm)
      (set! changed #t))
    (unless cautious
      (when (test f 'words)
	(store! f 'words (capdown (get f 'words))))
      (when (test f 'ranked)
	(store! f 'ranked (map capdown (get f 'ranked)))))
    changed))

;;; Generic prefetching

;;; These functions do generic prefetching for BRICO concepts,
;;;  retrieving both OIDs and the inverted slotid keys used by
;;;  inference.

(define default-brico-slotids
  (choice genls specls partof))

(defambda (brico-prefetch! concepts (slotids default-brico-slotids) (inparallel #f))
  (if inparallel
      (let ((threads {(thread/call prefetch-oids! concepts)
		      (for-choices (slotid {(get (pickoids slotids) 'inverse)
					    (get (pickoids (get slotids 'slots)) 'inverse)})
			(thread/call ??/prefetch! slotid concepts))}))
	(thread/wait threads))
      (begin (prefetch-oids! concepts)
	(??/prefetch! (get slotids 'inverse) concepts)
	(??/prefetch! (get (pick (get slotids 'slots) oid?) 'inverse) concepts))))

(defambda (brico-prefetch concepts (slotids default-brico-slotids))
  (brico-prefetch! concepts)
  concepts)

(define (prefetch-slots!)
  "Prefetches all known slot OIDs.  This is helpful for prefetch testing."
  (prefetch-oids! (?? 'type '{slot language gloss indexes})))

;;; Display an expansion string for a concept in a language

(define (get-expstring concept (language english) (slotid implies))
  "Returns an expansion string indicating a concept and its 'superiors'"
  (stringout
      (let ((words (get concept (choice language 'names))))
	(if (fail? words) (set! words (get concept (choice english 'names))))
	(if (fail? words) (printout "Je n'ai pas les mots.")
	    (do-choices (word words i)
	      (if (position #\Space word)
		  (printout (if (> i 0) " . ") "\"" word "\"")
		  (printout (if (> i 0) " . ") word)))))
    (printout " < ")
    (forgraph (lambda (x)
		(do-choices (word (get x (choice language 'names)) i)
		  (if (position #\Space word)
		      (printout (if (> i 0) " . " " ") "\"" word "\"")
		      (printout (if (> i 0) " . " " ") word))))
	      (get concept slotid) slotid)))

;;; Making %ID slots for BRICO concepts

(define (make%id f (lang default-language))
  (if (test f 'source @1/1)
      (make-roget-id f lang)
      (if (test f 'type 'wordform)
	  (make-wordform-id f)
	  (let* ((sensecats (get f 'sensecat))
		 (sensecat (try (singleton sensecats)
				(singleton (difference sensecats 'NOUN.TOPS))
				(intersection sensecats 'NOUN.PERSON)
				(intersection sensecats 'NOUN.ANIMAL)
				(intersection sensecats 'NOUN.LOCATION)
				(pick-one sensecats))))
	    `(,(if (ambiguous? sensecat) 'VAGUE sensecat)
	      ,@(if (ambiguous? sensecat) (list (qc sensecat)) '())
	      ,(get-norm f lang)
	      ,(cond ((and (test f 'sensecat 'noun.location)
			   (%test f partof))
		      'PARTOF)
		     ((%test f 'hypernym)
		      (if (%test f 'type 'individual) 'ISA 'GENLS))
		     ((%test f genls) 'GENLS)
		     ((%test f implies) 'ISA)
		     ((%test f partof) 'PARTOF)
		     ((%test f diffterms) 'SUMTERMS)
		     ((%test f sumterms) 'SUMTERMS)
		     (else 'TOP))
	      ,@(map get-norm
		     (choice->list
		      (try (tryif (test f 'sensecat 'noun.location)
			     (%get f partof))
			   (%get f 'hypernym)
			   (%get f genls)
			   (%get f implies)
			   (%get f partof)
			   (%get f diffterms)
			   (%get f sumterms))))
	      ,@(if (test f 'srcid) (list (qc (get f 'srcid))) '()))))))

(defambda (make%id! f (lang default-language))
  (do-choices f
    (let ((id (make%id f lang)))
      (unless (%test f '%id id) (store! f '%id id))))
  (if (singleton? f) (get f '%id)))

(define (make-wordform-id f)
  `(WORDFORM ,(get f 'word)
	     ,(try (get (get f 'lang) '%mnemonic)
		   (get (get f 'lang) 'iso639/1)
		   (get (get f 'lang) 'iso639/b)
		   '??)
	     ,(get f 'of)))

(define (make-roget-id f (lang default-language))
  `(ROGET
    ,(get-norm f lang)
    WITHIN
    , (try (get-norm (pick-one (get f 'roget-within)) lang)
	   (get (pick-one (get f 'roget-within)) '%id))))

;;;; GETABSFREQ (from tables)

(define (getabsfreq concept)
  (reduce-choice + absfreqs 0 (lambda (tbl) (get tbl concept))))

;;;; Getting concept frequency information (from DB)

;; This is a list of functions to get concept/term frequency information.
;;  Each item is a sequence whose first element is a name (typically a symbol)
;;  and whose remaining elements specify a way to get frequency information.
;;  These can be 
;;  of three arguments (concept, language, term) that returns a count of
;;  either co-occurences of term in language with concept or the absolute
;;  frequency of the concept if language and term are false.
(define freqfns '())

(define (concept-frequency concept (language #f) (term #f))
  (let ((sum 0))
    (dolist (method-spec freqfns)
      (let ((method (second method-spec)))
	(if (applicable? method)
	    (set! sum (+ sum (or (try (method concept language term) 0) 0)))
	    (if (slotid? method)
		(let ((wordslot
		       (and (> (length method-spec) 2) (third method-spec)))
		      (index (and (> (length method-spec) 3) (fourth method-spec))))
		  (set! sum
			(+ sum
			   (if (and wordslot term)
			       (if index
				   (choice-size (find-frames index method concept wordslot term))
				   (choice-size (?? method concept wordslot term)))
			       (if index
				   (choice-size (find-frames index method concept))
				   (choice-size (?? method concept)))))))))))
      sum))

(define use-wordforms #t)

(define (wordform-concept-frequency concept language term)
  (and use-wordforms (in-pool? concept brico.pool) (eq? language english)
       (try (tryif term
		   (get (?? 'of concept 'word term 'language language) 'freq))
	    (reduce-choice + (?? 'of concept) 0 'freq)
	    ;; Otherwise, this make a wild guess, biasing norms
	    (if term
		(if (overlaps? concept (?? (get norm-map language) term)) 3 1)
		1))))

(define usewordforms-config
  (slambda (var (value))
    (if (bound? value)
	(set! use-wordforms value)
	use-wordforms)))
(config-def! 'usewordforms usewordforms-config)

(set! freqfns (list (vector 'wordform wordform-concept-frequency)
		    (vector 'sumterms sumterms)
		    (vector 'refterms refterms)))

(defambda (concept-frequency-prefetch concepts language words)
  (??/prefetch! (pick (elts (map second freqfns)) oid?) concepts)
  (when (and use-wordforms (eq? language english) (exists? words))
    (parallel (??/prefetch! 'of (pick concepts brico.pool))
	      (??/prefetch! 'word words)))
  (prefetch-oids! (?? 'of (pick concepts brico.pool)
		      'word words)))

;;; Configuring freqfns

(define (edit-freqfns name fn scan)
  (if (null? scan) (list fn)
      (if (and name (equal? (first (car scan)) name))
	  (cons fn (cdr scan))
	  (cons (car scan) (edit-freqfns name fn (cdr scan))))))

(define (freqfns-config var (val))
  (if (bound? val)
      (set! freqfns (edit-freqfns (first val) val freqfns))
      freqfns))
(config-def! 'freqfns freqfns-config)

;; This returns a method for using indexed corpora to provided
;; absolute or contingent concept frequency information.
(defambda (use-corpus-frequency corpus conceptslot wordslot (langslot #f))
  (cons
   (vector (qc corpus) conceptslot wordslot)
   (lambda (concept language word)
     (if (not language)
	 (choice-size (find-frames corpus conceptslot concept))
	 (if langslot
	     (choice-size (find-frames corpus
			    conceptslot concept
			    wordslot word
			    langslot language))
	     (choice-size (find-frames corpus
			    conceptslot concept
			    wordslot word)))))))

(comment
 (define (check-prefetch index f)
   (clearcaches)
   (indexer-slotid-prefetch)
   (indexer-prefetch (qc f))
   (trackrefs (lambda () (index-concept index f)))))

;;; Apply ISA

(define (assign-genls f genl)
  (assert! f genls genl)
  (assert! f 'sensecat (get genl 'sensecat))
  (make%id! f))

(define (fixcaps wdpair wds)
  (cons (car wdpair)
	(let ((wd (cdr wdpair)))
	  (if (capitalized? wd) wd
	      (let ((pos (largest (search wds wd))))
		(if pos
		    (string-append (capitalize (subseq wd 0 pos))
				   (subseq wd pos))
		    (capitalize1 wd)))))))

(define (assign-isa f isa (fixcase #t))
  (assert! f implies isa)
  (assert! f 'sensecat (get isa 'sensecat))
  (when fixcase
    (let ((isa-terms (get isa english)))
      (store! f '%words (fixcaps (get f '%words) (qc isa-terms)))
      (store! f '%norms (fixcaps (get f '%norms) (qc isa-terms)))))
  (make%id! f))

;;; Getting depth information

(define (getpaths f (slotid genls) (seen (make-hashset)))
  (tryif (not (hashset-get seen f))
    (begin (hashset-add! seen f)
      (try (cons f (getpaths (get f slotid) slotid seen)) '()))))

(define (brico/update-depths! f (slotid genls) (total 'n-genls) (depths 'depths))
  (let ((paths (getpaths f slotid)))
    (store! f total (choice-size (elts paths)))
    (store! f depths (length paths))))

(module-export! '{brico/update-depths!})

;;; EXPORTS

(module-export!
 '{brico.pool
   brico.index brico.db
   xbrico.pool names-pool places-pool
   brico.pool brico.index brico.indexes
   xbrico.pool names.pool places.pool
   absfreqs getabsfreq
   default-language all-languages
   ;; Maps for particular languages
   language-map gloss-map norm-map alias-map indicator-map frag-map index-map
   all-languages all-glosses all-norms
   ;; Prefetchers for OIDs and inverted index slotids
   brico-prefetch! brico-prefetch prefetch-slots!})

(module-export!
 ;; OIDs by name
 '{english en en_norms en_aliases en_frags
   english-gloss english-norm 
   spanish french
   always sometimes never somenot commonly rarely
   /always /somenot /commonly /rarely
   isa implies implies* impliedby impliedby*
   entails entails* entailedby entailedby*
   genls genls* kindof kindof* specls specls*
   parts parts* partof partof*
   members members* memberof memberof*
   ingredients ingredients* ingredientof ingredientof*
   inverse =is= disjoint
   refterms sumterms /refterms /sumterms references
   relterms /relterms
   diffterms /diffterms differentiae differentiates
   ;; Legacy
   probably optional %optional /optional  %always  %somenot
   wordnet-source roget-source brico-source})

;; Getting glosses, norms, etc.
(module-export!
 '{getid
   get-gloss get-norm get-normterm get-expstring get-doc
   get-single-gloss get-short-gloss gloss})

;; Getting frequency information
(module-export! '{concept-frequency concept-frequency-prefetch})

;;; Miscellaneous functions
(module-export!
 '{make%id
   make%id!
   cap%wds cap%frame! low%wds low%frame!
   assign-isa assign-genls})

;;;; Read/write configuration

(define (set-brico:readonly! flag)
  (knodb/readonly! brico.pool flag)
  (knodb/readonly! brico.index flag)
  (set! brico:readonly flag))
(config-def! 'brico:readonly
  (lambda (var (val))
    (cond ((unbound? val) brico:readonly)
	  ((and val brico:readonly) #f)
	  ((not (or val brico:readonly)) #f)
	  ((and brico.pool brico.index)
	   (set-brico:readonly val))
	  ((or brico.pool brico.index)
	   (logwarn |IncompleteBricoDB|
	     "Can't set readonly to " val " for brico.pool=" wikid.pool " brico.index=" wikid.index)
	   (set-brico:readonly val))
	  (else (set! brico:readonly val)))))

;;;; Setup config

(propconfig! brico:background brico.opts 'background)

(define (setup-brico pool (opts #f))
  (and (pool? pool) (equal? (pool-base pool) @1/0)
       (let ((indexes (pool/getindexes pool opts)))
	 (lognotice |BricoConfig| pool)
	 (set! brico.pool pool)
	 (set! brico.index (pool/getindex pool opts))
	 (set! wikidref.index (pick-indexes indexes 'wikidref))
	 (set! core.index (pick indexes index-source has-suffix "core.index"))
	 (set! en.index (pick-indexes indexes en))
	 (set! en_norms.index (pick-indexes indexes en_norms))
	 (set! en_aliases.index (pick-indexes indexes en_aliases))
	 (set! words.index (pick-indexes indexes spanish))
	 (set! norms.index (pick-indexes indexes spanish-norms))
	 (set! aliases.index (pick-indexes indexes spanish-aliases))
	 (set! brico.source (pool-source pool))
	 (set-brico:readonly! brico:readonly)
	 pool)))

(define-init bricosource-configfn (knodb/configfn setup-brico brico.opts))
(config-def! 'bricosource bricosource-configfn)
(config-def! 'brico:source bricosource-configfn)

;;;; BRICO load hooks (not yet implemented)

#|
(define brico-onload '())
(define brico-onloaded '())

(define (addfn fn list (replace #f))
  "Returns #f if fn wasn't added to list, otherwise returns "
  "the updated list."
  (if (position fn list) #f
      (let ((name (procedure-name fn))
	    (module (procedure-module fn))
	    (scan list)
	    (found #f))
	(while (and (not found) (pair? scan))
	  (if (and (eq? name (procedure-name (car scan)))
		   (eq? module (procedure-module (car scan))))
	      (set! found scan)
	      (set! scan (cdr scan))))
	(if (and found replace)
	    (begin (set-car! found fn) list)
	    (and (not found) (cons fn list))))))

(define (brico:onload-configfn var (val))
  (cond ((unbound? val) brico-onload)
	((and (applicable? val) (zero? (procedure-min-arity val)))
	 (if brico.pool
	     (let ((edited (addfn val brico-onload #f)))
	       (when edited
		 (set! brico-onload edited)
		 (val))
	       (if edited #t #f))
	     (let ((edited (addfn val brico-onload #t)))
	       (when edited (set! brico-onload edited))
	       (if edited #t #f))))
	(else (irritant val |NotAThunk| config-brico:onload))))

(comment
 (dolist (onload brico-onload)
   (onerror (onload) 
       (lambda (ex)
	 (logerror |BricoConfig|
	   "Error applying " onload " " ex)
	 #f))))

(config-def! 'brico:onload brico:onload-configfn)
|#


;;;; For the compiler/optimizer

(set+! %constants
       '{english-gloss english-norm
	 english spanish french
	 always sometimes never somenot commonly rarely
	 /always /somenot /commonly /rarely
	 genls genls* kindof kindof*
	 specls specls*
	 parts parts* partof partof*
	 members members* memberof memberof*
	 ingredients ingredients* ingredientof ingredientof*
	 isa =is= sameas inverse disjoint implies implies*
	 diffterms /diffterms differentiae differentates
	 sumterms refterms /sumterms /refterms references
	 language-map gloss-map norm-map indicator-map})

