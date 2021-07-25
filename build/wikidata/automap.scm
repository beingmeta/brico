;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'brico/build/wikidata/automap)

(use-module '{logger varconfig binio engine optimize text/stringfmts knodb})
(use-module '{brico brico/wikid brico/build/wikidata brico/build/wikidata/map})
(define %optmods '{brico brico/wikid brico/indexing brico/build/wikidata brico/build/map})

(module-export! '{import-by-isa import-isa-type import-isa import-by-genls
		  import-by-occupation import-occupation})

(define (get-specls wf)
  (let ((all wf)
	(next (?? wikidata.index @?wikid_genls wf)))
    (while (exists? (difference next all))
      (set+! all next)
      (set! next (??  wikidata.index @?wikid_genls next)))
    all))

(define (import-by-isa item wf bf (opts #f))
  (let* ((spec (frame-create #f (getopt opts 'slotid @?isa) bf))
	 (known (?? 'wikidref (get item 'wikid))))
    (when (exists? known)  (lognotice |Found| known " for " item " genls* " bf))
    (try (tryif (not (getopt opts 'skipknown)) known)
	 (let* ((candidates (try known (wikid/getmap item spec (opt+ opts 'lower #t))))
		(unmapped (reject candidates 'wikiref)))
	   (cond ((and (exists? candidates) (fail? unmapped))
		  (logwarn |WikidImport|
		    ($count (|| candidates) "matches" "match") " for " )
		  (fail))
		 ((and (fail? candidates) (getopt opts 'import #t)
		       (not (or (getopt opts 'dryrun #t) (getopt opts 'skipknown #f))))
		  (logwarn |WikidImport| 
		    "Imported " item "\n   into "
		    (wikid/import! item [lower #t] #f
				   [@?genls bf sensecat (get bf 'sensecat)])))
		 ((and (singleton? candidates) 
		       (test candidates 'wikidref)
		       (not (test candidates 'wikidref (get item 'wikid))))
		  (logwarn |WikidMap|
		    "Found existing concept " candidates " for " item 
		    " already mapped to " (wikid/ref (get candidates 'wikidref))))
		 ((singleton? candidates)
		  (logwarn |WikidMap|
		    "Found existing concept " candidates " for " item " isa " bf)
		  (unless (not (or (getopt opts 'dryrun #t) (getopt opts 'skipknown #f)))
		    (wikidmap! candidates item)))
		 ((singleton? unmapped)
		  (logwarn |WikidMap|
		    "Found existing unmapped concept " candidates " for " item " isa " bf)
		  (unless (not (or (getopt opts 'dryrun #t) (getopt opts 'skipknown #f)))
		    (wikidmap! unmapped item)))
		 ((> (|| (ambiguous? candidates)) 4)
		  (logwarn |Wikidmap|
		    "Ambiguous wikidata item " item
		    " matches " ($count (|| candidates) "candidate")
		    " based on \n" (listdata spec)))
		 ((ambiguous? candidates)
		  (logwarn |Wikidmap|
		    "Ambiguous wikidata item " item
		    " matches " (|| candidates) " candidates: "
		    (listdata candidates)
		    "\n based on \n" (listdata spec)))
		 (else (fail)))))))

(define (import-isa wf (opts #f) (bf))
  (default! bf (?? 'wikidref (get wf 'wikid)))
  (when (fail? bf) (irritant wf |NoBricoMap|))
  (local classes (if (getopt opts 'specls) (get-specls wf) wf))
  (when (ambiguous? classes)
    (lognotice |ImportISA|
      "Importing instances of " ($count (|| classes) "classes" "class")
      " beneath " wf)
    (when (getopt opts 'newcats)
      (let ((import-classes (filter-choices (class classes)
			      (fail? (?? 'wikidref (get class 'wikid))))))
	(when (exists? import-classes)
	  (lognotice |ImportIntermediateISAs|
	    "Importing " ($count (|| import-classes) "wikid classes" "wikid class"))
	  (do-choices (import-class import-classes)
	    (wikid/import! import-class))))))
  (let* ((items (find-frames wikidata.index @?wikid_isa classes 'has wikid-instance-slotids))
	 (wikids (get (fetchoids items) 'wikid))
	 (useids (filter-choices (wikid wikids) (fail? (?? 'wikidref wikid))))
	 (imports (pick items 'wikid useids)))
    (if (getopt opts 'nthreads (config 'nthreads #t))
	(engine/run (lambda (item) (import-by-isa item wf bf opts))
	    imports)
	(do-choices (import imports)
	  (import-by-isa import wf bf opts)))))

(define (import-isa-type wf bf (opts #f))
  (wikidmap! bf wf)
  (import-isa wf opts bf))

(define (import-by-genls item wf bf (opts #f))
  (let* ((known (?? 'wikidref (get item 'wikid)))
	 (opts (opt+ opts 'lower #t)))
    (when (exists? known)  (lognotice |Found| known " for " item " genls* " bf))
    (try (tryif (not (getopt opts 'skipknown)) known)
	 (let* ((gspec [@?genls bf])
		(g2spec [@?genls* bf])
		(g3spec [@?genls* (get bf @?genls)])
		(spec gspec)
		(candidates (try (wikid/getmap item gspec opts)
				 (begin (set! spec g2spec) (fail))
				 (wikid/getmap item g2spec opts)
				 (begin (set! spec g3spec) (fail))
				 (wikid/getmap item g3spec opts))))
	   (cond ((and (fail? candidates) (getopt opts 'import #t)
		       (not (or (getopt opts 'dryrun #t) (getopt opts 'skipknown #f))))
		  (logwarn |WikidImport| 
		    "Imported " item "\n   into "
		    (wikid/import! item [lower #t] #f
				   [@?genls bf sensecat (get bf 'sensecat)])))
		 ((and (singleton? candidates) 
		       (test candidates 'wikidref)
		       (not (test candidates 'wikidref (get item 'wikid))))
		  (logwarn |WikidMap|
		    "Found existing concept " candidates " for " item 
		    " already mapped to " (wikid/ref (get candidates 'wikidref))))
		 ((singleton? candidates)
		  (logwarn |WikidMap|
		    "Found existing concept " candidates " for " item " isa " bf)
		  (unless (not (or (getopt opts 'dryrun #t)
				   (getopt opts 'skipknown #f)))
		    (wikidmap! candidates item)))
		 ((> (|| (ambiguous? candidates)) 4)
		  (logwarn |Wikidmap|
		    "Ambiguous wikidata item " item
		    " matches " ($count (|| candidates) "candidate")
		    " based on \n" (listdata spec)))
		 ((ambiguous? candidates)
		  (logwarn |Wikidmap|
		    "Ambiguous wikidata item " item
		    " matches " (|| candidates) " candidates: "
		    (listdata candidates)
		    "\n based on \n" (listdata spec)))
		 (else (fail)))))))

(define (import-by-occupation item occupation isa (opts #f))
  (let* ((spec [@?isa isa])
	 (known (?? 'wikidref (get item 'wikid)))
	 (candidates (try known (wikid/getmap item spec [lower #f])))
	 (unmapped (reject candidates 'wikidref)))
    (cond ((exists? known))
	  ((singleton? candidates)
	   (loginfo |WikidMap| "Found unique map for " item "\n   to " candidates))
	  ((fail? unmapped)
	   (lognotice |WikidImport| 
	     "Imported " item "\n   into " (wikid/import! item [lower #f])))
	  ((ambiguous? candidates)
	   (logwarn |Wikidmap| 
	     "Ambiguous wikidata item " item " search on " 
	     spec " (occupation)"
	     (if (> (|| candidates) 4)
		 (printout " matching " (|| candidates) " candidates")
		 (do-choices (c candidates) (printout "\n\t" c)))))
	  (else))))

(define (import-occupation occupation (isa) (opts #f))
  (default! isa (?? 'wikidref (get occupation 'wikid)))
  (unless (testopt opts 'lower #f) (set! opts (opt+ opts 'lower #f)))
  (if (fail? occupation)
      (logwarn |NoCognate| "No mapped BRICO concept for " occupation)
      (let ((items (find-frames wikidata.index @?wikid_occupation occupation)))
	(when (exists? items)
	  (if (config 'nthreads #t)
	      (engine/run (defn (occupation-importer item)
			    (import-by-occupation item occupation isa opts))
		  items)
	      (do-choices (item items)
		(import-by-occupation item occupation isa opts)))
	  (knodb/commit! {wikid.pool (pool/getindexes wikid.pool)
			  brico.pool (pool/getindexes brico.pool)})
	  (commit)
	  (swapout)))))

#|
(wikidmap! 
  @1/8b176(noun.person "Richard Starkey" genls "dessert apple")
  (wikidata/ref "Q2632" "Ringo Starr"))
(wikidmap! 
  @1/800593a(wikid "Ringo Starr" "Q2632")
  (wikidata/ref "Q41781790" "Starr"))
|#
