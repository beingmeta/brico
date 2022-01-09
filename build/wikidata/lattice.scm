;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'brico/build/wikidata/lattice)

(use-module '{logger varconfig binio engine optimize text/stringfmts knodb})
(use-module '{brico brico/wikid brico/build/wikidata})

(define %optmods '{engine fifo logger brico brico/wikid brico/build/wikidata})

(module-export! '{build-genls-table wikidata/index-lattice
		  init-instanceof.index init-subclassof.index
		  allclasses.map get-allclasses.map})

(define-init allclasses.map #f)

(define (build-allclasses-iterfn class batch-state loop-state task-state)
  (let ((g* (get* class wikid-subclassof)))
    (store! (get loop-state 'genls) class g*)))

(define (build-allclasses.map)
  (let* ((classes (wikid/find 'type 'wikidclass))
	 (table (make-hashtable (|| classes))))
    (logwarn |Wikidata| "Building allclasses.map for " (|| classes) " classes")
    (prefetch-oids! classes)
    (engine/run build-allclasses-iterfn classes `#[loop #[genls ,table]])
    table))

(defslambda (get-allclasses.map)
  (or allclasses.map
      (and wikidata.dir (file-exists? (mkpath wikidata.dir "allclasses.map"))
	   (let* ((filename (mkpath wikidata.dir "allclasses.map"))
		  (start-read (elapsed-time))
		  (map (read-xtype filename)))
	     (lognotice |AllClasses|
	       "Read " map " from " (write (mkpath wikidata.dir "allclasses.map"))
	       " in " (secs->string (elapsed-time start-read)))
	     (set! allclasses.map map)
	     map))
      (let ((map (build-allclasses.map)))
	(when wikidata.dir
	  (let* ((start-write (elapsed-time))
		 (filename (mkpath wikidata.dir "allclasses.map")))
	    (lognotice |WriteAllClasses| "Writing " map " to " (write filename))
	    (write-xtype map (mkpath wikidata.dir "allclasses.map") [embed #t])
	    (lognotice |WriteAllClasses|
	      "Wrote " (write filename) " in " (secs->string (elapsed-time start-write)))))
	(set! allclasses.map map)
	map)))

(define (wikidata-index-lattice-iterfn batch batch-state loop-state task-state)
  (let ((subclassof.index (try (get batch-state 'subclassof.index) (get loop-state 'subclassof.index)))
	(instanceof.index (try (get batch-state 'instanceof.index) (get loop-state 'instanceof.index)))
	(allclasses (try (get loop-state 'allclasses) allclasses.map (get-allclasses.map)))
	(root-keys (list batch)))
    (prefetch-keys! subclassof.index root-keys)
    (prefetch-keys! instanceof.index root-keys)
    (do-choices (class batch)
      (add! subclassof.index (get subclassof.index (list class)) (get allclasses class))
      (add! instanceof.index (get instanceof.index (list class)) (get allclasses class)))))

(define (wikidata/index-lattice)
  (let ((allclasses (or allclasses.map (get-allclasses.map))))
    (engine/run wikidata-index-lattice-iterfn (getkeys allclasses)
      [loop [instanceof.index instanceof.index subclassof.index subclassof.index allclasses allclasses]
       branchindexes {instanceof.index subclassof.index}
       batchsize (config 'lattice:batchsize (config 'batchsize 1000))
       checkpoint {instanceof.index subclassof.index}
       checktests (engine/interval (config 'savefreq 60))
       checkfreq 30])))

;;;; These definitions are one-time code used to initialize the
;;;; subclassof/instanceof indexes if they aren't indexed on
;;;; read. This also serves as an example of engine/run.

(define (init-subclassof-iterfn batch batch-state loop-state task-state)
  (let ((subclassof.index (get batch-state 'subclassof.index)))
    (doseq (class batch)
      (let ((genl (get class @?wikid_genls)))
	(index-frame subclassof.index class @?wikid_genls {genl (list genl)})
	(unless (or (overlaps? genl wikid-classes)
		    (test genl 'type 'wikidclass))
	  (wikidata/class! genl))))))

(define (init-subclassof.index)
  (engine/run init-subclassof-iterfn (wikid/find 'has @?wikid_genls)
    `#[loop #[subclassof.index ,subclassof.index]
       branchindexes subclassof.index
       batchsize 20000
       batchcall #t
       useconfig #f
       before ,engine/fetchoids
       after ,engine/swapout]))

(define (init-instanceof-iterfn batch batch-state loop-state task-state)
  (let ((instanceof.index (get batch-state 'instanceof.index)))
    (doseq (class batch)
      (let ((instances (find-frames props.index @?wikid_isa class)))
	(index-frame instanceof.index instances @?wikid_isa {class (list class)})
	(unless (overlaps? class wikid-classes)
	  (wikidata/class! class))))))

(defambda (init-instanceof.index (classes (wikid/find 'has @?wikid_genls)))
  (engine/run init-instanceof-iterfn classes
    `#[loop #[instanceof.index ,instanceof.index]
       branchindexes instanceof.index
       batchsize 20000
       batchcall #t]))
