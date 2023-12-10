#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/termlogic)

(use-module '{texttools varconfig logger optimize text/stringfmts engine})
(use-module '{knodb knodb/search knodb/fuzz})
(use-module '{brico brico/indexing brico/build/index})

(define %loglevel %notice%)

(define %optmods
  '{knodb knodb/branches knodb/search
    knodb/fuzz knodb/fuzz/strings knodb/fuzz/terms
    knodb/tinygis
    fifo engine
    brico brico/indexing})

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (index-lattice/prefetch (qc oids))))

(define brico-pools (choice brico.pool))
(define normal-slots (?? 'type 'slot))

(define relterm-slotids
  (choice relterms parts members ingredients partof memberof))

(define indexrels
  (difference {relterm-slotids (?? 'type 'wikidprop)}
	      (?? 'type '{stringslot numslot lexslot timeslot quantslot textslot})))

(defambda (index-slot index frame slot (values))
  (if (bound? values)
      (index-frame index frame slot values)
      (index-frame index frame slot)))

(defambda (termlogic-phase1 concepts batch-state loop-state task-state)
  (let ((termlogic (get loop-state 'termlogic)))
    (prefetch-oids! concepts)
    (do-choices (concept (%pick concepts '{words %words names hypernym genls}))
      ;; ALWAYS is transitive
      (knodb/index*! termlogic concept always always /always)
      (index-frame termlogic concept always (list (get concept always)))
      ;; termlogic the inverse relationship
      (index-frame termlogic concept /always (%get concept always))
      (index-frame termlogic concept /always (list (get concept /always)))
      ;; This is for cross-domain relationships
      (index-frame termlogic (%get concept /always) always concept)
      ;; ALWAYS implies sometimes
      ;; but can also compute it at search time
      ;; (index-frame* termlogic concept always sometimes sometimes)
      (let ((somevals (get concept sometimes))
	    (somenotvals (get concept somenot))
	    (/somenotvals (%get concept /somenot))
	    (nevervals (get concept never)))
	;; SOMETIMES is symmetric
	(index-frame termlogic concept sometimes somevals)
	(index-frame termlogic somevals sometimes concept)
	(index-frame termlogic concept sometimes (list somevals))
	;; NEVER is symmetric
	(index-frame termlogic concept never nevervals)
	(index-frame termlogic nevervals never concept)
	(index-frame termlogic concept never (list nevervals))
	;; SOMENOT is not symmetric
	(index-frame termlogic concept somenot somenotvals)
	(index-frame termlogic somenotvals /somenot concept)
	(index-frame termlogic concept somenot (list somenotvals))
	;; termlogic the inverse relationship
	(index-frame termlogic concept /somenot /somenotvals)
	(index-frame termlogic /somenotvals somenot concept)
	(index-frame termlogic concept /somenot (list /somenotvals)))
      ;; termlogic probablistic slots
      (index-frame termlogic concept commonly)
      (index-frame termlogic concept /commonly (%get concept /commonly))
      (index-frame termlogic concept rarely)
      (index-frame termlogic concept /rarely (%get concept /rarely))
      ;; These are for cross-domain relationships
      (index-frame termlogic (%get concept /commonly) commonly concept)
      (index-frame termlogic (%get concept /rarely) rarely concept))
    (swapout concepts)))

(defambda (termlogic-phase2 concepts batch-state loop-state task-state)
  (let ((indexes (get loop-state 'termlogic)))
    (prefetch-oids! concepts)
    (do-choices (concept (%pick concepts '{words %words names hypernym genls}))
      (let ((s (get concept sometimes))
	    (n (get concept never)))
	(when (exists? s)
	  (index-frame indexes concept sometimes (list s))
	  (index-frame indexes s sometimes (list concept))
	  (index-frame indexes concept
		      sometimes (find-frames indexes /always s))
	  (unless (test concept 'type 'individual)
	    (index-frame indexes (find-frames indexes /always s)
			sometimes concept)))
	(when (exists? n)
	  (index-frame indexes concept never (list n))
	  (index-frame indexes n never (list concept))
	  (index-frame indexes (find-frames indexes /always n)
		      never concept))
	(index-frame indexes concept probably
		    (find-frames indexes /always (get concept probably)))))
    (swapout concepts)))

(define (main poolname (output #f))
  (config! 'appid
	   (glom "index-" (basename poolname ".pool") "-termlogic"
	     (if (config 'phase2) ".2" ".1")))
  (let* ((pool (knodb/ref poolname (and output [altroot output])))
	 (termlogic.index (pool/index/target pool output 'name 'termlogic)))
    (knodb/writable! termlogic.index)
    (engine/run (if (config 'phase2 #f) termlogic-phase2 termlogic-phase1)
	(difference (pool-elts pool) (?? 'source @1/1) (?? 'status 'deleted))
      `#[loop #[termlogic ,termlogic.index]
	 batchsize ,(config 'batchsize 10000)
	 nthreads ,(config 'nthreads #t)
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pool termlogic.index}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t
	 logfreq 25])
    (commit)
    (if (not (config 'phase2))
	(chain "PHASE2=yes" poolname))))
(module-export! 'main)


