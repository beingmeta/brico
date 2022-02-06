#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/termlogic)

(use-module '{texttools varconfig logger optimize text/stringfmts engine})
(use-module '{knodb knodb/search knodb/fuzz})
(use-module 'brico/build/index)
(use-module '{brico brico/indexing})

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

(defambda (index-phase1 concepts batch-state loop-state task-state)
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

(defambda (index-phase2 concepts batch-state loop-state task-state)
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

#|
(define (index-relterms concept)
  ;; termlogic features
  (index-frame relations concept relterms (%get concept relterm-slotids))
  (let ((feature-slotids
	 (intersection (choice (pick (getkeys concept) brico-pools)) indexrels)))
    (index-frame relations concept relterms (%get concept feature-slotids))
    (do-choices (fs feature-slotids)
      (let ((v (pickoids (get concept fs))))
	(when (exists? v) 
	  (index-frame relations concept fs v)
	  (index-frame relations concept fs (list v)))))))
|#

(define (main . names)
  (config! 'appid (glom "index-" (basename (car names) ".pool") "-termlogic"
		    (if (config 'phase2) ".2" ".1")))
  (let* ((pools (getdbpool (try (elts names) brico-pool-names)))
	 (termlogic.index (target-index "termlogic.index" #f pools)))
    (do-choices (pool pools)
      (dbctl pool 'metadata 'indexes
	     (choice (dbctl pool 'metadata 'indexes) "termlogic.index")))
    (commit pools) ;; Save metadata
    (engine/run (if (config 'phase2 #f) index-phase2 index-phase1)
	(difference (pool-elts pools) (?? 'source @1/1) (?? 'status 'deleted))
      `#[loop #[termlogic ,termlogic.index]
	 batchsize 25000 batchrange 4
	 nthreads ,(config 'nthreads #t)
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pools termlogic.index}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t
	 logfreq 25])
    (commit)
    (if (not (config 'phase2))
	(apply chain "PHASE2=yes" names))))
(module-export! 'main)

(optimize! '{brico engine fifo brico/indexing})
(optimize-locals!)

(when (config 'optimize #t config:boolean)
  (optimize! '{brico engine fifo brico/indexing})
  (optimize-locals!))
