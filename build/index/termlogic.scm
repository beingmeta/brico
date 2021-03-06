#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/termlogic)

(use-module '{texttools varconfig logger optimize text/stringfmts knodb engine})
(use-module 'brico/build/index)
(use-module '{brico brico/indexing})

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (index-lattice/prefetch (qc oids))))

(define brico-pools (choice brico.pool))
(define normal-slots (?? 'type 'slot))

(define relterm-slotids
  (choice relterms parts members ingredients partof memberof))

(defambda (index-slot index frame slot (values))
  (if (bound? values)
      (index-frame index frame slot values)
      (index-frame index frame slot)))

(defambda (index-phase1 concepts batch-state loop-state task-state)
  (let ((indexes (get loop-state 'indexes)))
    (prefetch-oids! concepts)
    (do-choices (concept (%pick concepts '{words %words names hypernym genls}))
      ;; ALWAYS is transitive
      (index-frame* indexes concept always always /always)
      (index-frame indexes concept always (list (get concept always)))
      ;; indexes the inverse relationship
      (index-frame indexes concept /always (%get concept always))
      (index-frame indexes concept /always (list (get concept /always)))
      ;; This is for cross-domain relationships
      (index-frame indexes (%get concept /always) always concept)
      ;; ALWAYS implies sometimes
      ;; but can also compute it at search time
      ;; (index-frame* indexes concept always sometimes sometimes)
      (let ((somevals (get concept sometimes))
	    (somenotvals (get concept somenot))
	    (/somenotvals (%get concept /somenot))
	    (nevervals (get concept never)))
	;; SOMETIMES is symmetric
	(index-frame indexes concept sometimes somevals)
	(index-frame indexes somevals sometimes concept)
	(index-frame indexes concept sometimes (list somevals))
	;; NEVER is symmetric
	(index-frame indexes concept never nevervals)
	(index-frame indexes nevervals never concept)
	(index-frame indexes concept never (list nevervals))
	;; SOMENOT is not symmetric
	(index-frame indexes concept somenot somenotvals)
	(index-frame indexes somenotvals /somenot concept)
	(index-frame indexes concept somenot (list somenotvals))
	;; indexes the inverse relationship
	(index-frame indexes concept /somenot /somenotvals)
	(index-frame indexes /somenotvals somenot concept)
	(index-frame indexes concept /somenot (list /somenotvals)))
      ;; indexes probablistic slots
      (index-frame indexes concept commonly)
      (index-frame indexes concept /commonly (%get concept /commonly))
      (index-frame indexes concept rarely)
      (index-frame indexes concept /rarely (%get concept /rarely))
      ;; These are for cross-domain relationships
      (index-frame indexes (%get concept /commonly) commonly concept)
      (index-frame indexes (%get concept /rarely) rarely concept)
      ;; indexes features
      (index-frame indexes concept relterms (%get concept relterm-slotids))
      (let ((feature-slotids
	     (difference (choice (pick (getkeys concept) brico-pools))
			 normal-slots)))
	(index-frame indexes concept relterms (%get concept feature-slotids))
	(do-choices (fs feature-slotids)
	  (index-frame indexes concept fs (list (get concept fs))))
	(do-choices (fs feature-slotids)
	  (index-frame indexes concept fs (get concept fs)))))
    (swapout concepts)))

(defambda (index-phase2 concepts batch-state loop-state task-state)
  (let ((indexes (get loop-state 'indexes)))
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

(define (main . names)
  (let* ((pools (getdbpool (try (elts names) brico-pool-names)))
	 (termlogic.index (target-index "termlogic.index" #f pools))
	 (target termlogic.index))
    (commit pools) ;; Save metadata
    (if (config 'phase2) (config! 'appid "index4termlogic.2") (config! 'appid "index4termlogic.1"))
    (engine/run (if (config 'phase2 #f) index-phase2 index-phase1)
	(difference (pool-elts pools) (?? 'source @1/1) (?? 'status 'deleted))
      `#[loop #[indexes ,target]
	 batchsize 25000 batchrange 4
	 nthreads ,(config 'nthreads #t)
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pools target}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t
	 logfreq 25])
    (commit)
    (if (not (config 'phase2))
	(apply chain "PHASE2=yes" names))))

(optimize! '{brico engine fifo brico/indexing})
(optimize-locals!)

(when (config 'optimize #t config:boolean)
  (optimize! '{brico engine fifo brico/indexing})
  (optimize-locals!))
