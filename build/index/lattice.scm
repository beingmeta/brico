#!/usr/bin/env knox
;;; -*- Mode: Scheme; -*-

(in-module 'brico/build/index/lattice)

(use-module '{texttools varconfig logger optimize text/stringfmts knodb knodb/branches engine})
(use-module 'brico/build/index)
(use-module '{brico brico/indexing})

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (index-lattice/prefetch (qc oids))))

(define (index-lattices f thread-index)
  (index-frame* thread-index f genls* genls specls*)
  (index-frame* thread-index f partof* partof parts*)
  (index-frame* thread-index f memberof* memberof members*)
  (index-frame* thread-index f ingredientof* ingredientof ingredients*))

(defambda (index-batch frames batch-state loop-state task-state)
  (let* ((index (get loop-state 'index))
	 (thread-index (if (config 'THREADINDEX #t config:boolean)
			   (index/branch index)
			   index)))
    (prefetch-oids! frames)
    (do-choices (f frames) (index-lattices f thread-index))
    (unless (eq? index thread-index)
      (branch/commit! thread-index))
    (swapout frames)))

(define lattices-index #f)

(defambda (get-index pools)
  (or lattices-index
      (let* ((genls.index (target-index "genls.index" #f pools #default 
					{genls specls genls* specls*}))
	     (partof.index (target-index "partof.index" #f pools #default 
					 {parts partof partof* parts*
					  members memberof members* memberof*
					  ingredients ingredientof
					  ingredients* ingredientof*}))
	     (misc.index (target-index "misc.index" #f pools))
	     (combined (make-aggregate-index {genls.index partof.index misc.index}
					     [register #t])))
	(set! lattices-index combined)
	combined)))

(define (main . names)
  (config! 'appid "index-lattice")
  (let* ((pools (getdbpool (try (elts names) brico-pool-names)))
	 (frames (if (config 'JUST)
		     (sample-n (pool-elts pools) (config 'just))
		     (pool-elts pools)))
	 (target (get-index pools)))
    (engine/run index-batch (difference frames (?? 'source @1/1) (?? 'status 'deleted))
      `#[loop #[index ,target]
	 batchsize 25000 batchrange 4
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pools (get target (getkeys target))}
	 logfns {,engine/log ,engine/logrusage}
	 logfreq ,(config 'logfreq 50)
	 logchecks #t])
    (commit)))

(when (config 'optimize #t config:boolean)
  (optimize! '{brico engine fifo brico/indexing})
  (optimize-locals!))
